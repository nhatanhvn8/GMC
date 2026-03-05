# -*- coding: utf-8 -*-
"""AI DOM Analyzer — phân tích cấu trúc DOM (không dùng screenshot).

Ưu điểm so với vision:
- Language-independent: DOM attributes không phụ thuộc ngôn ngữ UI
- Rẻ hơn 10-20x: chỉ gửi text (50-100 tokens) thay vì ảnh
- Nhanh hơn: không cần chụp/encode ảnh

Flow:
  1. Thu thập DOM snapshot (inputs, buttons, URL, form action...)
  2. Kiểm tra cache vĩnh viễn (dom_patterns.json) theo URL pattern
  3. Nếu cache hit → trả về ngay (miễn phí)
  4. Nếu miss → gửi DOM text cho Claude Haiku → parse kết quả
  5. Lưu vào cache vĩnh viễn → lần sau không gọi AI nữa
"""
from __future__ import annotations

import hashlib
import json
import logging
import threading
import time
from pathlib import Path
from typing import Optional

_log = logging.getLogger(__name__)

_ANTHROPIC_API_KEY = ""  # loaded at startup from hconfig.ini via app_config


def set_anthropic_api_key(key: str) -> None:
    global _ANTHROPIC_API_KEY
    _ANTHROPIC_API_KEY = (key or "").strip()


def get_anthropic_api_key() -> str:
    return _ANTHROPIC_API_KEY


def _load_key_from_config() -> None:
    """Tự động đọc key từ hconfig.ini khi module được import."""
    try:
        from app_config import load_hconfig
        cfg = load_hconfig()
        key = str(cfg.get("anthropic_api_key", "") or "").strip()
        if key:
            set_anthropic_api_key(key)
    except Exception:
        pass


_load_key_from_config()

_CACHE_FILE = Path(__file__).resolve().parent / "config" / "dom_patterns.json"
_CACHE_LOCK = threading.Lock()
_CACHE: dict[str, dict] = {}  # in-memory cache (loaded từ file lúc import)

_RATE_LOCK = threading.Lock()
_LAST_CALL: float = 0.0
_MIN_INTERVAL = 1.0  # giây giữa các API call

_PAGE_TYPES = (
    # ── Trạng thái bình thường (flow login) ──
    "email_entry",
    "password_entry",
    "twofa_challenge",
    "phone_challenge",
    "image_captcha",
    "recaptcha",
    "recovery_confirm",
    "recovery_choice",
    "success",
    # ── Trạng thái lỗi tài khoản ──
    "password_changed",     # pass đã bị đổi (error: "Your password was changed X ago")
    "wrong_password",       # sai mật khẩu (error trên trang password)
    "wrong_2fa",            # sai mã 2FA (error trên trang TOTP)
    "wrong_phone",          # sai số điện thoại verify
    "need_verify_phone",    # bắt xác minh phone trước khi vào
    "too_many_attempts",    # quá nhiều lần thử, bị khóa tạm
    "account_disabled",     # tài khoản bị vô hiệu hóa vĩnh viễn
    "account_suspended",    # tài khoản bị suspend
    "account_not_found",    # không tìm thấy tài khoản
    "hard_block",           # bị chặn đăng nhập hoàn toàn
    "suspicious_activity",  # phát hiện hoạt động đáng ngờ
    "wrong_recovery_email", # email khôi phục không khớp
    "no_recovery_email",    # tài khoản không có email khôi phục
    "unknown",
)

# Map AI output → router kind / account state
_AI_TO_ROUTER: dict[str, str] = {
    "email_entry":          "email_entry",
    "password_entry":       "password_entry",
    "twofa_challenge":      "twofa_challenge",
    "phone_challenge":      "phone_challenge",
    "image_captcha":        "image_captcha",
    "recaptcha":            "recaptcha",
    "recovery_confirm":     "recovery_confirm",
    "recovery_choice":      "recovery_choice",
    "success":              "success",
    "password_changed":     "password_changed",
    "wrong_password":       "wrong_password",
    "wrong_2fa":            "wrong_2fa",
    "wrong_phone":          "wrong_phone",
    "need_verify_phone":    "need_verify_phone",
    "too_many_attempts":    "too_many_attempts",
    "account_disabled":     "disabled",
    "account_suspended":    "suspended",
    "account_not_found":    "not_found",
    "hard_block":           "hard_block",
    "suspicious_activity":  "suspicious_activity",
    "wrong_recovery_email": "wrong_recovery_email",
    "no_recovery_email":    "no_recovery_email",
    "unknown":              "unknown",
}

# Tập các state là lỗi tài khoản (dùng trong _detect_account_state)
ACCOUNT_ERROR_STATES = frozenset({
    "password_changed",
    "wrong_password", "wrong_2fa", "wrong_phone",
    "need_verify_phone", "too_many_attempts",
    "account_disabled", "account_suspended", "account_not_found",
    "hard_block", "suspicious_activity",
    "wrong_recovery_email", "no_recovery_email",
})

MIN_CONFIDENCE = 0.70


# ── Cache management ────────────────────────────────────────────────────────

def _load_cache() -> None:
    global _CACHE
    try:
        _CACHE_FILE.parent.mkdir(parents=True, exist_ok=True)
        if _CACHE_FILE.exists():
            with open(_CACHE_FILE, encoding="utf-8") as f:
                _CACHE = json.load(f)
            _log.info("AI_ANALYZER: loaded %d DOM patterns từ cache", len(_CACHE))
    except Exception as e:
        _log.warning("AI_ANALYZER: không load được cache: %s", e)
        _CACHE = {}


def _save_cache() -> None:
    try:
        _CACHE_FILE.parent.mkdir(parents=True, exist_ok=True)
        with open(_CACHE_FILE, "w", encoding="utf-8") as f:
            json.dump(_CACHE, f, ensure_ascii=False, indent=2)
    except Exception as e:
        _log.warning("AI_ANALYZER: không lưu được cache: %s", e)


def _url_pattern(url: str) -> str:
    """Rút gọn URL thành pattern để cache (bỏ query params thay đổi mỗi session)."""
    import re
    url = (url or "").lower().split("?")[0].split("#")[0]
    # Bỏ số cuối đường dẫn (session IDs)
    url = re.sub(r"/[a-f0-9]{16,}", "/<id>", url)
    return url


def _dom_cache_key(url: str, dom: dict) -> str:
    """Key cache = URL pattern + input structure + error flag.
    
    Quan trọng: phải include has_error để phân biệt:
    - password_entry (pass input, NO error)  
    - wrong_password / password_changed (pass input, HAS error)
    """
    pat = _url_pattern(url)
    inp_sig = "|".join(sorted(
        f"{i.get('name','')}:{i.get('id','')}:{i.get('type','')}:{i.get('autocomplete','')}"
        for i in (dom.get("inputs") or [])
    ))
    # Thêm error flag vào key để error state không bị cache nhầm với normal state
    has_error = "err1" if dom.get("has_error") else "err0"
    # Thêm snippet error text để phân biệt các loại error khác nhau (vd: wrong_password vs password_changed)
    err_snip = (dom.get("error_text") or "")[:30].lower().replace(" ", "_")
    raw = f"{pat}||{inp_sig}||{has_error}||{err_snip}"
    return hashlib.md5(raw.encode()).hexdigest()


# ── DOM snapshot ────────────────────────────────────────────────────────────

_DOM_JS = """
(function() {
  var inputs = [...document.querySelectorAll('input:not([type="hidden"])')].map(function(el) {
    return {
      name: el.name || '',
      id: el.id || '',
      type: el.type || '',
      autocomplete: el.autocomplete || '',
      placeholder: el.placeholder ? el.placeholder.substring(0,40) : '',
      maxlength: el.maxLength || '',
      'data-challengetype': el.getAttribute('data-challengetype') || '',
      visible: el.offsetParent !== null
    };
  }).filter(function(i){ return i.visible; });

  var buttons = [...document.querySelectorAll('button,[role="button"]')].map(function(b){
    return (b.innerText||'').trim().substring(0,30);
  }).filter(Boolean).slice(0,6);

  var form_action = (document.querySelector('form') || {}).action || '';
  var h1 = (document.querySelector('h1,h2') || {}).innerText || '';
  var data_type = document.querySelector('[data-challengetype]');
  var challenge_type = data_type ? data_type.getAttribute('data-challengetype') : '';

  // Thu thập TẤT CẢ error/alert elements (language-independent signals)
  var error_els = [...document.querySelectorAll(
    '[aria-live="assertive"],[role="alert"],[aria-live="polite"],' +
    '.LXRPh,.o6cuMc,.Ekjuhf,[data-error],.error-message,#error-message'
  )];
  var error_texts = error_els.map(function(e){ return (e.innerText||'').trim(); })
                             .filter(Boolean).join(' | ').substring(0,150);

  // DOM signals không phụ thuộc ngôn ngữ
  var has_error = error_els.some(function(e){ return (e.innerText||'').trim().length > 0; });
  var pass_input_visible = inputs.some(function(i){
    return (i.name==='Passwd'||i.name==='password'||i.type==='password');
  });
  var totp_input_visible = inputs.some(function(i){
    return i.name==='totpPin'||i.id==='totpPin'||i.autocomplete==='one-time-code';
  });
  var tel_input_visible = inputs.some(function(i){
    return i.type==='tel' && i.maxlength!='6';
  });

  // Lấy nội dung span/div có màu đỏ (thường là error) qua computed style
  var red_texts = [];
  try {
    [...document.querySelectorAll('span,div,p')].slice(0,200).forEach(function(el){
      var s = window.getComputedStyle(el);
      var c = s.color;
      if(c && (c.indexOf('rgb(234')===0||c.indexOf('rgb(217')===0||c.indexOf('rgb(197')===0)){
        var t = (el.innerText||'').trim();
        if(t.length>3 && t.length<120) red_texts.push(t);
      }
    });
  } catch(e){}
  var red_error_text = [...new Set(red_texts)].join(' | ').substring(0,150);

  return {
    url: location.href,
    path: location.pathname,
    inputs: inputs,
    buttons: buttons,
    form_action: form_action.substring(0,120),
    h1: h1.substring(0,60),
    challenge_type: challenge_type,
    error_text: error_texts,
    red_text: red_error_text,
    has_error: has_error,
    pass_input_visible: pass_input_visible,
    totp_input_visible: totp_input_visible,
    tel_input_visible: tel_input_visible,
    ready: document.readyState
  };
})();
"""


def collect_dom(driver) -> Optional[dict]:
    """Thu thập DOM snapshot từ driver. Trả None nếu lỗi."""
    try:
        result = driver.execute_script(_DOM_JS)
        return result
    except Exception as e:
        _log.debug("AI_ANALYZER: collect_dom lỗi: %s", e)
        return None


# ── AI call ─────────────────────────────────────────────────────────────────

def _get_client():
    try:
        import anthropic
        return anthropic.Anthropic(api_key=_ANTHROPIC_API_KEY)
    except ImportError:
        _log.warning("AI_ANALYZER: chưa cài anthropic. Chạy: pip install anthropic")
        return None


def _ask_ai(dom: dict) -> dict:
    """Gửi DOM snapshot cho Claude Haiku, nhận lại page_type."""
    client = _get_client()
    if not client:
        return {"page_type": "unknown", "confidence": 0.0, "reason": "no client"}

    # Rate limiting
    global _LAST_CALL
    with _RATE_LOCK:
        wait = _MIN_INTERVAL - (time.time() - _LAST_CALL)
        if wait > 0:
            time.sleep(wait)
        _LAST_CALL = time.time()

    # Xây prompt chỉ từ DOM attributes (không có ảnh, không phụ thuộc ngôn ngữ)
    dom_text = json.dumps({
        "url": dom.get("url", "")[:120],
        "path": dom.get("path", ""),
        "inputs": dom.get("inputs", []),
        "buttons": dom.get("buttons", []),
        "form_action": dom.get("form_action", ""),
        "h1": dom.get("h1", ""),
        "challenge_type": dom.get("challenge_type", ""),
        "error_text": dom.get("error_text", ""),
    }, ensure_ascii=False)

    prompt = f"""You are classifying Google account pages for a login automation tool.
Analyze ONLY the DOM structure below. Focus on language-independent signals.

DOM snapshot:
{dom_text}

KEY SIGNALS (priority order):
- error_text contains "password was changed" / "changed X ago" → password_changed (NOT wrong_password)
- has_error=true + pass_input_visible=true + no "changed" in error → wrong_password
- has_error=true + totp_input_visible=true → wrong_2fa
- has_error=true + tel_input_visible=true → wrong_phone
- challenge_type: "12" or "6" → phone/2fa challenge
- URL contains "/disabled" or "/deactivated" → account_disabled
- URL contains "/suspended" → account_suspended
- URL contains "/challenge/iknow" or "iknowmypassword=false" → need_verify_phone

Classify into exactly ONE page_type:
NORMAL FLOW:
- email_entry: input name="identifier" or id="identifierId"
- password_entry: input name="Passwd", NO error showing
- twofa_challenge: totpPin input, maxlength=6, no error
- phone_challenge: tel input, no maxlength=6, asking to enter phone number
- image_captcha: text/image captcha to type
- recaptcha: reCAPTCHA robot check
- recovery_confirm: asking to confirm/enter recovery email
- recovery_choice: showing multiple verification options to choose
- success: on myaccount.google.com or mail.google.com (logged in)

ERROR STATES (check when has_error=true or error_text not empty):
- password_changed: error_text says "password was changed X days/hours ago" — password input still visible but password is outdated (THIS IS DIFFERENT FROM wrong_password)
- wrong_password: entered wrong password, error says "wrong password" or "incorrect password"
- wrong_2fa: entered wrong 2FA code, error on TOTP page
- wrong_phone: entered wrong phone number
- need_verify_phone: forced to verify phone before continuing
- too_many_attempts: too many failed attempts, temporarily locked
- account_disabled: account permanently disabled/deactivated
- account_suspended: account suspended
- account_not_found: account doesn't exist
- hard_block: sign-in blocked/restricted permanently
- suspicious_activity: unusual activity detected, requires action
- wrong_recovery_email: entered wrong recovery email address
- no_recovery_email: account has no recovery email set up

- unknown: cannot determine

Return ONLY valid JSON, no markdown:
{{"page_type": "<type>", "confidence": <0.0-1.0>, "reason": "<one short sentence>"}}"""

    try:
        t0 = time.time()
        response = client.messages.create(
            model="claude-haiku-4-5",
            max_tokens=100,
            messages=[{"role": "user", "content": prompt}],
        )
        elapsed = time.time() - t0
        raw = (response.content[0].text or "").strip()

        if raw.startswith("```"):
            raw = raw.split("```")[1].lstrip("json").strip()

        parsed = json.loads(raw)
        page_type = str(parsed.get("page_type", "unknown")).strip()
        if page_type not in _PAGE_TYPES:
            page_type = "unknown"
        confidence = float(parsed.get("confidence", 0.0))
        reason = str(parsed.get("reason", ""))

        _log.info(
            "AI_ANALYZER: → %s (conf=%.2f, %.1fs) %s",
            page_type, confidence, elapsed, reason[:60],
        )
        return {"page_type": page_type, "confidence": confidence, "reason": reason}

    except json.JSONDecodeError as e:
        _log.warning("AI_ANALYZER: JSON parse lỗi: %s", e)
        return {"page_type": "unknown", "confidence": 0.0, "reason": "parse error"}
    except Exception as e:
        _log.warning("AI_ANALYZER: API lỗi: %s", e)
        return {"page_type": "unknown", "confidence": 0.0, "reason": str(e)[:60]}


# ── Local model (sklearn) ────────────────────────────────────────────────────

_LOCAL_MODEL = None
_LOCAL_MODEL_LOCK = threading.Lock()
_LOCAL_MODEL_FILE = Path(__file__).resolve().parent / "config" / "dom_model.pkl"
LOCAL_MODEL_MIN_CONFIDENCE = 0.80  # chỉ dùng kết quả model nếu đủ chắc


def _load_local_model():
    """Load sklearn model từ file (lazy, thread-safe)."""
    global _LOCAL_MODEL
    with _LOCAL_MODEL_LOCK:
        if _LOCAL_MODEL is not None:
            return _LOCAL_MODEL
        if not _LOCAL_MODEL_FILE.exists():
            return None
        try:
            import pickle
            with open(_LOCAL_MODEL_FILE, "rb") as f:
                _LOCAL_MODEL = pickle.load(f)
            le = _LOCAL_MODEL["label_encoder"]
            _log.info("AI_ANALYZER: local model loaded (%d classes)", len(le.classes_))
            return _LOCAL_MODEL
        except Exception as e:
            _log.warning("AI_ANALYZER: không load được local model: %s", e)
            return None


def predict_local(dom: dict) -> tuple[str, float]:
    """Dự đoán page type bằng local model. Trả (page_type, confidence)."""
    model_data = _load_local_model()
    if not model_data or not dom:
        return "unknown", 0.0
    try:
        from train_dom_model import extract_features
        import numpy as np
        feats = extract_features(dom)
        X = np.array([feats])
        clf = model_data["model"]
        le  = model_data["label_encoder"]
        probs = clf.predict_proba(X)[0]
        idx = probs.argmax()
        confidence = float(probs[idx])
        label = le.inverse_transform([idx])[0]
        return _AI_TO_ROUTER.get(label, label), confidence
    except Exception as e:
        _log.debug("AI_ANALYZER: local model predict lỗi: %s", e)
        return "unknown", 0.0


# ── Public API ───────────────────────────────────────────────────────────────

def get_page_kind_from_ai(driver, task_context: str = "") -> str:
    """Phân tích DOM → trả về page_kind cho router.

    - Kiểm tra cache trước (miễn phí)
    - Nếu miss → gọi AI 1 lần → lưu cache vĩnh viễn
    - Trả "unknown" nếu confidence < MIN_CONFIDENCE
    """
    dom = collect_dom(driver)
    if not dom:
        return "unknown"

    url = dom.get("url", "")
    cache_key = _dom_cache_key(url, dom)

    # ── Tầng 1: Cache ────────────────────────────────────────────────────────
    with _CACHE_LOCK:
        cached = _CACHE.get(cache_key)

    if cached:
        _log.info(
            "AI_ANALYZER: cache hit → %s (conf=%.2f) key=%s",
            cached["page_type"], cached.get("confidence", 1.0), cache_key[:8],
        )
        # Thu thập data khi có nhãn chắc chắn
        try:
            from data_collector import collect_sample
            collect_sample(driver, cached["page_type"], source="cache", confidence=cached.get("confidence", 1.0))
        except Exception:
            pass
        return _AI_TO_ROUTER.get(cached["page_type"], "unknown")

    # ── Tầng 2: Local model (sklearn, $0, ~1ms) ───────────────────────────────
    local_label, local_conf = predict_local(dom)
    if local_label != "unknown" and local_conf >= LOCAL_MODEL_MIN_CONFIDENCE:
        _log.info("AI_ANALYZER: local model → %s (conf=%.2f) ⚡ free", local_label, local_conf)
        # Lưu vào cache để lần sau nhanh hơn
        with _CACHE_LOCK:
            _CACHE[cache_key] = {
                "page_type": local_label, "confidence": local_conf,
                "reason": "local_model", "url_pattern": _url_pattern(url),
                "learned_at": time.strftime("%Y-%m-%d %H:%M:%S"),
            }
        try:
            from data_collector import collect_sample
            collect_sample(driver, local_label, source="local_model", confidence=local_conf)
        except Exception:
            pass
        return local_label

    # ── Tầng 3: Claude API (tốn tiền, chỉ khi cần) ───────────────────────────
    _log.info("AI_ANALYZER: local model miss (conf=%.2f), gọi Claude... (url=%s)", local_conf, url[:80])
    result = _ask_ai(dom)

    if result["confidence"] >= MIN_CONFIDENCE and result["page_type"] != "unknown":
        entry = {
            "page_type": result["page_type"],
            "confidence": result["confidence"],
            "reason": result["reason"],
            "url_pattern": _url_pattern(url),
            "input_names": [i.get("name", "") for i in dom.get("inputs", [])],
            "learned_at": time.strftime("%Y-%m-%d %H:%M:%S"),
        }
        with _CACHE_LOCK:
            _CACHE[cache_key] = entry
        _save_cache()
        _log.info(
            "AI_ANALYZER: học được pattern mới → %s (lưu cache, key=%s)",
            result["page_type"], cache_key[:8],
        )
        return _AI_TO_ROUTER.get(result["page_type"], "unknown")

    _log.info(
        "AI_ANALYZER: confidence %.2f < %.2f hoặc unknown → bỏ qua",
        result["confidence"], MIN_CONFIDENCE,
    )
    return "unknown"


def detect_account_state_ai(driver) -> str:
    """Phát hiện trạng thái lỗi tài khoản bằng AI khi text-based detection thất bại.

    Trả về: "wrong_password" | "wrong_2fa" | "disabled" | "suspended" |
             "not_found" | "too_many_attempts" | "need_verify_phone" |
             "suspicious_activity" | "hard_block" | "unknown"
    """
    dom = collect_dom(driver)
    if not dom:
        return "unknown"

    # Fast pre-check: nếu không có error signal thì skip AI
    if not dom.get("has_error") and not dom.get("error_text") and not dom.get("red_text"):
        return "unknown"

    url = dom.get("url", "")
    cache_key = _dom_cache_key(url, dom)

    with _CACHE_LOCK:
        cached = _CACHE.get(cache_key)
    if cached and cached.get("page_type") in ACCOUNT_ERROR_STATES:
        mapped = _AI_TO_ROUTER.get(cached["page_type"], "unknown")
        _log.info("AI_ANALYZER: account state cache hit → %s", mapped)
        return mapped

    # Thử local model trước
    local_label, local_conf = predict_local(dom)
    if local_label != "unknown" and local_conf >= LOCAL_MODEL_MIN_CONFIDENCE:
        mapped = _AI_TO_ROUTER.get(local_label, local_label)
        if mapped in ACCOUNT_ERROR_STATES or local_label in ACCOUNT_ERROR_STATES:
            _log.info("AI_ANALYZER: local model account state → %s (%.2f) ⚡", mapped, local_conf)
            return mapped

    result = _ask_ai(dom)
    page_type = result.get("page_type", "unknown")

    if result["confidence"] >= MIN_CONFIDENCE and page_type in ACCOUNT_ERROR_STATES:
        entry = {
            "page_type": page_type,
            "confidence": result["confidence"],
            "reason": result["reason"],
            "url_pattern": _url_pattern(url),
            "input_names": [i.get("name", "") for i in dom.get("inputs", [])],
            "has_error": dom.get("has_error"),
            "learned_at": time.strftime("%Y-%m-%d %H:%M:%S"),
        }
        with _CACHE_LOCK:
            _CACHE[cache_key] = entry
        _save_cache()
        mapped = _AI_TO_ROUTER.get(page_type, "unknown")
        _log.info("AI_ANALYZER: learned account state → %s (conf=%.2f)", mapped, result["confidence"])
        return mapped

    return "unknown"


def list_learned_patterns() -> list[dict]:
    """Trả về danh sách patterns đã học (để debug/xem trong GUI)."""
    with _CACHE_LOCK:
        return list(_CACHE.values())


def clear_cache() -> None:
    """Xóa toàn bộ cache đã học."""
    global _CACHE
    with _CACHE_LOCK:
        _CACHE = {}
    try:
        _CACHE_FILE.unlink(missing_ok=True)
    except Exception:
        pass
    _log.info("AI_ANALYZER: đã xóa cache")


# Load cache ngay khi import
_load_cache()
