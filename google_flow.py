# -*- coding: utf-8 -*-
from __future__ import annotations

import logging
import os
import random
import re
import subprocess
import threading
import time
import base64
from typing import Optional

from account_model import Account
from app_config import GOOGLE_LOGIN_URL, load_config, load_tool_config
from hero_sms import get_hero_sms_code, get_hero_sms_number, hero_sms_mark_ready

PAUSE_EVENT = threading.Event()
_worker_pause_events: dict[str, threading.Event] = {}
_worker_pause_lock = threading.Lock()

# Nhận diện đa ngôn ngữ: mọi locale (EN/VI/TH/ID/ES/FR/DE/IT/PT/RU/JA/KO/ZH/AR/TR/PL/NL/...). So khớp substring trong body (lower).
_COULDNT_SIGN_IN = (
    "couldn't sign you in", "không thể đăng nhập", "ไม่สามารถยืนยัน", "ลงชื่อเข้าใช้ให้คุณไม่ได้",
    "no se pudo iniciar", "no se ha podido iniciar", "podido iniciar sesión", "no ha podido verificar",
    "impossible de vous connecter", "não foi possível entrar", "não conseguiu entrar",
    "non è stato possibile accedere", "konnte nicht angemeldet", "aanmelden mislukt",
    "nie udało się zalogować", "could not sign in", "sign in unsuccessful",
    "无法登录", "无法验证", "サインインできません", "로그인할 수 없습니다", "tidak dapat masuk",
    "не удалось войти", "لم نتمكن من تسجيل الدخول", "oturum açılamadı",
    "inloggen mislukt", "couldn't verify", "verificar que esta cuenta",
)
_SUCCESS_SAVED = (
    "saved", "updated", "đã lưu", "đã cập nhật", "guardado", "actualizado", "บันทึกแล้ว", "berhasil disimpan",
    "저장됨", "已保存", "gespeichert", "enregistré", "salvato", "opgeslagen", "zapisano", "kaydedildi",
)
_VERIFY_STEP_KEYWORDS = (
    "verification", "verify", "code", "sent to", "gửi tới", "enter the code", "nhập mã", "confirm your recovery",
    "6-digit", "0/6", "one-time", "kiểm tra email", "check your email", "codigo", "código", "รหัส", "kode",
    "verificatie", "bestätigung", "codice", "código de", "código de verificación", "確認", "인증",
    "kód pro ověření", "cod de verificare", "verification code", "enter the verification",
)
_VERIFY_CODE_WRONG = (
    "isn't the right code", "wrong code", "incorrect code", "invalid code", "try again",
    "mã không đúng", "sai mã", "không đúng", "código incorrecto", "code incorrect", "รหัสไม่ถูกต้อง", "kode salah",
    "codice errato", "falscher code", "code incorrect", "ongeldige code", "nieprawidłowy kod",
)
_PASSWORD_MISMATCH = (
    "passwords don't match", "password don't match", "mật khẩu không khớp",
    "las contraseñas no coinciden", "รหัสผ่านไม่ตรงกัน", "passwords do not match",
    "senhas não coincidem", "les mots de passe ne correspondent", "passwörter stimmen nicht",
)
_ACCOUNT_DISABLED = (
    "account has been disabled", "your account has been disabled", "บัญชีถูกปิดใช้งาน", "akun dinonaktifkan",
    "cuenta deshabilitada", "compte désactivé", "konto deaktiviert", "conta desativada",
)
_ACCOUNT_SUSPENDED = (
    "account is suspended", "suspended", "บัญชีถูกระงับ", "account suspended",
    "cuenta suspendida", "compte suspendu", "konto gesperrt",
    "too many attempts. please try again later.",
    "you seem to be having trouble getting your code. please try again later.",
)
_ACCOUNT_DELETED = (
    "this account has been deleted", "บัญชีนี้ถูกลบแล้ว", "account has been deleted",
    "cuenta eliminada", "compte supprimé", "konto gelöscht",
)
_WRONG_PASSWORD = (
    "wrong password", "sai mật khẩu", "contraseña incorrecta", "รหัสผ่านผิด", "kata sandi salah",
    "mot de passe incorrect", "falsches passwort", "senha incorreta", "password errato",
    "enter a password",
    "wrong password. try again or click \"forgot password?\" for more options.",
    "wrong password. try again or click \"try another way\" for more options.",
)
# "Your password was changed X days/hours/months ago" — pass bị đổi rồi
_PASS_CHANGED = (
    "your password was changed",
    "password was changed",
    "mật khẩu đã được thay đổi",
    "passwort wurde geändert",
    "contraseña fue cambiada",
    "mot de passe a été modifié",
    "senha foi alterada",
    "รหัสผ่านถูกเปลี่ยน",
    "kata sandi telah diubah",
)
_WRONG_RECOVERY_EMAIL = (
    "doesn't match", "does not match", "try again", "check the email",
    "không khớp", "không trùng", "không đúng", "kiểm tra lại email",
    "ne correspond pas", "no coincide", "stimmt nicht überein",
    "não corresponde", "ไม่ตรงกัน", "tidak cocok",
    "the email you entered is incorrect. try again.",
    "try again with a valid email address",
)
_NO_RECOVERY_EMAIL = (
    "add a recovery email", "set up recovery email", "no recovery email",
    "add recovery email", "you haven't added", "haven't set up",
    "thêm email khôi phục", "chưa có email khôi phục", "chưa thiết lập",
    "ajouter une adresse", "agregar correo de recuperación",
    "wiederherstellungs-e-mail hinzufügen", "adicionar e-mail de recuperação",
)
_ACCOUNT_NOT_FOUND = (
    "couldn't find your google account", "ไม่พบบัญชี", "no se encontró la cuenta",
    "compte introuvable", "konto nicht gefunden", "conta não encontrada",
)
_RECOVERY_PAGE_KEYWORDS = (
    "recovery email", "confirm your recovery", "email khôi phục", "correo de recuperación",
    "อีเมลกู้คืน", "email pemulihan", "e-mail de recuperação", "e-mail de récupération",
    "wiederherstellungs-e-mail", "email di recupero",
)
_RECOVERY_OPTION_KEYWORDS = (
    # EN/VI
    "recovery email", "confirm your recovery", "confirm recovery email", "email khôi phục",
    # RU
    "подтвердите резервный адрес электронной почты", "резервный адрес электронной почты", "резервн",
    # ES/FR/DE/IT/PT/TH/ID/JA/KO/ZH
    "correo de recuperación", "adresse e-mail de récupération", "wiederherstellungs-e-mail",
    "email di recupero", "e-mail de recuperação", "อีเมลกู้คืน", "email pemulihan",
    "再設定用のメール", "복구 이메일", "恢复邮箱",
)


def get_worker_pause_event(worker_id: str) -> threading.Event:
    with _worker_pause_lock:
        if worker_id not in _worker_pause_events:
            _worker_pause_events[worker_id] = threading.Event()
        return _worker_pause_events[worker_id]


def remove_worker_pause_event(worker_id: str):
    with _worker_pause_lock:
        _worker_pause_events.pop(worker_id, None)


def get_totp_code(secret: str) -> str:
    """Mã TOTP 6 số. Secret có thể viết thường hoặc hoa (base32)."""
    import pyotp
    _log = logging.getLogger(__name__)
    # Base32: chỉ A-Z 2-7; chuẩn hóa sang HOA để pyotp chắc chắn nhận
    padded = re.sub(r"[^A-Za-z2-7]", "", (secret or "").strip()).upper()
    if len(padded) < 16:
        _log.warning("2FA secret quá ngắn (len=%s) — cần ~16-32 ký tự base32, có thể sai", len(padded))
    while len(padded) % 8 != 0:
        padded += "="
    try:
        base64.b32decode(padded)
    except Exception:
        pass
    return pyotp.TOTP(padded).now()


def _parse_chrome_major(version_text: str) -> Optional[int]:
    m = re.search(r"(\d+)\.\d+\.\d+\.\d+", version_text or "")
    try:
        return int(m.group(1)) if m else None
    except Exception:
        return None


def _detect_local_chrome_major() -> Optional[int]:
    if os.name == "nt":
        try:
            import winreg
            for root, path in [
                (winreg.HKEY_CURRENT_USER, r"Software\Google\Chrome\BLBeacon"),
                (winreg.HKEY_LOCAL_MACHINE, r"Software\WOW6432Node\Google\Chrome\BLBeacon"),
            ]:
                try:
                    key = winreg.OpenKey(root, path)
                    ver, _ = winreg.QueryValueEx(key, "version")
                    major = _parse_chrome_major(str(ver))
                    if major:
                        return major
                except OSError:
                    continue
        except Exception:
            pass

    candidates = []
    local = os.environ.get("LOCALAPPDATA", "")
    pf = os.environ.get("PROGRAMFILES", r"C:\Program Files")
    pf86 = os.environ.get("PROGRAMFILES(X86)", r"C:\Program Files (x86)")
    for base in [local, pf, pf86]:
        exe = os.path.join(base, "Google", "Chrome", "Application", "chrome.exe")
        candidates.append(exe)
    for exe in candidates:
        if os.path.exists(exe):
            try:
                proc = subprocess.run(
                    [exe, "--version"],
                    stdout=subprocess.PIPE, stderr=subprocess.PIPE
                )
                text = proc.stdout.decode().strip()
                return _parse_chrome_major(text)
            except Exception:
                pass
    return None


_ANTI_DETECT_JS = """
try {
  Object.defineProperty(navigator, 'webdriver', {get: () => undefined});
} catch (e) {}
try {
  Object.defineProperty(navigator, 'languages', {get: () => ['en-US', 'en']});
} catch (e) {}
try {
  Object.defineProperty(navigator, 'platform', {get: () => 'Win32'});
} catch (e) {}
try {
  window.chrome = window.chrome || {};
  window.chrome.runtime = window.chrome.runtime || {};
} catch (e) {}
"""


def _apply_startup_fingerprint_patches(driver) -> None:
    try:
        driver.execute_cdp_cmd("Page.addScriptToEvaluateOnNewDocument", {"source": _ANTI_DETECT_JS})
    except Exception:
        pass
    try:
        driver.execute_script(_ANTI_DETECT_JS)
    except Exception:
        pass


def create_driver(proxy: str = "", user_data_dir: str = ""):
    _log = logging.getLogger(__name__)
    proxy = (proxy or "").strip()
    if proxy and not proxy.startswith("http") and not proxy.startswith("socks"):
        proxy = "http://" + proxy

    tcfg = load_tool_config()
    use_stealth = bool(tcfg.get("use_anti_fingerprint", True))

    uc = None
    stealth = None
    try:
        import undetected_chromedriver as uc_mod
        uc = uc_mod
    except ImportError:
        _log.warning("undetected-chromedriver not installed, falling back to standard Selenium")

    if use_stealth:
        try:
            from selenium_stealth import stealth as stealth_fn
            stealth = stealth_fn
        except ImportError:
            pass

    if uc:
        opts = uc.ChromeOptions()
        for arg in [
            "--incognito", "--start-maximized", "--no-sandbox",
            "--disable-dev-shm-usage",
            "--disable-blink-features=AutomationControlled",
            "--lang=en-US", "--no-first-run", "--no-default-browser-check",
        ]:
            opts.add_argument(arg)
        if proxy:
            opts.add_argument(f"--proxy-server={proxy}")
        if user_data_dir:
            opts.add_argument(f"--user-data-dir={user_data_dir}")

        chrome_major = _detect_local_chrome_major()
        uc_kwargs = {}
        if chrome_major:
            uc_kwargs["version_main"] = chrome_major
            _log.info("Detected local Chrome major version: %s", chrome_major)

        try:
            driver = uc.Chrome(options=opts, **uc_kwargs)
        except Exception:
            _log.exception("undetected-chromedriver init failed, falling back to standard Selenium")
            uc = None

    if uc is None:
        from selenium import webdriver
        from selenium.webdriver.chrome.options import Options
        from selenium.webdriver.chrome.service import Service

        opts = Options()
        for arg in [
            "--incognito", "--start-maximized", "--no-sandbox",
            "--disable-dev-shm-usage",
            "--disable-blink-features=AutomationControlled",
            "--lang=en-US", "--no-first-run", "--no-default-browser-check",
        ]:
            opts.add_argument(arg)
        if proxy:
            opts.add_argument(f"--proxy-server={proxy}")
        if user_data_dir:
            opts.add_argument(f"--user-data-dir={user_data_dir}")
        opts.add_experimental_option("excludeSwitches", ["enable-automation"])

        service = None
        try:
            from webdriver_manager.chrome import ChromeDriverManager
            service = Service(ChromeDriverManager().install())
        except Exception:
            pass
        driver = webdriver.Chrome(service=service, options=opts) if service else webdriver.Chrome(options=opts)
        _log.info("Driver created (standard Selenium)")

    _apply_startup_fingerprint_patches(driver)

    if stealth and use_stealth:
        try:
            stealth(driver,
                    languages=["en-US", "en"],
                    vendor="Google Inc.",
                    platform="Win32",
                    webgl_vendor="Intel Inc.",
                    renderer="Intel Iris OpenGL Engine")
        except Exception:
            _log.warning("selenium-stealth injection failed; continue without stealth JS")

    try:
        from gpm_mode_patch import register_driver
        register_driver(driver)
    except Exception:
        try:
            driver.set_window_size(1600, 1000)
        except Exception:
            pass

    return driver


def _human_type(element, text: str, delay_min: float = 0.04, delay_max: float = 0.12):
    for c in str(text):
        element.send_keys(c)
        time.sleep(random.uniform(delay_min, delay_max))


def _type_and_confirm_input(driver, element, value: str, *, exact: bool = False, min_len: int = 1, retries: int = 3, refind_selector: str = "") -> bool:
    """Nhập input và xác nhận ô đã có value trước khi submit để tránh bấm Next khi ô còn trống.
    refind_selector: CSS selector để tìm lại element nếu bị stale (DOM re-render).
    """
    from selenium.common.exceptions import StaleElementReferenceException
    from selenium.webdriver.common.by import By
    from human_click import human_move_and_click
    target = str(value or "")
    if not target:
        return False

    _log = logging.getLogger(__name__)
    stale_count = 0
    MAX_STALE = 3

    def _ok(cur: str) -> bool:
        cur = (cur or "").strip()
        if exact:
            return cur == target.strip()
        return len(cur) >= max(1, int(min_len))

    def _is_stale(exc) -> bool:
        return isinstance(exc, StaleElementReferenceException) or "stale element" in str(exc).lower()

    def _read_value(el) -> str:
        try:
            v = el.get_attribute("value")
            if v is not None:
                return str(v)
        except Exception:
            pass
        try:
            v = driver.execute_script("return arguments[0] && arguments[0].value ? arguments[0].value : '';", el)
            return str(v or "")
        except Exception:
            return ""

    def _refind():
        nonlocal element, stale_count
        stale_count += 1
        if stale_count > MAX_STALE:
            _log.warning("_type_and_confirm_input: quá %d lần stale — trang có thể đã chuyển", MAX_STALE)
            return False
        if refind_selector:
            try:
                time.sleep(0.3)
                found = driver.find_elements(By.CSS_SELECTOR, refind_selector)
                for el in found:
                    if el.is_displayed() and el.is_enabled():
                        element = el
                        _log.debug("_type_and_confirm_input: tìm lại element qua %s", refind_selector)
                        return True
            except Exception:
                pass
        _log.debug("_type_and_confirm_input: stale element, không tìm lại được")
        return False

    for attempt in range(max(1, retries)):
        try:
            human_move_and_click(driver, element, pause_after=0.03)
        except Exception as e:
            if _is_stale(e) and not _refind():
                return False

        try:
            element.clear()
        except Exception as e:
            if _is_stale(e) and not _refind():
                return False

        try:
            _human_type(element, target)
        except Exception as e:
            if _is_stale(e) and not _refind():
                return False

        time.sleep(0.2)
        cur = _read_value(element)
        if _ok(cur):
            return True

        try:
            element.send_keys(target)
            time.sleep(0.15)
            cur = _read_value(element)
            if _ok(cur):
                return True
        except Exception as e:
            if _is_stale(e) and not _refind():
                return False

        try:
            driver.execute_script(
                "arguments[0].value = arguments[1];"
                "arguments[0].dispatchEvent(new Event('input', {bubbles:true}));"
                "arguments[0].dispatchEvent(new Event('change', {bubbles:true}));",
                element, target,
            )
            time.sleep(0.15)
            cur = _read_value(element)
            if _ok(cur):
                return True
        except Exception as e:
            if _is_stale(e) and not _refind():
                return False

        time.sleep(0.15)
    return False


def _find_next_button(driver):
    """Tìm nút Next/Submit theo DOM (không phụ thuộc ngôn ngữ)."""
    from selenium.webdriver.common.by import By
    for id_ in ["identifierNext", "passwordNext", "totpNext", "next"]:
        try:
            btn = driver.find_element(By.ID, id_)
            if btn.is_displayed() and btn.is_enabled():
                return btn
        except Exception:
            pass
    for sel in ["button[type='submit']", "button[type='button']", "div[role='button']", "span[role='button']"]:
        try:
            for el in driver.find_elements(By.CSS_SELECTOR, sel):
                if el.is_displayed() and el.is_enabled():
                    return el
        except Exception:
            pass
    return None


# Text nút Next/Submit trên trang captcha — đa ngôn ngữ (EN/DE/ES/FR/...)
_NEXT_BUTTON_TEXTS = ("next", "weiter", "continue", "submit", "next step", "ต่อไป", "siguiente", "continuer", "下一步", "다음")

def _find_submit_or_primary_button(driver):
    """Tìm nút gửi form / primary action theo DOM (đa ngôn ngữ). Trang captcha có thể dùng nút 'Weiter'/'Next' không có id."""
    from selenium.webdriver.common.by import By
    for id_ in ["passwordNext", "totpNext", "next"]:
        try:
            btn = driver.find_element(By.ID, id_)
            if btn.is_displayed() and btn.is_enabled():
                return btn
        except Exception:
            pass
    for sel in ["button[type='submit']", "div[role='button'][tabindex='0']", "button", "div[role='button']"]:
        try:
            els = driver.find_elements(By.CSS_SELECTOR, sel)
            visible = [e for e in els if e.is_displayed() and e.is_enabled()]
            if visible:
                return visible[-1]
        except Exception:
            pass
    # Trang captcha Google: nút "Weiter" / "Next" có thể chỉ là span trong div — tìm theo nội dung
    try:
        for text in _NEXT_BUTTON_TEXTS:
            try:
                el = driver.find_element(By.XPATH, f"//*[@role='button' and contains(translate(., 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), '{text}')]")
                if el.is_displayed() and el.is_enabled():
                    return el
            except Exception:
                pass
        for text in _NEXT_BUTTON_TEXTS:
            try:
                el = driver.find_element(By.XPATH, f"//*[contains(translate(., 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), '{text}') and (@role='button' or self::button)]")
                if el.is_displayed() and el.is_enabled():
                    return el
            except Exception:
                pass
    except Exception:
        pass
    return None


def _find_verify_next_button(driver):
    """Nút Next cho các màn nhập mã (2FA/OTP) — tránh bấm nhầm Try another way."""
    from selenium.webdriver.common.by import By
    for id_ in ["totpNext", "idvPreregisteredPhoneNext", "next", "passwordNext"]:
        try:
            btn = driver.find_element(By.ID, id_)
            if btn.is_displayed() and btn.is_enabled():
                return btn
        except Exception:
            pass
    # Tránh nút đổi phương thức/chuyển cách xác minh.
    for sel in ["button[type='submit']", "div[role='button'][jsname]", "button", "div[role='button']"]:
        try:
            for el in driver.find_elements(By.CSS_SELECTOR, sel):
                if not (el.is_displayed() and el.is_enabled()):
                    continue
                txt = (el.text or "").strip().lower()
                if not txt:
                    continue
                if any(bad in txt for bad in ("try another way", "use another way", "another way", "need help")):
                    continue
                if any(ok in txt for ok in ("next", "continue", "submit", "weiter", "siguiente", "continuer", "다음", "下一步")):
                    return el
        except Exception:
            pass
    return _find_submit_or_primary_button(driver)


def _submit_with_enter_first(driver, input_el, *, button_kind: str = "verify", log_prefix: str = "SUBMIT") -> bool:
    """Ưu tiên Enter; chỉ click nút đúng loại khi Enter không phản hồi."""
    from selenium.webdriver.common.keys import Keys
    from human_click import human_move_and_click
    _log = logging.getLogger(__name__)

    if input_el is not None:
        try:
            input_el.send_keys(Keys.ENTER)
            _log.info("%s: submit bằng Enter", log_prefix)
            return True
        except Exception:
            pass

    btn = None
    if button_kind == "next":
        btn = _find_next_button(driver)
    elif button_kind == "verify":
        btn = _find_verify_next_button(driver)
    else:
        btn = _find_submit_or_primary_button(driver)
    if btn:
        human_move_and_click(driver, btn, pause_after=0.08)
        _log.info("%s: Enter fail -> fallback click", log_prefix)
        return True
    _log.warning("%s: không submit được (Enter và click đều fail)", log_prefix)
    return False


def _is_password_challenge_page(driver) -> bool:
    """Trang yêu cầu nhập mật khẩu (re-enter / confirm) — nhận diện theo DOM, không theo text."""
    from selenium.webdriver.common.by import By
    try:
        inps = driver.find_elements(By.CSS_SELECTOR, "input[type='password']")
        return any(e.is_displayed() for e in inps)
    except Exception:
        return False


def _is_reauth_page(driver) -> bool:
    """Phát hiện trang re-auth Google (password hoặc 2FA) sau khi navigate tới myaccount settings.
    Nhận diện bằng URL chứa accounts.google.com/v3/signin hoặc /challenge/ hoặc /ServiceLogin."""
    try:
        url = (driver.current_url or "").lower()
    except Exception:
        return False
    return (
        "accounts.google.com/v3/signin" in url or
        "accounts.google.com/servicelog" in url or
        "/challenge/" in url or
        ("/signin/" in url and "accounts.google.com" in url)
    )


def _handle_reauth(driver, current_password: str, two_fa_secret: str,
                    target_url: str = "", timeout: float = 30.0,
                    log_prefix: str = "REAUTH") -> bool:
    """Xử lý vòng lặp re-authentication: chờ trang render, nhập password, nhập 2FA, chờ redirect.
    Trả về True nếu đã xong reauth (đã tới trang target hoặc không còn ở trang reauth).
    Trả về False nếu hết timeout mà vẫn stuck.
    """
    _log = logging.getLogger(__name__)
    from selenium.webdriver.common.by import By

    deadline = time.time() + timeout
    max_loops = 15
    loops = 0

    while time.time() < deadline and loops < max_loops:
        loops += 1

        if target_url:
            try:
                cur = (driver.current_url or "").lower()
                if target_url.lower().rstrip("/") in cur:
                    _log.info("%s: đã tới target URL", log_prefix)
                    return True
            except Exception:
                pass

        if not _is_reauth_page(driver):
            _log.info("%s: không còn ở trang reauth — done", log_prefix)
            return True

        _wait_page_ready(driver, timeout=min(10.0, deadline - time.time()), poll=0.5)

        body = ""
        try:
            body = (driver.execute_script(
                "return document.body ? document.body.innerText.trim().substring(0, 200) : ''"
            ) or "").lower()
        except Exception:
            pass

        if body in ("", "loading"):
            _log.info("%s: trang vẫn đang loading — chờ 2s", log_prefix)
            time.sleep(2.0)
            continue

        if _is_password_challenge_page(driver):
            _log.info("%s: phát hiện password challenge — nhập password", log_prefix)
            if current_password and _fill_password_and_submit(driver, current_password):
                _wait_page_ready(driver, timeout=8.0)
                time.sleep(2)
                continue
            else:
                _log.warning("%s: không thể nhập password", log_prefix)
                return False

        if _is_2fa_challenge_page(driver):
            _log.info("%s: phát hiện 2FA challenge — nhập mã TOTP", log_prefix)
            if two_fa_secret and two_fa_secret.strip():
                _handle_2fa(driver, two_fa_secret)
                _wait_page_ready(driver, timeout=8.0)
                time.sleep(2)
                continue
            else:
                _log.warning("%s: 2FA cần nhưng không có secret", log_prefix)
                return False

        try:
            url = (driver.current_url or "").lower()
        except Exception:
            url = ""

        page_kind = _detect_login_page_kind_once(driver)
        if page_kind == "password_entry":
            _log.info("%s: detect password_entry — nhập password", log_prefix)
            if current_password:
                _submit_password(driver, current_password)
                _wait_page_ready(driver, timeout=8.0)
                time.sleep(2)
                continue
        elif page_kind == "twofa_challenge":
            _log.info("%s: detect twofa_challenge — nhập 2FA", log_prefix)
            if two_fa_secret:
                _handle_2fa(driver, two_fa_secret)
                _wait_page_ready(driver, timeout=8.0)
                time.sleep(2)
                continue
        elif page_kind in ("email_entry",):
            _log.info("%s: detect email_entry — skip (unexpected)", log_prefix)
            return False

        if not _is_reauth_page(driver):
            return True

        _log.info("%s: vẫn ở trang reauth, page_kind=%s — chờ 2s", log_prefix, page_kind)
        time.sleep(2.0)

    _log.warning("%s: timeout — vẫn stuck ở reauth (loops=%s)", log_prefix, loops)
    return not _is_reauth_page(driver)


def _fill_password_and_submit(driver, password: str) -> bool:
    """Điền mật khẩu vào ô hiện tại và bấm nút gửi (đa ngôn ngữ). Trả về True nếu đã điền và bấm."""
    from selenium.webdriver.common.by import By
    if not (password or str(password).strip()):
        return False
    try:
        inps = driver.find_elements(By.CSS_SELECTOR, "input[type='password']")
        inp = next((e for e in inps if e.is_displayed() and e.is_enabled()), None)
        if not inp:
            return False
        if not _type_and_confirm_input(driver, inp, password, exact=False, min_len=min(6, len(str(password or ""))), retries=3):
            return False
        time.sleep(0.5)
        ok_submit = _submit_with_enter_first(driver, inp, button_kind="verify", log_prefix="PASS_FILL")
        if ok_submit:
            time.sleep(2)
            return True
    except Exception:
        pass
    return False


def wait_if_paused(status_cb=None, worker_id: str = "main"):
    evt = get_worker_pause_event(worker_id)
    while PAUSE_EVENT.is_set() or evt.is_set():
        try:
            if status_cb:
                status_cb("Dang tam dung... bam 'Tiep tuc' de chay tiep.")
        except Exception:
            pass
        time.sleep(1)


def _submit_email(driver, email: str):
    from selenium.webdriver.common.by import By
    from selenium.webdriver.support import expected_conditions as EC
    from selenium.webdriver.support.ui import WebDriverWait
    from human_click import inject_fake_cursor

    inject_fake_cursor(driver)
    try:
        inp = WebDriverWait(driver, 15).until(
            EC.presence_of_element_located((By.ID, "identifierId"))
        )
    except Exception:
        inp = driver.find_element(By.CSS_SELECTOR, "input[type='email']")
    ok = _type_and_confirm_input(driver, inp, email, exact=True, min_len=len(str(email or "")), retries=3,
                                  refind_selector="#identifierId, input[type='email']")
    if not ok:
        raise RuntimeError("Không thể nhập email vào ô identifierId")
    time.sleep(0.5)
    pre_url = driver.current_url
    _submit_with_enter_first(driver, inp, button_kind="next", log_prefix="EMAIL")
    _wait_page_transition(driver, old_url=pre_url, timeout=15.0)


def _submit_password(driver, password: str):
    from selenium.webdriver.common.by import By
    from selenium.webdriver.support import expected_conditions as EC
    from selenium.webdriver.support.ui import WebDriverWait
    from human_click import inject_fake_cursor

    _log = logging.getLogger(__name__)
    inject_fake_cursor(driver)

    def _find_password_input():
        for by, val in [
            (By.NAME, "Passwd"),
            (By.NAME, "password"),
            (By.CSS_SELECTOR, "input[type='password']"),
        ]:
            try:
                el = WebDriverWait(driver, 6).until(EC.element_to_be_clickable((by, val)))
                if el.is_displayed():
                    return el
            except Exception:
                continue
        return None

    def _password_input_gone() -> bool:
        """Kiểm tra password input đã biến mất (trang đã chuyển sang bước kế)."""
        try:
            for sel in ["input[name='Passwd']", "input[name='password']", "input[type='password']"]:
                for el in driver.find_elements(By.CSS_SELECTOR, sel):
                    if el.is_displayed() and el.is_enabled():
                        return False
            return True
        except Exception:
            return True

    inp = _find_password_input()
    if not inp:
        raise RuntimeError("Không tìm thấy ô mật khẩu. Có thể trang đang hiện captcha — bật API EzCaptcha trong config.")

    ok = _type_and_confirm_input(
        driver,
        inp,
        password,
        exact=True,
        min_len=max(1, len(str(password or ""))),
        retries=4,
        refind_selector="input[name='Passwd'], input[name='password'], input[type='password']",
    )
    if not ok:
        time.sleep(0.5)
        if _password_input_gone():
            _log.info("PASSWORD: nhập stale nhưng password input đã biến mất — trang đã chuyển, coi như OK")
            time.sleep(2)
            return
        inp = _find_password_input()
        if inp:
            _log.info("PASSWORD: tìm lại password input, thử nhập lại")
            ok = _type_and_confirm_input(
                driver, inp, password,
                exact=True, min_len=max(1, len(str(password or ""))),
                retries=2,
                refind_selector="input[name='Passwd'], input[name='password'], input[type='password']",
            )
            if not ok:
                if _password_input_gone():
                    _log.info("PASSWORD: input biến mất sau retry — trang đã chuyển")
                    time.sleep(2)
                    return
                raise RuntimeError("Nhập mật khẩu thất bại (ô vẫn trống)")
        else:
            _log.info("PASSWORD: không tìm lại được input — trang có thể đã chuyển")
            time.sleep(2)
            return

    time.sleep(0.5)

    try:
        cur = driver.execute_script("return arguments[0] && arguments[0].value ? arguments[0].value : '';", inp) or ""
    except Exception:
        cur = ""
    if len(str(cur)) < 1:
        if _password_input_gone():
            _log.info("PASSWORD: verify value stale nhưng input đã biến mất — trang chuyển rồi")
            time.sleep(2)
            return
        raise RuntimeError("Ô mật khẩu trống, huỷ bấm Next để tránh pending")

    pre_url = driver.current_url
    _submit_with_enter_first(driver, inp, button_kind="verify", log_prefix="PASSWORD")
    _wait_page_transition(driver, old_url=pre_url, timeout=15.0)


def _handle_2fa(driver, two_fa_secret: str):
    """Nhập mã 2FA/TOTP. Log rõ: có nhập được không, có bấm nút không."""
    from selenium.webdriver.common.by import By
    from selenium.webdriver.common.keys import Keys
    from selenium.webdriver.support import expected_conditions as EC
    from selenium.webdriver.support.ui import WebDriverWait
    from human_click import human_move_and_click, human_type as _ht, inject_fake_cursor

    _log = logging.getLogger(__name__)
    raw = (two_fa_secret or "").strip()
    if not raw:
        _log.warning("2FA: secret rỗng — không nhập được")
        return

    if raw.startswith("+") or raw.replace("-", "").replace(" ", "").isdigit():
        _log.error("2FA: giá trị '%s' giống số điện thoại, KHÔNG phải TOTP secret! Bỏ qua.", raw[:20])
        return

    base32_chars = re.sub(r"[^A-Za-z2-7]", "", raw)
    if len(base32_chars) < 10:
        _log.error("2FA: secret '%s' quá ngắn hoặc không hợp lệ (base32 chars=%d). Bỏ qua.", raw[:10] + "...", len(base32_chars))
        return

    inject_fake_cursor(driver)
    code = get_totp_code(raw)
    _log.info("2FA: secret len=%s, đã sinh mã TOTP='%s', đang tìm ô nhập...", len(raw), code)

    wait_short = 2
    inp = None
    for by, val in [
        (By.CSS_SELECTOR, "input[autocomplete='one-time-code']"),
        (By.CSS_SELECTOR, "input[type='tel']"),
        (By.ID, "totpPin"),
        (By.NAME, "totpPin"),
        (By.CSS_SELECTOR, "input[maxlength='6']"),
        (By.CSS_SELECTOR, "input[inputmode='numeric']"),
    ]:
        try:
            el = WebDriverWait(driver, wait_short).until(
                EC.presence_of_element_located((by, val))
            )
            if el and el.is_displayed() and el.is_enabled():
                inp = el
                break
        except Exception:
            continue

    if not inp:
        for el in driver.find_elements(By.CSS_SELECTOR, "input"):
            if not el.is_displayed() or not el.is_enabled():
                continue
            t = (el.get_attribute("type") or "text").lower()
            if t in ("email", "hidden", "checkbox", "submit", "button"):
                continue
            inp = el
            break

    if inp:
        _log.info("2FA: tìm thấy ô nhập, đang nhập mã...")
        try:
            human_move_and_click(driver, inp, pause_after=0.08)
            time.sleep(0.2)
            inp.clear()
            _ht(driver, inp, code)
            time.sleep(0.3)
        except Exception as ex:
            _log.warning("2FA human_type lỗi, thử send_keys: %s", ex)
            try:
                inp.click()
                inp.send_keys(code)
                time.sleep(0.3)
            except Exception as ex2:
                _log.warning("2FA send_keys lỗi, thử JS: %s", ex2)
                try:
                    driver.execute_script(
                        "arguments[0].value = arguments[1]; arguments[0].dispatchEvent(new Event('input', { bubbles: true }));",
                        inp, code,
                    )
                    time.sleep(0.3)
                except Exception as ex3:
                    _log.warning("2FA JS set value lỗi: %s", ex3)
        # Kiểm tra đã nhập đủ 6 ký tự chưa
        try:
            v = inp.get_attribute("value") or ""
            _log.info("2FA: sau khi nhập, ô có %s ký tự", len(v))
        except Exception:
            pass
    else:
        _log.warning("2FA: KHÔNG tìm thấy ô nhập mã — xem log để biết bước trước đó")

    submitted = False
    # Ưu tiên Enter như yêu cầu (Google thường submit trực tiếp).
    if inp:
        try:
            inp.send_keys(Keys.ENTER)
            submitted = True
            _log.info("2FA: đã submit bằng Enter")
        except Exception:
            submitted = False
    if not submitted:
        btn = _find_verify_next_button(driver)
        if btn:
            human_move_and_click(driver, btn, pause_after=0.08)
            _log.info("2FA: fallback bấm nút gửi (theo DOM)")
        else:
            _log.warning("2FA: KHÔNG submit được bằng Enter và cũng không thấy nút gửi")
    pre_url = driver.current_url
    _wait_page_transition(driver, old_url=pre_url, timeout=12.0)


def close_current_tab_and_switch_to_blank(driver) -> None:
    """Đóng tab hiện tại và chuyển sang tab trống (about:blank). Gọi khi xong task và không còn nvụ."""
    try:
        old_handle = driver.current_window_handle
        driver.execute_script("window.open('about:blank', '_blank');")
        time.sleep(0.3)
        handles = list(driver.window_handles)
        new_handles = [h for h in handles if h != old_handle]
        if new_handles:
            driver.switch_to.window(new_handles[0])
            driver.switch_to.window(old_handle)
            driver.close()
            driver.switch_to.window(new_handles[0])
    except Exception:
        pass


def handle_2fa_challenge_if_present(driver, two_fa_secret: str) -> bool:
    """Nếu đang ở trang yêu cầu nhập 2FA thì điền mã từ twofasecret. Gọi từ bất kỳ task nào (change pass, đổi mail, xóa phone, ...).
    Chỉ hoạt động trên trang accounts.google.com (signin/challenge), KHÔNG phải myaccount settings.
    """
    raw = (two_fa_secret or "").strip()
    if not raw:
        return False
    if raw.startswith("+") or raw.replace("-", "").replace(" ", "").isdigit():
        return False
    url = (driver.current_url or "").lower()
    if "myaccount.google.com" in url:
        return False
    if not _is_2fa_challenge_page(driver):
        return False
    _log = logging.getLogger(__name__)
    _log.info("Phát hiện trang 2FA trong task — điền mã từ twofasecret của acc")
    _handle_2fa(driver, two_fa_secret)
    time.sleep(3)
    return True


def _page_says_2fa_wrong(driver) -> bool:
    """Trang có báo mã 2FA sai không (đa ngôn ngữ)."""
    from selenium.webdriver.common.by import By
    try:
        body = (driver.find_element(By.TAG_NAME, "body").text or "").lower()
        wrong_hints = [
            "wrong code", "incorrect", "wrong", "invalid", "invalid code", "try again",
            "sai", "không đúng", "mã không", "thử lại",
            "неверн", "неправильн", "ошибк",  # Nga
            "erroneo", "incorrecto", "inválido",  # TBN/Ý
        ]
        return any(h in body for h in wrong_hints)
    except Exception:
        return False


def _handle_recovery_email(driver, recovery_email: str) -> bool:
    from selenium.webdriver.common.by import By
    from selenium.webdriver.support import expected_conditions as EC
    from selenium.webdriver.support.ui import WebDriverWait
    _log = logging.getLogger(__name__)

    recovery_email = (recovery_email or "").strip()
    if not recovery_email:
        return False
    if not re.match(r"^[^@\s]+@[^@\s]+\.[^@\s]+$", recovery_email):
        _log.warning("RECOVERY_CONFIRM: recovery_email không đúng định dạng, bỏ qua submit để tránh pending")
        return False

    def _has_visible_email_input() -> bool:
        try:
            for sel in ("input[type='email']", "input[aria-label*='email']", "input[name*='email']"):
                for el in driver.find_elements(By.CSS_SELECTOR, sel):
                    if el.is_displayed() and el.is_enabled():
                        return True
        except Exception:
            pass
        return False

    def _page_moved_away() -> bool:
        """Trang đã chuyển khỏi recovery confirm (sang phone, 2fa, success, ...)."""
        if not _has_visible_email_input():
            return True
        return False

    for attempt in range(3):
        if not _has_visible_email_input():
            _log.info("RECOVERY_CONFIRM: không còn email input — trang đã chuyển bước (attempt %d)", attempt)
            return True

        for xpath in [
            "//input[@type='email']",
            "//input[@type='text']",
            "//input[contains(translate(@aria-label,'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz'),'email')]",
            "//input[contains(translate(@name,'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz'),'email')]",
        ]:
            try:
                els = driver.find_elements(By.XPATH, xpath)
                for el in els:
                    if not (el.is_displayed() and el.is_enabled()):
                        continue
                    ok = _type_and_confirm_input(
                        driver, el, recovery_email, exact=True, min_len=len(recovery_email), retries=4,
                        refind_selector="input[type='email'], input[aria-label*='email'], input[name*='email']",
                    )
                    if not ok:
                        if _page_moved_away():
                            _log.info("RECOVERY_CONFIRM: stale sau nhập — trang đã chuyển bước")
                            return True
                        continue
                    time.sleep(0.4)
                    pre_url = driver.current_url
                    _submit_with_enter_first(driver, el, button_kind="next", log_prefix="RECOVERY_EMAIL")
                    _wait_page_transition(driver, old_url=pre_url, timeout=12.0)
                    if _page_moved_away():
                        _log.info("RECOVERY_CONFIRM: nhập recovery email thành công và đã qua bước confirm")
                        return True
            except Exception:
                continue
        time.sleep(0.8)

    if _page_moved_away():
        _log.info("RECOVERY_CONFIRM: trang đã chuyển sau 3 attempts")
        return True

    _log.warning("RECOVERY_CONFIRM: không thể xác nhận recovery email (input vẫn lỗi/không qua trang)")
    return False


def _classify_verify_challenge_kind(driver) -> tuple[str, int, int]:
    """Phân loại challenge verify theo điểm: phone / twofa / unknown.
    Hoàn toàn dựa vào URL + DOM structure — không đọc body text nên hoạt động mọi ngôn ngữ.
    """
    from selenium.webdriver.common.by import By
    phone_score = 0
    twofa_score = 0
    try:
        url = (driver.current_url or "").lower()
    except Exception:
        url = ""

    # ── URL signals (trọng số cao nhất) ──
    if any(k in url for k in ("/challenge/totp", "/challenge/az", "/challenge/sk",
                               "/challenge/ootp", "/challenge/duo", "/signinoptions")):
        twofa_score += 6
    if any(k in url for k in ("/challenge/ipp", "/challenge/iap", "/iap/verify",
                               "/challenge/sms", "/challenge/phone")):
        phone_score += 6

    # ── Input DOM signals ──
    try:
        inputs = driver.find_elements(By.CSS_SELECTOR, "input")
    except Exception:
        inputs = []
    for el in inputs:
        try:
            if not (el.is_displayed() and el.is_enabled()):
                continue
            typ = (el.get_attribute("type") or "").lower()
            ac = (el.get_attribute("autocomplete") or "").lower()
            ml = (el.get_attribute("maxlength") or "").strip()
            nm = ((el.get_attribute("name") or "") + " " + (el.get_attribute("id") or "")).lower()
            im = (el.get_attribute("inputmode") or "").lower()

            if ac == "one-time-code":
                twofa_score += 5
            if ml == "6":
                twofa_score += 4
            if ml == "8" and im == "numeric":
                twofa_score += 3  # backup code
            if typ == "tel" and ml not in ("", "6", "8"):
                phone_score += 4
            if "tel" in ac or "phone" in ac:
                phone_score += 4
            if "code" in nm or "totp" in nm or "otp" in nm:
                twofa_score += 3
            if "phone" in nm or "mobile" in nm:
                phone_score += 3
            if im == "numeric" and ml and ml.isdigit() and int(ml) <= 8:
                twofa_score += 3
            if typ == "tel" and (not ml or (ml.isdigit() and int(ml) > 10)):
                phone_score += 3
        except Exception:
            pass

    # ── Page-level structural signals ──
    try:
        js_signals = driver.execute_script("""
            var r = {phone: 0, totp: 0};
            // Country code selector = phone
            if (document.querySelector('[data-country-code],[aria-label*="country" i],select[name*="country" i]')) r.phone += 3;
            // Timer/countdown = TOTP expiry
            if (document.querySelector('[role="timer"],[data-remaining-time],.countdown')) r.totp += 3;
            // form action
            for (var f of document.forms) {
                var a = (f.action || '').toLowerCase();
                if (a.includes('ipp') || a.includes('/phone') || a.includes('/sms')) r.phone += 3;
                if (a.includes('totp') || a.includes('/az/') || a.includes('/otp')) r.totp += 3;
            }
            // data-challengetype
            for (var el of document.querySelectorAll('[data-challengetype]')) {
                var ct = (el.getAttribute('data-challengetype') || '').toLowerCase();
                if (ct.includes('totp') || ct === '6' || ct === '33') r.totp += 4;
                if (ct.includes('phone') || ct.includes('sms') || ct === '9' || ct === '10') r.phone += 4;
            }
            return r;
        """) or {}
        phone_score += js_signals.get("phone", 0)
        twofa_score += js_signals.get("totp", 0)
    except Exception:
        pass

    if twofa_score >= phone_score + 2 and twofa_score >= 3:
        return ("twofa", phone_score, twofa_score)
    if phone_score >= twofa_score + 2 and phone_score >= 3:
        return ("phone", phone_score, twofa_score)
    return ("unknown", phone_score, twofa_score)


def _is_phone_number_challenge_page(driver) -> bool:
    """Nhận diện trang nhập SĐT / SMS bằng classifier ưu tiên ngữ cảnh thực tế."""
    from selenium.webdriver.common.by import By
    try:
        kind, p_score, t_score = _classify_verify_challenge_kind(driver)
        if kind == "phone":
            return True
        if kind == "twofa":
            return False

        # fallback structural — không đọc body text (ngôn ngữ nào cũng hoạt động)
        inps = driver.find_elements(By.CSS_SELECTOR,
            "input[type='tel'], input[autocomplete*='tel'], input[autocomplete*='phone']")
        for el in inps:
            if not el.is_displayed():
                continue
            ml = (el.get_attribute("maxlength") or "").strip()
            # maxlength=6 → likely TOTP, skip
            if ml == "6":
                continue
            return True
        # Country code selector = phone number entry
        try:
            has_phone_ui = driver.execute_script("""
                if (document.querySelector('[data-country-code],[aria-label*="country" i],select[name*="country" i]')) return true;
                var forms = document.querySelectorAll('form');
                for (var f of forms) {
                    var a = (f.action || '').toLowerCase();
                    if (a.includes('ipp') || a.includes('/phone') || a.includes('/sms')) return true;
                }
                var telInps = document.querySelectorAll('input[type="tel"]:not([maxlength="6"])');
                for (var inp of telInps) { if (inp.offsetParent) return true; }
                return false;
            """)
            if has_phone_ui:
                return True
        except Exception:
            pass
        _ = (p_score, t_score)  # giữ biến cho debug khi cần
    except Exception:
        pass
    return False


def _submit_phone_verify_code(driver, code: str):
    from selenium.webdriver.common.by import By
    from selenium.webdriver.common.keys import Keys
    from selenium.webdriver.support import expected_conditions as EC
    from selenium.webdriver.support.ui import WebDriverWait

    inp = None
    for by, val in [
        (By.CSS_SELECTOR, "input[autocomplete='one-time-code']"),
        (By.CSS_SELECTOR, "input[type='tel']"),
        (By.CSS_SELECTOR, "input[inputmode='numeric']"),
        (By.CSS_SELECTOR, "input[name*='code']"),
        (By.CSS_SELECTOR, "input[id*='code']"),
    ]:
        try:
            inp = WebDriverWait(driver, 5).until(EC.element_to_be_clickable((by, val)))
            if inp.is_displayed():
                break
        except Exception:
            inp = None

    if inp:
        inp.clear()
        _human_type(inp, code)
        time.sleep(0.5)
        try:
            inp.send_keys(Keys.ENTER)
        except Exception:
            pass
    else:
        from human_click import human_move_and_click
        btn = _find_verify_next_button(driver)
        if btn:
            human_move_and_click(driver, btn)
    time.sleep(3)


def _click_not_now_if_present(driver):
    """Bấm nút Not now / Later — 3 lớp locator (L1/L2 không phụ thuộc ngôn ngữ)."""
    from human_click import human_move_and_click
    from selenium.webdriver.common.by import By

    def _click(el):
        if el and el.is_displayed() and el.is_enabled():
            human_move_and_click(driver, el)
            return True
        return False

    # Lớp 1: data-testid / data-qa
    for attr, key in [("data-testid", "not-now"), ("data-testid", "later"), ("data-qa", "not-now"), ("data-qa", "later")]:
        try:
            for el in driver.find_elements(By.CSS_SELECTOR, f"[{attr}='{key}']"):
                if _click(el):
                    return
            for el in driver.find_elements(By.CSS_SELECTOR, f"button[{attr}='{key}'], [{attr}='{key}'] button, [{attr}='{key}'] [role='button']"):
                if _click(el):
                    return
        except Exception:
            pass

    # Lớp 2: ARIA ổn định
    for label in ["not now", "Not now", "later", "Later"]:
        try:
            for el in driver.find_elements(By.CSS_SELECTOR, f"[aria-label='{label}']"):
                if _click(el):
                    return
        except Exception:
            pass

    # Lớp 3: fallback text đa ngôn ngữ
    for text in ["Not now", "Không phải bây giờ", "Later", "Để sau", "今はしない", "稍后", "Não agora"]:
        try:
            for btn in driver.find_elements(By.XPATH, f"//span[text()='{text}'] | //button[contains(.,'{text}')]"):
                if _click(btn):
                    return
        except Exception:
            pass


def _click_skip_if_present(driver):
    """Bấm nút Skip — 3 lớp locator (không phụ thuộc ngôn ngữ khi có L1/L2)."""
    from human_click import human_move_and_click
    from selenium.webdriver.common.by import By

    def _click(el):
        if el and el.is_displayed() and el.is_enabled():
            human_move_and_click(driver, el)
            return True
        return False

    # Lớp 1 (bền nhất): data-testid / data-qa — key không dịch
    for attr, key in [("data-testid", "skip-btn"), ("data-testid", "skip"), ("data-qa", "skip-btn"), ("data-qa", "skip")]:
        try:
            for el in driver.find_elements(By.CSS_SELECTOR, f"[{attr}='{key}']"):
                if _click(el):
                    return
            for el in driver.find_elements(By.CSS_SELECTOR, f"button[{attr}='{key}'], [{attr}='{key}'] button, [{attr}='{key}'] [role='button']"):
                if _click(el):
                    return
        except Exception:
            pass

    # Lớp 2: ARIA ổn định — aria-label dùng key (không dịch, chỉ text hiển thị mới dịch)
    for label in ["skip", "Skip", "skip for now", "skip-btn", "Skip for now"]:
        try:
            for el in driver.find_elements(By.CSS_SELECTOR, f"[aria-label='{label}']"):
                if _click(el):
                    return
        except Exception:
            pass

    # Lớp 2b: Cấu trúc — 2 nút (Skip trái, Continue phải), không dựa vào chữ
    try:
        for sel in ["button[type='button']", "div[role='button']", "[jsname='LgbsSe']"]:
            btns = driver.find_elements(By.CSS_SELECTOR, sel)
            visible = [b for b in btns if b.is_displayed() and b.is_enabled()]
            if len(visible) == 2:
                if _click(visible[0]):
                    return
    except Exception:
        pass
    try:
        containers = driver.find_elements(By.CSS_SELECTOR, "[action-label]")
        for c in containers:
            if not c.is_displayed():
                continue
            btn = c.find_element(By.CSS_SELECTOR, "button, div[role='button']")
            if _click(btn):
                return
    except Exception:
        pass

    # Lớp 3 (fallback): text đa ngôn ngữ — khi không kiểm soát được attribute
    for text in [
        "Skip", "Bỏ qua", "ข้ามไปก่อน", "Skip for now", "稍后", "今はスキップ",
        "Überspringen", "Passer", "Saltar", "건너뛰기", "Lewati", "Atla",
    ]:
        try:
            for btn in driver.find_elements(By.XPATH, f"//span[text()='{text}'] | //button[contains(.,'{text}')]"):
                if _click(btn):
                    return
        except Exception:
            pass


def _click_done_if_present(driver):
    """Bấm nút Done / OK — 3 lớp locator (L1/L2 không phụ thuộc ngôn ngữ)."""
    from human_click import human_move_and_click
    from selenium.webdriver.common.by import By

    def _click(el):
        if el and el.is_displayed():
            human_move_and_click(driver, el)
            return True
        return False

    # Lớp 1: data-testid / data-qa
    for attr, key in [("data-testid", "done-btn"), ("data-testid", "done"), ("data-qa", "done"), ("data-qa", "ok")]:
        try:
            for el in driver.find_elements(By.CSS_SELECTOR, f"[{attr}='{key}']"):
                if _click(el):
                    return
            for el in driver.find_elements(By.CSS_SELECTOR, f"button[{attr}='{key}'], [{attr}='{key}'] button, [{attr}='{key}'] [role='button']"):
                if _click(el):
                    return
        except Exception:
            pass

    # Lớp 2: ARIA ổn định
    for label in ["done", "Done", "ok", "OK", "got it", "Got it"]:
        try:
            for el in driver.find_elements(By.CSS_SELECTOR, f"[aria-label='{label}']"):
                if _click(el):
                    return
        except Exception:
            pass

    # Lớp 3: fallback text đa ngôn ngữ
    for text in ["Done", "Xong", "OK", "Got it", "完了", "完成", "Concluído"]:
        try:
            for btn in driver.find_elements(By.XPATH, f"//span[text()='{text}'] | //button[contains(.,'{text}')]"):
                if _click(btn):
                    return
        except Exception:
            pass


def _is_2fa_on_page(driver) -> bool:
    from selenium.webdriver.common.by import By
    try:
        body = driver.find_element(By.TAG_NAME, "body").text.lower()
        on_indicators = [
            "2-step verification is on", "turn off 2-step",
            "authenticator app", "xác minh 2 bước đã bật",
        ]
        return any(k in body for k in on_indicators)
    except Exception:
        return False


def _extract_setup_key_from_page(driver) -> str:
    from selenium.webdriver.common.by import By
    _log = logging.getLogger(__name__)

    def _valid_secret(s: str) -> bool:
        s = re.sub(r"[^A-Za-z2-7]", "", s).upper()
        return len(s) >= 16

    try:
        strongs = driver.find_elements(By.CSS_SELECTOR, "strong, b, .secret-key, [data-secret]")
        for idx_el, el in enumerate(strongs):
            try:
                txt = el.text.strip()
                if _valid_secret(txt):
                    ss = re.sub(r"[^A-Za-z2-7]", "", txt).upper()
                    _log.info("Found setup key in <strong>[%d]: %s", idx_el, ss)
                    return ss
            except Exception:
                continue
    except Exception as e:
        _log.warning("_extract_setup_key_from_page strong scan error: %s", e)

    try:
        all_strongs = driver.find_elements(By.CSS_SELECTOR, "strong")
        for el in all_strongs:
            txt = el.text.strip()
            if _valid_secret(txt):
                return re.sub(r"[^A-Za-z2-7]", "", txt).upper()
    except Exception:
        pass

    try:
        body = driver.find_element(By.TAG_NAME, "body").text
        m = re.search(r"[A-Z2-7]{16,}", body)
        if m:
            return m.group(0)
    except Exception:
        pass

    try:
        m_loose = re.search(r"[a-zA-Z2-7 ]{20,}", driver.find_element(By.TAG_NAME, "body").text)
        if m_loose:
            cleaned = re.sub(r"[^A-Za-z2-7]", "", m_loose.group(0)).upper()
            if len(cleaned) >= 16:
                return cleaned
    except Exception:
        pass

    try:
        html = driver.page_source
        from urllib.parse import unquote
        haystack = unquote(html)
        m2 = re.search(r"secret[=%3D]+([A-Za-z2-7]{16,})", haystack, re.I)
        if m2:
            return m2.group(1).upper()
    except Exception:
        pass

    try:
        for el in driver.find_elements(By.CSS_SELECTOR, "[data-key], [data-secret]"):
            for attr in ["data-key", "data-secret"]:
                try:
                    key_attr = el.get_attribute(attr)
                    if key_attr and _valid_secret(key_attr):
                        return re.sub(r"[^A-Za-z2-7]", "", key_attr).upper()
                except Exception:
                    pass
    except Exception:
        pass

    return ""


def _click_turn_on_2sv_if_present(driver):
    _log = logging.getLogger(__name__)
    from human_click import human_move_and_click
    from selenium.webdriver.common.by import By

    for css in [
        "button[aria-label*='2-Step']", "a[href*='two-step']",
        "[data-action='turn-on-2sv']",
    ]:
        try:
            els = driver.find_elements(By.CSS_SELECTOR, css)
            for el in els:
                if el.is_displayed():
                    human_move_and_click(driver, el)
                    return True
        except Exception:
            pass

    for xp in [
        "//button[contains(.,'Turn on')]", "//a[contains(.,'Turn on')]",
        "//button[contains(.,'Get started')]", "//a[contains(.,'Get started')]",
        "//span[contains(.,'Turn on 2-Step')]",
        "//button[contains(.,'Bật')]", "//a[contains(.,'Bật')]",
    ]:
        try:
            els = driver.find_elements(By.XPATH, xp)
            for el in els:
                if el.is_displayed():
                    human_move_and_click(driver, el)
                    return True
        except Exception:
            pass

    return False


def ensure_2fa_authenticator_flow(
    driver, existing_secret: str = "", worker_id: str = "main", password: str = ""
) -> tuple[bool, str]:
    import logging
    from selenium.webdriver.support import expected_conditions as EC

    _log = logging.getLogger(__name__)
    secret = ""
    _existing_secret_from_db = (existing_secret or "").strip()

    TWOSV_URL = "https://myaccount.google.com/signinoptions/two-step-verification"

    def _safe_click_css(sel_list):
        from selenium.webdriver.common.by import By
        from human_click import human_move_and_click
        for s in sel_list:
            try:
                els = driver.find_elements(By.CSS_SELECTOR, s)
                for e in els:
                    if e.is_displayed():
                        human_move_and_click(driver, e)
                        return True
            except Exception:
                pass
        return False

    def _pending(step=""):
        _log.warning("2FA PENDING [%s] worker=%s", step, worker_id)
        evt = get_worker_pause_event(worker_id)
        while evt.is_set() or PAUSE_EVENT.is_set():
            time.sleep(1)

    def _current_url():
        try:
            return driver.current_url
        except Exception:
            return ""

    def _try_otp_with_secret(sec):
        from selenium.webdriver.common.by import By
        from human_click import human_move_and_click, human_type as _ht
        code = get_totp_code(sec)
        try:
            inp = driver.find_element(By.CSS_SELECTOR, "input[type='tel'], input[type='text']")
            inp.clear()
            _ht(driver, inp, code)
            time.sleep(0.5)
        except Exception:
            return False
        btn = _find_submit_or_primary_button(driver)
        if btn:
            human_move_and_click(driver, btn)
        time.sleep(3)
        return True

    def _reacquire_secret_after_wrong_code():
        return _extract_setup_key_from_page(driver)

    _log.info("2FA: navigating to %s", TWOSV_URL)
    if not _safe_navigate(driver, TWOSV_URL):
        _log.warning("2FA: page broken sau navigate — return (False, None)")
        return (False, None)

    if _is_reauth_page(driver):
        reauth_ok = _handle_reauth(
            driver, password, existing_secret,
            target_url="myaccount.google.com/signinoptions/two-step-verification",
            timeout=45.0, log_prefix="2FA_REAUTH",
        )
        if not reauth_ok:
            _log.warning("2FA: reauth failed")
            return (False, None)
        _wait_page_ready(driver, timeout=10.0)

    if _is_2fa_on_page(driver):
        _log.info("2FA: already ON. Checking for authenticator app...")
        for xp in [
            "//a[contains(.,'Authenticator')]", "//button[contains(.,'Authenticator')]",
            "//a[contains(.,'authenticator')]", "//span[contains(.,'Authenticator')]",
        ]:
            try:
                from selenium.webdriver.common.by import By
                from human_click import human_move_and_click
                els = driver.find_elements(By.XPATH, xp)
                for e in els:
                    if e.is_displayed():
                        human_move_and_click(driver, e)
                        time.sleep(3)
                        break
            except Exception:
                pass

        key = _extract_setup_key_from_page(driver)
        if key:
            return True, key
        if _existing_secret_from_db:
            return True, _existing_secret_from_db
        return True, ""

    clicked_setup = _click_turn_on_2sv_if_present(driver)
    if clicked_setup:
        time.sleep(4)

    for i in range(5):
        body_text = ""
        try:
            from selenium.webdriver.common.by import By
            body_text = driver.find_element(By.TAG_NAME, "body").text.lower()
        except Exception:
            pass

        if "authenticator" in body_text:
            break

        for xp in [
            "//button[contains(.,'Authenticator')]",
            "//a[contains(.,'Authenticator')]",
            "//span[contains(.,'Authenticator')]",
            "//button[contains(.,'Set up')]",
            "//a[contains(.,'Set up')]",
        ]:
            try:
                from selenium.webdriver.common.by import By
                from human_click import human_move_and_click
                els = driver.find_elements(By.XPATH, xp)
                for e in els:
                    if e.is_displayed():
                        human_move_and_click(driver, e)
                        time.sleep(3)
                        break
            except Exception:
                pass
        time.sleep(2)

    clicked_cant_scan = False
    for xp in [
        "//a[contains(.,'scan it')]", "//button[contains(.,'scan it')]",
        "//a[contains(.,'enter a setup key')]", "//span[contains(.,'setup key')]",
        "//a[contains(.,'quét')]", "//button[contains(.,'quét')]",
    ]:
        try:
            from selenium.webdriver.common.by import By
            from human_click import human_move_and_click
            els = driver.find_elements(By.XPATH, xp)
            for e in els:
                if e.is_displayed():
                    human_move_and_click(driver, e)
                    clicked_cant_scan = True
                    time.sleep(3)
                    break
            if clicked_cant_scan:
                break
        except Exception:
            pass

    for i in range(5):
        probe = _extract_setup_key_from_page(driver)
        if probe:
            secret = probe
            break
        time.sleep(2)

    if not secret:
        _log.warning("2FA: could not extract setup key from page")
        return False, ""

    _log.info("2FA: extracted setup key = %s", secret[:6] + "...")
    setup_key_check = _try_otp_with_secret(secret)

    if not setup_key_check:
        new_key = _reacquire_secret_after_wrong_code()
        if new_key and new_key != secret:
            secret = new_key
            _try_otp_with_secret(secret)

    _click_done_if_present(driver)
    _click_not_now_if_present(driver)
    time.sleep(3)

    # Chỉ trả về secret khi đã xác nhận tiến trình add 2FA thành công (trang bật 2FA / không còn form nhập mã)
    if _is_2fa_on_page(driver):
        _log.info("2FA: authenticator flow completed. secret=%s", secret[:6] + "...")
        return True, secret
    try:
        from selenium.webdriver.common.by import By
        body = driver.find_element(By.TAG_NAME, "body").text.lower()
        if "authenticator" in body or "2-step" in body or "verification" in body:
            _log.info("2FA: flow completed (page shows 2FA/authenticator). secret=%s", secret[:6] + "...")
            return True, secret
    except Exception:
        pass
    _log.warning("2FA: chưa xác nhận thành công — không lưu secret vào DB")
    return False, ""


def _handle_phone_number_challenge(
    driver, hero_api_key: str, hero_service: str,
    hero_retries: int = 3, phone: str = "", activation_id: str = "", rent_wait_sec: int = 300, otp_wait_sec: int = 300
):
    from selenium.webdriver.common.by import By
    from selenium.webdriver.support import expected_conditions as EC
    from selenium.webdriver.support.ui import WebDriverWait
    _log = logging.getLogger(__name__)

    if not phone:
        if not hero_api_key or not hero_service:
            _log.warning("PHONE_CHALLENGE: thiếu Hero-SMS API key/service")
            return {"ok": False, "error": "No Hero-SMS API key or service configured"}
        _log.info(
            "PHONE_CHALLENGE: đang lấy số từ Hero-SMS (service=%s, retries=%s, wait<=%ss)...",
            hero_service, hero_retries, rent_wait_sec,
        )
        activation_id, phone = get_hero_sms_number(
            hero_api_key, hero_service, hero_retries, max_wait_sec=rent_wait_sec
        )
        if not phone:
            _log.warning("PHONE_CHALLENGE: không lấy được số từ Hero-SMS (service=%s)", hero_service)
            return {"ok": False, "error": "Could not get phone number from Hero-SMS"}
        _log.info("PHONE_CHALLENGE: đã lấy số %s (activation_id=%s)", phone, activation_id)

    inp = None
    for by, val in [
        (By.CSS_SELECTOR, "input[type='tel']"),
        (By.CSS_SELECTOR, "input[autocomplete*='tel']"),
    ]:
        try:
            inp = WebDriverWait(driver, 5).until(EC.element_to_be_clickable((by, val)))
            if inp.is_displayed():
                break
        except Exception:
            inp = None

    if inp:
        digits_only = re.sub(r"\D", "", str(phone or ""))
        phone_e164 = f"+{digits_only}" if digits_only else ""
        if not phone_e164:
            _log.warning("PHONE_CHALLENGE: số thuê được không hợp lệ: %r", phone)
            return {"ok": False, "error": "Invalid rented phone number", "phone": phone, "activation_id": activation_id}
        ok = _type_and_confirm_input(
            driver, inp, phone_e164, exact=True, min_len=len(phone_e164), retries=4
        )
        if not ok:
            _log.warning("PHONE_CHALLENGE: nhập số điện thoại thất bại (value không giữ được)")
            return {"ok": False, "error": "Phone input did not keep typed value", "phone": phone, "activation_id": activation_id}
        time.sleep(0.5)
        submitted = _submit_with_enter_first(driver, inp, button_kind="next", log_prefix="PHONE_NUMBER")
        if not submitted:
            _log.warning("PHONE_CHALLENGE: không tìm thấy nút Next/Submit sau khi nhập phone")
            return {"ok": False, "error": "Phone submit button not found", "phone": phone, "activation_id": activation_id}
        time.sleep(3)
    else:
        _log.warning("PHONE_CHALLENGE: chưa thấy ô nhập phone dù đã ở trang challenge")
        return {"ok": False, "error": "Phone input not found", "phone": phone, "activation_id": activation_id}

    if activation_id:
        hero_sms_mark_ready(hero_api_key, activation_id)

    code = get_hero_sms_code(hero_api_key, activation_id, max_wait_sec=otp_wait_sec) if activation_id else ""
    if code:
        _log.info("PHONE_CHALLENGE: đã nhận OTP từ Hero-SMS, đang submit")
        _submit_phone_verify_code(driver, code)
        return {"ok": True, "phone": phone, "activation_id": activation_id}
    else:
        _log.warning("PHONE_CHALLENGE: không nhận được OTP từ Hero-SMS trong %ss", int(otp_wait_sec))
        return {
            "ok": False,
            "error": f"OTP timeout after {int(otp_wait_sec)}s",
            "phone": phone,
            "activation_id": activation_id,
        }


class No2FASecretError(Exception):
    pass


class LoginFailedError(Exception):
    pass


def _is_post_login_setup_page(driver) -> bool:
    """Trang post-login: 'Make sure you can always sign in', add recovery phone/email, speedbump.
    Đây là trang Google hiện SAU khi login thành công, yêu cầu bổ sung thông tin — KHÔNG phải fail.
    """
    try:
        url = (driver.current_url or "").lower()
    except Exception:
        return False
    if "google.com" not in url:
        return False
    if any(h in url for h in ("speedbump", "informationandrequirements", "addrecovery",
                               "signinoptions", "recovery/phone", "recovery/email")):
        return True
    from selenium.webdriver.common.by import By
    try:
        # Structural: trang post-login setup thường có nút "Skip" hoặc link "Not now"
        # và không có input email/password thông thường
        pw_inps = driver.find_elements(By.CSS_SELECTOR, "input[type='password']")
        if any(e.is_displayed() for e in pw_inps):
            return False
        id_inps = driver.find_elements(By.CSS_SELECTOR, "#identifierId, input[name='identifier']")
        if any(e.is_displayed() for e in id_inps):
            return False
        # URL này đã được kiểm tra ở trên; tuy nhiên cũng check data-scrname + page context
        try:
            has_setup_structure = driver.execute_script("""
                // Skip/Not now button = post-login setup screen
                var btns = document.querySelectorAll('button,[role="button"],a');
                for (var b of btns) {
                    var href = (b.href || b.getAttribute('href') || '').toLowerCase();
                    var js = (b.getAttribute('jsname') || '').toLowerCase();
                    if (href.includes('skip') || href.includes('notnow') || href.includes('not-now')) return true;
                    if (js === 'e3opfe' || js === 'nupqlb') return true;  // Google's skip button jsnames
                }
                // accounts.google.com URL without challenge/signin path = setup
                var url = location.href.toLowerCase();
                if (url.includes('accounts.google.com') &&
                    !url.includes('/v3/signin') &&
                    !url.includes('/challenge/') &&
                    !url.includes('/identifier') &&
                    !url.includes('/pwd')) {
                    var inps = document.querySelectorAll('input:not([type="hidden"])');
                    var visInps = 0;
                    for (var i of inps) { if (i.offsetParent) visInps++; }
                    if (visInps === 0) return true;  // no visible inputs = likely setup/info page
                }
                return false;
            """)
            if has_setup_structure:
                return True
        except Exception:
            pass
    except Exception:
        pass
    return False


def is_login_successful(driver) -> bool:
    from selenium.webdriver.common.by import By
    url = driver.current_url or ""
    success_indicators = [
        "myaccount.google.com", "mail.google.com", "drive.google.com",
        "accounts.google.com/b/", "accounts.google.com/SignOutOptions",
    ]
    fail_indicators = [
        "accounts.google.com/v3/signin",
    ]

    if _is_post_login_setup_page(driver):
        return True

    good = any(s in url for s in success_indicators)
    bad = any(s in url for s in fail_indicators)
    if good and not bad:
        return True

    try:
        stuck_hints = driver.find_elements(By.XPATH,
            "//input[@name='Passwd'] | //input[@id='identifierId']")
        if stuck_hints:
            return False
    except Exception:
        pass

    return good


def ensure_logged_in(driver, acc: Account, status_cb=None, worker_id: str = "main", stop_check=None):
    """
    Kiểm tra đang ở trang đã login; nếu bị log out thì tự login lại và trả về driver mới.
    Trả về driver (cùng hoặc mới) nếu đã login, None nếu login lại thất bại.
    """
    if driver and is_login_successful(driver):
        return driver
    _log = logging.getLogger(__name__)
    if driver:
        try:
            driver.quit()
        except Exception:
            pass
        driver = None
    if status_cb:
        status_cb("relogin (phát hiện đã log out)")
    result = run_login(acc, status_cb=status_cb, worker_id=worker_id, stop_check=stop_check)
    if result.get("status") == "login ok":
        return result.get("driver")
    _log.warning("ensure_logged_in: login lại thất bại status=%s", result.get("status"))
    return None


def _is_2fa_challenge_page(driver) -> bool:
    """Nhận diện trang nhập mã 2FA/TOTP bằng classifier + fallback.
    KHÔNG nhầm lẫn với trang myaccount settings (change pass, recovery, etc.).
    """
    from selenium.webdriver.common.by import By
    url = (driver.current_url or "").lower()

    _NOT_2FA_URLS = (
        "myaccount.google.com/signinoptions/password",
        "myaccount.google.com/signinoptions/rescuephone",
        "myaccount.google.com/signinoptions/rescueemail",
        "myaccount.google.com/recovery",
        "myaccount.google.com/apppasswords",
        "myaccount.google.com/security",
        "myaccount.google.com/personal-info",
        "myaccount.google.com/signinoptions/two-step-verification",
    )
    if any(u in url for u in _NOT_2FA_URLS):
        return False

    try:
        kind, _, _ = _classify_verify_challenge_kind(driver)
        if kind == "twofa":
            return True
        if kind == "phone":
            return False
    except Exception:
        pass

    if "accounts.google.com" not in url and "google.com" not in url:
        return False
    if "myaccount.google.com" in url:
        return False

    _2FA_URL_HINTS = ["challenge/totp", "challenge/az", "challenge/sk",
                      "signin/challenge", "signinoptions/two-step-verification/enroll"]
    if any(h in url for h in _2FA_URL_HINTS):
        return True
    try:
        # Structural check: 6/8-digit numeric input = 2FA (không cần đọc text)
        inps = driver.find_elements(By.CSS_SELECTOR,
            "input[autocomplete='one-time-code'], "
            "input[maxlength='6']:not([type='hidden']), "
            "input[maxlength='8'][inputmode='numeric']:not([type='hidden']), "
            "input[type='tel'][maxlength='6']")
        visible = [e for e in inps if e.is_displayed() and e.is_enabled()]
        if visible:
            pw_inps = driver.find_elements(By.CSS_SELECTOR, "input[type='password']")
            visible_pw = [e for e in pw_inps if e.is_displayed()]
            if visible_pw:
                return False
            return True
        # Page-level signals: TOTP countdown/timer, form action chứa 'totp'/'az'
        try:
            js_check = driver.execute_script("""
                if (document.querySelector('[role="timer"],[data-remaining-time]')) return true;
                var forms = document.querySelectorAll('form');
                for (var f of forms) {
                    var a = (f.action || '').toLowerCase();
                    if (a.includes('totp') || a.includes('/az/') || a.includes('/sk/')) return true;
                }
                var inpAll = document.querySelectorAll('input:not([type="hidden"])');
                for (var inp of inpAll) {
                    var ml = inp.maxLength;
                    var im = (inp.inputMode || '').toLowerCase();
                    if (ml === 6 && ['tel','text','number',''].includes(inp.type.toLowerCase())) return true;
                    if (ml === 8 && im === 'numeric') return true;
                }
                return false;
            """)
            if js_check:
                pw_inps = driver.find_elements(By.CSS_SELECTOR, "input[type='password']")
                if not any(e.is_displayed() for e in pw_inps):
                    return True
        except Exception:
            pass
    except Exception:
        pass
    return False


def _is_verify_recovery_page(driver) -> bool:
    """Trang xác minh / thiết lập email khôi phục — hoạt động với mọi ngôn ngữ.
    Dùng URL và cấu trúc DOM thay vì đọc text.
    """
    from selenium.webdriver.common.by import By
    try:
        url = (driver.current_url or "").lower()
        # URL signals — chắc nhất
        if any(h in url for h in ("recovery/email", "rescueemail", "recovery-email",
                                   "signinoptions/rescueemail")):
            return True
        # Nếu đang ở trang challenges/recovery của accounts.google.com
        if "accounts.google.com" not in url:
            return False

        # Tel/phone inputs có nghĩa đây là phone challenge, không phải recovery email
        tel_inputs = driver.find_elements(By.CSS_SELECTOR,
            "input[type='tel'], input[autocomplete*='tel'], input[autocomplete*='phone']")
        if any(el.is_displayed() for el in tel_inputs):
            return False

        # Email input → recovery email step
        email_inps = driver.find_elements(By.CSS_SELECTOR,
            "input[type='email'], input[autocomplete='email'], input[autocomplete*='recovery-email']")
        if any(el.is_displayed() for el in email_inps):
            return True

        # Form action chứa 'rescue' hoặc 'recovery-email'
        try:
            fa = driver.execute_script("""
                var r = '';
                for (var f of document.forms) r += ' ' + (f.action || '').toLowerCase();
                return r;
            """) or ""
            if any(h in fa for h in ("rescue", "recovery", "recoveryemail")):
                return True
        except Exception:
            pass

        # Fallback: keyword list đa ngôn ngữ (không đọc toàn bộ body, chỉ innerText ngắn)
        try:
            snippet = driver.execute_script(
                "return (document.body.innerText || '').substring(0, 800).toLowerCase();"
            ) or ""
            if any(k in snippet for k in _RECOVERY_PAGE_KEYWORDS):
                return True
        except Exception:
            pass
        return False
    except Exception:
        return False


def _select_recovery_email_option_if_present(driver, expected_recovery_email: str = "") -> bool:
    """Chọn phương thức xác minh bằng recovery email trên trang choice.
    Scan tất cả clickable elements, match text chứa recovery/email keywords, click vào đó.
    """
    from selenium.webdriver.common.by import By
    from human_click import human_move_and_click
    _log = logging.getLogger(__name__)

    _RECOVERY_KW = ("recovery email", "confirm your recovery", "confirm recovery",
                     "email khôi phục", "xác nhận email")
    _VERIF_CODE_KW = ("verification code", "get a verification", "mã xác minh",
                       "get a code", "send code")
    _TRY_ANOTHER_KW = ("try another way", "another way", "cách khác")

    def _collect_choice_options():
        opts = []
        seen = set()
        selectors = [
            "[role='radio']", "[data-challengetype]", "[data-challengeid]",
            "[data-sendmethod]",
            "li[role='link']", "li[role='button']",
            "div[role='link']", "div[role='button']",
            "li", "button",
        ]
        raw = []
        for sel in selectors:
            try:
                raw.extend(driver.find_elements(By.CSS_SELECTOR, sel))
            except Exception:
                continue
        for el in raw:
            try:
                if not el.is_displayed():
                    continue
                txt = (el.text or "").strip()
                if len(txt) < 3:
                    continue
                txt_l = txt.lower()
                if any(kw in txt_l for kw in ("learn more", "help", "terms", "privacy")):
                    continue
                rect = el.rect or {}
                y = int(rect.get("y", 0))
                x = int(rect.get("x", 0))
                key = (txt[:40].lower(), y // 8, x // 20)
                if key in seen:
                    continue
                seen.add(key)
                opts.append({"y": y, "x": x, "el": el, "text": txt_l})
            except Exception:
                continue
        opts.sort(key=lambda t: (t["y"], t["x"]))
        return opts

    def _is_recovery_confirm_step() -> bool:
        try:
            url = (driver.current_url or "").lower()
        except Exception:
            url = ""
        if "accounts.google.com" not in url:
            return False
        try:
            for sel in ("input[type='email']", "input[autocomplete='email']", "input[name*='email']"):
                for el in driver.find_elements(By.CSS_SELECTOR, sel):
                    if el.is_displayed() and el.is_enabled():
                        return True
        except Exception:
            pass
        em = (expected_recovery_email or "").strip().lower()
        if em and "@" in em:
            dom = em.split("@", 1)[1]
            try:
                body = (driver.find_element(By.TAG_NAME, "body").text or "").lower()
                if dom and dom in body:
                    return True
            except Exception:
                pass
        return False

    try:
        current_url = (driver.current_url or "").lower()
        if "accounts.google.com" not in current_url:
            return False
    except Exception:
        return False

    choices = _collect_choice_options()
    if len(choices) < 2:
        _log.debug("_select_recovery: chỉ tìm được %d options — không phải trang choice", len(choices))
        return False

    _log.info("_select_recovery: tìm thấy %d options trên trang choice", len(choices))
    for opt in choices:
        _log.debug("  option: y=%d text=%s", opt["y"], opt["text"][:80])

    recovery_idx = None
    for i, opt in enumerate(choices):
        if any(kw in opt["text"] for kw in _RECOVERY_KW):
            recovery_idx = i
            break

    if recovery_idx is None:
        for i, opt in enumerate(choices):
            if "email" in opt["text"] and not any(kw in opt["text"] for kw in _VERIF_CODE_KW):
                recovery_idx = i
                break

    order = []
    if recovery_idx is not None:
        order.append(recovery_idx)
    order.extend(i for i in range(len(choices)) if i not in order and
                 not any(kw in choices[i]["text"] for kw in _TRY_ANOTHER_KW))

    choice_page_url = driver.current_url
    for idx in order:
        try:
            current_choices = _collect_choice_options()
            if idx >= len(current_choices):
                continue
            target_opt = current_choices[idx]
            _log.info("_select_recovery: đang thử option #%d: %s", idx, target_opt["text"][:60])
            human_move_and_click(driver, target_opt["el"])
            time.sleep(0.8)
            btn = _find_next_button(driver) or _find_submit_or_primary_button(driver)
            if btn:
                try:
                    human_move_and_click(driver, btn)
                except Exception:
                    pass
            time.sleep(2.0)
            if _is_recovery_confirm_step():
                _log.info("_select_recovery: đã vào bước confirm recovery email")
                return True
            try:
                driver.get(choice_page_url)
                time.sleep(1.5)
            except Exception:
                try:
                    driver.back()
                    time.sleep(1.5)
                except Exception:
                    pass
        except Exception as e:
            _log.debug("_select_recovery: lỗi thử option #%d: %s", idx, e)
    return False


def _is_recovery_choice_page(driver) -> bool:
    """Trang chọn phương thức verify (có >=2 lựa chọn challenge), độc lập ngôn ngữ.
    Detect bằng DOM: tìm các clickable items (radio, challengetype, li, div[role=button/link]).
    Cũng check: không có visible input nào (nếu có input thì đây là trang hành động, không phải trang chọn).
    """
    from selenium.webdriver.common.by import By
    try:
        url = (driver.current_url or "").lower()
        if "accounts.google.com" not in url:
            return False

        has_visible_input = False
        try:
            for el in driver.find_elements(By.CSS_SELECTOR,
                    "input[type='text'], input[type='email'], input[type='tel'], input[type='password']"):
                if el.is_displayed() and el.is_enabled():
                    has_visible_input = True
                    break
        except Exception:
            pass
        if has_visible_input:
            return False

        choices = 0
        for sel in (
            "[role='radio']", "[data-challengetype]",
            "[data-challengeid]", "[data-sendmethod]",
            "li[role='link']", "li[role='button']",
            "div[role='link'][data-challengetype]",
        ):
            for el in driver.find_elements(By.CSS_SELECTOR, sel):
                try:
                    if el.is_displayed() and el.is_enabled():
                        choices += 1
                except Exception:
                    pass

        if choices >= 2:
            return True

        # Fallback structural: tìm các item có cùng cấu trúc (list-like challenge options)
        try:
            structural_count = driver.execute_script("""
                var count = 0;
                // Các pattern Google dùng cho challenge choice list
                var candidates = document.querySelectorAll(
                    'li[jsname], li[jscontroller], ' +
                    'div[jsname][role="button"], div[jsname][role="link"], ' +
                    '[data-challengetype], [data-challengeid], ' +
                    'div[role="listitem"], [role="menuitem"]'
                );
                for (var el of candidates) {
                    if (el.offsetParent !== null) count++;
                }
                return count;
            """) or 0
            if structural_count >= 2:
                return True
        except Exception:
            pass
        return False

    except Exception:
        return False


def _wait_page_ready(driver, timeout: float = 8.0, poll: float = 0.6) -> bool:
    """Chờ trang load xong dùng 1 JS call duy nhất mỗi vòng (không gọi is_displayed per-element).
    Giảm WebDriver HTTP calls để tránh "Connection pool full" khi proxy chậm.
    """
    _JS = (
        "var r=document.readyState;"
        "if(r!=='complete'&&r!=='interactive')return r;"
        "var els=document.querySelectorAll('input,button,div[role=\"button\"],[data-challengetype],form');"
        "var v=0;for(var i=0;i<els.length;i++){"
        "var s=window.getComputedStyle(els[i]);"
        "if(s.display!=='none'&&s.visibility!=='hidden'&&s.opacity!=='0')v++;"
        "}"
        "return v>0?'ready':'empty';"
    )
    deadline = time.time() + timeout
    ready_count = 0
    broken_checks = 0

    while time.time() < deadline:
        try:
            result = driver.execute_script(_JS) or ""
            if result == "ready":
                ready_count += 1
                if ready_count >= 2:
                    return True
            elif result == "empty":
                broken_checks += 1
                if broken_checks >= 4:
                    return False
            elif result in ("loading", "interactive"):
                pass  # trang đang load, tiếp tục chờ
            else:
                # readyState lạ hoặc lỗi
                pass
        except Exception:
            pass
        time.sleep(poll)

    return False


def _wait_page_transition(driver, old_url: str = "", timeout: float = 12.0, poll: float = 0.7) -> str:
    """Chờ trang chuyển đổi dùng JS duy nhất mỗi vòng (giảm WebDriver calls).
    Trả về URL mới. Dùng sau mỗi lần submit.
    """
    _JS_STATE = (
        "return [document.location.href, document.querySelectorAll('input').length,"
        " document.readyState];"
    )
    _log = logging.getLogger(__name__)
    deadline = time.time() + timeout

    old_url = (old_url or "").lower()
    old_inputs = -1
    try:
        res = driver.execute_script(_JS_STATE) or []
        if not old_url:
            old_url = str(res[0] or "").lower()
        old_inputs = int(res[1] or 0)
    except Exception:
        try:
            old_url = old_url or (driver.current_url or "").lower()
        except Exception:
            pass

    while time.time() < deadline:
        try:
            res = driver.execute_script(_JS_STATE) or []
            cur_url = str(res[0] or "").lower()
            cur_inputs = int(res[1] or 0)
            cur_state = str(res[2] or "")
        except Exception:
            time.sleep(poll)
            continue

        if _is_page_broken(driver):
            _log.debug("PAGE_TRANSITION: trang broken giữa transition")
            return cur_url or old_url

        if cur_url and cur_url != old_url:
            _log.debug("PAGE_TRANSITION: URL thay đổi → %s", cur_url[:100])
            _wait_page_ready(driver, timeout=min(8.0, deadline - time.time()))
            return cur_url

        if old_inputs >= 0 and cur_inputs != old_inputs:
            _log.debug("PAGE_TRANSITION: input count %d → %d", old_inputs, cur_inputs)
            _wait_page_ready(driver, timeout=min(5.0, deadline - time.time()))
            return cur_url

        time.sleep(poll)

    try:
        return (driver.current_url or "").lower()
    except Exception:
        return ""


def _safe_navigate(driver, url: str, max_retries: int = 3, wait_timeout: float = 20.0) -> bool:
    """Navigate đến URL với auto-retry khi page broken (proxy lag/disconnect).
    Trả True nếu navigate thành công, False nếu vẫn broken sau max_retries.
    """
    _log = logging.getLogger(__name__)
    for attempt in range(max_retries):
        try:
            driver.get(url)
        except Exception as e:
            _log.warning("_safe_navigate: lỗi get(%s) lần %d: %s", url[:80], attempt + 1, e)
            time.sleep(3)
            continue
        _wait_page_ready(driver, timeout=wait_timeout, poll=0.5)
        if not _is_page_broken(driver):
            return True
        _log.warning("_safe_navigate: page broken sau load lần %d — chờ 3s rồi retry", attempt + 1)
        time.sleep(3)
    _log.warning("_safe_navigate: vẫn broken sau %d lần retry URL=%s", max_retries, url[:80])
    return False


def _is_page_broken(driver) -> bool:
    """Detect trang bị hỏng do proxy lag/disconnect: blank, chrome-error, ERR_, timeout, empty body.
    Trả True nếu cần reload.
    """
    try:
        url = (driver.current_url or "").lower().strip()
    except Exception:
        return True

    if not url or url in ("about:blank", "data:,", "about:blank#blocked"):
        return True
    if "chrome-error://" in url or "chrome://error" in url:
        return True

    _BROKEN_TITLES = ("is not available", "can't be reached", "took too long",
                      "connection was reset", "err_", "dns_probe", "not found",
                      "timed out", "no internet", "proxy error")
    try:
        title = (driver.title or "").lower()
        if any(k in title for k in _BROKEN_TITLES):
            return True
    except Exception:
        return True

    try:
        state = driver.execute_script("return document.readyState") or ""
        if state not in ("complete", "interactive", "loading"):
            return True
    except Exception:
        return True

    try:
        body_text = driver.execute_script(
            "return document.body ? document.body.innerText.trim().substring(0, 300) : ''") or ""
        if len(body_text) < 5 and "google.com" in url:
            return True
        _ERR_KEYWORDS = ("err_connection", "err_timed_out", "err_proxy", "err_tunnel",
                         "err_name_not_resolved", "err_internet_disconnected",
                         "this site can't be reached", "took too long to respond",
                         "the connection was reset", "no internet",
                         "proxy authentication required", "502 bad gateway",
                         "503 service", "504 gateway")
        body_low = body_text.lower()
        if any(k in body_low for k in _ERR_KEYWORDS):
            return True
    except Exception:
        pass

    return False


def _read_page_body_text(driver, max_len: int = 600) -> str:
    """Read visible body text."""
    try:
        return (driver.execute_script(
            "return document.body ? document.body.innerText.trim().substring(0, arguments[0]) : ''",
            max_len,
        ) or "").lower()
    except Exception:
        return ""


def _detect_login_page_kind_once(driver) -> str:
    """Phân loại trang login/challenge bằng nhiều tầng tín hiệu, ưu tiên:
      1. URL path (load trước DOM, luôn chính xác)
      2. Input name/id (DOM attribute cứng, có ngay khi render)
      3. data-* attributes & form action (Google inject challenge metadata)
      4. Body text keywords (fallback cuối — chỉ dùng khi các tầng trên chưa rõ)
    """
    from selenium.webdriver.common.by import By

    if is_login_successful(driver):
        return "success"
    if _is_signin_hard_block_page(driver):
        return "hard_block"
    if _is_couldnt_sign_in_page(driver):
        return "couldnt_sign_in"
    if _is_recaptcha_page(driver):
        return "recaptcha"
    if _is_image_captcha_page(driver):
        return "image_captcha"
    if _is_recovery_choice_page(driver):
        return "recovery_choice"

    try:
        url = (driver.current_url or "").lower()
    except Exception:
        url = ""

    # ── LAYER 1: URL path — tín hiệu mạnh nhất, có trước cả DOM ──
    _TWOFA_URL = ("/challenge/totp", "/challenge/az", "/challenge/sk")
    _PHONE_URL = ("/challenge/ipp", "/challenge/iap", "/iap/verify")
    if any(h in url for h in _TWOFA_URL):
        return "twofa_challenge"
    if any(h in url for h in _PHONE_URL):
        return "phone_challenge"

    # ── LAYER 2: DOM inputs — name/id/autocomplete cứng ──
    visible_inputs = []
    try:
        for el in driver.find_elements(By.CSS_SELECTOR, "input"):
            try:
                if el.is_displayed() and el.is_enabled():
                    visible_inputs.append({
                        "el": el,
                        "type": (el.get_attribute("type") or "").lower(),
                        "autocomplete": (el.get_attribute("autocomplete") or "").lower(),
                        "maxlength": (el.get_attribute("maxlength") or "").strip(),
                        "name": (el.get_attribute("name") or "").lower(),
                        "id": (el.get_attribute("id") or "").lower(),
                        "inputmode": (el.get_attribute("inputmode") or "").lower(),
                        "aria": (el.get_attribute("aria-label") or "").lower(),
                    })
            except Exception:
                continue
    except Exception:
        pass

    has_identifier = False
    has_password = False
    has_email_only = False
    has_definite_2fa = False
    has_definite_phone = False
    ambiguous_inputs = []

    for inp in visible_inputs:
        t, ac, ml, nm, iid, im, aria = (
            inp["type"], inp["autocomplete"], inp["maxlength"],
            inp["name"], inp["id"], inp["inputmode"], inp.get("aria", ""),
        )
        nm_id = nm + " " + iid

        if iid == "identifierid" or nm == "identifier":
            has_identifier = True
            continue
        if t == "password" or nm == "passwd":
            has_password = True
            continue
        if t == "email" or "email" in ac or "email" in nm:
            has_email_only = True
            continue
        if t == "hidden":
            continue

        # name/id/autocomplete xác định dứt khoát
        if "totppin" in nm_id or "totp" in nm_id or "otp" in nm_id:
            has_definite_2fa = True
            continue
        if ac == "one-time-code":
            has_definite_2fa = True
            continue
        if "phonenumber" in nm_id.replace("_", "").replace("-", ""):
            has_definite_phone = True
            continue
        if "phone" in nm or "phone" in iid:
            has_definite_phone = True
            continue

        # ── Structural: maxlength/inputmode — ngôn ngữ nào cũng dùng được ──
        # Google TOTP luôn 6 chữ số; backup code 8 chữ số
        if ml in ("6", "8") and t in ("tel", "text", "number", ""):
            has_definite_2fa = True
            continue
        if im == "numeric" and ml and ml.isdigit() and int(ml) <= 8:
            has_definite_2fa = True
            continue
        # type=tel với maxlength rất dài (hoặc không có) → nhập số điện thoại thật
        if t == "tel" and (not ml or (ml.isdigit() and int(ml) > 10)):
            has_definite_phone = True
            continue

        if t in ("tel", "text", "number", ""):
            ambiguous_inputs.append(inp)

    if has_identifier:
        return "email_entry"
    if has_password and not has_identifier:
        return "password_entry"
    if has_definite_2fa:
        return "twofa_challenge"
    if has_definite_phone and not has_definite_2fa:
        return "phone_challenge"

    # ── LAYER 3: data-* attributes & form action ──
    try:
        challenge_attrs = driver.execute_script("""
            var r = {formAction:'', challengeId:'', challengeType:''};
            var forms = document.querySelectorAll('form');
            for (var f of forms) { if (f.action) r.formAction += ' ' + f.action.toLowerCase(); }
            var els = document.querySelectorAll('[data-challengeid],[data-challengetype],[data-challenge-id],[data-challenge-type]');
            for (var e of els) {
                r.challengeId += ' ' + (e.getAttribute('data-challengeid') || e.getAttribute('data-challenge-id') || '').toLowerCase();
                r.challengeType += ' ' + (e.getAttribute('data-challengetype') || e.getAttribute('data-challenge-type') || '').toLowerCase();
            }
            return r;
        """) or {}
    except Exception:
        challenge_attrs = {}

    fa = challenge_attrs.get("formAction", "")
    cid = challenge_attrs.get("challengeId", "")
    ctype = challenge_attrs.get("challengeType", "")
    dom_meta = fa + " " + cid + " " + ctype

    if any(h in dom_meta for h in ("totp", "authenticator", "otp")):
        return "twofa_challenge"
    if any(h in dom_meta for h in ("phone", "sms", "ipp")):
        return "phone_challenge"

    # ── LAYER 4: ambiguous inputs — phân loại bằng autocomplete/aria ──
    if ambiguous_inputs:
        for inp in ambiguous_inputs:
            ac, aria = inp["autocomplete"], inp.get("aria", "")
            if "tel" in ac or "phone" in ac or "phone" in aria:
                return "phone_challenge"
            if "one-time" in ac or "code" in aria or "pin" in aria:
                return "twofa_challenge"

    # ── LAYER 5: Phân tích cấu trúc DOM — không phụ thuộc ngôn ngữ ──
    if ambiguous_inputs:
        for inp in ambiguous_inputs:
            ml_v = inp["maxlength"]
            # 6/8 ký số → TOTP
            if ml_v in ("6", "8"):
                return "twofa_challenge"
            # numeric ngắn → TOTP
            if inp["inputmode"] == "numeric" and ml_v and ml_v.isdigit() and int(ml_v) <= 8:
                return "twofa_challenge"
            # tel không có maxlength ngắn → số điện thoại
            if inp["type"] == "tel" and (not ml_v or (ml_v.isdigit() and int(ml_v) > 10)):
                return "phone_challenge"
            # aria-label chứa hint (Google dùng English nội bộ cho aria)
            aria = inp.get("aria", "")
            if any(h in aria for h in ("phone", "mobile", "number", "telephone")):
                return "phone_challenge"
            if any(h in aria for h in ("code", "pin", "otp", "passcode")):
                return "twofa_challenge"
        # Phân tích page-level: country code selector = phone; timer/countdown = TOTP
        try:
            js_hint = driver.execute_script("""
                if (document.querySelector('[data-country-code],[data-country],[aria-label*="country" i],select[name*="country" i]')) return 'phone';
                if (document.querySelector('[role="timer"],[data-remaining-time],.countdown')) return 'totp';
                var forms = document.querySelectorAll('form');
                for (var f of forms) {
                    var a = (f.action || '').toLowerCase();
                    if (a.includes('ipp') || a.includes('/phone') || a.includes('/sms')) return 'phone';
                    if (a.includes('totp') || a.includes('/az/') || a.includes('/otp')) return 'totp';
                }
                return '';
            """) or ""
            if js_hint == "phone":
                return "phone_challenge"
            if js_hint == "totp":
                return "twofa_challenge"
        except Exception:
            pass
        # Tiebreaker cuối: maxlength="6" → TOTP, không có/dài → phone
        first = ambiguous_inputs[0]
        return "twofa_challenge" if first.get("maxlength") == "6" else "phone_challenge"

    if has_email_only and not has_password and not has_identifier:
        return "recovery_confirm"

    return "unknown"


def _detect_login_page_kind(driver) -> str:
    """DOM-first reactive router:
    Layer 1-5: URL / input attrs / data-* / autocomplete / body text  (miễn phí, nhanh)
    Layer 6:   AI Claude vision fallback                              (khi vẫn unknown)
    """
    _log = logging.getLogger(__name__)

    kind = _detect_login_page_kind_once(driver)
    if kind != "unknown":
        return kind

    _wait_page_ready(driver, timeout=6.0, poll=0.5)

    kind = _detect_login_page_kind_once(driver)
    if kind != "unknown":
        return kind

    time.sleep(1.5)
    kind = _detect_login_page_kind_once(driver)
    if kind != "unknown":
        return kind

    # ── Layer 6: AI vision fallback ──
    try:
        from ai_analyzer import get_page_kind_from_ai
        ai_kind = get_page_kind_from_ai(driver, task_context="Google login flow")
        if ai_kind != "unknown":
            _log.info("PAGE_DETECT: AI fallback → %s", ai_kind)
            return ai_kind
    except Exception as _ai_err:
        _log.debug("PAGE_DETECT: AI fallback lỗi: %s", _ai_err)

    return "unknown"


def _is_email_entry_page(driver) -> bool:
    """Trang bước nhập email/phone — nhận diện bằng DOM, không phụ thuộc ngôn ngữ."""
    from selenium.webdriver.common.by import By
    try:
        url = (driver.current_url or "").lower()
        if "accounts.google.com" not in url:
            return False
        # identifier input (id="identifierId" hoặc name="identifier") = email entry
        for sel in ("#identifierId", "input[name='identifier']", "input[autocomplete='username']"):
            try:
                inp = driver.find_element(By.CSS_SELECTOR, sel)
                if inp.is_displayed():
                    # Đảm bảo không phải trang password
                    pw = driver.find_elements(By.CSS_SELECTOR, "input[type='password']")
                    if not any(p.is_displayed() for p in pw):
                        return True
            except Exception:
                pass
    except Exception:
        pass
    return False


def _is_recaptcha_page(driver) -> bool:
    from selenium.webdriver.common.by import By
    try:
        cur = (driver.current_url or "").lower()
        # Dấu hiệu chắc chắn nhất: URL challenge/recaptcha
        if "/challenge/recaptcha" in cur:
            return True
        # Dấu hiệu DOM thực: iframe/widget recaptcha hoặc textarea token
        iframe_cnt = len(driver.find_elements(By.CSS_SELECTOR, "iframe[src*='recaptcha']"))
        token_cnt = len(driver.find_elements(By.CSS_SELECTOR, "textarea[name='g-recaptcha-response'], textarea[id*='g-recaptcha-response']"))
        widget_cnt = len(driver.find_elements(By.CSS_SELECTOR, ".g-recaptcha, [data-sitekey]"))
        if iframe_cnt > 0 or token_cnt > 0 or widget_cnt > 0:
            return True
    except Exception:
        pass
    return False


def _is_couldnt_sign_in_page(driver) -> bool:
    """Trang Google báo không thể đăng nhập / bị chặn đăng nhập.
    Ưu tiên URL + DOM structure, fallback sang keyword list đa ngôn ngữ."""
    from selenium.webdriver.common.by import By
    try:
        if _is_signin_hard_block_page(driver):
            return True
        url = (driver.current_url or "").lower()
        # URL signals: Google hay dùng các path này khi block
        if any(h in url for h in ("/couldnotsignin", "/blocked", "/interstitial", "/disabled")):
            return True
        # Structural: có error-container nhưng không có input thao tác
        try:
            has_error_no_input = driver.execute_script("""
                var errorEls = document.querySelectorAll(
                    '[data-error],[aria-live="assertive"],[role="alert"],.error-msg'
                );
                var hasError = false;
                for (var e of errorEls) { if (e.offsetParent && (e.textContent||'').trim().length > 5) { hasError = true; break; } }
                if (!hasError) return false;
                var inps = document.querySelectorAll('input:not([type="hidden"])');
                var visibleInps = 0;
                for (var i of inps) { if (i.offsetParent) visibleInps++; }
                return visibleInps === 0;
            """)
            if has_error_no_input:
                return True
        except Exception:
            pass
        # Fallback: keyword list đa ngôn ngữ — đọc snippet ngắn để tiết kiệm bandwidth
        try:
            snippet = driver.execute_script(
                "return (document.body.innerText || '').substring(0, 1000).toLowerCase().replace(/\u2019/g, \"'\");"
            ) or ""
            return any(h in snippet for h in _COULDNT_SIGN_IN)
        except Exception:
            pass
    except Exception:
        pass
    return False


def _is_signin_hard_block_page(driver) -> bool:
    """Nhận diện trang block đăng nhập theo URL/DOM (không phụ thuộc text dịch)."""
    from selenium.webdriver.common.by import By
    try:
        url = (driver.current_url or "").lower()
        if "accounts.google.com" not in url or "/v3/signin/challenge" not in url:
            return False
        # Không phải recaptcha/input challenge thông thường.
        if _is_recaptcha_page(driver) or _is_email_entry_page(driver) or _is_password_challenge_page(driver) or _is_2fa_challenge_page(driver):
            return False

        # Nếu có nhiều lựa chọn challenge (radio/challengetype), không coi là hard-block.
        choice_like = 0
        for sel in ("[role='radio']", "[data-challengetype]"):
            try:
                choice_like += len([e for e in driver.find_elements(By.CSS_SELECTOR, sel) if e.is_displayed()])
            except Exception:
                pass
        if choice_like >= 2:
            return False

        # Không có input thao tác xác minh.
        input_like = 0
        for sel in (
            "input[type='email']",
            "input[type='password']",
            "input[type='tel']",
            "input[autocomplete='one-time-code']",
            "input[maxlength='6']",
            "textarea[name='g-recaptcha-response']",
        ):
            try:
                input_like += len([e for e in driver.find_elements(By.CSS_SELECTOR, sel) if e.is_displayed()])
            except Exception:
                pass
        if input_like > 0:
            return False

        # Nếu không có action button xác minh rõ ràng, xem như hard-block.
        action_buttons = 0
        try:
            for el in driver.find_elements(By.CSS_SELECTOR, "button, [role='button']"):
                if not el.is_displayed():
                    continue
                txt = (el.text or "").strip()
                if len(txt) >= 2:
                    action_buttons += 1
        except Exception:
            pass
        return action_buttons <= 1
    except Exception:
        return False


def _get_recaptcha_sitekey(driver) -> Optional[str]:
    from selenium.webdriver.common.by import By
    try:
        iframe = driver.find_element(By.CSS_SELECTOR, "iframe[src*='recaptcha']")
        src = iframe.get_attribute("src") or ""
        m = re.search(r"k=([A-Za-z0-9_-]+)", src)
        if m:
            return m.group(1)
    except Exception:
        pass
    try:
        for el in driver.find_elements(By.CSS_SELECTOR, "[data-sitekey]"):
            html = el.get_attribute("data-sitekey")
            if html:
                return html
    except Exception:
        pass
    return None


def _solve_recaptcha_ezcaptcha(driver, api_key: str) -> bool:
    """Giải reCAPTCHA qua API ez-captcha: createTask → getTaskResult (poll) → inject token.
    Theo hướng dẫn dev: truyền đầy đủ URL, website title, và đúng type V2 vs V2 Enterprise."""
    import json
    import urllib.request
    _log = logging.getLogger(__name__)
    sitekey = _get_recaptcha_sitekey(driver)
    if not sitekey:
        _log.warning("No recaptcha sitekey found")
        return False

    # 1. Complete URL (theo dev: transmit the complete URL)
    try:
        page_url = driver.execute_script("return window.location.href || ''") or driver.current_url or ""
    except Exception:
        page_url = driver.current_url or ""
    if not page_url or page_url == "about:blank":
        page_url = driver.current_url or ""

    # 2. Website title (theo dev: transmit the website title)
    try:
        page_title = driver.execute_script("return document.title || ''") or ""
    except Exception:
        page_title = ""

    # 3. Phân biệt V2 vs V2 Enterprise (theo dev: transmit corresponding parameters)
    is_enterprise = "recaptcha/enterprise" in (page_url or "")
    sa_param = None
    try:
        from selenium.webdriver.common.by import By
        iframe = driver.find_element(By.CSS_SELECTOR, "iframe[src*='recaptcha']")
        src = (iframe.get_attribute("src") or "")
        is_enterprise = is_enterprise or "recaptcha/enterprise" in src
        if "sa=" in src:
            m = re.search(r"[?&]sa=([^&]+)", src)
            if m:
                sa_param = m.group(1)
    except Exception:
        pass
    task_type = "RecaptchaV2EnterpriseTaskProxyless" if is_enterprise else "ReCaptchaV2TaskProxyless"
    _log.info(
        "EzCaptcha: type=%s, sitekey=%s, url=%s, title=%s",
        task_type, sitekey[:20] + "..." if len(sitekey) > 20 else sitekey,
        page_url[:60] + "..." if len(page_url) > 60 else page_url,
        (page_title[:30] + "...") if len(page_title) > 30 else page_title or "(empty)",
    )

    max_slot_retries = 3
    slot_retry_delay = 12

    task = {
        "type": task_type,
        "websiteURL": page_url,
        "websiteKey": sitekey,
        "isInvisible": False,
    }
    if page_title:
        task["websiteTitle"] = page_title
    if sa_param:
        task["sa"] = sa_param

    for slot_attempt in range(max_slot_retries):
        create_body = json.dumps({
            "clientKey": api_key,
            "task": task,
        }).encode("utf-8")
        try:
            req = urllib.request.Request(
                "https://api.ez-captcha.com/createTask",
                data=create_body,
                method="POST",
                headers={"Content-Type": "application/json"},
            )
            with urllib.request.urlopen(req, timeout=30) as resp:
                create_data = json.loads(resp.read().decode("utf-8", errors="replace"))
        except Exception as e:
            _log.exception("EzCaptcha createTask error: %s", e)
            return False

        if create_data.get("errorId", 1) != 0:
            _log.warning("EzCaptcha createTask error: %s", create_data)
            return False
        task_id = create_data.get("taskId")
        if not task_id:
            return False

        token = None
        deadline = time.time() + 120
        processing_log_count = 0
        while time.time() < deadline:
            get_body = json.dumps({"clientKey": api_key, "taskId": task_id}).encode("utf-8")
            try:
                req = urllib.request.Request(
                    "https://api.ez-captcha.com/getTaskResult",
                    data=get_body,
                    method="POST",
                    headers={"Content-Type": "application/json"},
                )
                with urllib.request.urlopen(req, timeout=30) as resp:
                    result = json.loads(resp.read().decode("utf-8", errors="replace"))
            except Exception as e:
                _log.debug("getTaskResult error: %s", e)
                time.sleep(2.5)
                continue

            if result.get("errorId", 1) != 0:
                err_code = result.get("errorCode", "")
                err_desc = result.get("errorDescription", "")
                if err_code == "ERROR_NO_SLOT_AVAILABLE" and slot_attempt < max_slot_retries - 1:
                    _log.warning(
                        "EzCaptcha: ERROR_NO_SLOT_AVAILABLE — dịch vụ tạm hết slot. Sẽ thử lại (lần %s/%s).",
                        slot_attempt + 2, max_slot_retries,
                    )
                    break
                _log.warning("EzCaptcha getTaskResult error: %s — %s", err_code, err_desc)
                return False
            status = result.get("status", "")
            if status == "ready":
                sol = result.get("solution") or {}
                token = sol.get("gRecaptchaResponse") or sol.get("token") or ""
                break
            if status == "error":
                _log.warning("EzCaptcha task failed: %s", result)
                if slot_attempt < max_slot_retries - 1:
                    _log.info("Thử tạo task mới sau %ss (lần %s/%s).", slot_retry_delay, slot_attempt + 2, max_slot_retries)
                    time.sleep(slot_retry_delay)
                    break
                return False
            # status == "processing": log mỗi ~15s để biết đang chờ, tránh "pending" im lặng
            processing_log_count += 1
            if processing_log_count % 6 == 1 and processing_log_count > 1:
                _log.info("EzCaptcha: đang chờ giải (processing)... còn ~%ds", max(0, int(deadline - time.time())))
            time.sleep(2.5)

        if token:
            break
        if not token and slot_attempt < max_slot_retries - 1:
            _log.info("EzCaptcha: chờ %ss rồi tạo task mới...", slot_retry_delay)
            time.sleep(slot_retry_delay)

    if not token:
        _log.warning("EzCaptcha no token after poll (đã thử %s lần)", max_slot_retries)
        return False

    # Inject token: chỉ dùng .value (tránh Trusted Types). Gọi callback + dispatch event để form nhận token.
    inject_script = """
    var token = arguments[0];
    var els = Array.from(document.querySelectorAll('textarea[name="g-recaptcha-response"], textarea[id*="g-recaptcha-response"], [id*="g-recaptcha-response"]'));
    if (!els.length) return "no_el";
    for (var i = 0; i < els.length; i++) {
      var el = els[i];
      try { el.value = token; } catch(e) {}
      try { el.setAttribute('value', token); } catch(e) {}
      try { el.dispatchEvent(new Event('input', { bubbles: true })); } catch(e) {}
      try { el.dispatchEvent(new Event('change', { bubbles: true })); } catch(e) {}
    }
    var called = false;
    var visited = new WeakSet();
    function walk(obj, depth) {
      if (!obj || typeof obj !== 'object' || depth > 6) return;
      if (visited.has(obj)) return;
      visited.add(obj);
      for (var k in obj) {
        var v = null;
        try { v = obj[k]; } catch(e) { continue; }
        if (!v) continue;
        if (k === 'callback' && typeof v === 'function') {
          try { v(token); called = true; } catch(e) {}
        } else if (typeof v === 'object') {
          walk(v, depth + 1);
        }
      }
    }
    try {
      if (typeof ___grecaptcha_cfg !== 'undefined') walk(___grecaptcha_cfg, 0);
    } catch(e) {}
    return called ? "ok_cb" : "ok_no_cb";
    """
    try:
        result = driver.execute_script(inject_script, token)
        if result != "ok" and result != "no_el":
            _log.debug("Inject token result: %s", result)
        if result == "no_el":
            _log.warning("Không tìm thấy textarea g-recaptcha-response trên trang")
    except Exception as e:
        _log.warning("Inject token script error: %s", e)

    # Sau inject token: ưu tiên chờ callback/auto-submit, tránh click bừa.
    for _ in range(3):
        time.sleep(1.0)
        if not _is_recaptcha_page(driver):
            _log.info("EzCaptcha: captcha da qua (auto sau inject token)")
            return True

    # Nếu vẫn còn recaptcha thì chỉ click 1 lần nút submit an toàn (không quét mọi button).
    from human_click import human_move_and_click
    try:
        from selenium.webdriver.common.by import By
        safe_btn = None
        # Ưu tiên id chuẩn của Google trước.
        for id_ in ["identifierNext", "next", "passwordNext"]:
            try:
                b = driver.find_element(By.ID, id_)
                if b.is_displayed() and b.is_enabled():
                    safe_btn = b
                    break
            except Exception:
                pass
        # Fallback hạn chế: chỉ button[type='submit'] hiển thị.
        if not safe_btn:
            subs = [b for b in driver.find_elements(By.CSS_SELECTOR, "button[type='submit']") if b.is_displayed() and b.is_enabled()]
            if subs:
                safe_btn = subs[-1]
        if safe_btn:
            human_move_and_click(driver, safe_btn)
            _log.info("EzCaptcha: click 1 lan nut submit an toan sau inject")
    except Exception as e:
        _log.warning("EzCaptcha: safe-click submit loi: %s", e)

    # Chỉ coi là success khi thực sự thoát trang recaptcha/challenge.
    for _ in range(5):
        time.sleep(1.5)
        if not _is_recaptcha_page(driver):
            _log.info("EzCaptcha: captcha da qua (khong con trang recaptcha)")
            return True

    _log.warning("EzCaptcha: da inject token nhung van o trang recaptcha/challenge")
    return False


# ---------------------------------------------------------------------------
#  Image captcha (captcha chữ) — giải qua 2captcha.com
# ---------------------------------------------------------------------------

def _is_image_captcha_page(driver) -> bool:
    """Detect trang Google hiện captcha chữ/ảnh (không phải reCAPTCHA widget).
    Dạng 1: trang /sorry/ hoặc img[src*='captcha']
    Dạng 2: Google login có text 'type the text you hear or see' + ảnh captcha inline
    """
    from selenium.webdriver.common.by import By
    try:
        url = (driver.current_url or "").lower()
        if "google.com" not in url:
            return False

        if _is_recaptcha_page(driver):
            return False

        body_text = ""
        try:
            body_text = (driver.find_element(By.TAG_NAME, "body").text or "").lower()
        except Exception:
            pass

        _CAPTCHA_TEXT_HINTS = (
            "type the text you hear or see",
            "type the text",
            "enter the characters",
            "type the characters",
            "characters you see",
            "what you see in the image",
            "nhập văn bản bạn nghe hoặc thấy",
        )
        has_captcha_text = any(kw in body_text for kw in _CAPTCHA_TEXT_HINTS)

        captcha_imgs = driver.find_elements(By.CSS_SELECTOR,
            "img[src*='captcha'], img[id*='captcha'], img[class*='captcha'], "
            "img[src*='sorry/image'], img[src*='Captcha'], "
            "img[src*='AccountChallenge'], img[src*='challenge/image']")
        visible_captcha = [el for el in captcha_imgs if el.is_displayed()]

        if not visible_captcha and has_captcha_text:
            for img in driver.find_elements(By.CSS_SELECTOR, "img"):
                try:
                    if not img.is_displayed():
                        continue
                    src = (img.get_attribute("src") or "").lower()
                    w = img.size.get("width", 0)
                    h = img.size.get("height", 0)
                    if src.startswith("data:image") or "captcha" in src or "sorry" in src or "challenge" in src:
                        visible_captcha.append(img)
                        break
                    if 60 < w < 400 and 30 < h < 200:
                        visible_captcha.append(img)
                        break
                except Exception:
                    continue

        if not visible_captcha and not has_captcha_text:
            return False

        if has_captcha_text and not visible_captcha:
            pass

        text_inputs = driver.find_elements(By.CSS_SELECTOR,
            "input[type='text'], input[name*='captcha'], input[id*='captcha'], "
            "input[name='q'], input[name='answer'], input[aria-label*='Type the text']")
        for inp in text_inputs:
            try:
                if inp.is_displayed() and inp.is_enabled():
                    return True
            except Exception:
                continue

        if has_captcha_text:
            for inp in driver.find_elements(By.CSS_SELECTOR, "input"):
                try:
                    t = (inp.get_attribute("type") or "").lower()
                    if t in ("text", "search", "") and inp.is_displayed() and inp.is_enabled():
                        return True
                except Exception:
                    continue
    except Exception:
        pass
    return False


def _solve_image_captcha_2captcha(driver, api_key: str) -> bool:
    """Giải captcha chữ/ảnh qua 2captcha.com:
    1. Screenshot captcha image → base64
    2. Gửi 2captcha API (in.php) → nhận task ID
    3. Poll (res.php) → nhận text
    4. Nhập text vào input + submit
    """
    import json
    import base64
    import urllib.request
    import urllib.parse
    from selenium.webdriver.common.by import By
    from selenium.webdriver.common.keys import Keys
    _log = logging.getLogger(__name__)

    captcha_img = None
    for sel in ("img[src*='captcha']", "img[id*='captcha']", "img[class*='captcha']",
                "img[src*='sorry/image']", "img[src*='Captcha']"):
        try:
            for el in driver.find_elements(By.CSS_SELECTOR, sel):
                if el.is_displayed():
                    captcha_img = el
                    break
        except Exception:
            continue
        if captcha_img:
            break

    if not captcha_img:
        for img in driver.find_elements(By.CSS_SELECTOR, "img"):
            try:
                if not img.is_displayed():
                    continue
                w = img.size.get("width", 0)
                h = img.size.get("height", 0)
                src = (img.get_attribute("src") or "").lower()
                if (80 < w < 400 and 30 < h < 150) or "captcha" in src:
                    captcha_img = img
                    break
            except Exception:
                continue

    if not captcha_img:
        _log.warning("2CAPTCHA: không tìm thấy captcha image")
        return False

    try:
        img_base64 = captcha_img.screenshot_as_base64
    except Exception:
        try:
            img_src = captcha_img.get_attribute("src") or ""
            if img_src.startswith("data:"):
                img_base64 = img_src.split(",", 1)[1] if "," in img_src else ""
            else:
                req = urllib.request.Request(img_src, headers={"User-Agent": "Mozilla/5.0"})
                with urllib.request.urlopen(req, timeout=15) as resp:
                    img_base64 = base64.b64encode(resp.read()).decode("ascii")
        except Exception as e:
            _log.warning("2CAPTCHA: không thể lấy ảnh captcha: %s", e)
            return False

    if not img_base64:
        _log.warning("2CAPTCHA: ảnh captcha rỗng")
        return False

    _log.info("2CAPTCHA: đang gửi ảnh captcha (%d bytes base64)...", len(img_base64))

    try:
        create_data = urllib.parse.urlencode({
            "key": api_key,
            "method": "base64",
            "body": img_base64,
            "json": "1",
            "min_len": "4",
            "max_len": "8",
            "language": "2",
        }).encode("utf-8")
        req = urllib.request.Request(
            "https://2captcha.com/in.php",
            data=create_data,
            method="POST",
        )
        with urllib.request.urlopen(req, timeout=30) as resp:
            result = json.loads(resp.read().decode("utf-8", errors="replace"))
    except Exception as e:
        _log.warning("2CAPTCHA: gửi task lỗi: %s", e)
        return False

    if result.get("status") != 1:
        _log.warning("2CAPTCHA: tạo task fail: %s", result)
        return False

    task_id = result.get("request", "")
    if not task_id:
        _log.warning("2CAPTCHA: không nhận được task ID")
        return False

    _log.info("2CAPTCHA: task_id=%s, đang chờ giải...", task_id)

    captcha_text = ""
    deadline = time.time() + 90
    while time.time() < deadline:
        time.sleep(5)
        try:
            poll_url = f"https://2captcha.com/res.php?key={api_key}&action=get&id={task_id}&json=1"
            with urllib.request.urlopen(poll_url, timeout=15) as resp:
                poll_result = json.loads(resp.read().decode("utf-8", errors="replace"))
        except Exception as e:
            _log.debug("2CAPTCHA: poll lỗi: %s", e)
            continue

        if poll_result.get("status") == 1:
            captcha_text = poll_result.get("request", "").strip()
            break
        req_text = poll_result.get("request", "")
        if req_text == "CAPCHA_NOT_READY":
            continue
        _log.warning("2CAPTCHA: poll error: %s", poll_result)
        return False

    if not captcha_text:
        _log.warning("2CAPTCHA: timeout — không nhận được kết quả")
        return False

    _log.info("2CAPTCHA: đã giải được: '%s'", captcha_text)

    inp = None
    for sel in ("input[name*='captcha']", "input[id*='captcha']",
                "input[name='q']", "input[name='answer']", "input[type='text']"):
        try:
            for el in driver.find_elements(By.CSS_SELECTOR, sel):
                if el.is_displayed() and el.is_enabled():
                    inp = el
                    break
        except Exception:
            continue
        if inp:
            break

    if not inp:
        _log.warning("2CAPTCHA: không tìm thấy input để nhập captcha text")
        return False

    try:
        from human_click import human_move_and_click
        human_move_and_click(driver, inp, pause_after=0.05)
        inp.clear()
        _human_type(inp, captcha_text)
        time.sleep(0.3)

        v = inp.get_attribute("value") or ""
        if not v:
            inp.send_keys(captcha_text)
            time.sleep(0.2)
    except Exception as e:
        _log.warning("2CAPTCHA: nhập text lỗi: %s", e)
        try:
            inp.send_keys(captcha_text)
        except Exception:
            return False

    old_url = driver.current_url or ""
    submitted = False

    try:
        inp.send_keys(Keys.ENTER)
        _log.info("2CAPTCHA: submit bằng Enter trên input")
        submitted = True
    except Exception:
        pass

    if not submitted:
        btn = _find_next_button(driver) or _find_submit_or_primary_button(driver)
        if btn:
            try:
                from human_click import human_move_and_click
                human_move_and_click(driver, btn)
                _log.info("2CAPTCHA: đã click nút Next/Submit")
                submitted = True
            except Exception:
                pass

    if not submitted:
        try:
            driver.execute_script(
                "var el = arguments[0]; el.form && el.form.submit();", inp)
            _log.info("2CAPTCHA: submit qua form.submit()")
            submitted = True
        except Exception:
            pass

    time.sleep(4)

    try:
        new_url = driver.current_url or ""
        if new_url != old_url:
            _log.info("2CAPTCHA: URL đã thay đổi — captcha có vẻ đã qua!")
            return True
    except Exception:
        pass

    if not _is_image_captcha_page(driver):
        _log.info("2CAPTCHA: captcha đã qua!")
        return True

    _log.warning("2CAPTCHA: vẫn ở trang captcha sau khi nhập — có thể sai, thử lại Enter...")
    try:
        inp.send_keys(Keys.ENTER)
        time.sleep(3)
        if not _is_image_captcha_page(driver):
            _log.info("2CAPTCHA: captcha đã qua sau Enter lần 2!")
            return True
    except Exception:
        pass

    return False


def change_password_flow(driver, new_password: str, worker_id: str = "main", current_password: str = "", two_fa_secret: str = "") -> bool:
    _log = logging.getLogger(__name__)
    from selenium.webdriver.support import expected_conditions as EC
    from selenium.webdriver.support.ui import WebDriverWait
    from human_click import human_move_and_click, human_type as _ht, inject_fake_cursor
    from selenium.webdriver.common.by import By

    inject_fake_cursor(driver)
    CHANGE_PASS_URL = "https://myaccount.google.com/signinoptions/password"

    def _pending(step):
        _log.warning("CHANGE_PASS PENDING [%s] worker=%s", step, worker_id)
        evt = get_worker_pause_event(worker_id)
        while evt.is_set() or PAUSE_EVENT.is_set():
            time.sleep(1)

    def _body():
        try:
            return driver.find_element(By.TAG_NAME, "body").text.lower()
        except Exception:
            return ""

    def _on_change_pass_page() -> bool:
        """Xác nhận đang ở trang New password (có >= 1 input[type=password] visible)."""
        try:
            inps = driver.find_elements(By.CSS_SELECTOR, "input[type='password']")
            vis = [i for i in inps if i.is_displayed() and i.is_enabled()]
            if not vis:
                return False
            body = _body()
            return any(kw in body for kw in ("new password", "confirm new password",
                        "change password", "mật khẩu mới", "password strength"))
        except Exception:
            return False

    def _has_validation_error() -> bool:
        # DOM-based: kiểm tra aria-invalid hoặc error elements — không phụ thuộc ngôn ngữ
        try:
            has_err = driver.execute_script("""
                // Input với aria-invalid="true" = validation error
                if (document.querySelector('input[aria-invalid="true"]')) return true;
                // Error message elements (Google dùng aria-live hoặc role=alert)
                var errEls = document.querySelectorAll(
                    '[aria-live="assertive"],[role="alert"],[aria-live="polite"],' +
                    '.LXRPh,.o6cuMc,.Ekjuhf,[data-error],[aria-describedby]'
                );
                for (var e of errEls) {
                    if (e.offsetParent && (e.textContent || '').trim().length > 2) return true;
                }
                return false;
            """)
            if has_err:
                return True
        except Exception:
            pass
        # Fallback multilingual text
        body = _body()
        _ERRORS = (
            "too short", "at least 8", "don't match", "do not match",
            "mật khẩu quá ngắn", "mật khẩu không khớp",
            "sehr kurz", "zu kurz", "stimmen nicht", "trop court",
            "ne correspondent pas", "demasiado corta", "contraseñas no coinciden",
            "troppo corta", "não coincidem", "短すぎ", "일치하지", "太短",
        )
        return any(e in body for e in _ERRORS)

    pass_val = str(new_password or "").strip()
    if not pass_val:
        _log.warning("CHANGE_PASS: new_password rỗng")
        return False
    if len(pass_val) < 8:
        _log.warning("CHANGE_PASS: password '%s' quá ngắn (<%d chars), Google yêu cầu >= 8", pass_val[:3] + "***", len(pass_val))

    _log.info("CHANGE_PASS: navigating to %s", CHANGE_PASS_URL)
    if not _safe_navigate(driver, CHANGE_PASS_URL):
        _log.warning("CHANGE_PASS: page broken sau navigate — return False")
        return False

    if _is_reauth_page(driver):
        reauth_ok = _handle_reauth(
            driver, current_password, two_fa_secret,
            target_url="myaccount.google.com/signinoptions/password",
            timeout=45.0, log_prefix="CHANGE_PASS_REAUTH",
        )
        if not reauth_ok:
            _log.warning("CHANGE_PASS: reauth failed")
            return False
        _wait_page_ready(driver, timeout=10.0)

    if not _on_change_pass_page():
        _log.warning("CHANGE_PASS: không ở trang change password sau reauth. URL=%s", driver.current_url)
        return False

    def _find_pass_inputs():
        inputs = driver.find_elements(By.CSS_SELECTOR, "input[type='password']")
        visible = [i for i in inputs if i.is_displayed() and i.is_enabled()]
        if len(visible) >= 2:
            return visible[0], visible[1]
        elif len(visible) == 1:
            return visible[0], None
        return None, None

    for attempt in range(2):
        inp_new, inp_confirm = _find_pass_inputs()
        if not inp_new:
            _log.warning("CHANGE_PASS: không tìm thấy input password")
            return False

        try:
            human_move_and_click(driver, inp_new, pause_after=0.05)
            inp_new.clear()
            time.sleep(0.1)
            _ht(driver, inp_new, pass_val)
            time.sleep(0.3)
        except Exception as e:
            _log.warning("CHANGE_PASS: lỗi nhập New password: %s", e)
            return False

        if inp_confirm:
            try:
                human_move_and_click(driver, inp_confirm, pause_after=0.05)
                inp_confirm.clear()
                time.sleep(0.1)
                _ht(driver, inp_confirm, pass_val)
                time.sleep(0.3)
            except Exception as e:
                _log.warning("CHANGE_PASS: lỗi nhập Confirm password: %s", e)
                return False

        btn = None
        for sel in ("button", "div[role='button']", "span[role='button']"):
            try:
                for el in driver.find_elements(By.CSS_SELECTOR, sel):
                    if el.is_displayed():
                        txt = (el.text or "").lower().strip()
                        if any(kw in txt for kw in ("change password", "save", "đổi mật khẩu",
                                                     "lưu", "cambiar", "speichern", "enregistrer")):
                            btn = el
                            break
            except Exception:
                continue
            if btn:
                break
        if not btn:
            btn = _find_submit_or_primary_button(driver) or _find_next_button(driver)

        if btn:
            human_move_and_click(driver, btn)
            _log.info("CHANGE_PASS: đã click nút submit")
        else:
            _log.warning("CHANGE_PASS: không tìm thấy nút submit")

        time.sleep(4)
        body = _body()

        if any(w in body for w in ("password changed", "updated", "đã đổi")) or any(s in body for s in _SUCCESS_SAVED):
            _log.info("CHANGE_PASS: success!")
            return True

        url_now = (driver.current_url or "").lower()
        if "myaccount.google.com" in url_now and "password" not in url_now:
            _log.info("CHANGE_PASS: URL chuyển khỏi trang password — coi như thành công")
            return True

        if _has_validation_error() and attempt == 0:
            _log.warning("CHANGE_PASS: validation error (too short / don't match) — retry lần %d", attempt + 2)
            inp_new, inp_confirm = _find_pass_inputs()
            if inp_new:
                try:
                    inp_new.clear()
                    time.sleep(0.1)
                except Exception:
                    pass
                if inp_confirm:
                    try:
                        inp_confirm.clear()
                        time.sleep(0.1)
                    except Exception:
                        pass
            continue

        if any(h in body for h in _PASSWORD_MISMATCH) and attempt == 0:
            _log.warning("CHANGE_PASS: passwords don't match — retry")
            continue

        if any(v in body for v in _VERIFY_STEP_KEYWORDS) or "0/6" in body:
            _log.warning("CHANGE_PASS: đang ở bước verify recovery (chưa điền code)")
            return False

        if btn:
            _log.info("CHANGE_PASS: đã click save, coi như thành công")
            return True

    _log.warning("CHANGE_PASS: hết retry — return False")
    return False


def change_recovery_email_flow(
    driver, new_recovery_email: str, worker_id: str = "main",
    current_password: str = "", two_fa_secret: str = "",
    use_temp_mail: bool = False, temp_mail_base_url: str = "",
) -> tuple[bool, str]:
    """Trả về (thành_công, email_đã_dùng) để GUI lưu đúng vào item (khi dùng mail tạm)."""
    _log = logging.getLogger(__name__)
    from selenium.webdriver.support import expected_conditions as EC
    from selenium.webdriver.support.ui import WebDriverWait
    from human_click import human_move_and_click, human_type as _ht, inject_fake_cursor
    from selenium.webdriver.common.by import By

    inject_fake_cursor(driver)
    RECOVERY_URL = "https://myaccount.google.com/recovery/email"

    def _pending(step):
        _log.warning("CHANGE_MAIL_KP PENDING [%s] worker=%s", step, worker_id)
        evt = get_worker_pause_event(worker_id)
        while evt.is_set() or PAUSE_EVENT.is_set():
            time.sleep(1)

    def _body():
        try:
            return driver.find_element(By.TAG_NAME, "body").text.lower()
        except Exception:
            return ""

    _log.info("CHANGE_MAIL_KP: navigating to %s", RECOVERY_URL)
    if not _safe_navigate(driver, RECOVERY_URL):
        _log.warning("CHANGE_MAIL_KP: page broken sau navigate — return False")
        return False

    if _is_reauth_page(driver):
        reauth_ok = _handle_reauth(
            driver, current_password, two_fa_secret,
            target_url="myaccount.google.com/recovery/email",
            timeout=45.0, log_prefix="CHANGE_MAIL_REAUTH",
        )
        if not reauth_ok:
            _log.warning("CHANGE_MAIL_KP: reauth failed")
            return False
        _wait_page_ready(driver, timeout=10.0)

    edit_clicked = False
    for _ in range(2):
        for sel in [
            "[aria-label*='edit']", "[aria-label*='Edit']",
            "[aria-label*='change']", "[aria-label*='Change']",
            "[aria-label*='Edit recovery']", "[aria-label*='Change recovery']",
        ]:
            try:
                els = driver.find_elements(By.CSS_SELECTOR, sel)
                for el in els:
                    if not el.is_displayed():
                        continue
                    al = (el.get_attribute("aria-label") or "").lower()
                    if "edit" in al or "change" in al or "chỉnh" in al or "đổi" in al or "sửa" in al:
                        human_move_and_click(driver, el, pause_after=0.05)
                        edit_clicked = True
                        _log.info("CHANGE_MAIL_KP: da click nut edit/change de mo dialog")
                        break
                if edit_clicked:
                    break
            except Exception:
                pass
        if edit_clicked:
            break
        try:
            for el in driver.find_elements(By.XPATH, "//*[contains(@aria-label,'dit') or contains(@aria-label,'hange') or contains(.,'Set up recovery')]"):
                if el.is_displayed() and el.get_attribute("role") == "button":
                    human_move_and_click(driver, el, pause_after=0.05)
                    edit_clicked = True
                    break
        except Exception:
            pass
        if edit_clicked:
            break
        time.sleep(2)

    if edit_clicked:
        time.sleep(2)

    inp = None
    for _ in range(3):
        for sel in [
            "input[type='email']", "input[type='text']",
            "input[aria-label*='email']", "input[aria-label*='Email']",
            "input[placeholder*='email']", "input",
        ]:
            try:
                els = driver.find_elements(By.CSS_SELECTOR, sel)
                for el in els:
                    if el.is_displayed() and el.is_enabled():
                        t = (el.get_attribute("type") or "text").lower()
                        if t == "hidden":
                            continue
                        inp = el
                        break
            except Exception:
                pass
            if inp:
                break
        if inp:
            break
        time.sleep(2)

    if not inp:
        # Kiểm tra xem trang có hiển thị "không có mail khôi phục" không
        body_check = _body()
        if any(h in body_check for h in _NO_RECOVERY_EMAIL):
            _log.warning("CHANGE_MAIL_KP: tài khoản không có email khôi phục — no_recovery_email")
            return ("no_recovery_email", "")
        _log.warning("CHANGE_MAIL_KP: khong tim thay o nhap email (co the can click icon edit truoc)")
        _pending("no-email-input")
        return (False, new_recovery_email)

    try:
        driver.execute_script("arguments[0].value = ''; arguments[0].dispatchEvent(new Event('input'));", inp)
    except Exception:
        inp.clear()
    _ht(driver, inp, new_recovery_email)
    time.sleep(0.5)

    def _is_verify_step():
        body2 = _body()
        if any(w in body2 for w in _VERIFY_STEP_KEYWORDS):
            return True
        try:
            code_inps = driver.find_elements(By.CSS_SELECTOR, "input[maxlength='6'], input[inputmode='numeric']")
            if any(e.is_displayed() for e in code_inps):
                return True
        except Exception:
            pass
        return False

    def _is_verify_code_error():
        """Trang báo mã verify sai — đa ngôn ngữ."""
        return any(x in _body() for x in _VERIFY_CODE_WRONG)

    btn = _find_submit_or_primary_button(driver)
    clicked = bool(btn)
    if btn:
        human_move_and_click(driver, btn)
    time.sleep(5)

    def _is_final_success_page():
        """Check trang kết quả hiển thị email mới — đã thay xong hoàn toàn."""
        b = _body()
        u = (driver.current_url or "").lower()
        if ("your recovery email" in b or "last updated" in b) and "recovery" in u:
            email_prefix = new_recovery_email.lower().split("@")[0][:8]
            if email_prefix and email_prefix in b:
                return True
        if "your recovery email" in b and not _is_verify_step():
            return True
        return False

    if _is_final_success_page():
        _log.info("CHANGE_MAIL_KP: trang hiện email mới — success ngay sau submit!")
        return (True, new_recovery_email)

    body = _body()
    if any(w in body for w in _SUCCESS_SAVED) and not _is_verify_step() and not _is_verify_code_error():
        _log.info("CHANGE_MAIL_KP: success")
        return (True, new_recovery_email)

    for _ in range(3):
        if _is_final_success_page():
            _log.info("CHANGE_MAIL_KP: trang hiện email mới — success!")
            return (True, new_recovery_email)
        body = _body()
        if any(w in body for w in _SUCCESS_SAVED) and not _is_verify_step() and not _is_verify_code_error():
            return (True, new_recovery_email)
        if _is_verify_step():
            break
        time.sleep(3)

    if _is_final_success_page():
        _log.info("CHANGE_MAIL_KP: đã verify xong — success!")
        return (True, new_recovery_email)

    base_url = (temp_mail_base_url or "").strip()
    if _is_verify_step() and not _is_final_success_page() and base_url and new_recovery_email:
        # Luôn thử lấy code từ mail khôi phục qua API (mail trong list phải la dia chi thuoc inboxesmail)
        try:
            from temp_mail_api import wait_for_verification_email
            _log.info("CHANGE_MAIL_KP: lay code tu mail khoi phuc %s qua API...", new_recovery_email)
            code_6, verify_url = wait_for_verification_email(
                new_recovery_email,
                base_url=base_url,
                from_contains="google",
                max_wait_sec=120,
                poll_interval_sec=5,
            )
            if verify_url:
                _log.info("CHANGE_MAIL_KP: mo link verify tu mail khoi phuc")
                driver.get(verify_url)
                time.sleep(5)
                body = _body()
                if any(w in body for w in _SUCCESS_SAVED) and not _is_verify_step() and not _is_verify_code_error():
                    return (True, new_recovery_email)
            if code_6:
                _log.info("CHANGE_MAIL_KP: dien ma verify %s tu mail khoi phuc", code_6)
                for sel in ["input[type='text']", "input[maxlength='6']", "input[inputmode='numeric']", "input[autocomplete='one-time-code']", "input"]:
                    try:
                        inps = driver.find_elements(By.CSS_SELECTOR, sel)
                        for inp in inps:
                            if inp.is_displayed() and inp.is_enabled():
                                inp.clear()
                                _ht(driver, inp, code_6)
                                time.sleep(0.5)
                                b = _find_submit_or_primary_button(driver)
                                if b:
                                    human_move_and_click(driver, b)
                                time.sleep(5)
                                body = _body()
                                if _is_verify_code_error():
                                    _log.warning("CHANGE_MAIL_KP: trang bao ma verify sai — khong danh dau thanh cong")
                                    break
                                if any(w in body for w in _SUCCESS_SAVED):
                                    return (True, new_recovery_email)
                                break
                        else:
                            continue
                        break
                    except Exception:
                        continue
        except Exception as e:
            _log.warning("CHANGE_MAIL_KP: lay code tu mail khoi phuc loi: %s", e)

    def _is_recovery_success_page():
        """Trang hiển thị recovery email đã thay thành công:
        'Your recovery email' + email mới + 'Last updated'
        """
        body2 = _body()
        url2 = (driver.current_url or "").lower()
        if "recovery/email" in url2 or "recovery" in url2:
            if "your recovery email" in body2 or "last updated" in body2:
                if new_recovery_email.lower().split("@")[0][:6] in body2:
                    return True
            if any(w in body2 for w in _SUCCESS_SAVED):
                return True
        if any(w in body2 for w in ("password changed", "updated", "đã đổi")) and not _is_verify_step():
            return True
        return False

    if _is_recovery_success_page():
        _log.info("CHANGE_MAIL_KP: trang hiển thị recovery email mới — thành công!")
        return (True, new_recovery_email)

    if _is_verify_step():
        _log.warning("CHANGE_MAIL_KP: Vẫn ở bước verify — chờ thêm...")
        for _final_wait in range(6):
            time.sleep(5)
            if _is_recovery_success_page():
                _log.info("CHANGE_MAIL_KP: verify xong — thành công!")
                return (True, new_recovery_email)
            if not _is_verify_step():
                body_final = _body()
                if any(w in body_final for w in _SUCCESS_SAVED) or "your recovery email" in body_final:
                    return (True, new_recovery_email)
                break
        _log.warning("CHANGE_MAIL_KP: vẫn ở bước verify sau khi chờ thêm")
        return (False, new_recovery_email)

    if _is_verify_code_error():
        _log.warning("CHANGE_MAIL_KP: trang báo mã verify sai — không thành công")
        return (False, new_recovery_email)

    body_final = _body()
    if any(w in body_final for w in _SUCCESS_SAVED) or "your recovery email" in body_final or "last updated" in body_final:
        _log.info("CHANGE_MAIL_KP: detect success từ body text cuối")
        return (True, new_recovery_email)

    if clicked:
        _log.info("CHANGE_MAIL_KP: đã click save/next, coi như thành công")
        return (True, new_recovery_email)

    _log.warning("CHANGE_MAIL_KP: chưa xác nhận — kiểm tra Chrome")
    return (False, new_recovery_email)


def delete_phone_recovery_flow(driver, worker_id: str = "main", current_password: str = "", two_fa_secret: str = "") -> bool:
    _log = logging.getLogger(__name__)
    from selenium.webdriver.support.ui import WebDriverWait
    from human_click import human_move_and_click, inject_fake_cursor
    from selenium.webdriver.common.by import By

    inject_fake_cursor(driver)
    PHONE_URL = "https://myaccount.google.com/recovery/phone"

    def _pending(step):
        _log.warning("DEL_PHONE PENDING [%s] worker=%s", step, worker_id)
        evt = get_worker_pause_event(worker_id)
        while evt.is_set() or PAUSE_EVENT.is_set():
            time.sleep(1)

    def _body():
        try:
            return driver.find_element(By.TAG_NAME, "body").text.lower()
        except Exception:
            return ""

    _log.info("DEL_PHONE: navigating to %s", PHONE_URL)
    if not _safe_navigate(driver, PHONE_URL):
        _log.warning("DEL_PHONE: page broken sau navigate — return False")
        return False

    if _is_reauth_page(driver):
        reauth_ok = _handle_reauth(
            driver, current_password, two_fa_secret,
            target_url="myaccount.google.com/recovery/phone",
            timeout=45.0, log_prefix="DEL_PHONE_REAUTH",
        )
        if not reauth_ok:
            _log.warning("DEL_PHONE: reauth failed")
            return False
        _wait_page_ready(driver, timeout=10.0)

    body = _body()
    if "no recovery phone" in body or "add recovery phone" in body:
        _log.info("DEL_PHONE: no phone number found. Done.")
        return True

    removed_any = False
    for attempt in range(10):
        delete_clicked = False
        for text in ["Delete", "Remove", "Xóa", "Gỡ"]:
            try:
                els = driver.find_elements(By.XPATH,
                    f"//span[text()='{text}'] | //button[contains(.,'{text}')] | //a[contains(.,'{text}')]")
                for el in els:
                    if el.is_displayed() and el.is_enabled():
                        human_move_and_click(driver, el)
                        delete_clicked = True
                        break
                if delete_clicked:
                    break
            except Exception:
                pass

        if not delete_clicked:
            try:
                trash_icons = driver.find_elements(By.CSS_SELECTOR,
                    "[aria-label*='Delete'], [aria-label*='Remove'], [data-delete-phone]")
                for icon in trash_icons:
                    if icon.is_displayed():
                        human_move_and_click(driver, icon)
                        delete_clicked = True
                        break
            except Exception:
                pass

        if not delete_clicked:
            if removed_any:
                _log.info("DEL_PHONE: no more delete buttons. Done (removed some).")
            else:
                _log.info("DEL_PHONE: no delete button found on attempt %d.", attempt)
            break

        time.sleep(2)
        confirm_text = _body()
        for text in ["Remove", "Delete", "Confirm", "OK", "Xóa", "Xác nhận"]:
            try:
                els = driver.find_elements(By.XPATH,
                    f"//span[text()='{text}'] | //button[contains(.,'{text}')]")
                for el in els:
                    if el.is_displayed():
                        human_move_and_click(driver, el)
                        break
            except Exception:
                pass
        time.sleep(3)
        removed_any = True
        _log.info("DEL_PHONE: removed a phone (attempt=%d).", attempt)

        body = _body()
        if "no recovery phone" in body or "add recovery phone" in body:
            break

    return True


def create_app_password_flow(driver, worker_id: str = "main", current_password: str = "") -> str:
    _log = logging.getLogger(__name__)
    from selenium.webdriver.support.ui import WebDriverWait
    from human_click import human_move_and_click, human_type as _ht, inject_fake_cursor
    from selenium.webdriver.common.by import By

    inject_fake_cursor(driver)
    APP_PASS_URL = "https://myaccount.google.com/apppasswords"

    def _pending(step):
        _log.warning("APP_PASS PENDING [%s] worker=%s", step, worker_id)
        evt = get_worker_pause_event(worker_id)
        while evt.is_set() or PAUSE_EVENT.is_set():
            time.sleep(1)

    def _body():
        try:
            return driver.find_element(By.TAG_NAME, "body").text.lower()
        except Exception:
            return ""

    _log.info("APP_PASS: navigating to %s", APP_PASS_URL)
    if not _safe_navigate(driver, APP_PASS_URL):
        _log.warning("APP_PASS: page broken sau navigate — return False")
        return False

    if _is_reauth_page(driver):
        reauth_ok = _handle_reauth(
            driver, current_password, "",
            target_url="myaccount.google.com/apppasswords",
            timeout=45.0, log_prefix="APP_PASS_REAUTH",
        )
        if not reauth_ok:
            _log.warning("APP_PASS: reauth failed")
            return False
        _wait_page_ready(driver, timeout=10.0)

    name_input = None
    for sel in ["input[type='text']", "input[aria-label*='name']", "input"]:
        try:
            els = driver.find_elements(By.CSS_SELECTOR, sel)
            for el in els:
                if el.is_displayed() and el.is_enabled():
                    name_input = el
                    break
        except Exception:
            pass
        if name_input:
            break

    if name_input:
        name_input.clear()
        _ht(driver, name_input, "Mail")
        time.sleep(0.3)

    btn = _find_submit_or_primary_button(driver)
    create_clicked = bool(btn)
    if btn:
        human_move_and_click(driver, btn)

    if not create_clicked:
        _log.warning("APP_PASS: could not click Create/Generate")
        return ""

    time.sleep(5)

    app_password = ""
    try:
        all_spans = driver.find_elements(By.TAG_NAME, "span")
        for sp in all_spans:
            t = sp.text.strip().replace(" ", "")
            if len(t) == 16 and t.isalpha():
                app_password = sp.text.strip()
                break
    except Exception:
        pass

    if not app_password:
        try:
            _re = re.search(r"[a-z]{4}\s[a-z]{4}\s[a-z]{4}\s[a-z]{4}",
                            driver.find_element(By.TAG_NAME, "body").text)
            if _re:
                app_password = _re.group(0)
        except Exception:
            pass

    if app_password:
        _log.info("APP_PASS: created successfully: %s", app_password)
    else:
        _log.warning("APP_PASS: could not extract app password from page.")

    _click_done_if_present(driver)
    return app_password


def verify_phone_flow(
    driver, hero_api_key: str, hero_service: str,
    hero_retries: int = 3, worker_id: str = "main", current_password: str = "", two_fa_secret: str = "",
) -> tuple[bool, str]:
    _log = logging.getLogger(__name__)
    from selenium.webdriver.support.ui import WebDriverWait
    from human_click import human_move_and_click, human_type as _ht, inject_fake_cursor
    from selenium.webdriver.common.by import By

    inject_fake_cursor(driver)
    RESCUE_PHONE_URL = "https://myaccount.google.com/signinoptions/rescuephone"

    def _pending(step):
        _log.warning("VERIFY_PHONE PENDING [%s] worker=%s", step, worker_id)
        evt = get_worker_pause_event(worker_id)
        while evt.is_set() or PAUSE_EVENT.is_set():
            time.sleep(1)

    def _body():
        try:
            return driver.find_element(By.TAG_NAME, "body").text.lower()
        except Exception:
            return ""

    _log.info("VERIFY_PHONE: navigating to %s", RESCUE_PHONE_URL)
    if not _safe_navigate(driver, RESCUE_PHONE_URL):
        _log.warning("VERIFY_PHONE: page broken sau navigate — return (False, '')")
        return (False, "")

    if _is_reauth_page(driver):
        reauth_ok = _handle_reauth(
            driver, current_password, two_fa_secret,
            target_url="myaccount.google.com/signinoptions/rescuephone",
            timeout=45.0, log_prefix="VERIFY_PHONE_REAUTH",
        )
        if not reauth_ok:
            _log.warning("VERIFY_PHONE: reauth failed — return (False, '')")
            return (False, "")
        _wait_page_ready(driver, timeout=10.0)

    if not hero_api_key or not hero_service:
        _log.warning("VERIFY_PHONE: no Hero-SMS credentials configured")
        return False, ""

    activation_id, phone = get_hero_sms_number(hero_api_key, hero_service, hero_retries)
    if not phone:
        _log.warning("VERIFY_PHONE: could not get temp phone from Hero-SMS")
        return False, ""

    _log.info("VERIFY_PHONE: got temp phone %s (id=%s)", phone, activation_id)

    inp = None
    for sel in ["input[type='tel']", "input[autocomplete*='tel']"]:
        try:
            els = driver.find_elements(By.CSS_SELECTOR, sel)
            for el in els:
                if el.is_displayed() and el.is_enabled():
                    inp = el
                    break
        except Exception:
            pass
        if inp:
            break

    if not inp:
        for xp in [
            "//button[contains(.,'Add')]", "//button[contains(.,'Thêm')]",
            "//a[contains(.,'Add')]", "//button[contains(.,'Update')]",
        ]:
            try:
                els = driver.find_elements(By.XPATH, xp)
                for el in els:
                    if el.is_displayed():
                        human_move_and_click(driver, el)
                        time.sleep(3)
                        break
            except Exception:
                pass

        for sel in ["input[type='tel']", "input[autocomplete*='tel']"]:
            try:
                els = driver.find_elements(By.CSS_SELECTOR, sel)
                for el in els:
                    if el.is_displayed():
                        inp = el
                        break
            except Exception:
                pass
            if inp:
                break

    if not inp:
        _log.warning("VERIFY_PHONE: no phone input field found")
        return False, ""

    digits = re.sub(r"[^\d+]", "", phone)
    inp.clear()
    _ht(driver, inp, digits)
    time.sleep(0.5)

    btn = _find_next_button(driver)
    if btn:
        human_move_and_click(driver, btn)
    time.sleep(3)

    hero_sms_mark_ready(hero_api_key, activation_id)
    code = get_hero_sms_code(hero_api_key, activation_id)
    if code:
        _submit_phone_verify_code(driver, code)
        _log.info("VERIFY_PHONE: submitted code %s for phone %s", code, phone)
        _click_done_if_present(driver)
        _click_not_now_if_present(driver)
        return True, phone
    else:
        _log.warning("VERIFY_PHONE: did not receive SMS code")
        return False, phone


def _detect_account_state(driver) -> str:
    """Phát hiện trạng thái lỗi tài khoản theo 3 tầng:
    1. URL-based (nhanh, chính xác nhất)
    2. DOM error elements (language-independent)
    3. Body text keywords (fallback đa ngôn ngữ hardcoded)
    4. AI DOM analysis (fallback khi có error signal nhưng không nhận ra)
    """
    from selenium.webdriver.common.by import By
    _log = logging.getLogger(__name__)

    if _is_signin_hard_block_page(driver):
        return "signin_blocked"

    url = (driver.current_url or "").lower()

    # ── Tầng 1: URL-based (không phụ thuộc ngôn ngữ) ──────────────────────
    if "/v3/signin/disabled" in url or "/account/disabled" in url or "accountdisabled" in url:
        return "disabled"
    if "suspended" in url and "accounts.google.com" in url:
        return "suspended"
    if "too-many-attempts" in url or "toomanyrequests" in url:
        return "too_many_attempts"
    if "challenge/iknow" in url or "iknowmypassword=false" in url:
        return "need_verify_phone"

    # ── Tầng 2: DOM error elements (language-independent) ─────────────────
    try:
        error_js = """
        var els = document.querySelectorAll(
            '[aria-live="assertive"],[role="alert"],[aria-live="polite"],' +
            '.LXRPh,.o6cuMc,.Ekjuhf,[data-error]'
        );
        var has_err = [...els].some(function(e){ return (e.innerText||'').trim().length > 2; });
        var pass_vis = !!document.querySelector('input[name="Passwd"]:not([type="hidden"]),input[type="password"]:not([type="hidden"])');
        var totp_vis = !!document.querySelector('input[name="totpPin"],input[autocomplete="one-time-code"]');
        return {has_err: has_err, pass_vis: pass_vis, totp_vis: totp_vis};
        """
        dom_sig = driver.execute_script(error_js) or {}
        has_err = dom_sig.get("has_err", False)
        pass_vis = dom_sig.get("pass_vis", False)
        totp_vis = dom_sig.get("totp_vis", False)

        if has_err and pass_vis:
            _log.debug("ACCOUNT_STATE: error element + password input → wrong_password candidate")
        if has_err and totp_vis:
            _log.debug("ACCOUNT_STATE: error element + totp input → wrong_2fa candidate")
    except Exception:
        has_err = pass_vis = totp_vis = False

    # ── Tầng 3: Body text keywords ─────────────────────────────────────────
    try:
        body = (driver.execute_script(
            "return document.body ? document.body.innerText.substring(0,800) : ''"
        ) or "").lower()
    except Exception:
        body = ""

    if any(h in body for h in _ACCOUNT_DISABLED):
        return "disabled"
    if any(h in body for h in _ACCOUNT_SUSPENDED):
        return "suspended"
    if any(h in body for h in _ACCOUNT_DELETED):
        return "deleted"
    # Multilingual: restricted/unusual activity/too many attempts
    _RESTRICTED = ("restricted", "hạn chế", "eingeschränkt", "restreint", "restringida",
                   "limitada", "ограничена", "제한", "制限", "dibatasi")
    _SUSPICIOUS = ("unusual activity", "suspicious", "hoạt động bất thường", "actividad inusual",
                   "activité inhabituelle", "ungewöhnliche aktivität", "nécessite une vérification",
                   "活動異常", "異常なアクティビティ", "비정상적인 활동")
    _TOO_MANY = ("too many", "too many attempts", "quá nhiều lần", "demasiados intentos",
                  "trop de tentatives", "zu viele versuche", "troppi tentativi", "слишком много",
                  "回数制限", "너무 많은 시도")
    if any(h in body for h in _RESTRICTED) and "account" in body:
        return "restricted"
    if any(h in body for h in _SUSPICIOUS):
        return "suspicious_activity"
    if any(h in body for h in _TOO_MANY):
        return "too_many_attempts"
    if any(h in body for h in _PASS_CHANGED):
        return "password_changed"
    if any(h in body for h in _WRONG_PASSWORD):
        return "wrong_password"
    if any(h in body for h in _ACCOUNT_NOT_FOUND):
        return "not_found"

    # Recovery email errors (chỉ check khi đang ở trang recovery)
    if "recovery" in url or "myaccount.google.com" in url:
        if any(h in body for h in _NO_RECOVERY_EMAIL):
            return "no_recovery_email"
        if has_err and any(h in body for h in _WRONG_RECOVERY_EMAIL):
            return "wrong_recovery_email"

    # ── Tầng 4: AI DOM analysis (khi có error signal nhưng text không nhận ra) ──
    if has_err:
        try:
            from ai_analyzer import detect_account_state_ai
            ai_state = detect_account_state_ai(driver)
            if ai_state != "unknown":
                _log.info("ACCOUNT_STATE: AI phát hiện → %s", ai_state)
                return ai_state
        except Exception as _e:
            _log.debug("ACCOUNT_STATE: AI fallback lỗi: %s", _e)

    return "unknown"


def run_login(
    acc: Account,
    keep_open: bool = False,
    status_cb=None,
    worker_id: str = "main",
    stop_check=None,
):
    import os
    from selenium.webdriver.common.by import By
    _log = logging.getLogger(__name__)

    tcfg = load_tool_config()
    cfg = load_config()
    ez_api_key = str(tcfg.get("ez_captcha_api_key", "") or "").strip()
    twocaptcha_api_key = str(tcfg.get("twocaptcha_api_key", "") or "").strip()
    hero_sms_api_key = str(tcfg.get("hero_sms_api_key", "") or "").strip()
    hero_sms_service = str(tcfg.get("hero_sms_service", "") or "").strip()
    hero_sms_get_number_retries = int(tcfg.get("hero_sms_get_number_retries", 3))
    proxy = str(tcfg.get("proxy", "") or "").strip()
    use_stealth = bool(tcfg.get("use_anti_fingerprint", True))
    timeout_login = int(tcfg.get("timeoutlogin", 300))
    login_deadline = time.time() + timeout_login

    user_data_dir = ""
    no_save_profile = bool(cfg.get("no_save_profile", False))
    _temp_profile_dir_to_cleanup: str = ""
    profile_base = str(cfg.get("profile_dir", "") or "").strip()
    if no_save_profile:
        import tempfile as _tempfile
        active_user_data_dir = _tempfile.mkdtemp(prefix="chrome_nosave_", suffix="_" + acc.email[:8])
        _temp_profile_dir_to_cleanup = active_user_data_dir
        _log.info("no_save_profile: dùng profile tạm %s", active_user_data_dir)
    else:
        if not profile_base:
            profile_base = os.path.join(os.path.dirname(__file__), "data", "chrome_profiles")
        safe_name = re.sub(r"[^\w@.\-]", "_", acc.email)
        active_user_data_dir = os.path.join(profile_base, safe_name)
        os.makedirs(active_user_data_dir, exist_ok=True)

    driver = None
    phone_challenge_phone = ""
    phone_challenge_activation_id = ""
    try:
        if status_cb:
            status_cb(f"login {GOOGLE_LOGIN_URL}")

        driver = create_driver(proxy=proxy, user_data_dir=active_user_data_dir)
        if _temp_profile_dir_to_cleanup:
            import shutil as _shutil
            _tpd = _temp_profile_dir_to_cleanup
            _orig_quit = driver.quit
            def _quit_cleanup(*a, **kw):
                try:
                    _orig_quit(*a, **kw)
                finally:
                    try:
                        if os.path.exists(_tpd):
                            _shutil.rmtree(_tpd, ignore_errors=True)
                    except Exception:
                        pass
            driver.quit = _quit_cleanup
        try:
            driver.set_page_load_timeout(90)
        except Exception:
            pass

        from human_click import inject_fake_cursor
        inject_fake_cursor(driver)

        wait_if_paused(status_cb, worker_id)
        if stop_check and stop_check():
            return {"status": "stopped", "driver": driver, "user_data_dir": active_user_data_dir}
        _log.info("run_login: mo trang %s", GOOGLE_LOGIN_URL)
        try:
            driver.get(GOOGLE_LOGIN_URL)
        except Exception as e:
            if "timeout" in str(type(e).__name__).lower() or "timeout" in str(e).lower():
                _log.warning("run_login: page load timeout (90s) — mang hoac trang qua cham")
                if status_cb:
                    status_cb("page load timeout")
                return {"status": "page_load_timeout", "driver": driver, "user_data_dir": active_user_data_dir}
            raise
        _wait_page_ready(driver, timeout=15.0, poll=0.5)

        if is_login_successful(driver) or _is_post_login_setup_page(driver):
            _log.info("run_login: đã login sẵn (session cũ còn) — skip submit email")
            return {"status": "login ok", "driver": driver, "user_data_dir": active_user_data_dir}

        _submit_email(driver, acc.email)

        max_couldnt_retry = 2
        couldnt_reload_count = 0
        lag_reload_count = 0
        phone_fail_count = 0
        twofa_wrong_count = 0
        last_signature = ""
        last_page_kind = ""
        stagnant_rounds = 0

        def _page_signature() -> str:
            try:
                url = (driver.current_url or "").lower()
            except Exception:
                url = ""
            try:
                body = (driver.execute_script(
                    "return document.body ? document.body.innerText.substring(0, 220) : ''"
                ) or "").lower()
                body = re.sub(r"\s+", " ", body).strip()[:180]
            except Exception:
                body = ""
            return f"{url}|{body}"

        def _return_status(st: str):
            if status_cb:
                status_cb(st)
            return {"status": st, "driver": driver, "user_data_dir": active_user_data_dir}

        def _reload_login_from_stall(reason: str):
            nonlocal lag_reload_count, last_signature, stagnant_rounds
            lag_reload_count += 1
            _log.warning("LOGIN_STALL: %s -> reload login (%s/2)", reason, lag_reload_count)
            _safe_navigate(driver, GOOGLE_LOGIN_URL, max_retries=3, wait_timeout=20.0)

            if is_login_successful(driver) or _is_post_login_setup_page(driver):
                _log.info("LOGIN_STALL: navigate về login nhưng đã login rồi — skip submit email")
            else:
                try:
                    _submit_email(driver, acc.email)
                except Exception as e:
                    _log.warning("LOGIN_STALL: submit email lỗi: %s", e)
            last_signature = ""
            stagnant_rounds = 0

        while True:
            wait_if_paused(status_cb, worker_id)
            if stop_check and stop_check():
                return _return_status("stopped")

            if time.time() > login_deadline:
                _log.warning("Login timeout for %s after %ds", acc.email, timeout_login)
                return _return_status("login timeout")

            _wait_page_ready(driver, timeout=5.0, poll=0.6)

            if _is_page_broken(driver):
                broken_reload_count = getattr(_is_page_broken, '_reload_count', 0) + 1
                _is_page_broken._reload_count = broken_reload_count
                if broken_reload_count > 5:
                    _log.warning("PAGE_BROKEN: đã reload %d lần — proxy quá lag, dừng", broken_reload_count)
                    return _return_status("error: proxy_broken")
                _log.warning("PAGE_BROKEN: trang hỏng/trống/error (lần %d) — reload %s",
                             broken_reload_count, GOOGLE_LOGIN_URL)
                try:
                    driver.get(GOOGLE_LOGIN_URL)
                    _wait_page_ready(driver, timeout=15.0, poll=0.5)
                except Exception as e:
                    _log.warning("PAGE_BROKEN: reload lỗi: %s — chờ 5s rồi thử lại", e)
                    time.sleep(5)
                    try:
                        driver.get(GOOGLE_LOGIN_URL)
                        _wait_page_ready(driver, timeout=20.0, poll=0.5)
                    except Exception:
                        pass
                if _is_page_broken(driver):
                    _log.warning("PAGE_BROKEN: vẫn hỏng sau reload — chờ 8s")
                    time.sleep(8)
                last_signature = ""
                stagnant_rounds = 0
                continue

            if hasattr(_is_page_broken, '_reload_count'):
                _is_page_broken._reload_count = 0

            account_state = _detect_account_state(driver)
            if account_state in ("disabled", "suspended", "deleted"):
                return _return_status(account_state)
            if account_state not in ("unknown",):
                # Thu thập error state data để train
                if account_state in ("password_changed", "wrong_password", "wrong_2fa",
                                     "too_many_attempts", "need_verify_phone",
                                     "suspicious_activity", "disabled", "suspended"):
                    try:
                        from data_collector import collect_sample
                        collect_sample(driver, account_state, source="dom", confidence=1.0)
                    except Exception:
                        pass
            if account_state == "password_changed":
                return _return_status("password_changed")
            if account_state == "wrong_password":
                return _return_status("wrong_password")
            if account_state == "wrong_2fa":
                return _return_status("wrong_2fa")
            if account_state == "too_many_attempts":
                return _return_status("too_many_attempts")
            if account_state == "need_verify_phone":
                return _return_status("need_verify_phone")
            if account_state == "suspicious_activity":
                return _return_status("suspicious_activity")
            if account_state == "not_found":
                return _return_status("not_found")
            if account_state == "restricted":
                return _return_status("restricted")
            if account_state == "signin_blocked":
                return _return_status("disconnect.login")

            page_kind = _detect_login_page_kind(driver)
            if page_kind != last_page_kind:
                # Lightweight router log for proxy-heavy runs.
                # Full classifier/body preview is only collected on DEBUG level.
                if _log.isEnabledFor(logging.DEBUG):
                    try:
                        vk, p_sc, t_sc = _classify_verify_challenge_kind(driver)
                    except Exception:
                        vk, p_sc, t_sc = ("unknown", 0, 0)
                    body_preview = _read_page_body_text(driver, 200).replace("\n", " | ")
                    _log.debug(
                        "LOGIN_ROUTER_DETAIL: page=%s verify_kind=%s score(phone=%s,twofa=%s) url=%s body=[%s]",
                        page_kind, vk, p_sc, t_sc, driver.current_url, body_preview
                    )
                _log.info("LOGIN_ROUTER: page=%s url=%s", page_kind, driver.current_url)
                last_page_kind = page_kind
                # Thu thập data training khi nhận diện được page (source=dom/url)
                if page_kind not in ("unknown", "loading", "broken"):
                    try:
                        from data_collector import collect_sample
                        collect_sample(driver, page_kind, source="dom", confidence=1.0)
                    except Exception:
                        pass

            if page_kind == "success":
                if _is_post_login_setup_page(driver):
                    _log.info("Login ok nhưng đang ở trang post-login setup — skip")
                _log.info("Login ok: %s (url=%s)", acc.email, driver.current_url)
                return _return_status("login ok")

            if page_kind == "hard_block":
                _log.warning("Phát hiện hard-block trang đăng nhập (phi ngôn ngữ) — trả về disconnect.login")
                return _return_status("disconnect.login")

            if page_kind == "couldnt_sign_in":
                if couldnt_reload_count >= max_couldnt_retry:
                    _log.warning("Couldn't sign you in đã retry %s lần — dừng.", max_couldnt_retry)
                    return _return_status("disconnect.login")
                couldnt_reload_count += 1
                _log.info(
                    "Phát hiện Couldn't sign in -> quay về login và thử lại (%s/%s)",
                    couldnt_reload_count, max_couldnt_retry,
                )
                driver.get(GOOGLE_LOGIN_URL)
                _wait_page_ready(driver, timeout=15.0, poll=0.5)
                if is_login_successful(driver) or _is_post_login_setup_page(driver):
                    _log.info("couldnt_sign_in: navigate về login nhưng đã login — skip")
                else:
                    try:
                        _submit_email(driver, acc.email)
                    except Exception as e:
                        _log.warning("couldnt_sign_in: submit email lỗi: %s", e)
                last_signature = ""
                stagnant_rounds = 0
                continue

            if page_kind == "recaptcha":
                if not ez_api_key:
                    return _return_status("captcha_fail")
                _log.info("Captcha detected -> solve EzCaptcha")
                _solve_recaptcha_ezcaptcha(driver, ez_api_key)
                time.sleep(1.2)
                last_signature = ""
                stagnant_rounds = 0
                continue

            if page_kind == "image_captcha":
                if not twocaptcha_api_key:
                    _log.warning("Image captcha detected nhưng chưa có 2captcha API key!")
                    return _return_status("captcha_fail")
                _log.info("Image captcha detected -> solve 2captcha")
                solved = _solve_image_captcha_2captcha(driver, twocaptcha_api_key)
                if not solved:
                    _log.warning("2captcha giải image captcha thất bại")
                time.sleep(1.2)
                last_signature = ""
                stagnant_rounds = 0
                continue

            if page_kind == "recovery_choice":
                if _select_recovery_email_option_if_present(driver, acc.recovery_email):
                    _log.info("Da tu dong chon recovery email option")
                    time.sleep(1.4)
                    last_signature = ""
                    stagnant_rounds = 0
                    continue

            if page_kind == "recovery_confirm":
                if acc.recovery_email:
                    ok = _handle_recovery_email(driver, acc.recovery_email)
                    if not ok:
                        new_kind = _detect_login_page_kind(driver)
                        if new_kind != "recovery_confirm" and new_kind != "unknown":
                            _log.info("RECOVERY_CONFIRM: handle fail nhưng trang đã chuyển sang %s — tiếp tục", new_kind)
                        else:
                            return _return_status("recovery_email_fail")
                else:
                    _click_skip_if_present(driver)
                    _click_not_now_if_present(driver)
                time.sleep(1.5)
                last_signature = ""
                stagnant_rounds = 0
                continue

            # Luôn bắt được 2FA ở mọi thời điểm, kể cả sau khi đã nhập password.
            if page_kind == "twofa_challenge":
                raw_2fa = (acc.two_fa or "").strip()
                if not raw_2fa:
                    _log.warning("2FA challenge xuất hiện nhưng account không có secret")
                    return _return_status("need_2fa")
                if raw_2fa.startswith("+") or raw_2fa.replace("-", "").replace(" ", "").isdigit():
                    _log.error("2FA: giá trị twofasecret='%s' là số điện thoại, không phải TOTP secret!", raw_2fa[:20])
                    return _return_status("2FA secret sai (phone?)")
                _handle_2fa(driver, raw_2fa)
                time.sleep(2)
                if _page_says_2fa_wrong(driver):
                    twofa_wrong_count += 1
                    _log.warning("2FA wrong code (%s/4)", twofa_wrong_count)
                    if twofa_wrong_count >= 4:
                        return _return_status("2FA code sai?")
                    if status_cb:
                        status_cb("2FA wrong, retry...")
                    time.sleep(4)
                else:
                    twofa_wrong_count = 0
                last_signature = ""
                stagnant_rounds = 0
                continue

            if page_kind == "phone_challenge":
                _log.info("Phát hiện PHONE challenge: dùng Hero-SMS để thuê số và nhập OTP")
                result = _handle_phone_number_challenge(
                    driver, hero_sms_api_key, hero_sms_service, hero_sms_get_number_retries,
                    phone=phone_challenge_phone,
                    activation_id=phone_challenge_activation_id,
                    rent_wait_sec=min(300, max(45, int(max(1, timeout_login)))),
                    otp_wait_sec=300,
                )
                phone_challenge_phone = result.get("phone", "") or phone_challenge_phone
                phone_challenge_activation_id = result.get("activation_id", "") or phone_challenge_activation_id
                if not result.get("ok"):
                    phone_fail_count += 1
                    _log.warning("PHONE_CHALLENGE fail (%s/3): %s", phone_fail_count, result.get("error", "unknown"))

                    if is_login_successful(driver) or _is_post_login_setup_page(driver):
                        _log.info("PHONE_CHALLENGE: fail nhưng phát hiện đã login thành công — bỏ qua retry")
                        phone_fail_count = 0
                        last_signature = ""
                        stagnant_rounds = 0
                        continue

                    phone_challenge_phone = ""
                    phone_challenge_activation_id = ""
                    if phone_fail_count >= 3:
                        return _return_status("phone_challenge_fail")

                    _log.info("PHONE_CHALLENGE: quay lại login và lấy số mới")
                    driver.get(GOOGLE_LOGIN_URL)
                    _wait_page_ready(driver, timeout=15.0, poll=0.5)

                    if is_login_successful(driver) or _is_post_login_setup_page(driver):
                        _log.info("PHONE_CHALLENGE: navigate về login nhưng đã login rồi — skip submit email")
                    else:
                        _submit_email(driver, acc.email)
                else:
                    phone_fail_count = 0
                    time.sleep(1.5)

                    if is_login_successful(driver) or _is_post_login_setup_page(driver):
                        _log.info("PHONE_CHALLENGE: OTP thành công, đã login — tiếp tục")

                last_signature = ""
                stagnant_rounds = 0
                continue

            if page_kind == "email_entry":
                _log.info("Dang o email entry -> submit email")
                _submit_email(driver, acc.email)
                last_signature = ""
                stagnant_rounds = 0
                continue

            if page_kind == "password_entry":
                if status_cb:
                    status_cb("Đang điền mật khẩu và bấm Next...")
                password_ok = False
                try:
                    _submit_password(driver, acc.password)
                    password_ok = True
                except Exception as e:
                    _log.warning("PASS_STEP_FAIL: _submit_password lỗi (%s), thử fallback", e)
                    try:
                        password_ok = _fill_password_and_submit(driver, acc.password)
                    except Exception:
                        password_ok = False
                if not password_ok:
                    time.sleep(1)
                    try:
                        from selenium.webdriver.common.by import By as _By
                        pw_still = any(
                            el.is_displayed() and el.is_enabled()
                            for el in driver.find_elements(_By.CSS_SELECTOR,
                                "input[name='Passwd'], input[name='password'], input[type='password']")
                        )
                    except Exception:
                        pw_still = False
                    if not pw_still:
                        _log.info("PASSWORD: input đã biến mất sau fail — trang đã chuyển, tiếp tục flow")
                        password_ok = True
                    else:
                        return _return_status("password_input_fail")
                last_signature = ""
                stagnant_rounds = 0
                continue

            # --- Không có handler match: rescan chủ động ---
            _click_not_now_if_present(driver)
            _click_skip_if_present(driver)

            # Rescan: chờ trang thay đổi rồi detect lại ngay
            if page_kind == "unknown":
                _log.debug("NO_ACTION: page unknown — chờ trang load thêm rồi rescan")
                for _rescan_i in range(3):
                    time.sleep(2.0)
                    _wait_page_ready(driver, timeout=4.0, poll=0.4)
                    rescan = _detect_login_page_kind(driver)
                    if rescan != "unknown":
                        _log.info("RESCAN: sau chờ %ds, detect được page=%s", (_rescan_i + 1) * 2, rescan)
                        page_kind = rescan
                        last_page_kind = ""
                        break
                if page_kind != "unknown":
                    stagnant_rounds = 0
                    last_signature = ""
                    continue

            sig = _page_signature()
            if sig == last_signature:
                stagnant_rounds += 1
            else:
                last_signature = sig
                stagnant_rounds = 0

            if stagnant_rounds in (4, 8):
                _log.warning("LOGIN_STALL: trang đứng yên %s vòng, thử thao tác phục hồi", stagnant_rounds)
                recovered = False
                try:
                    rescan = _detect_login_page_kind(driver)
                    if rescan != "unknown" and rescan != page_kind:
                        _log.info("STALL_RESCAN: detect lại được page=%s (trước đó %s)", rescan, page_kind)
                        last_signature = ""
                        stagnant_rounds = 0
                        continue
                    if _is_email_entry_page(driver):
                        _submit_email(driver, acc.email)
                        recovered = True
                    elif _is_password_challenge_page(driver):
                        _submit_password(driver, acc.password)
                        recovered = True
                    elif _is_recaptcha_page(driver) and ez_api_key:
                        _solve_recaptcha_ezcaptcha(driver, ez_api_key)
                        recovered = True
                    elif _is_image_captcha_page(driver) and twocaptcha_api_key:
                        _solve_image_captcha_2captcha(driver, twocaptcha_api_key)
                        recovered = True
                    else:
                        btn = _find_next_button(driver) or _find_submit_or_primary_button(driver)
                        if btn:
                            from human_click import human_move_and_click
                            human_move_and_click(driver, btn)
                            recovered = True
                except Exception:
                    recovered = False
                if recovered:
                    last_signature = ""
                    stagnant_rounds = 0
                    time.sleep(1.8)
                    continue

            if stagnant_rounds >= 12:
                if lag_reload_count >= 2:
                    _log.warning("LOGIN_STALL: đã reload tối đa nhưng vẫn kẹt")
                    return _return_status("login_stuck")
                _reload_login_from_stall("stagnant page")
                continue

            time.sleep(1.2)

    except Exception as e:
        _log.exception("run_login exception: %s", e)
        err_str = str(e).lower()
        if "session not created" in err_str or "invalid session id" in err_str:
            short_status = "error: session"
        elif "disconnect" in err_str or "connection" in err_str:
            short_status = "error: disconnect"
        elif "timeout" in err_str:
            short_status = "error: timeout"
        elif "captcha" in err_str:
            short_status = "captcha_fail"
        else:
            short_status = f"error: {str(e)[:40]}…" if len(str(e)) > 40 else f"error: {e}"
        if status_cb:
            status_cb(short_status)
        return {"status": short_status, "driver": driver, "user_data_dir": active_user_data_dir}
