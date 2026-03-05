# -*- coding: utf-8 -*-
"""
data_collector.py — Thu thập DOM snapshots + labels trong khi tool chạy bình thường.

Tích hợp vào google_flow.py: mỗi lần nhận diện được page → lưu mẫu.
Data lưu tại: training_data/dom_samples.jsonl

Format mỗi dòng:
{
    "timestamp": "...",
    "url": "...",
    "dom": {...},          # DOM snapshot từ ai_analyzer.collect_dom
    "label": "password_entry",
    "source": "url|dom|ai|manual",
    "confidence": 1.0
}
"""
from __future__ import annotations

import json
import logging
import threading
import time
from datetime import datetime
from pathlib import Path
from typing import Optional

_log = logging.getLogger(__name__)

TRAIN_DIR = Path(__file__).resolve().parent / "training_data"
DOM_SAMPLES_FILE = TRAIN_DIR / "dom_samples.jsonl"
SCREENSHOT_DIR = TRAIN_DIR / "screenshots"

_write_lock = threading.Lock()
_session_count = 0  # số mẫu đã thu thập trong session này

# Chỉ lưu các loại page này (bỏ qua unknown/loading)
COLLECTIBLE_LABELS = {
    "email_entry", "password_entry", "twofa_challenge", "phone_challenge",
    "image_captcha", "recaptcha", "recovery_confirm", "recovery_choice",
    "success", "password_changed", "wrong_password", "wrong_2fa", "wrong_phone",
    "need_verify_phone", "too_many_attempts", "account_disabled", "account_suspended",
    "account_not_found", "hard_block", "suspicious_activity",
    "wrong_recovery_email", "no_recovery_email",
}

# Giới hạn tối đa mẫu trùng lặp cùng label + URL pattern
_DEDUP_WINDOW = 50   # trong 50 mẫu gần nhất, không lưu trùng
_recent_keys: list[str] = []


def _ensure_dirs():
    TRAIN_DIR.mkdir(exist_ok=True)
    SCREENSHOT_DIR.mkdir(exist_ok=True)


def _dedup_key(label: str, dom: dict) -> str:
    """Key để check trùng: label + URL path + input structure."""
    path = (dom.get("path") or "").split("?")[0]
    inp = "|".join(sorted(
        f"{i.get('name','')}:{i.get('type','')}"
        for i in (dom.get("inputs") or [])
    ))
    err = "err" if dom.get("has_error") else "ok"
    return f"{label}|{path}|{inp}|{err}"


def collect_sample(
    driver,
    label: str,
    source: str = "dom",
    confidence: float = 1.0,
    save_screenshot: bool = False,
) -> bool:
    """
    Lưu 1 mẫu DOM + label vào dom_samples.jsonl.
    Trả True nếu lưu thành công, False nếu bỏ qua (trùng / label không hợp lệ).
    
    Tham số:
        driver:          Selenium WebDriver
        label:           Page type đã biết (vd: "password_entry")
        source:          Nguồn label: "url"=từ URL, "dom"=từ DOM rules, "ai"=từ Claude, "manual"=tay
        confidence:      Độ chắc chắn 0.0-1.0
        save_screenshot: Có lưu ảnh không (tốn disk, không dùng khi train DOM model)
    """
    global _session_count

    if label not in COLLECTIBLE_LABELS:
        return False
    if confidence < 0.75:
        return False

    try:
        from ai_analyzer import collect_dom
        dom = collect_dom(driver)
    except Exception:
        return False

    if not dom:
        return False

    # Dedup check
    key = _dedup_key(label, dom)
    with _write_lock:
        if key in _recent_keys:
            return False  # Trùng, bỏ qua
        _recent_keys.append(key)
        if len(_recent_keys) > _DEDUP_WINDOW:
            _recent_keys.pop(0)

    _ensure_dirs()
    ts = datetime.now().strftime("%Y%m%d_%H%M%S_%f")

    screenshot_path = None
    if save_screenshot:
        try:
            screenshot_path = str(SCREENSHOT_DIR / f"{ts}_{label}.png")
            driver.save_screenshot(screenshot_path)
        except Exception:
            screenshot_path = None

    record = {
        "timestamp": ts,
        "url": dom.get("url", ""),
        "dom": dom,
        "label": label,
        "source": source,
        "confidence": confidence,
        "screenshot": screenshot_path,
    }

    try:
        with _write_lock:
            with open(DOM_SAMPLES_FILE, "a", encoding="utf-8") as f:
                f.write(json.dumps(record, ensure_ascii=False) + "\n")
        _session_count += 1
        if _session_count % 10 == 0:
            _log.info("DATA_COLLECTOR: %d mẫu đã thu thập session này", _session_count)
        return True
    except Exception as e:
        _log.debug("DATA_COLLECTOR: lỗi ghi file: %s", e)
        return False


def get_stats() -> dict:
    """Thống kê số mẫu đã thu thập."""
    if not DOM_SAMPLES_FILE.exists():
        return {"total": 0, "by_label": {}}
    
    by_label: dict[str, int] = {}
    total = 0
    try:
        with open(DOM_SAMPLES_FILE, encoding="utf-8") as f:
            for line in f:
                try:
                    rec = json.loads(line)
                    lbl = rec.get("label", "unknown")
                    by_label[lbl] = by_label.get(lbl, 0) + 1
                    total += 1
                except Exception:
                    pass
    except Exception:
        pass
    return {"total": total, "by_label": by_label}


def print_stats():
    stats = get_stats()
    print(f"\n=== DATA COLLECTOR: {stats['total']} mẫu tổng ===")
    for lbl, cnt in sorted(stats["by_label"].items(), key=lambda x: -x[1]):
        bar = "█" * min(30, cnt // max(1, stats["total"] // 30))
        print(f"  {lbl:<35} {cnt:>5}  {bar}")
