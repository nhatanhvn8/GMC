# -*- coding: utf-8 -*-
from __future__ import annotations

import random
import string
import threading
import time
from collections import deque
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path
from typing import Any

from account_db import (
    _ACCOUNT_FIELDS, append_logrun, filter_accounts,
    load_accounts_db, save_accounts_db, sync_accounts_to_db_from_text,
    update_account_field,
)
from account_model import Account
from app_config import (
    CONFIG_DIR, DATA_DIR, EXPORT_DIR,
    LIST_MAIL_KP_FILE, LIST_PASS_FILE,
    load_hconfig, save_hconfig, load_tool_config,
    get_hconfig_bool, get_hconfig_int, get_hconfig_str,
)
from google_flow import (
    PAUSE_EVENT, LoginFailedError, No2FASecretError,
    change_password_flow, change_recovery_email_flow,
    close_current_tab_and_switch_to_blank, delete_phone_recovery_flow,
    ensure_2fa_authenticator_flow, ensure_logged_in, get_worker_pause_event,
    is_login_successful, remove_worker_pause_event, run_login,
)
from gpm_mode_patch import (
    unregister_driver, tile_browser_windows, set_browser_scale, get_browser_scale,
    set_use_proxy, get_use_proxy, set_single_proxy, load_proxy_list,
    set_no_save_profile, get_no_save_profile,
)
from ai_analyzer import set_anthropic_api_key
from logging_setup import log_exception, setup_logging

def _shorten_status(msg: str, max_len: int = 50) -> str:
    """Rút gọn status/error dài để không tràn cột."""
    s = str(msg or "").strip()
    if len(s) <= max_len:
        return s
    s_lower = s.lower()
    if "session not created" in s_lower or "invalid session id" in s_lower:
        return "error: session"
    if "invalid session" in s_lower:
        return "error: session"
    if "disconnect" in s_lower or "connection" in s_lower:
        return "error: disconnect"
    if "timeout" in s_lower:
        return "error: timeout"
    if "captcha" in s_lower:
        return "captcha_fail"
    return s[: max_len - 3] + "…" if len(s) > max_len else s


TASK_LOGIN_ONLY = "login_only"
TASK_ADD_2FA = "add_2fa"
TASK_CHANGE_PASS = "change_pass"
TASK_CHANGE_MAIL = "change_mail"
TASK_DELETE_PHONE = "delete_phone"
TASK_VERIFY_PHONE = "verify_phone"

_EXPORT_TAB_COLS = [
    "email", "password", "emailrecovery", "oldemail", "oldpassword", "oldemailrecovery",
    "datechangepass", "securityquestion", "securitycode", "securitycodeused",
    "phonerecovery", "checkphonerecovery", "birthday", "groups", "user",
    "datecreate", "updater", "device", "youtube", "youtubeverify", "youtubepre",
    "country", "googleadw", "googleadsense", "get2facode", "chplay",
    "checkreviewgooglemap", "paymentmethod", "googlevoice", "devicelogout",
    "displayname", "language", "proxy", "proxylist", "phonehide",
    "datedeletephone", "statusdeletephone", "checkhiddenphone",
    "statusconfirmsecurity", "googlemap", "useragent", "disableforwarding",
    "password_app", "checkgoogleadw", "createbrandaccountyoutube",
    "create_brand_account_youtube", "phone_verify", "disable_2fa", "browser",
    "checkemailrecovery", "checkclickcontinuegooglevoice",
    "deletealternativeemail", "addalternativeemail", "first_name", "last_name",
    "chuyentiepemail", "new_emailrecovery", "password_new_emailrecovery",
    "uploadavatar", "twofasecret",
]


def _format_record_tab(acc: dict[str, Any]) -> str:
    parts = []
    for col in _EXPORT_TAB_COLS:
        parts.append(str(acc.get(col, "") or ""))
    return "\t".join(parts)


def _auto_export_by_status(items: list[dict[str, Any]], logger=None):
    EXPORT_BASE = EXPORT_DIR
    EXPORT_BASE.mkdir(parents=True, exist_ok=True)
    now = datetime.now()
    folder_name = f"{now.day}-{now.month}-{now.year} {now.hour}h{now.minute}m{now.second}s"
    out_dir = EXPORT_BASE / folder_name
    out_dir.mkdir(parents=True, exist_ok=True)

    all_records, success_records, nosuccess_records = [], [], []
    login_ok_records, login_fail_records, norun_records = [], [], []

    for acc in items:
        record_line = _format_record_tab(acc)
        all_records.append(record_line)
        st = str(acc.get("status", "")).lower()

        if st in ("not_run", ""):
            norun_records.append(record_line)
        elif any(k in st for k in ["ok", "success", "done"]):
            success_records.append(record_line)
            login_ok_records.append(record_line)
        elif any(k in st for k in ["fail", "error", "disconnect", "timeout",
                                     "disabled", "suspended", "deleted",
                                     "wrong_password", "not_found", "restricted",
                                     "captcha", "rejected"]):
            nosuccess_records.append(record_line)
            login_fail_records.append(record_line)
        else:
            nosuccess_records.append(record_line)

    def _write(fp: Path, records: list[str]):
        if records:
            fp.write_text("\n".join(records), encoding="utf-8")

    _write(out_dir / f"ALL - {len(all_records)}.txt", all_records)
    if success_records:
        _write(out_dir / f"SUCCESS - {len(success_records)}.txt", success_records)
    if nosuccess_records:
        _write(out_dir / f"NOSUCCESS - {len(nosuccess_records)}.txt", nosuccess_records)
    if login_ok_records:
        _write(out_dir / f"LOGIN_OK - {len(login_ok_records)}.txt", login_ok_records)
    if login_fail_records:
        _write(out_dir / f"LOGIN_FAIL - {len(login_fail_records)}.txt", login_fail_records)
    if norun_records:
        _write(out_dir / f"NORUN - {len(norun_records)}.txt", norun_records)

    if logger:
        logger.info("Exported %d records to %s", len(all_records), out_dir)


STATUS_COLORS = {
    "not_run": "#888888",
    "running": "#2196F3",
    "login ok": "#4CAF50",
    "success": "#009688",
    "login_fail": "#E91E63",
    "fail": "#FF9800",
    "2fa_done": "#9C27B0",
    "bat_2fa_new ok": "#9C27B0",
    "bat2fa_new ok": "#9C27B0",
    "changeemailrecovery ok": "#00BCD4",
    "changeemailrecovery timeout": "#FF5722",
    "changepas ok": "#8BC34A",
    "error": "#F44336",
    "pending": "#FFC107",
    "disabled": "#9E9E9E",
    "suspended": "#795548",
    "deleted": "#607D8B",
    "password_changed": "#FF6E40",
    "wrong_password": "#FF5252",
    "wrong_2fa": "#FF1744",
    "wrong_phone": "#FF4081",
    "too_many_attempts": "#FF6D00",
    "need_verify_phone": "#FF9100",
    "suspicious_activity": "#AA00FF",
    "wrong_recovery_email": "#E65100",
    "no_recovery_email": "#BF360C",
    "not_found": "#BDBDBD",
    "restricted": "#FF6F00",
    "disconnect.login": "#E040FB",
    "login timeout": "#FF9800",
    "captcha_fail": "#FFD600",
    "password_changed": "#FF6E40",
    "session_expired": "#FF8F00",
    "edu_account": "#5C6BC0",
    "challenge_totp": "#26C6DA",
    "challenge_identity": "#26A69A",
    "challenge_phone": "#29B6F6",
    "challenge_device_prompt": "#42A5F5",
    "challenge_in_app": "#7E57C2",
    "challenge_app": "#AB47BC",
    "challenge_2fa_phone": "#EC407A",
    "change_mail_timeout": "#FF5722",
    "change_pass_fail": "#E53935",
    "cannot_get_phone": "#FF7043",
    "cannot_get_code": "#FF7043",
    "verify_ok_signin.rejected": "#AB47BC",
}

ALL_COLUMNS = ["email", "password", "emailrecovery", "twofasecret", "phonerecovery",
               "password_app", "country", "new_emailrecovery", "status", "logrun"]
DEFAULT_VISIBLE_COLS = ["email", "password", "emailrecovery", "twofasecret", "phonerecovery", "status"]
PAGE_SIZE = 200

_stop_flag = threading.Event()
_activity_log: deque[str] = deque(maxlen=5000)
_activity_lock = threading.Lock()


def _add_activity(msg: str):
    ts = datetime.now().strftime("%H:%M:%S")
    entry = f"[{ts}] {msg}"
    with _activity_lock:
        _activity_log.appendleft(entry)


def _random_from_list_file(path: Path) -> str:
    try:
        lines = [l.strip() for l in path.read_text(encoding="utf-8", errors="replace").splitlines() if l.strip()]
        return random.choice(lines) if lines else ""
    except Exception:
        return ""


def _phase_login_only(item, idx, total, status_cb, logger, on_db_changed):
    """Chỉ login, trả về (driver, item, login_status). Dùng cho chạy theo phase (nhiều luồng cùng task)."""
    email = str(item.get("email", ""))
    worker_id = f"w_{idx}"
    tab_label = f"Tab {idx + 1}"
    stop_check = lambda: _stop_flag.is_set()

    def _status(msg):
        full = f"{tab_label} {msg}"
        _add_activity(full)
        if status_cb:
            status_cb(email, msg, idx)

    def _logrun(msg):
        append_logrun([item], email, f"{datetime.now().strftime('%H:%M:%S')} {msg}")

    driver = None
    try:
        two_fa_raw = (str(item.get("twofasecret", "") or item.get("two_fa", "") or "")).strip()
        acc = Account(
            email=email,
            password=str(item.get("password", "")),
            recovery_email=str(item.get("emailrecovery", "") or item.get("recovery_email", "")),
            two_fa=two_fa_raw,
        )
        item["status"] = "running"
        item.pop("emailrecovery_change_status", None)
        _status(f"login {idx + 1}/{total}")
        _logrun("login start")
        on_db_changed()

        result = run_login(acc, status_cb=lambda msg: _status(f"login {msg}"), worker_id=worker_id, stop_check=stop_check)
        login_status = _shorten_status(result.get("status", "error"))
        driver = result.get("driver")
        _status(f"login {login_status}")
        _logrun(f"login {login_status}")

        if login_status != "login ok":
            item["status"] = login_status
            item.pop("emailrecovery_change_status", None)
            on_db_changed()
            return (driver, item, login_status)

        item["status"] = "login ok"
        on_db_changed()
        return (driver, item, login_status)
    except Exception as e:
        logger.exception("Phase login error for %s: %s", email, e)
        item["status"] = _shorten_status(f"error: {e}")
        on_db_changed()
        return (driver, item, item.get("status", "error"))


def _process_single_account(item, idx, total, tasks, status_cb, logger, on_db_changed):
    if _stop_flag.is_set():
        return
    email = str(item.get("email", ""))
    worker_id = f"w_{idx}"
    tab_label = f"Tab {idx + 1}"
    stop_check = lambda: _stop_flag.is_set()

    def _status(msg):
        full = f"{tab_label} {msg}"
        _add_activity(full)
        if status_cb:
            status_cb(email, msg, idx)

    def _logrun(msg):
        append_logrun([item], email, f"{datetime.now().strftime('%H:%M:%S')} {msg}")

    driver = None
    try:
        # Chỉ 1 field 2FA: twofasecret (two_fa chỉ fallback cho dữ liệu cũ, không ghi two_fa nữa)
        two_fa_raw = (str(item.get("twofasecret", "") or item.get("two_fa", "") or "")).strip()
        acc = Account(
            email=email,
            password=str(item.get("password", "")),
            recovery_email=str(item.get("emailrecovery", "") or item.get("recovery_email", "")),
            two_fa=two_fa_raw,
        )
        if two_fa_raw:
            _logrun(f"2FA secret loaded (len={len(two_fa_raw)})")
        else:
            logger.warning("Account %s: twofasecret empty — login sẽ báo need_2fa nếu Google yêu cầu 2FA", email)
            _logrun("2FA secret EMPTY — sẽ báo need_2fa nếu trang yêu cầu 2FA")

        # Ưu tiên kết quả lần chạy này: xóa cờ thành công từ lần chạy trước (nếu lần sau login/tiến trình fail thì không còn "change mail ok")
        item["status"] = "running"
        item.pop("emailrecovery_change_status", None)
        _status(f"login {idx + 1}/{total}")
        _logrun("login start")
        on_db_changed()

        result = run_login(
            acc,
            status_cb=lambda msg: _status(f"login {msg}"),
            worker_id=worker_id,
            stop_check=stop_check,
        )
        login_status = _shorten_status(result.get("status", "error"))
        driver = result.get("driver")

        _status(f"login {login_status}")
        _logrun(f"login {login_status}")

        if login_status != "login ok":
            item["status"] = login_status
            item.pop("emailrecovery_change_status", None)  # kết quả lần sau ưu tiên: không giữ "change ok" từ lần trước
            on_db_changed()
            _status(f"done - {login_status}")
            return

        item["status"] = "login ok"
        on_db_changed()

        if _stop_flag.is_set():
            item["status"] = "stopped"
            on_db_changed()
            return

        def _ensure_driver():
            nonlocal driver
            driver = ensure_logged_in(
                driver, acc,
                status_cb=lambda msg: _status(msg),
                worker_id=worker_id,
                stop_check=stop_check,
            )
            if driver is None:
                item["status"] = "relogin_fail"
                item.pop("emailrecovery_change_status", None)
                on_db_changed()
                return False
            return True

        hcfg = load_hconfig()

        if TASK_ADD_2FA in tasks:
            if not _ensure_driver():
                return
            _status("bat_2fa_new")
            _logrun("bat_2fa_new start")
            try:
                ok_2fa, secret = ensure_2fa_authenticator_flow(
                    driver, existing_secret=item.get("twofasecret", ""), worker_id=worker_id,
                    password=item.get("password", ""),
                )
                if ok_2fa and secret:
                    item["twofasecret"] = secret
                    item["twofasecret_status"] = "ok"
                    item["status"] = "bat_2fa_new ok"
                    _status("bat_2fa_new ok")
                    _logrun(f"bat_2fa_new ok secret={secret[:6]}...")
                    on_db_changed()
                elif ok_2fa:
                    _status("bat_2fa_new already_on")
                    _logrun("bat_2fa_new already_on")
            except Exception as e2fa:
                logger.exception("2FA error for %s: %s", email, e2fa)
                _logrun(f"bat_2fa_new error: {e2fa}")

            if _stop_flag.is_set():
                item["status"] = "stopped"
                on_db_changed()
                return
            if get_hconfig_bool(hcfg, "bat2fa_new_deleteallphone"):
                _status("delete_phone (after 2fa)")
                _logrun("delete_phone start (after 2fa)")
                try:
                    ok_del = delete_phone_recovery_flow(driver, worker_id=worker_id, current_password=item.get("password", ""), two_fa_secret=item.get("twofasecret", "") or item.get("two_fa", ""))
                    if ok_del:
                        item["phonerecovery"] = ""
                        item["datedeletephone"] = datetime.now().isoformat()
                        _logrun("delete_phone ok")
                except Exception as edp:
                    logger.exception("Delete phone error for %s: %s", email, edp)
                    _logrun(f"delete_phone error: {edp}")

        if TASK_VERIFY_PHONE in tasks:
            # Verify login only: không chạy flow thêm/sửa recovery phone.
            item["phone_verify"] = "login_ok"
            _status("verify_login ok")
            _logrun("verify_login ok (login only, no verify_phone flow)")
            on_db_changed()

            if _stop_flag.is_set():
                item["status"] = "stopped"
                on_db_changed()
                return
        if TASK_CHANGE_PASS in tasks:
            if not _ensure_driver():
                return
            _status("changepass")
            _logrun("changepass start")
            try:
                new_pass = _random_from_list_file(LIST_PASS_FILE)
                if not new_pass:
                    new_pass = "".join(random.choices(string.ascii_letters + string.digits, k=12))
                ok = change_password_flow(
                    driver, new_pass, worker_id=worker_id,
                    current_password=item.get("password", ""),
                    two_fa_secret=item.get("twofasecret", "") or item.get("two_fa", ""),
                )
                if ok:
                    item["oldpassword"] = item.get("password", "")
                    item["password"] = new_pass
                    item["new_password"] = new_pass
                    item["datechangepass"] = datetime.now().isoformat()
                    item["status"] = "changepas ok"
                    _status("changepass ok")
                    _logrun(f"changepass ok new={new_pass}")
                    if TASK_CHANGE_MAIL not in tasks and TASK_DELETE_PHONE not in tasks:
                        close_current_tab_and_switch_to_blank(driver)
                        _logrun("changepass xong, khong con nvu — da tat tab, chuyen sang tab trong")
                else:
                    _logrun("changepass fail")
                on_db_changed()
            except Exception:
                logger.exception("Change pass error for %s", email)
                _logrun("changepass error")

            if _stop_flag.is_set():
                item["status"] = "stopped"
                on_db_changed()
                return
        if TASK_CHANGE_MAIL in tasks:
            if not _ensure_driver():
                return
            _status("changeemailrecovery")
            _logrun("changeemailrecovery start")
            try:
                mail_kp = _random_from_list_file(LIST_MAIL_KP_FILE)
                if mail_kp:
                    new_email = mail_kp
                else:
                    base_name = email.split("@")[0]
                    suffix = random.randint(1000, 9999)
                    new_email = f"{base_name}{suffix}@hotmail.com"

                use_tm = get_hconfig_bool(hcfg, "use_temp_mail_recovery")
                base_tm = get_hconfig_str(hcfg, "temp_mail_base_url", "https://inboxesmail.app")
                result = change_recovery_email_flow(
                    driver, new_email, worker_id=worker_id,
                    current_password=item.get("password", ""),
                    two_fa_secret=item.get("twofasecret", "") or item.get("two_fa", ""),
                    use_temp_mail=use_tm, temp_mail_base_url=base_tm.strip() or "",
                )
                # result có thể là (bool, str) hoặc (str_error_code, str)
                ok = result[0] if isinstance(result, tuple) else result
                used_email = result[1] if isinstance(result, tuple) and len(result) > 1 else new_email
                if ok is True:
                    item["oldemailrecovery"] = item.get("emailrecovery", "")
                    item["emailrecovery"] = used_email or new_email
                    item["new_emailrecovery"] = used_email or new_email
                    item["emailrecovery_change_status"] = "ok"
                    item["status"] = "changeEmailRecovery ok"
                    _status("changeemailrecovery ok")
                    _logrun(f"changeemailrecovery ok new={new_email}")
                    if TASK_DELETE_PHONE not in tasks:
                        close_current_tab_and_switch_to_blank(driver)
                        _logrun("doi mail KP xong, khong con nvu — da tat tab, chuyen sang tab trong")
                elif ok == "no_recovery_email":
                    item["status"] = "no_recovery_email"
                    _status("no_recovery_email")
                    _logrun("khong co email khoi phuc — no_recovery_email")
                elif ok == "wrong_recovery_email":
                    item["status"] = "wrong_recovery_email"
                    _status("wrong_recovery_email")
                    _logrun("email khoi phuc sai — wrong_recovery_email")
                else:
                    item["status"] = "changeEmailRecovery Timeout"
                    _status("changeemailrecovery timeout")
                    _logrun("changeemailrecovery timeout")
                on_db_changed()
            except Exception:
                logger.exception("Change recovery email error for %s", email)
                _logrun("changeemailrecovery error")

            if _stop_flag.is_set():
                item["status"] = "stopped"
                on_db_changed()
                return
        if TASK_DELETE_PHONE in tasks and TASK_ADD_2FA not in tasks:
            if not _ensure_driver():
                return
            _status("delete_phone")
            _logrun("delete_phone start")
            try:
                delete_phone_recovery_flow(driver, worker_id=worker_id, current_password=item.get("password", ""), two_fa_secret=item.get("twofasecret", "") or item.get("two_fa", ""))
                item["phonerecovery"] = ""
                item["datedeletephone"] = datetime.now().isoformat()
                _logrun("delete_phone ok")
                close_current_tab_and_switch_to_blank(driver)
                _logrun("xoa phone xong — da tat tab, chuyen sang tab trong")
            except Exception:
                logger.exception("Delete phone error for %s", email)
                _logrun("delete_phone error")

        _status("done1")
        time.sleep(0.5)
        _status("done2")
        time.sleep(0.5)
        _status("done3")
        _logrun("done")

        on_db_changed()
        _add_activity(f"Run done 1 account")

    except Exception as e:
        logger.exception("Process error for %s: %s", email, e)
        item["status"] = _shorten_status(f"error: {e}")
        _logrun(f"error: {e}")
        on_db_changed()
    finally:
        if driver:
            try:
                unregister_driver(driver)
            except Exception:
                pass
            try:
                driver.quit()
            except Exception:
                pass
        remove_worker_pause_event(worker_id)


def _run_one_task_phase(driver, item, idx, task_name, tasks, total, status_cb, logger, on_db_changed):
    """Chạy đúng một nhiệm vụ (2FA / verify_login / changepass / change_mail / delete_phone) cho (driver, item). Cập nhật item tại chỗ."""
    if not driver:
        return
    email = str(item.get("email", ""))
    worker_id = f"w_{idx}"
    tab_label = f"Tab {idx + 1}"
    stop_check = lambda: _stop_flag.is_set()
    two_fa_raw = (str(item.get("twofasecret", "") or item.get("two_fa", "") or "")).strip()
    acc = Account(email=email, password=str(item.get("password", "")), recovery_email=str(item.get("emailrecovery", "") or item.get("recovery_email", "")), two_fa=two_fa_raw)

    def _status(msg):
        _add_activity(f"{tab_label} {msg}")
        if status_cb:
            status_cb(email, msg, idx)

    def _logrun(msg):
        append_logrun([item], email, f"{datetime.now().strftime('%H:%M:%S')} {msg}")

    def _ensure_driver():
        nonlocal driver
        driver = ensure_logged_in(driver, acc, status_cb=lambda m: _status(m), worker_id=worker_id, stop_check=stop_check)
        if driver is None:
            item["status"] = "relogin_fail"
            item.pop("emailrecovery_change_status", None)
            on_db_changed()
            return False
        return True

    hcfg = load_hconfig()
    try:
        if task_name == TASK_ADD_2FA:
            if not _ensure_driver():
                return
            _status("bat_2fa_new")
            try:
                ok_2fa, secret = ensure_2fa_authenticator_flow(driver, existing_secret=item.get("twofasecret", ""), worker_id=worker_id, password=item.get("password", ""))
                if ok_2fa and secret:
                    item["twofasecret"] = secret
                    item["twofasecret_status"] = "ok"
                    item["status"] = "bat_2fa_new ok"
                    _status("bat_2fa_new ok")
                    on_db_changed()
                elif ok_2fa:
                    _status("bat_2fa_new already_on")
                if get_hconfig_bool(hcfg, "bat2fa_new_deleteallphone"):
                    delete_phone_recovery_flow(driver, worker_id=worker_id, current_password=item.get("password", ""), two_fa_secret=item.get("twofasecret", "") or item.get("two_fa", ""))
                    item["phonerecovery"] = ""
            except Exception as e2fa:
                logger.exception("2FA error for %s: %s", email, e2fa)
        elif task_name == TASK_VERIFY_PHONE:
            item["phone_verify"] = "login_ok"
            _status("verify_login ok")
            on_db_changed()
        elif task_name == TASK_CHANGE_PASS:
            if not _ensure_driver():
                return
            _status("changepass")
            try:
                new_pass = _random_from_list_file(LIST_PASS_FILE) or "".join(random.choices(string.ascii_letters + string.digits, k=12))
                ok = change_password_flow(driver, new_pass, worker_id=worker_id, current_password=item.get("password", ""), two_fa_secret=item.get("twofasecret", "") or item.get("two_fa", ""))
                if ok:
                    item["oldpassword"] = item.get("password", "")
                    item["password"] = item["new_password"] = new_pass
                    item["datechangepass"] = datetime.now().isoformat()
                    item["status"] = "changepas ok"
                    _status("changepass ok")
                    on_db_changed()
            except Exception:
                logger.exception("Change pass error for %s", email)
        elif task_name == TASK_CHANGE_MAIL:
            if not _ensure_driver():
                return
            _status("changeemailrecovery")
            try:
                new_email = _random_from_list_file(LIST_MAIL_KP_FILE) or f"{email.split('@')[0]}{random.randint(1000,9999)}@hotmail.com"
                base_tm = get_hconfig_str(hcfg, "temp_mail_base_url", "https://inboxesmail.app")
                ok, used_email = change_recovery_email_flow(driver, new_email, worker_id=worker_id, current_password=item.get("password", ""), two_fa_secret=item.get("twofasecret", "") or item.get("two_fa", ""), use_temp_mail=True, temp_mail_base_url=base_tm.strip() or "")
                if ok:
                    item["oldemailrecovery"] = item.get("emailrecovery", "")
                    item["emailrecovery"] = item["new_emailrecovery"] = used_email or new_email
                    item["emailrecovery_change_status"] = "ok"
                    item["status"] = "changeEmailRecovery ok"
                    _status("changeemailrecovery ok")
                else:
                    item["status"] = "changeEmailRecovery Timeout"
                    _status("changeemailrecovery timeout")
                on_db_changed()
            except Exception:
                logger.exception("Change recovery email error for %s", email)
        elif task_name == TASK_DELETE_PHONE:
            if not _ensure_driver():
                return
            _status("delete_phone")
            try:
                delete_phone_recovery_flow(driver, worker_id=worker_id, current_password=item.get("password", ""), two_fa_secret=item.get("twofasecret", "") or item.get("two_fa", ""))
                item["phonerecovery"] = ""
                item["datedeletephone"] = datetime.now().isoformat()
                _logrun("delete_phone ok")
            except Exception:
                logger.exception("Delete phone error for %s", email)
    except Exception:
        pass
    if _stop_flag.is_set():
        item["status"] = "stopped"
        on_db_changed()


def _run_accounts_queue(queue_items, tasks, status_cb, logger, on_db_changed, max_workers=1):
    total = len(queue_items)
    _stop_flag.clear()
    _add_activity(f"Run start {total} account(s), threads={max_workers}")

    if max_workers == 1:
        # 1 luồng: tuần tự từng acc
        for idx, item in enumerate(queue_items):
            if _stop_flag.is_set():
                _add_activity("Stopped by user, no more accounts")
                break
            _process_single_account(item, idx, total, tasks, status_cb, logger, on_db_changed)
    else:
        # Batch mode: chia thành từng nhóm max_workers acc.
        # Chờ TẤT CẢ acc trong nhóm xong hết mới mở nhóm tiếp → không có tab N+1 khi tab 1..N còn chạy.
        for batch_start in range(0, total, max_workers):
            if _stop_flag.is_set():
                _add_activity("Stopped by user, no more accounts")
                break

            batch = queue_items[batch_start: batch_start + max_workers]
            batch_num = batch_start // max_workers + 1
            total_batches = (total + max_workers - 1) // max_workers
            _add_activity(f"Batch {batch_num}/{total_batches}: {len(batch)} account(s)")

            with ThreadPoolExecutor(max_workers=len(batch)) as pool:
                futures = {
                    pool.submit(
                        _process_single_account,
                        item, batch_start + i, total, tasks,
                        status_cb, logger, on_db_changed,
                    ): item
                    for i, item in enumerate(batch)
                }
                for f in as_completed(futures):
                    try:
                        f.result()
                    except Exception as exc:
                        email = futures[f].get("email", "?")
                        logger.exception("Worker exception for %s: %s", email, exc)
            # Toàn bộ batch xong → vòng lặp tiếp mới chạy batch sau

    on_db_changed()
    _add_activity(f"Run done {total} account(s)")
    logger.info("Run done %d account(s)", total)


def run_gui():
    import tkinter as tk
    import tkinter.ttk as ttk
    from tkinter import filedialog, messagebox, scrolledtext
    import logging

    logger = logging.getLogger(__name__)

    # Đọc version
    try:
        from pathlib import Path as _Path
        _ver = (_Path(__file__).resolve().parent / "VERSION").read_text(encoding="utf-8").strip()
    except Exception:
        _ver = "?"

    root = tk.Tk()
    root.title(f"Gmail Tool  v{_ver}")

    # Mở tối đa hóa (phù hợp mọi màn hình, không bị cắt)
    sw = root.winfo_screenwidth()
    sh = root.winfo_screenheight()
    # Đặt geometry trước khi maximize để fallback hợp lý
    init_w = min(sw, 1600)
    init_h = min(sh, 980)
    root.geometry(f"{init_w}x{init_h}+0+0")
    root.minsize(1100, 700)
    try:
        root.state("zoomed")   # Windows: mở toàn màn hình ngay
    except Exception:
        root.attributes("-zoomed", True)  # Linux fallback

    style = ttk.Style()
    try:
        style.theme_use("clam")
    except Exception:
        pass

    hcfg = load_hconfig()

    var_changepass = tk.BooleanVar(value=get_hconfig_bool(hcfg, "check_changepass"))
    var_changemail = tk.BooleanVar(value=get_hconfig_bool(hcfg, "check_changeemailrecovery"))
    var_deletephone = tk.BooleanVar(value=get_hconfig_bool(hcfg, "check_deletephonerecovery"))
    var_verifyphone = tk.BooleanVar(value=get_hconfig_bool(hcfg, "check_verifyphone"))
    var_bat2fa = tk.BooleanVar(value=get_hconfig_bool(hcfg, "bat2fa_new"))
    var_bat2fa_delphone = tk.BooleanVar(value=get_hconfig_bool(hcfg, "bat2fa_new_deleteallphone"))

    var_numthread = tk.StringVar(value=get_hconfig_str(hcfg, "text_numthread", "3"))
    var_proxy = tk.StringVar(value=get_hconfig_str(hcfg, "proxy_active"))
    var_use_proxy = tk.BooleanVar(value=get_hconfig_bool(hcfg, "use_proxy", True))
    set_use_proxy(var_use_proxy.get())
    var_no_save_profile = tk.BooleanVar(value=get_hconfig_bool(hcfg, "no_save_profile", False))
    set_no_save_profile(var_no_save_profile.get())
    # Đồng bộ proxy single + load proxy list ngay khi khởi động
    set_single_proxy(get_hconfig_str(hcfg, "proxy_active"))
    load_proxy_list()
    var_ezcaptcha = tk.StringVar(value=get_hconfig_str(hcfg, "text_dialog_recaptcha_apikey"))
    var_2captcha = tk.StringVar(value=get_hconfig_str(hcfg, "text_dialog_2captcha_apikey"))
    var_anthropic = tk.StringVar(value=get_hconfig_str(hcfg, "anthropic_api_key"))
    set_anthropic_api_key(var_anthropic.get())
    var_hero_key = tk.StringVar(value=get_hconfig_str(hcfg, "text_dialog_otp_apikey"))
    var_hero_site = tk.StringVar(value=get_hconfig_str(hcfg, "text_dialog_otp_site", "hero-sms.com"))
    var_hero_service = tk.StringVar(value=get_hconfig_str(hcfg, "text_dialog_otp_serviceid"))
    var_hero_retries = tk.StringVar(value=get_hconfig_str(hcfg, "text_dialog_otp_times_try", "3"))
    var_use_temp_mail = tk.BooleanVar(value=get_hconfig_bool(hcfg, "use_temp_mail_recovery"))
    var_temp_mail_base_url = tk.StringVar(value=get_hconfig_str(hcfg, "temp_mail_base_url", "https://inboxesmail.app"))
    _raw_browser = get_hconfig_str(hcfg, "text_optionbrowser", "gpmlogin115off")
    _browser_norm = {"gpmlogin115off": "browser-gpm142", "gpm142": "browser-gpm142"}.get(
        _raw_browser.strip().lower(), _raw_browser
    )
    _VALID_BROWSERS = (
        "browser-gpm142", "chrome-incognito",
        "gologin-139", "gologin-143",
    )
    if not _browser_norm or _browser_norm not in _VALID_BROWSERS:
        _browser_norm = "browser-gpm142"
    var_browser = tk.StringVar(value=_browser_norm)
    var_timeoutlogin = tk.StringVar(value=get_hconfig_str(hcfg, "timeoutlogin", "300"))
    var_delay = tk.StringVar(value=get_hconfig_str(hcfg, "delay", "100"))

    vis_str = get_hconfig_str(hcfg, "visible_columns")
    visible_cols = [c.strip() for c in vis_str.split(",") if c.strip()] if vis_str else list(DEFAULT_VISIBLE_COLS)
    if "twofasecret" not in visible_cols:
        visible_cols = list(visible_cols)
        idx = visible_cols.index("emailrecovery") + 1 if "emailrecovery" in visible_cols else 1
        visible_cols.insert(idx, "twofasecret")
    # Chỉ hiển thị 1 cột 2FA: bỏ two_fa nếu đã có twofasecret (dữ liệu dùng chung, cập nhật thay thế một chỗ)
    if "twofasecret" in visible_cols and "two_fa" in visible_cols:
        visible_cols = [c for c in visible_cols if c != "two_fa"]

    all_items: list[dict[str, Any]] = load_accounts_db()
    filtered_items: list[dict[str, Any]] = list(all_items)
    page_idx = [0]
    is_running = [False]

    # ── Tab bar + controls (sắp xếp tab & scale) ──
    tab_control_frame = ttk.Frame(root)
    tab_control_frame.pack(fill="x", padx=5, pady=(5, 0))

    notebook = ttk.Notebook(root)
    notebook.pack(fill="both", expand=True, padx=5, pady=(0, 5))

    tab_dashboard = ttk.Frame(notebook)
    tab_settings = ttk.Frame(notebook)
    tab_logs = ttk.Frame(notebook)
    notebook.add(tab_dashboard, text="Dashboard")
    notebook.add(tab_settings, text="Settings")
    notebook.add(tab_logs, text="Activity Log")

    # ── Nút sắp xếp tab + Scale ──
    tab_btn_frame = ttk.Frame(tab_control_frame)
    tab_btn_frame.pack(side="left", padx=2)

    _global_scale = [100]

    def _move_tab(direction: int):
        cur = notebook.index("current")
        total = notebook.index("end")
        new_pos = cur + direction
        if new_pos < 0 or new_pos >= total:
            return
        tab_id = notebook.tabs()[cur]
        notebook.insert(new_pos, tab_id)
        notebook.select(new_pos)

    def _set_font_recursive(widget, size):
        try:
            current_font = widget.cget("font")
            if current_font:
                if isinstance(current_font, tuple):
                    family = current_font[0] if current_font else "TkDefaultFont"
                    widget.config(font=(family, size))
                elif isinstance(current_font, str) and current_font:
                    widget.config(font=(current_font, size))
                else:
                    widget.config(font=("TkDefaultFont", size))
        except Exception:
            pass
        for child in widget.winfo_children():
            _set_font_recursive(child, size)

    def _apply_scale_all(pct: int):
        _global_scale[0] = pct
        base_font_size = 9
        new_size = max(6, int(base_font_size * pct / 100))
        lbl_scale_val.config(text=f"{pct}%")

        for frame in (tab_dashboard, tab_settings, tab_logs):
            _set_font_recursive(frame, new_size)
        try:
            log_text.config(font=("Consolas", new_size))
        except Exception:
            pass
        try:
            status_text.config(font=("Consolas", new_size))
        except Exception:
            pass

    def _scale_up():
        _apply_scale_all(min(200, _global_scale[0] + 10))

    def _scale_down():
        _apply_scale_all(max(50, _global_scale[0] - 10))

    def _scale_reset():
        _apply_scale_all(100)

    ttk.Label(tab_btn_frame, text="Tab:").pack(side="left", padx=(0, 2))
    ttk.Button(tab_btn_frame, text="◀", width=3, command=lambda: _move_tab(-1)).pack(side="left", padx=1)
    ttk.Button(tab_btn_frame, text="▶", width=3, command=lambda: _move_tab(1)).pack(side="left", padx=1)
    ttk.Separator(tab_btn_frame, orient="vertical").pack(side="left", fill="y", padx=6)
    ttk.Label(tab_btn_frame, text="Zoom:").pack(side="left", padx=(0, 2))
    ttk.Button(tab_btn_frame, text="−", width=3, command=_scale_down).pack(side="left", padx=1)
    lbl_scale_val = ttk.Label(tab_btn_frame, text="100%", width=5, anchor="center")
    lbl_scale_val.pack(side="left", padx=1)
    ttk.Button(tab_btn_frame, text="+", width=3, command=_scale_up).pack(side="left", padx=1)
    ttk.Button(tab_btn_frame, text="Reset", width=5, command=_scale_reset).pack(side="left", padx=2)

    # Right-click menu trên tab
    _tab_menu = tk.Menu(root, tearoff=0)
    _tab_menu.add_command(label="◀ Move Left", command=lambda: _move_tab(-1))
    _tab_menu.add_command(label="▶ Move Right", command=lambda: _move_tab(1))
    _tab_menu.add_separator()
    _tab_menu.add_command(label="Zoom In (+10%)", command=_scale_up)
    _tab_menu.add_command(label="Zoom Out (−10%)", command=_scale_down)
    _tab_menu.add_command(label="Reset Zoom (100%)", command=_scale_reset)

    def _on_tab_right_click(event):
        try:
            clicked = notebook.index(f"@{event.x},{event.y}")
            notebook.select(clicked)
            _tab_menu.tk_popup(event.x_root, event.y_root)
        except Exception:
            pass

    notebook.bind("<Button-3>", _on_tab_right_click)

    # ── Dashboard ──
    f_filter = ttk.Frame(tab_dashboard)
    f_filter.pack(fill="x", padx=5, pady=3)

    ttk.Label(f_filter, text="Search:").pack(side="left")
    var_search = tk.StringVar()
    ent_search = ttk.Entry(f_filter, textvariable=var_search, width=25)
    ent_search.pack(side="left", padx=5)

    ttk.Label(f_filter, text="Status:").pack(side="left", padx=(10, 0))
    var_status_filter = tk.StringVar(value="all")
    statuses = ["all", "not_run", "running", "login ok", "login_fail", "success",
                "fail", "error", "bat_2fa_new ok", "changeEmailRecovery ok",
                "changeEmailRecovery Timeout", "changepas ok",
                "disabled", "suspended", "wrong_password", "not_found",
                "disconnect.login", "login timeout", "verify_ok_signin.rejected"]
    cb_status = ttk.Combobox(f_filter, textvariable=var_status_filter, values=statuses, width=25, state="readonly")
    cb_status.pack(side="left", padx=5)

    lbl_count = ttk.Label(f_filter, text="0 accounts")
    lbl_count.pack(side="right", padx=10)

    cols_for_tree = visible_cols
    tree = ttk.Treeview(tab_dashboard, columns=cols_for_tree, show="headings", selectmode="extended")
    _screen_w = root.winfo_screenwidth()
    _col_scale = max(1.0, _screen_w / 1280)  # scale lên theo màn hình
    for col in cols_for_tree:
        tree.heading(col, text="2FA" if col == "twofasecret" else col)
        w = 100
        if col == "email":
            w = int(260 * _col_scale)
        elif col in ("password", "emailrecovery", "twofasecret", "new_emailrecovery"):
            w = int(190 * _col_scale)
        elif col == "logrun":
            w = int(280 * _col_scale)
        elif col == "status":
            w = int(240 * _col_scale)
        else:
            w = int(100 * _col_scale)
        tree.column(col, width=w, minwidth=50)
    tree.pack(fill="both", expand=True, padx=5, pady=3)

    scroll_y = ttk.Scrollbar(tab_dashboard, orient="vertical", command=tree.yview)
    tree.configure(yscrollcommand=scroll_y.set)
    scroll_y.place(relx=1.0, rely=0, relheight=1.0, anchor="ne")

    f_page = ttk.Frame(tab_dashboard)
    f_page.pack(fill="x", padx=5, pady=2)
    lbl_page = ttk.Label(f_page, text="Page 1")
    lbl_page.pack(side="left")

    def _refresh_table():
        nonlocal filtered_items
        filtered_items = filter_accounts(all_items, var_search.get(), var_status_filter.get())
        lbl_count.config(text=f"{len(filtered_items)} accounts")
        start = page_idx[0] * PAGE_SIZE
        end = start + PAGE_SIZE
        page_items = filtered_items[start:end]

        STATUS_DISPLAY_LEN = 45
        tree.delete(*tree.get_children())
        for it in page_items:
            vals = []
            for col in cols_for_tree:
                v = it.get(col, "")
                # Chỉ 1 field 2FA: twofasecret (fallback two_fa cho dữ liệu cũ)
                if col == "twofasecret":
                    v = it.get("twofasecret") or it.get("two_fa", "")
                if col == "password" and v:
                    v = str(v)[:3] + "***"
                if col == "logrun" and v:
                    lines = str(v).split("\n")
                    v = lines[-1] if lines else ""
                if col == "status" and v and len(str(v)) > STATUS_DISPLAY_LEN:
                    v = str(v)[: STATUS_DISPLAY_LEN - 1] + "…"
                vals.append(str(v) if v else "")
            iid = tree.insert("", "end", values=vals)
            st = str(it.get("status", "")).lower()
            tag = st.replace(" ", "_")
            color = STATUS_COLORS.get(st, "")
            if not color and st.startswith("error"):
                color = STATUS_COLORS.get("error", "#F44336")
            if color:
                tree.tag_configure(tag, foreground=color)
                tree.item(iid, tags=(tag,))

        total_pages = max(1, (len(filtered_items) + PAGE_SIZE - 1) // PAGE_SIZE)
        lbl_page.config(text=f"Page {page_idx[0] + 1}/{total_pages}")

    def _prev_page():
        if page_idx[0] > 0:
            page_idx[0] -= 1
            _refresh_table()

    def _next_page():
        total_pages = max(1, (len(filtered_items) + PAGE_SIZE - 1) // PAGE_SIZE)
        if page_idx[0] < total_pages - 1:
            page_idx[0] += 1
            _refresh_table()

    ttk.Button(f_page, text="< Prev", command=_prev_page).pack(side="left", padx=5)
    ttk.Button(f_page, text="Next >", command=_next_page).pack(side="left", padx=5)

    var_search.trace_add("write", lambda *_: _refresh_table())
    var_status_filter.trace_add("write", lambda *_: _refresh_table())
    cb_status.bind("<<ComboboxSelected>>", lambda _: _refresh_table())

    # ── Toolbar ──
    f_toolbar = ttk.Frame(tab_dashboard)
    f_toolbar.pack(fill="x", padx=5, pady=3)

    def _get_selected_items():
        sel = tree.selection()
        if not sel:
            return []
        start = page_idx[0] * PAGE_SIZE
        children = tree.get_children()
        items_sel = []
        for iid in sel:
            idx_in_page = children.index(iid)
            real_idx = start + idx_in_page
            if real_idx < len(filtered_items):
                items_sel.append(filtered_items[real_idx])
        return items_sel

    def _select_all():
        for iid in tree.get_children():
            tree.selection_add(iid)

    def _deselect_all():
        tree.selection_remove(*tree.get_children())

    def _select_not_run():
        for iid in tree.get_children():
            vals = tree.item(iid, "values")
            st_idx = cols_for_tree.index("status") if "status" in cols_for_tree else -1
            if st_idx >= 0 and st_idx < len(vals) and vals[st_idx].lower() in ("not_run", ""):
                tree.selection_add(iid)

    def _delete_selected():
        nonlocal all_items
        sel_items = _get_selected_items()
        if not sel_items:
            messagebox.showwarning("Delete", "Chưa chọn account nào.")
            return
        if not messagebox.askyesno(
            "Xác nhận xóa",
            f"Xóa {len(sel_items)} account khỏi DB?\nHành động này không thể hoàn tác.",
        ):
            return
        emails_to_del = {str(it.get("email", "")).lower() for it in sel_items}
        all_items = [a for a in all_items if a.get("email", "").lower() not in emails_to_del]
        save_accounts_db(all_items)
        # Reset page về cuối hợp lệ nếu page hiện tại vượt ra ngoài
        total_pages = max(1, (len(all_items) + PAGE_SIZE - 1) // PAGE_SIZE)
        if page_idx[0] >= total_pages:
            page_idx[0] = total_pages - 1
        _refresh_table()
        _add_activity(f"Đã xóa {len(emails_to_del)} account")

    def _do_import():
        fp = filedialog.askopenfilename(filetypes=[("Text files", "*.txt"), ("All", "*.*")])
        if not fp:
            return
        nonlocal all_items
        text = Path(fp).read_text(encoding="utf-8", errors="replace")
        all_items = sync_accounts_to_db_from_text(text)
        _refresh_table()
        messagebox.showinfo("Import", f"Imported. Total: {len(all_items)} accounts")

    def _do_export():
        _auto_export_by_status(all_items, logger)
        messagebox.showinfo("Export", f"Exported to {EXPORT_DIR}")

    def _do_export_selected():
        sel_items = _get_selected_items()
        if not sel_items:
            messagebox.showwarning("Export", "No accounts selected")
            return
        _auto_export_by_status(sel_items, logger)
        messagebox.showinfo("Export", f"Exported {len(sel_items)} accounts to {EXPORT_DIR}")

    def _do_run():
        if is_running[0]:
            messagebox.showwarning("Running", "Already running!")
            return

        sel_items = _get_selected_items()
        queue = sel_items if sel_items else list(filtered_items)

        if not queue:
            messagebox.showwarning("No accounts", "No accounts to run")
            return

        tasks = set()
        if var_bat2fa.get():
            tasks.add(TASK_ADD_2FA)
        if var_changepass.get():
            tasks.add(TASK_CHANGE_PASS)
        if var_changemail.get():
            tasks.add(TASK_CHANGE_MAIL)
        if var_deletephone.get():
            tasks.add(TASK_DELETE_PHONE)
        if var_verifyphone.get():
            tasks.add(TASK_VERIFY_PHONE)
        if not tasks:
            tasks.add(TASK_LOGIN_ONLY)

        # 3 tiến trình mặc định; tối đa 250 luồng (giới hạn port debug)
        max_w = max(1, min(250, int(var_numthread.get() or 3)))
        is_running[0] = True

        # Hủy debounce đang chờ (nếu có) rồi save ngay — tránh proxy cũ ghi đè sau khi Start
        if _save_debounce_id[0]:
            root.after_cancel(_save_debounce_id[0])
            _save_debounce_id[0] = None
        _do_save()

        # Hiển thị đủ tiến trình: mỗi worker (tab) một dòng — xong hoặc timeout đều hiện
        worker_statuses: dict[int, tuple[str, str]] = {}

        def _update_status_display():
            lines = [f"[{i + 1}/{len(queue)}] {e}: {m}" for i, (e, m) in sorted(worker_statuses.items())]
            text = "\n".join(lines) if lines else "—"
            try:
                status_text.config(state="normal")
                status_text.delete("1.0", "end")
                status_text.insert("1.0", text)
                status_text.config(state="disabled")
            except Exception:
                pass

        def _status_cb(email_addr, msg, idx_item):
            worker_statuses[idx_item] = (email_addr, msg)
            try:
                root.after(0, lambda: (_update_status_display(), _refresh_table()))
            except Exception:
                pass

        def _on_db_changed():
            save_accounts_db(all_items)
            try:
                root.after(0, _refresh_table)
            except Exception:
                pass

        def status_text_insert_end(s: str):
            try:
                status_text.config(state="normal")
                status_text.insert("end", s)
                status_text.see("end")
                status_text.config(state="disabled")
            except Exception:
                pass

        def _worker():
            try:
                _run_accounts_queue(queue, tasks, _status_cb, logger, _on_db_changed, max_w)
            finally:
                is_running[0] = False
                try:
                    root.after(0, _update_status_display)
                    root.after(0, lambda: status_text_insert_end(f"\n—— Done {len(queue)} account(s) ——"))
                    root.after(0, _refresh_table)
                except Exception:
                    pass

        threading.Thread(target=_worker, daemon=True).start()

    def _do_stop():
        _stop_flag.set()
        is_running[0] = False
        try:
            status_text.config(state="normal")
            status_text.insert("end", "\n—— STOPPED by user ——")
            status_text.see("end")
            status_text.config(state="disabled")
        except Exception:
            pass
        _add_activity("STOPPED by user")

    def _do_pause():
        if PAUSE_EVENT.is_set():
            PAUSE_EVENT.clear()
            btn_pause.config(text="Pause")
            _add_activity("RESUMED")
        else:
            PAUSE_EVENT.set()
            btn_pause.config(text="Resume")
            _add_activity("PAUSED")

    ttk.Button(f_toolbar, text="Select All", command=_select_all).pack(side="left", padx=2)
    ttk.Button(f_toolbar, text="Not Run", command=_select_not_run).pack(side="left", padx=2)
    ttk.Button(f_toolbar, text="Deselect", command=_deselect_all).pack(side="left", padx=2)
    ttk.Button(f_toolbar, text="Delete Sel", command=_delete_selected).pack(side="left", padx=2)

    def _delete_by_status():
        nonlocal all_items
        # Thu thập các status hiện có trong DB
        existing_statuses = sorted({
            str(it.get("status", "not_run") or "not_run").lower()
            for it in all_items
        })
        if not existing_statuses:
            messagebox.showinfo("Delete by Status", "Không có account nào trong DB.")
            return

        # Popup chọn status
        dlg = tk.Toplevel(root)
        dlg.title("Xóa theo Status")
        dlg.resizable(False, False)
        dlg.grab_set()

        ttk.Label(dlg, text="Chọn status muốn xóa:").pack(padx=15, pady=(12, 4))

        listbox_frame = ttk.Frame(dlg)
        listbox_frame.pack(padx=15, pady=4, fill="both")
        lb = tk.Listbox(listbox_frame, selectmode="multiple", height=min(14, len(existing_statuses)),
                        exportselection=False, font=("Consolas", 9))
        sb = ttk.Scrollbar(listbox_frame, orient="vertical", command=lb.yview)
        lb.config(yscrollcommand=sb.set)
        for s in existing_statuses:
            count = sum(1 for it in all_items if str(it.get("status","") or "").lower() == s)
            lb.insert("end", f"{s}  ({count})")
        lb.pack(side="left", fill="both", expand=True)
        sb.pack(side="right", fill="y")

        def _confirm_delete():
            nonlocal all_items
            sel_indices = lb.curselection()
            if not sel_indices:
                messagebox.showwarning("Delete by Status", "Chưa chọn status nào.", parent=dlg)
                return
            sel_statuses = {existing_statuses[i] for i in sel_indices}
            count = sum(1 for it in all_items if str(it.get("status","") or "").lower() in sel_statuses)
            if not messagebox.askyesno(
                "Xác nhận",
                f"Xóa {count} account có status:\n{', '.join(sorted(sel_statuses))}?\n\nKhông thể hoàn tác!",
                parent=dlg,
            ):
                return
            all_items = [it for it in all_items
                         if str(it.get("status","") or "").lower() not in sel_statuses]
            save_accounts_db(all_items)
            total_pages = max(1, (len(all_items) + PAGE_SIZE - 1) // PAGE_SIZE)
            if page_idx[0] >= total_pages:
                page_idx[0] = total_pages - 1
            _refresh_table()
            _add_activity(f"Xóa theo status {sel_statuses}: còn {len(all_items)} account")
            dlg.destroy()

        btn_frame = ttk.Frame(dlg)
        btn_frame.pack(pady=10)
        ttk.Button(btn_frame, text="Xóa", command=_confirm_delete).pack(side="left", padx=8)
        ttk.Button(btn_frame, text="Hủy", command=dlg.destroy).pack(side="left", padx=8)

    ttk.Button(f_toolbar, text="Del by Status", command=_delete_by_status).pack(side="left", padx=2)
    ttk.Separator(f_toolbar, orient="vertical").pack(side="left", fill="y", padx=5)

    # ── Update button ──
    def _do_update():
        import threading, subprocess, sys as _sys
        btn_update.config(text="Updating...", state="disabled")
        def _run():
            try:
                import update as _upd
                ok = _upd.main(force=True)
                if ok:
                    # Đọc version mới
                    try:
                        new_ver = (Path(__file__).resolve().parent / "VERSION").read_text(encoding="utf-8").strip()
                    except Exception:
                        new_ver = "?"
                    root.after(0, lambda: [
                        root.title(f"Gmail Tool  v{new_ver}"),
                        lbl_version.config(text=f"v{new_ver}"),
                        btn_update.config(text="✓ Updated", state="normal"),
                        messagebox.showinfo("Update", f"Đã cập nhật lên v{new_ver}!\nKhởi động lại tool để áp dụng hoàn toàn."),
                    ])
                else:
                    root.after(0, lambda: btn_update.config(text="⟳ Update", state="normal"))
            except Exception as e:
                root.after(0, lambda: [
                    btn_update.config(text="⟳ Update", state="normal"),
                    messagebox.showerror("Update lỗi", str(e)),
                ])
        threading.Thread(target=_run, daemon=True).start()

    btn_update = ttk.Button(f_toolbar, text="⟳ Update", command=_do_update)
    btn_update.pack(side="left", padx=2)
    lbl_version = ttk.Label(f_toolbar, text=f"v{_ver}", foreground="#888")
    lbl_version.pack(side="left", padx=4)

    ttk.Separator(f_toolbar, orient="vertical").pack(side="left", fill="y", padx=5)
    ttk.Button(f_toolbar, text="Import", command=_do_import).pack(side="left", padx=2)
    ttk.Button(f_toolbar, text="Export All", command=_do_export).pack(side="left", padx=2)
    ttk.Button(f_toolbar, text="Export Sel", command=_do_export_selected).pack(side="left", padx=2)
    ttk.Separator(f_toolbar, orient="vertical").pack(side="left", fill="y", padx=5)

    btn_run = ttk.Button(f_toolbar, text="▶ Start", command=_do_run)
    btn_run.pack(side="left", padx=2)
    btn_pause = ttk.Button(f_toolbar, text="Pause", command=_do_pause)
    btn_pause.pack(side="left", padx=2)
    ttk.Button(f_toolbar, text="■ Stop", command=_do_stop).pack(side="left", padx=2)

    # ── Browser Windows control ──
    ttk.Separator(f_toolbar, orient="vertical").pack(side="left", fill="y", padx=5)

    def _do_tile():
        tile_browser_windows()
        _add_activity("Browser windows tiled")

    def _browser_scale_apply():
        try:
            v = float(browser_scale_entry.get().strip().replace(",", "."))
            v = max(0.1, min(1.0, v))
            browser_scale_entry.delete(0, "end")
            browser_scale_entry.insert(0, f"{v:.2f}")
            browser_scale_slider.set(v)
            set_browser_scale(v)
        except ValueError:
            pass

    def _browser_scale_change(val):
        v = float(val)
        browser_scale_entry.delete(0, "end")
        browser_scale_entry.insert(0, f"{v:.2f}")
        set_browser_scale(v)

    def _browser_scale_reset():
        browser_scale_slider.set(1.0)
        browser_scale_entry.delete(0, "end")
        browser_scale_entry.insert(0, "1.0")
        set_browser_scale(1.0)

    ttk.Button(f_toolbar, text="⊞ Tile", command=_do_tile).pack(side="left", padx=2)
    ttk.Label(f_toolbar, text="Size (0.1–1):").pack(side="left", padx=(4, 0))
    browser_scale_entry = tk.Entry(f_toolbar, width=5)
    browser_scale_entry.insert(0, "1.0")
    browser_scale_entry.pack(side="left", padx=2)
    browser_scale_entry.bind("<Return>", lambda e: _browser_scale_apply())
    ttk.Button(f_toolbar, text="Apply", command=_browser_scale_apply, width=5).pack(side="left", padx=1)
    browser_scale_slider = tk.Scale(
        f_toolbar, from_=0.1, to=1.0, resolution=0.05, orient="horizontal",
        length=100, showvalue=False, command=_browser_scale_change,
    )
    browser_scale_slider.set(1.0)
    browser_scale_slider.pack(side="left", padx=2)
    ttk.Button(f_toolbar, text="Reset", command=_browser_scale_reset).pack(side="left", padx=2)

    # ── Settings tab ──
    settings_canvas = tk.Canvas(tab_settings)
    settings_scroll = ttk.Scrollbar(tab_settings, orient="vertical", command=settings_canvas.yview)
    settings_inner = ttk.Frame(settings_canvas)

    def _on_settings_configure(event):
        settings_canvas.configure(scrollregion=settings_canvas.bbox("all"))

    def _on_canvas_resize(event):
        settings_canvas.itemconfig("inner_win", width=event.width)

    settings_inner.bind("<Configure>", _on_settings_configure)
    _inner_win_id = settings_canvas.create_window((0, 0), window=settings_inner, anchor="nw", tags="inner_win")
    settings_canvas.bind("<Configure>", _on_canvas_resize)
    settings_canvas.configure(yscrollcommand=settings_scroll.set)
    settings_canvas.pack(side="left", fill="both", expand=True)
    settings_scroll.pack(side="right", fill="y")

    # Cho phép scroll bằng chuột trong Settings (bind đệ quy)
    def _settings_mousewheel(event):
        settings_canvas.yview_scroll(int(-1 * (event.delta / 120)), "units")

    def _bind_mousewheel(widget):
        widget.bind("<MouseWheel>", _settings_mousewheel)
        for child in widget.winfo_children():
            _bind_mousewheel(child)

    settings_canvas.bind("<MouseWheel>", _settings_mousewheel)
    settings_inner.bind("<MouseWheel>", _settings_mousewheel)
    # Bind lại sau khi tất cả widget đã được tạo
    settings_canvas.after(200, lambda: _bind_mousewheel(settings_inner))

    # ── 2-column layout ──────────────────────────────────────────────────────
    settings_inner.columnconfigure(0, weight=1)
    settings_inner.columnconfigure(1, weight=1)

    col_left = ttk.Frame(settings_inner)
    col_left.grid(row=0, column=0, sticky="nsew", padx=(8, 4), pady=5)
    col_left.columnconfigure(0, weight=1)

    col_right = ttk.Frame(settings_inner)
    col_right.grid(row=0, column=1, sticky="nsew", padx=(4, 8), pady=5)
    col_right.columnconfigure(0, weight=1)

    # ── CỘT TRÁI: Tasks + Config ─────────────────────────────────────────────
    tasks_frame = ttk.LabelFrame(col_left, text="Tasks (chỉ các chức năng có cột trên bảng)")
    tasks_frame.pack(fill="x", pady=(0, 6))
    ttk.Label(tasks_frame, text="Không chọn = chỉ login, cập nhật status.", font=("", 8)).grid(row=0, column=0, columnspan=2, sticky="w", padx=5, pady=(0, 4))
    r = 1
    ttk.Checkbutton(tasks_frame, text="Bật 2FA", variable=var_bat2fa).grid(row=r, column=0, sticky="w", padx=5)
    ttk.Checkbutton(tasks_frame, text="Xóa phone khi bật 2FA", variable=var_bat2fa_delphone).grid(row=r, column=1, sticky="w", padx=5)
    r += 1
    ttk.Checkbutton(tasks_frame, text="Verify login (chỉ check login ok)", variable=var_verifyphone).grid(row=r, column=0, sticky="w", padx=5)
    ttk.Checkbutton(tasks_frame, text="Xóa phone recovery", variable=var_deletephone).grid(row=r, column=1, sticky="w", padx=5)
    r += 1
    ttk.Checkbutton(tasks_frame, text="Đổi mật khẩu", variable=var_changepass).grid(row=r, column=0, sticky="w", padx=5)
    ttk.Checkbutton(tasks_frame, text="Đổi email recovery", variable=var_changemail).grid(row=r, column=1, sticky="w", padx=5)
    tasks_frame.columnconfigure(0, weight=1)
    tasks_frame.columnconfigure(1, weight=1)

    config_frame = ttk.LabelFrame(col_left, text="Config chạy (dùng khi bấm Start)")
    config_frame.pack(fill="x", pady=(0, 6))

    BROWSER_OPTIONS = [
        "browser-gpm142",
        "gologin-139",
        "gologin-143",
        "chrome-incognito",
    ]

    row = 0
    for label, var, widget_type in [
        ("Số luồng (vd. 40–50/64GB RAM):", var_numthread, "entry"),
        ("Timeout login (s):", var_timeoutlogin, "entry"),
        ("Delay (ms):", var_delay, "entry"),
        ("Browser option:", var_browser, "combobox"),
        ("EzCaptcha API Key:", var_ezcaptcha, "entry"),
        ("2Captcha API Key:", var_2captcha, "entry"),
        ("Anthropic API Key (AI):", var_anthropic, "entry"),
    ]:
        ttk.Label(config_frame, text=label).grid(row=row, column=0, sticky="w", padx=5, pady=2)
        if widget_type == "combobox":
            ttk.Combobox(
                config_frame, textvariable=var, values=BROWSER_OPTIONS,
                state="readonly",
            ).grid(row=row, column=1, sticky="ew", padx=5, pady=2)
        else:
            ttk.Entry(config_frame, textvariable=var).grid(row=row, column=1, sticky="ew", padx=5, pady=2)
        row += 1

    ttk.Checkbutton(
        config_frame, text="Không lưu profile (xóa cache browser sau mỗi session)",
        variable=var_no_save_profile,
    ).grid(row=row, column=0, columnspan=2, sticky="w", padx=5, pady=(4, 2))

    config_frame.columnconfigure(1, weight=1)

    # ── CỘT PHẢI: Proxy + OTP + TempMail ─────────────────────────────────────
    proxy_frame_outer = ttk.LabelFrame(col_right, text="Proxy")
    proxy_frame_outer.pack(fill="x", pady=(0, 6))

    ttk.Checkbutton(proxy_frame_outer, text="Bật proxy", variable=var_use_proxy).grid(
        row=0, column=0, columnspan=2, sticky="w", padx=5, pady=(4, 2))

    ttk.Label(proxy_frame_outer, text="Proxy đơn (fallback):").grid(
        row=1, column=0, sticky="w", padx=5, pady=2)
    ttk.Entry(proxy_frame_outer, textvariable=var_proxy).grid(
        row=1, column=1, sticky="ew", padx=5, pady=2)

    ttk.Label(proxy_frame_outer, text="Proxy list\n(host:port:user:pass\nmỗi dòng 1 proxy):").grid(
        row=2, column=0, sticky="nw", padx=5, pady=2)

    _proxy_list_frame = ttk.Frame(proxy_frame_outer)
    _proxy_list_frame.grid(row=2, column=1, sticky="ew", padx=5, pady=2)

    proxy_list_text = tk.Text(_proxy_list_frame, width=40, height=6, font=("Consolas", 9))
    proxy_list_text.pack(side="left", fill="both", expand=True)
    _proxy_scrollbar = ttk.Scrollbar(_proxy_list_frame, orient="vertical", command=proxy_list_text.yview)
    _proxy_scrollbar.pack(side="right", fill="y")
    proxy_list_text.config(yscrollcommand=_proxy_scrollbar.set)

    _proxy_list_path = str(DATA_DIR / "list_proxy.txt")
    try:
        _existing_list = Path(_proxy_list_path).read_text(encoding="utf-8")
        proxy_list_text.insert("1.0", _existing_list)
    except Exception:
        pass

    def _save_proxy_list():
        content = proxy_list_text.get("1.0", "end").rstrip("\n") + "\n"
        try:
            Path(_proxy_list_path).write_text(content, encoding="utf-8")
        except Exception:
            pass
        count = load_proxy_list(_proxy_list_path)
        set_single_proxy(var_proxy.get())
        _add_activity(f"Proxy list: reload {count} proxy")

    ttk.Button(proxy_frame_outer, text="Reload list", command=_save_proxy_list).grid(
        row=3, column=1, sticky="e", padx=5, pady=(2, 4))
    proxy_frame_outer.columnconfigure(1, weight=1)

    otp_frame = ttk.LabelFrame(col_right, text="Hero-SMS OTP (chỉ dùng cho flow verify phone cũ)")
    otp_frame.pack(fill="x", pady=(0, 6))
    row = 0
    for label, var in [
        ("API Key:", var_hero_key),
        ("Site (hero-sms.com):", var_hero_site),
        ("Service (go,33,any,0.1):", var_hero_service),
        ("Retries:", var_hero_retries),
    ]:
        ttk.Label(otp_frame, text=label).grid(row=row, column=0, sticky="w", padx=5, pady=2)
        ttk.Entry(otp_frame, textvariable=var).grid(row=row, column=1, sticky="ew", padx=5, pady=2)
        row += 1
    otp_frame.columnconfigure(1, weight=1)

    temp_mail_frame = ttk.LabelFrame(col_right, text="Đổi email khôi phục — Đọc code verify từ API")
    temp_mail_frame.pack(fill="x", pady=(0, 6))
    ttk.Checkbutton(temp_mail_frame, text="Mail KP lấy từ list; dùng API inboxesmail để đọc code/link verify từ các mail đó", variable=var_use_temp_mail).grid(row=0, column=0, columnspan=2, sticky="w", padx=5, pady=2)
    ttk.Label(temp_mail_frame, text="Base URL (inboxesmail):").grid(row=1, column=0, sticky="w", padx=5, pady=2)
    ttk.Entry(temp_mail_frame, textvariable=var_temp_mail_base_url).grid(row=1, column=1, sticky="ew", padx=5, pady=2)
    temp_mail_frame.columnconfigure(1, weight=1)

    # ── Auto-save (không cần nút Save Config) ──
    _save_debounce_id = [None]

    def _collect_config() -> dict:
        return {
            "check_changepass": str(var_changepass.get()),
            "check_changeemailrecovery": str(var_changemail.get()),
            "check_deletephonerecovery": str(var_deletephone.get()),
            "check_verifyphone": str(var_verifyphone.get()),
            "bat2fa_new": str(var_bat2fa.get()),
            "bat2fa_new_deleteallphone": str(var_bat2fa_delphone.get()),
            "text_numthread": var_numthread.get(),
            "timeoutlogin": var_timeoutlogin.get(),
            "delay": var_delay.get(),
            "proxy_active": var_proxy.get(),
            "use_proxy": str(var_use_proxy.get()),
            "no_save_profile": str(var_no_save_profile.get()),
            "text_optionbrowser": var_browser.get(),
            "text_dialog_recaptcha_apikey": var_ezcaptcha.get(),
            "text_dialog_2captcha_apikey": var_2captcha.get(),
            "anthropic_api_key": var_anthropic.get(),
            "text_dialog_otp_apikey": var_hero_key.get(),
            "text_dialog_otp_site": var_hero_site.get(),
            "text_dialog_otp_serviceid": var_hero_service.get(),
            "text_dialog_otp_times_try": var_hero_retries.get(),
            "use_temp_mail_recovery": str(var_use_temp_mail.get()),
            "temp_mail_base_url": var_temp_mail_base_url.get(),
        }

    def _do_save(*_):
        """Lưu ngay, không debounce — dùng cho checkbox/combobox."""
        try:
            save_hconfig(_collect_config())
        except Exception:
            pass
        # Đồng bộ proxy + profile + AI settings vào module in-memory
        set_use_proxy(var_use_proxy.get())
        set_single_proxy(var_proxy.get())
        set_no_save_profile(var_no_save_profile.get())
        set_anthropic_api_key(var_anthropic.get())

    def _schedule_save(*_):
        """Debounce 1.5s — dùng cho Entry (tránh lưu từng ký tự)."""
        if _save_debounce_id[0]:
            root.after_cancel(_save_debounce_id[0])
        _save_debounce_id[0] = root.after(1500, _do_save)

    # Checkbox và combobox → lưu ngay
    for _v in (
        var_changepass, var_changemail, var_deletephone, var_verifyphone,
        var_bat2fa, var_bat2fa_delphone, var_use_proxy, var_use_temp_mail,
        var_no_save_profile, var_browser,
    ):
        _v.trace_add("write", _do_save)

    # Entry → debounce
    for _v in (
        var_numthread, var_timeoutlogin, var_delay,
        var_proxy, var_ezcaptcha, var_2captcha, var_anthropic,
        var_hero_key, var_hero_site, var_hero_service, var_hero_retries,
        var_temp_mail_base_url,
    ):
        _v.trace_add("write", _schedule_save)

    # ── Activity Log tab ──
    log_text = scrolledtext.ScrolledText(tab_logs, wrap="word", font=("Consolas", 9), state="disabled")
    log_text.pack(fill="both", expand=True, padx=5, pady=5)

    def _refresh_activity_log():
        try:
            with _activity_lock:
                entries = list(_activity_log)
            log_text.config(state="normal")
            log_text.delete("1.0", "end")
            log_text.insert("1.0", "\n".join(entries[:500]))
            log_text.config(state="disabled")
        except Exception:
            pass
        try:
            root.after(2000, _refresh_activity_log)
        except Exception:
            pass

    root.after(2000, _refresh_activity_log)

    # ── Status bar: hiển thị tiến trình các luồng đang chạy ──
    from tkinter import scrolledtext
    status_frame = ttk.LabelFrame(root, text="Status")
    status_frame.pack(side="bottom", fill="x", expand=False, padx=5, pady=(2, 4))
    status_text = scrolledtext.ScrolledText(status_frame, height=4, wrap="word", font=("Consolas", 9))
    status_text.pack(fill="x", expand=False, padx=2, pady=2)
    status_text.insert("1.0", "Ready")
    status_text.config(state="disabled")

    _refresh_table()
    root.mainloop()
