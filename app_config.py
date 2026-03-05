# -*- coding: utf-8 -*-
from __future__ import annotations

import configparser
import json
from pathlib import Path
from typing import Any

BASE_DIR = Path(__file__).resolve().parent
CONFIG_DIR = BASE_DIR / "config"
DATA_DIR = BASE_DIR / "data"
EXPORT_DIR = BASE_DIR / "export"

HCONFIG_PATH = DATA_DIR / "hconfig.ini"
ACCOUNTS_DB_PATH = CONFIG_DIR / "accounts_db.json"

LOG_FILE = EXPORT_DIR / "app.log"
HERO_SMS_LOG_FILE = EXPORT_DIR / "hero_sms.log"

DATA_MAIL_FILE = DATA_DIR / "data_mail.txt"
LIST_PASS_FILE = DATA_DIR / "list_pass.txt"
LIST_MAIL_KP_FILE = DATA_DIR / "list_mail_kp.txt"
LIST_PROXY_FILE = DATA_DIR / "list_proxy.txt"

GOOGLE_LOGIN_URL = "https://accounts.google.com/ServiceLogin?service=accountsettings&hl=en-US&continue=https://myaccount.google.com/intro/security"

_SECTION = "hconfig"

_DEFAULTS: dict[str, str] = {
    "text_numthread": "3",
    "text_optionproxy": "",
    "proxy_active": "",
    "use_proxy": "True",
    "no_save_profile": "False",
    "timeoutlogin": "300",
    "delay": "100",
    "check_changepass": "False",
    "check_changeemailrecovery": "False",
    "check_deletephonerecovery": "False",
    "check_verifyphone": "False",
    "bat2fa_new": "False",
    "bat2fa_new_deleteallphone": "False",
    "create_password_app": "False",
    "text_dialog_recaptcha_apikey": "",
    "anthropic_api_key": "",
    "text_dialog_otp_apikey": "",
    "text_dialog_otp_site": "hero-sms.com",
    "text_dialog_otp_serviceid": "",
    "text_dialog_otp_times_try": "3",
    "text_optionbrowser": "browser-gpm142",
    "visible_columns": "",
    "use_temp_mail_recovery": "False",
    "temp_mail_base_url": "https://inboxesmail.app",
}


def _ensure_hconfig():
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    if HCONFIG_PATH.exists():
        return
    cp = configparser.ConfigParser()
    cp[_SECTION] = dict(_DEFAULTS)
    with open(HCONFIG_PATH, "w", encoding="utf-8") as f:
        cp.write(f)


def load_hconfig() -> dict[str, str]:
    _ensure_hconfig()
    cp = configparser.ConfigParser()
    cp.read(str(HCONFIG_PATH), encoding="utf-8-sig")  # utf-8-sig tự bỏ BOM nếu có
    result = dict(_DEFAULTS)
    if cp.has_section(_SECTION):
        for k, v in cp.items(_SECTION):
            result[k] = v
    return result


def save_hconfig(data: dict[str, Any]) -> None:
    _ensure_hconfig()
    cp = configparser.ConfigParser()
    cp.read(str(HCONFIG_PATH), encoding="utf-8-sig")
    if not cp.has_section(_SECTION):
        cp.add_section(_SECTION)
    for k, v in data.items():
        cp.set(_SECTION, str(k), str(v))
    with open(HCONFIG_PATH, "w", encoding="utf-8") as f:
        cp.write(f)


def get_hconfig_bool(cfg: dict[str, str], key: str, default: bool = False) -> bool:
    val = str(cfg.get(key, "")).strip().lower()
    if val in ("true", "1", "yes"):
        return True
    if val in ("false", "0", "no", ""):
        return default
    return default


def get_hconfig_int(cfg: dict[str, str], key: str, default: int = 0) -> int:
    try:
        return int(cfg.get(key, default))
    except (ValueError, TypeError):
        return default


def get_hconfig_str(cfg: dict[str, str], key: str, default: str = "") -> str:
    return str(cfg.get(key, default) or default)


def load_tool_config() -> dict[str, Any]:
    cfg = load_hconfig()
    return {
        "change_pass": get_hconfig_bool(cfg, "check_changepass"),
        "change_mail_kp": get_hconfig_bool(cfg, "check_changeemailrecovery"),
        "check_deletephonerecovery": get_hconfig_bool(cfg, "check_deletephonerecovery"),
        "check_verifyphone": get_hconfig_bool(cfg, "check_verifyphone"),
        "bat2fa_new": get_hconfig_bool(cfg, "bat2fa_new"),
        "bat2fa_new_deleteallphone": get_hconfig_bool(cfg, "bat2fa_new_deleteallphone"),
        "create_password_app": get_hconfig_bool(cfg, "create_password_app"),
        "ez_captcha_api_key": get_hconfig_str(cfg, "text_dialog_recaptcha_apikey"),
        "twocaptcha_api_key": get_hconfig_str(cfg, "text_dialog_2captcha_apikey"),
        "anthropic_api_key": get_hconfig_str(cfg, "anthropic_api_key"),
        "hero_sms_api_key": get_hconfig_str(cfg, "text_dialog_otp_apikey"),
        "hero_sms_site": get_hconfig_str(cfg, "text_dialog_otp_site", "hero-sms.com"),
        "hero_sms_service": get_hconfig_str(cfg, "text_dialog_otp_serviceid"),
        "hero_sms_get_number_retries": get_hconfig_int(cfg, "text_dialog_otp_times_try", 3),
        "max_workers": get_hconfig_int(cfg, "text_numthread", 1),
        "proxy": get_hconfig_str(cfg, "proxy_active"),
        "proxy_mode": get_hconfig_str(cfg, "text_optionproxy", "none"),
        "use_proxy": get_hconfig_bool(cfg, "use_proxy", True),
        "no_save_profile": get_hconfig_bool(cfg, "no_save_profile", False),
        "browser_option": get_hconfig_str(cfg, "text_optionbrowser", "browser-gpm142"),
        "visible_columns": [c.strip() for c in get_hconfig_str(cfg, "visible_columns").split(",") if c.strip()] or None,
        "timeoutlogin": get_hconfig_int(cfg, "timeoutlogin", 300),
        "delay": get_hconfig_int(cfg, "delay", 100),
        "use_anti_fingerprint": True,
        "profile_dir": get_hconfig_str(cfg, "profile_dir"),
        "use_temp_mail_recovery": get_hconfig_bool(cfg, "use_temp_mail_recovery"),
        "temp_mail_base_url": get_hconfig_str(cfg, "temp_mail_base_url", "https://inboxesmail.app"),
    }


def save_tool_config(data: dict[str, Any]) -> None:
    mapping = {
        "check_changepass": data.get("change_pass", False),
        "check_changeemailrecovery": data.get("change_mail_kp", False),
        "check_deletephonerecovery": data.get("check_deletephonerecovery", False),
        "check_verifyphone": data.get("check_verifyphone", False),
        "bat2fa_new": data.get("bat2fa_new", False),
        "bat2fa_new_deleteallphone": data.get("bat2fa_new_deleteallphone", False),
        "create_password_app": data.get("create_password_app", False),
        "text_dialog_recaptcha_apikey": data.get("ez_captcha_api_key", ""),
        "text_dialog_2captcha_apikey": data.get("twocaptcha_api_key", ""),
        "anthropic_api_key": data.get("anthropic_api_key", ""),
        "text_dialog_otp_apikey": data.get("hero_sms_api_key", ""),
        "text_dialog_otp_site": data.get("hero_sms_site", "hero-sms.com"),
        "text_dialog_otp_serviceid": data.get("hero_sms_service", ""),
        "text_dialog_otp_times_try": data.get("hero_sms_get_number_retries", 3),
        "text_numthread": data.get("max_workers", 1),
        "proxy_active": data.get("proxy", ""),
        "text_optionproxy": data.get("proxy_mode", "none"),
        "use_proxy": data.get("use_proxy", True),
        "no_save_profile": data.get("no_save_profile", False),
        "text_optionbrowser": data.get("browser_option", "browser-gpm142"),
        "timeoutlogin": data.get("timeoutlogin", 300),
        "delay": data.get("delay", 100),
        "profile_dir": data.get("profile_dir", ""),
        "use_temp_mail_recovery": data.get("use_temp_mail_recovery", False),
        "temp_mail_base_url": data.get("temp_mail_base_url", "https://inboxesmail.app"),
    }
    cols = data.get("visible_columns")
    if cols and isinstance(cols, list):
        mapping["visible_columns"] = ",".join(cols)
    save_hconfig(mapping)


def load_config() -> dict[str, Any]:
    return load_tool_config()
