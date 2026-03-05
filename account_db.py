# -*- coding: utf-8 -*-
from __future__ import annotations

import json
from datetime import datetime
from typing import Any

from app_config import ACCOUNTS_DB_PATH, CONFIG_DIR
from account_model import parse_acc_text

_ACCOUNT_FIELDS = [
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
    "uploadavatar", "twofasecret", "twofasecret_status", "searchmail",
    "hotmail", "password_hotmail", "listemails", "createrandomdocument",
    "deletesecurityquestion", "checkdatecreatedrive", "new_password",
    "checkcard", "emailfresh", "transfervoice", "emailrecovery_suggest",
    "emailrecovery_change_status", "count_inbox", "refresh_token_hotmail",
    "client_id_hotmail", "status_add_phone_recovery",
    "logrun", "status", "status2", "status3", "status4",
    "created_at", "updated_at",
]


def _blank_record() -> dict[str, Any]:
    now = datetime.now().isoformat()
    rec: dict[str, Any] = {f: "" for f in _ACCOUNT_FIELDS}
    rec["status"] = "not_run"
    rec["created_at"] = now
    rec["updated_at"] = now
    return rec


def load_accounts_db() -> list[dict[str, Any]]:
    if not ACCOUNTS_DB_PATH.exists():
        return []
    try:
        data = json.loads(ACCOUNTS_DB_PATH.read_text(encoding="utf-8"))
        if isinstance(data, list):
            return [x for x in data if isinstance(x, dict) and x.get("email")]
    except Exception:
        pass
    return []


def save_accounts_db(items: list[dict[str, Any]]) -> None:
    CONFIG_DIR.mkdir(exist_ok=True)
    for it in items:
        it["updated_at"] = datetime.now().isoformat()
    ACCOUNTS_DB_PATH.write_text(
        json.dumps(items, ensure_ascii=False, indent=2), encoding="utf-8"
    )


def sync_accounts_to_db_from_text(text: str) -> list[dict[str, Any]]:
    existing = {str(x.get("email", "")).lower(): x for x in load_accounts_db()}
    for acc in parse_acc_text(text):
        key = acc.email.lower()
        item = existing.get(key, _blank_record())
        item["email"] = acc.email
        item["password"] = acc.password
        item["emailrecovery"] = acc.recovery_email or item.get("emailrecovery", "")
        # Chỉ thay twofasecret khi đã xong tiến trình add/thay 2FA trong tool — import không ghi đè 2FA có sẵn
        existing_secret = (item.get("twofasecret", "") or "").strip()
        if existing_secret:
            pass  # giữ nguyên twofasecret trong DB, không ghi từ text
        else:
            item["twofasecret"] = (acc.two_fa or "").strip()
        if item.get("status", "not_run") in ("not_run", ""):
            item["status"] = "not_run"
        existing[key] = item
    result = list(existing.values())
    save_accounts_db(result)
    return result


def update_account_field(items: list[dict[str, Any]], email: str, field: str, value: Any) -> None:
    key = email.lower()
    for it in items:
        if str(it.get("email", "")).lower() == key:
            it[field] = value
            it["updated_at"] = datetime.now().isoformat()
            break


def append_logrun(items: list[dict[str, Any]], email: str, log_entry: str) -> None:
    key = email.lower()
    for it in items:
        if str(it.get("email", "")).lower() == key:
            existing = str(it.get("logrun", "") or "")
            if existing:
                it["logrun"] = existing + "\n" + log_entry
            else:
                it["logrun"] = log_entry
            it["updated_at"] = datetime.now().isoformat()
            break


def filter_accounts(
    items: list[dict[str, Any]],
    search_text: str = "",
    status_filter: str = "",
) -> list[dict[str, Any]]:
    st = (status_filter or "all").strip().lower()
    kw = (search_text or "").strip().lower()
    out: list[dict[str, Any]] = []
    for it in items:
        email = str(it.get("email", ""))
        if kw and kw not in email.lower():
            continue
        status = str(it.get("status", "not_run")).lower()
        if st == "all":
            pass
        elif st not in status:
            continue
        out.append(it)
    return out
