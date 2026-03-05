# -*- coding: utf-8 -*-
"""
Test: đọc thư tại địa chỉ mail KP và lấy mã verify (6 số) hoặc link.
Base URL mặc định: https://inboxesmail.app (domain như anhnhat.online nằm trong danh sách).

Cách dùng:
  python test_get_code_from_mail.py <email_kp>
  python test_get_code_from_mail.py <email_kp> [base_url]

Ví dụ:
  python test_get_code_from_mail.py abc123@anhnhat.online
  python test_get_code_from_mail.py abc123@anhnhat.online https://inboxesmail.app

Sau khi đổi mail KP (trên web) sang địa chỉ đó và Google gửi mã, chạy lệnh trên để đọc thư và lấy code.
"""
from __future__ import annotations

import sys
from pathlib import Path

# project root
sys.path.insert(0, str(Path(__file__).resolve().parent))

DEFAULT_BASE = "https://inboxesmail.app"


def main():
    # Tham số: email [base_url]   VD: python test_get_code_from_mail.py abc@anhnhat.online
    args = [a.strip() for a in sys.argv[1:] if a.strip()]
    if not args or "@" not in args[0]:
        print("Cach dung: python test_get_code_from_mail.py <email_kp> [base_url]")
        print("VD: python test_get_code_from_mail.py abc123@anhnhat.online")
        sys.exit(1)
    recipient = args[0]
    base_url = args[1].rstrip("/") if len(args) >= 2 else DEFAULT_BASE
    base_urls = [base_url]

    import re
    from temp_mail_api import get_emails, get_inbox, wait_for_verification_email, _extract_verification_code

    print(f"Recipient: {recipient}")
    print("=" * 60)

    for base_url in base_urls:
        print(f"\n[Base URL] {base_url}")
        try:
            emails = get_emails(recipient, base_url)
            print(f"  get_emails -> {len(emails)} email(s)")
            if not emails:
                continue
            for i, em in enumerate(emails):
                eid = em.get("id")
                subj = em.get("subject", "")
                from_addr = em.get("fromAddress", "")
                print(f"  [{i+1}] id={eid}, from={from_addr}, subject={subj[:50]}...")
                if not eid:
                    continue
                inbox = get_inbox(eid, base_url)
                if not inbox:
                    print(f"       get_inbox -> None")
                    continue
                subject = inbox.get("subject") or ""
                body_raw = inbox.get("body") or ""
                body_text = re.sub(r"<[^>]+>", " ", body_raw)
                body_text = re.sub(r"\s+", " ", body_text)
                code = _extract_verification_code(subject, body_text)
                combined = subject + " " + body_text
                link = re.search(r"https://accounts\.google\.com/[^\s\"'<>]+", combined)
                if code:
                    print(f"       -> CODE 6 so: {code}")
                if link:
                    print(f"       -> LINK: {link.group(0)[:70]}...")
                if not code and not link:
                    print(f"       -> (khong tim thay ma 6 so hay link Google)")
        except Exception as e:
            print(f"  Error: {e}")

    print("\n" + "=" * 60)
    print("Thu goi wait_for_verification_email (poll 30s, interval 3s)...")
    base = base_urls[0]
    code_6, verify_url = wait_for_verification_email(
        recipient,
        base_url=base,
        from_contains="google",
        max_wait_sec=30,
        poll_interval_sec=3,
    )
    if code_6:
        print(f"  -> Lay duoc CODE: {code_6}")
    elif verify_url:
        print(f"  -> Lay duoc LINK: {verify_url[:70]}...")
    else:
        print("  -> Khong lay duoc code/link trong 30s (hoac API khong tra ve thu tu Google)")


if __name__ == "__main__":
    main()
