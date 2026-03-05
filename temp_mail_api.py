# -*- coding: utf-8 -*-
"""
Temp Mail API (inboxesmail.app) — lấy domain, danh sách email, nội dung inbox.
Dùng cho flow đổi email khôi phục: mail tạm nhận code/link verify từ Google.
Doc: https://inboxesmail.app/api-docs
"""
from __future__ import annotations

import logging
import random
import re
import string
import time
import urllib.parse
import urllib.request

_DEFAULT_BASE = "https://inboxesmail.app"
_LOG = logging.getLogger(__name__)


def get_domains(base_url: str = _DEFAULT_BASE) -> list[str]:
    """GET /api/domains — trả về list domain (vd: ['@example.com']), trả về dạng ['example.com']."""
    url = urllib.parse.urljoin(base_url.rstrip("/") + "/", "api/domains")
    try:
        req = urllib.request.Request(url, method="GET")
        with urllib.request.urlopen(req, timeout=15) as resp:
            data = resp.read().decode("utf-8", errors="replace")
            import json
            raw = json.loads(data)
            out = []
            for d in raw if isinstance(raw, list) else []:
                s = (d if isinstance(d, str) else str(d)).strip().lstrip("@")
                if s:
                    out.append(s)
            return out if out else []
    except Exception as e:
        _LOG.warning("temp_mail get_domains %s: %s", url, e)
        return []


def get_emails(recipient: str, base_url: str = _DEFAULT_BASE) -> list[dict]:
    """GET /api/email/{recipient} — danh sách email gửi tới recipient."""
    path = "api/email/" + urllib.parse.quote(recipient, safe="")
    url = urllib.parse.urljoin(base_url.rstrip("/") + "/", path)
    try:
        req = urllib.request.Request(url, method="GET")
        with urllib.request.urlopen(req, timeout=15) as resp:
            data = resp.read().decode("utf-8", errors="replace")
            import json
            raw = json.loads(data)
            out = raw if isinstance(raw, list) else []
            _LOG.info("temp_mail get_emails %s @ %s -> %d email(s)", recipient, base_url, len(out))
            return out
    except Exception as e:
        _LOG.warning("temp_mail get_emails %s @ %s: %s", recipient, base_url, e)
        return []


def get_inbox(email_id: str, base_url: str = _DEFAULT_BASE) -> dict | None:
    """GET /api/inbox/{email_id} — nội dung chi tiết email (có body)."""
    path = "api/inbox/" + urllib.parse.quote(str(email_id), safe="")
    url = urllib.parse.urljoin(base_url.rstrip("/") + "/", path)
    try:
        req = urllib.request.Request(url, method="GET")
        with urllib.request.urlopen(req, timeout=15) as resp:
            data = resp.read().decode("utf-8", errors="replace")
            import json
            return json.loads(data)
    except Exception as e:
        _LOG.debug("temp_mail get_inbox %s: %s", email_id, e)
        return None


def generate_temp_email(base_url: str = _DEFAULT_BASE) -> str | None:
    """Tạo địa chỉ email tạm: lấy domains, chọn 1 domain + local part ngẫu nhiên."""
    domains = get_domains(base_url)
    if not domains:
        _LOG.warning("temp_mail: no domains, cannot generate temp email")
        return None
    local = "".join(random.choices(string.ascii_lowercase + string.digits, k=12))
    domain = random.choice(domains).lstrip("@")
    return f"{local}@{domain}"


def _extract_verification_code(subject: str, body_text: str) -> str | None:
    """
    Trích mã 6 số từ subject/body. Ưu tiên mã đi kèm 'verification code' / 'code:' (thường trong subject).
    Bỏ qua placeholder như 000000, 111111, 123456.
    """
    subject = (subject or "").strip()
    body_text = (body_text or "").strip()
    combined = subject + " " + body_text
    # Bỏ qua các mã thường là placeholder
    skip_codes = {"000000", "111111", "222222", "123456", "123123"}
    # Ưu tiên: subject thường có "Email verification code: 230444"
    for text in (subject, body_text, combined):
        # Mã ngay sau "code:" hoặc "verification code" hoặc "mã"
        m = re.search(r"(?:verification\s+code|code|mã\s*xác\s*minh)\s*[:\s]*(\d{6})\b", text, re.I)
        if m and m.group(1) not in skip_codes:
            return m.group(1)
    # Tất cả nhóm 6 chữ số, lấy cái không phải placeholder
    for m in re.finditer(r"\b(\d{6})\b", combined):
        if m.group(1) not in skip_codes:
            return m.group(1)
    return None


def wait_for_verification_email(
    recipient: str,
    base_url: str = _DEFAULT_BASE,
    from_contains: str = "google",
    max_wait_sec: float = 120,
    poll_interval_sec: float = 5,
) -> tuple[str | None, str | None]:
    """
    Poll inbox cho tới khi có email từ Google (fromAddress chứa from_contains).
    Trả về (code_6_digits hoặc None, verification_url hoặc None).
    """
    deadline = time.time() + max_wait_sec
    poll_count = 0
    while time.time() < deadline:
        poll_count += 1
        emails = get_emails(recipient, base_url)
        if poll_count <= 3 or emails:
            _LOG.info("CHANGE_MAIL_KP wait_for_verification_email poll #%s -> %d email(s) for %s", poll_count, len(emails), recipient)
        for em in emails:
            from_addr = (em.get("fromAddress") or "").lower()
            if from_contains.lower() not in from_addr:
                continue
            eid = em.get("id")
            if not eid:
                continue
            inbox = get_inbox(eid, base_url)
            if not inbox:
                continue
            subject = inbox.get("subject") or ""
            body_raw = inbox.get("body") or ""
            # Body có thể là HTML — lấy text thô để tìm mã, bỏ tag
            body_text = re.sub(r"<[^>]+>", " ", body_raw)
            body_text = re.sub(r"\s+", " ", body_text)
            code = _extract_verification_code(subject, body_text)
            if code:
                return (code, None)
            combined = subject + " " + body_text
            link = re.search(r"https://accounts\.google\.com/[^\s\"'<>]+", combined)
            if link:
                return (None, link.group(0))
        time.sleep(poll_interval_sec)
    return (None, None)
