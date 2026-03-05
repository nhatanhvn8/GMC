# -*- coding: utf-8 -*-
from __future__ import annotations

import time
import logging
from typing import Any, Optional


def parse_hero_service_input(raw: str) -> tuple[str, int, str, Optional[float]]:
    if not raw or not raw.strip():
        return ("", 0, "", None)
    parts = [p.strip() for p in raw.split(",")]
    service = parts[0] if len(parts) >= 1 else ""
    country = 0
    if len(parts) >= 2:
        try:
            country = int(parts[1])
        except Exception:
            country = 0
    operator = ""
    if len(parts) >= 3:
        op = parts[2].strip()
        if op:
            # giữ "any/all" nếu user truyền vào để đồng nhất với API cũ
            operator = op
    max_price: Optional[float] = None
    if len(parts) >= 4:
        try:
            val = float(parts[3])
            if val > 0:
                max_price = val
        except Exception:
            pass
    return (service, country, operator, max_price)


def hero_sms_api_call(api_key: str, params: dict[str, Any], site: str = "hero-sms.com") -> str:
    import urllib.parse
    import urllib.request
    import urllib.error

    _log = logging.getLogger(__name__)
    sites = [site, "www.hero-sms.com", "api.hero-sms.com"]
    # giữ thứ tự, loại bỏ trùng lặp
    uniq_sites: list[str] = []
    for s in sites:
        s = (s or "").strip()
        if s and s not in uniq_sites:
            uniq_sites.append(s)

    query = urllib.parse.urlencode({"api_key": api_key, **params})
    last_error = "ERROR"
    for host in uniq_sites:
        for scheme in ("https", "http"):
            url = f"{scheme}://{host}/stubs/handler_api.php?{query}"
            req = urllib.request.Request(
                url,
                headers={
                    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                                  "AppleWebKit/537.36 (KHTML, like Gecko) "
                                  "Chrome/124.0.0.0 Safari/537.36",
                    "Accept": "text/plain, */*",
                    "Cache-Control": "no-cache",
                    "Pragma": "no-cache",
                },
                method="GET",
            )
            try:
                with urllib.request.urlopen(req, timeout=20) as resp:
                    return (resp.read().decode("utf-8") or "").strip()
            except urllib.error.HTTPError as e:
                body = ""
                try:
                    body = (e.read().decode("utf-8", errors="ignore") or "").strip()
                except Exception:
                    pass
                last_error = f"HTTP_{e.code}:{body or e.reason}"
                _log.warning(
                    "hero_sms_api_call HTTP error %s://%s code=%s body=%s",
                    scheme, host, e.code, body[:180]
                )
                # 403/5xx thử host/scheme kế tiếp
                continue
            except Exception as e:
                last_error = f"ERROR:{e}"
                _log.warning("hero_sms_api_call error %s://%s err=%s", scheme, host, e)
                continue

    return last_error


def get_hero_sms_number(api_key: str, service: str, max_retries: int = 3, max_wait_sec: Optional[int] = None) -> tuple[str, str]:
    _log = logging.getLogger(__name__)
    service_name, country, operator, max_price = parse_hero_service_input(service)
    if not service_name:
        _log.warning("Hero-SMS: service rỗng/không hợp lệ: %r", service)
        return ("", "")
    params: dict[str, Any] = {"action": "getNumber", "service": service_name}
    if country:
        params["country"] = country
    if operator:
        params["operator"] = operator
    if max_price is not None:
        params["maxPrice"] = max_price

    if max_wait_sec is not None and max_wait_sec > 0:
        deadline = time.time() + int(max_wait_sec)
        tries = 0
        while time.time() < deadline:
            tries += 1
            response = hero_sms_api_call(api_key, params)
            if "ACCESS_NUMBER" in response:
                parts = response.split(":")
                if len(parts) >= 3:
                    activation_id = parts[1]
                    phone = parts[2]
                    _log.info(
                        "Hero-SMS getNumber ok sau %s lan: service=%s country=%s operator=%s price=%s",
                        tries, service_name, country or "auto", operator or "any", max_price if max_price is not None else "auto"
                    )
                    return (activation_id, phone)
            if tries == 1 or tries % 8 == 0:
                _log.warning("Hero-SMS getNumber pending lan %s: %s", tries, (response or "")[:160])
            # Trong cửa sổ chờ thuê số, tiếp tục retry liên tục.
            # NO_NUMBERS/NO_BALANCE/ERROR/response lạ đều chờ ngắn rồi request lại.
            time.sleep(2.5)
        _log.warning(
            "Hero-SMS getNumber timeout sau %ss: service=%s country=%s operator=%s price=%s",
            int(max_wait_sec), service_name, country or "auto", operator or "any", max_price if max_price is not None else "auto"
        )
        return ("", "")

    for _ in range(max_retries * 30):
        response = hero_sms_api_call(api_key, params)
        if "ACCESS_NUMBER" in response:
            parts = response.split(":")
            if len(parts) >= 3:
                activation_id = parts[1]
                phone = parts[2]
                return (activation_id, phone)
        if "NO_NUMBERS" in response or "NO_BALANCE" in response:
            time.sleep(2.5)
            continue
        _log.warning("Hero-SMS getNumber fail nhanh: %s", (response or "")[:160])
        return ("", "")
    return ("", "")


def hero_sms_mark_ready(api_key: str, activation_id: str) -> None:
    import logging

    params = {"action": "setStatus", "id": activation_id, "status": 1}
    result = hero_sms_api_call(api_key, params)
    logging.getLogger(__name__).info("hero_sms_mark_ready: %s", result)


def get_hero_sms_code(api_key: str, activation_id: str, max_wait_sec: Optional[int] = None) -> str:
    _log = logging.getLogger(__name__)
    if not api_key or not activation_id:
        return ""
    tries = 0
    deadline = time.time() + int(max_wait_sec) if max_wait_sec is not None and max_wait_sec > 0 else None
    while True:
        if deadline is not None and time.time() >= deadline:
            _log.warning("Hero-SMS getStatus timeout sau %ss (activation_id=%s)", int(max_wait_sec), activation_id)
            return ""
        tries += 1
        response = hero_sms_api_call(api_key, {"action": "getStatus", "id": activation_id})
        if response.startswith("STATUS_OK:"):
            code = response.split(":")[1]
            _log.info("Hero-SMS getStatus OK sau %s lan", tries)
            return code
        if tries == 1 or tries % 12 == 0:
            _log.warning("Hero-SMS getStatus pending lan %s: %s", tries, (response or "")[:160])
        time.sleep(2.5)
