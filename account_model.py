# -*- coding: utf-8 -*-
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Optional


@dataclass
class Account:
    email: str
    password: str
    recovery_email: str = ""
    two_fa: str = ""


def _make_account(raw: dict) -> Optional[Account]:
    email = raw.get("email", "").strip()
    password = raw.get("password", "").strip()
    recovery_email = raw.get("recovery_email", "").strip()
    # Cho phép key two_fa, twofasecret, 2fa (import từ file thường dùng twofasecret)
    two_fa = (
        raw.get("two_fa", "") or raw.get("twofasecret", "") or raw.get("2fa", "")
    ).strip()
    if not email or not password:
        return None
    return Account(email=email, password=password, recovery_email=recovery_email, two_fa=two_fa)


def _split_line(line: str) -> list[str]:
    """Tách dòng theo | hoặc tab hoặc dấu phẩy (CSV) để nhận đủ cột email, pass, recovery, twofasecret."""
    line = line.strip()
    if "|" in line:
        return [p.strip() for p in line.split("|")]
    if "\t" in line:
        return [p.strip() for p in line.split("\t")]
    return [p.strip() for p in line.split(",")]


def _parse_line_format(lines: list[str]) -> list[Account]:
    accounts: list[Account] = []
    for line in lines:
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        parts = _split_line(line)
        # Bỏ qua dòng header (email, password, ...)
        if len(parts) >= 1 and parts[0].lower() in ("email", "mail"):
            continue
        raw: dict = {}
        if len(parts) >= 1:
            raw["email"] = parts[0]
        if len(parts) >= 2:
            raw["password"] = parts[1]
        if len(parts) >= 3:
            raw["recovery_email"] = parts[2]
        if len(parts) >= 4:
            raw["two_fa"] = parts[3]
        acc = _make_account(raw)
        if acc:
            accounts.append(acc)
    return accounts


def _parse_block_format(lines: list[str]) -> list[Account]:
    accounts: list[Account] = []
    block: dict = {}
    for line in lines:
        line = line.strip()
        if not line:
            if block:
                acc = _make_account(block)
                if acc:
                    accounts.append(acc)
                block = {}
            continue
        if "=" in line:
            key, _, value = line.partition("=")
            key = key.strip().lower()
            value = value.strip()
            if key in ("email", "password", "recovery_email", "two_fa", "twofasecret", "2fa"):
                block["two_fa" if key in ("twofasecret", "2fa") else key] = value
    if block:
        acc = _make_account(block)
        if acc:
            accounts.append(acc)
    return accounts


def _detect_format(lines: list[str]) -> str:
    """'line' = pipe/tab/comma separated; 'block' = key=value."""
    for line in lines:
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        if "|" in line or "\t" in line or ("," in line and "=" not in line):
            return "line"
        if "=" in line:
            key = line.split("=", 1)[0].strip().lower()
            if key in ("email", "password", "recovery_email", "two_fa", "twofasecret", "2fa"):
                return "block"
        break
    return "block"


def parse_acc_file(path: Path) -> list[Account]:
    text = path.read_text(encoding="utf-8")
    lines = text.splitlines()
    fmt = _detect_format(lines)
    return _parse_line_format(lines) if fmt == "line" else _parse_block_format(lines)


def parse_acc_text(text: str) -> list[Account]:
    lines = text.splitlines()
    fmt = _detect_format(lines)
    return _parse_line_format(lines) if fmt == "line" else _parse_block_format(lines)
