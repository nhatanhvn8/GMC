# -*- coding: utf-8 -*-
"""
update.py — Tải bản mới nhất từ GitHub, so sánh version, cập nhật file.
Repo: https://github.com/nhatanhvn8/GMC (PUBLIC)
Chạy: python update.py
"""
import io
import json
import sys
import zipfile
from pathlib import Path
from urllib.request import Request, urlopen
from urllib.error import URLError

REPO   = "nhatanhvn8/GMC"
BRANCH = "main"

KEEP_LOCAL = {
    "data/accounts_db.json",
    "data/hconfig.ini",
    "data/list_proxy.txt",
    "data/list_pass.txt",
    "data/list_mail_kp.txt",
    "config/tool_config.json",
    "config/dom_patterns.json",
    "config/dom_model.pkl",
    "training_data/",
    "logs/",
    "export/",
    "browser/",
    "profiles/",
    "Run/",
}

BASE_DIR = Path(__file__).resolve().parent
VERSION_FILE = BASE_DIR / "VERSION"


def get_local_version() -> str:
    try:
        return VERSION_FILE.read_text(encoding="utf-8").strip()
    except Exception:
        return "0.0.0"


def _fetch_text(url: str) -> str:
    req = Request(url, headers={"User-Agent": "gmail-tool-updater"})
    with urlopen(req, timeout=10) as r:
        return r.read().decode("utf-8").strip()


def get_remote_version() -> str:
    url = f"https://raw.githubusercontent.com/{REPO}/{BRANCH}/VERSION"
    try:
        return _fetch_text(url)
    except Exception:
        return "0.0.0"


def get_changelog() -> str:
    url = f"https://raw.githubusercontent.com/{REPO}/{BRANCH}/CHANGELOG.md"
    try:
        return _fetch_text(url)
    except Exception:
        return ""


def _should_keep(rel_path: str) -> bool:
    p = rel_path.replace("\\", "/")
    for keep in KEEP_LOCAL:
        if keep.endswith("/"):
            if p.startswith(keep):
                return True
        else:
            if p == keep:
                return True
    return False


def download_zip() -> bytes:
    url = f"https://github.com/{REPO}/archive/refs/heads/{BRANCH}.zip"
    req = Request(url, headers={"User-Agent": "gmail-tool-updater"})
    print(f"  Dang tai ({REPO})...", end="", flush=True)
    with urlopen(req, timeout=60) as resp:
        data = b""
        while True:
            buf = resp.read(65536)
            if not buf:
                break
            data += buf
            print(".", end="", flush=True)
    print(f" {len(data)//1024}KB")
    return data


def apply_update(zip_data: bytes) -> tuple[int, int]:
    updated = skipped = 0
    with zipfile.ZipFile(io.BytesIO(zip_data)) as zf:
        names = zf.namelist()
        prefix = next((n for n in names if n.endswith("/") and n.count("/") == 1), "")
        if not prefix and names:
            prefix = names[0].split("/")[0] + "/"
        for name in names:
            if not name.startswith(prefix):
                continue
            rel = name[len(prefix):]
            if not rel or rel.endswith("/"):
                continue
            if _should_keep(rel):
                skipped += 1
                continue
            dest = BASE_DIR / rel
            dest.parent.mkdir(parents=True, exist_ok=True)
            dest.write_bytes(zf.read(name))
            updated += 1
    return updated, skipped


def main(force: bool = False) -> bool:
    local_ver  = get_local_version()
    print(f"  Phien ban hien tai : v{local_ver}")

    try:
        remote_ver = get_remote_version()
        print(f"  Phien ban moi nhat: v{remote_ver}")
    except URLError as e:
        print(f"  Loi ket noi: {e}")
        return False

    if local_ver == remote_ver and not force:
        print("  Tool dang la ban moi nhat, khong can cap nhat.")
        return True

    # Hiển thị changelog
    changelog = get_changelog()
    if changelog:
        # Chỉ lấy phần của version mới nhất
        lines = changelog.split("\n")
        section = []
        in_section = False
        for line in lines:
            if line.startswith(f"## [{remote_ver}]"):
                in_section = True
            elif line.startswith("## [") and in_section:
                break
            if in_section:
                section.append(line)
        if section:
            print("\n  --- WHAT'S NEW ---")
            for l in section[:8]:
                print(f"  {l}")
            print()

    try:
        zip_data = download_zip()
    except Exception as e:
        print(f"  Loi tai file: {e}")
        return False

    updated, skipped = apply_update(zip_data)
    print(f"  Cap nhat: {updated} files | Giu nguyen: {skipped} files")
    print(f"  v{local_ver} -> v{remote_ver}  XONG!")
    return True


if __name__ == "__main__":
    print("=" * 45)
    print("  GMAIL TOOL - UPDATE")
    print("=" * 45)
    force = "--force" in sys.argv
    ok = main(force=force)
    sys.exit(0 if ok else 1)
