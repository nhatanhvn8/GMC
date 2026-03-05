from __future__ import annotations

import json
import logging
import os
import re
import secrets
import sqlite3
import subprocess
import time
import uuid
from datetime import datetime
from pathlib import Path
from typing import Any, Optional

_log = logging.getLogger(__name__)

TOOL_DIR = Path(__file__).resolve().parent
LOCAL_BROWSER_DIR = TOOL_DIR / "browser"
LOCAL_PROFILES_DIR = TOOL_DIR / "profiles"
GPM_ROAMING = Path(os.environ.get("APPDATA", "")) / "GPMLoginGlobal"
GPM_DEFAULT_STORAGE = Path("C:/gpm")

LOCAL_GOLOGIN_BROWSER_DIR = LOCAL_BROWSER_DIR / "gologin"
GOLOGIN_DIR = Path.home() / ".gologin"
GOLOGIN_BROWSER_DIR = GOLOGIN_DIR / "browser"

_ORBITA_VERSIONS: dict[str, str] = {
    "139": "orbita-browser-139",
    "143": "orbita-browser-143",
}


def _project_root() -> Path:
    return Path(__file__).resolve().parent


def _load_tool_config_json() -> dict[str, Any]:
    cfg_path = _project_root() / "config" / "tool_config.json"
    if not cfg_path.exists():
        return {}
    try:
        return json.loads(cfg_path.read_text(encoding="utf-8", errors="replace"))
    except Exception:
        return {}


def _save_tool_config_json(data: dict[str, Any]) -> None:
    cfg_path = _project_root() / "config" / "tool_config.json"
    cfg_path.parent.mkdir(parents=True, exist_ok=True)
    cfg_path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")


def _read_gpm_setting() -> dict[str, Any]:
    setting_path = GPM_ROAMING / "setting.dat"
    if not setting_path.exists():
        return {}
    try:
        return json.loads(setting_path.read_text(encoding="utf-8", errors="replace"))
    except Exception:
        return {}


def _get_gpm_storage_path() -> Path:
    """Return profile storage dir. Prefers local profiles/ for portability."""
    LOCAL_PROFILES_DIR.mkdir(parents=True, exist_ok=True)
    local_db = LOCAL_PROFILES_DIR / "database.db"
    if local_db.exists() or LOCAL_BROWSER_DIR.exists():
        return LOCAL_PROFILES_DIR

    setting = _read_gpm_setting()
    local_path = setting.get("local_storage_path", "")
    if local_path and Path(local_path).exists():
        return Path(local_path)
    if GPM_DEFAULT_STORAGE.exists():
        return GPM_DEFAULT_STORAGE
    return LOCAL_PROFILES_DIR


def _get_gpm_db_path() -> Path:
    return _get_gpm_storage_path() / "database.db"


def _find_gpm_chrome() -> tuple[str, str]:
    """Return (chrome_exe, gpmdriver_exe). Checks local browser/ first, then GPM install."""
    local_chrome = LOCAL_BROWSER_DIR / "chrome.exe"
    local_driver = LOCAL_BROWSER_DIR / "gpmdriver.exe"
    if local_chrome.exists():
        _log.info("Using local bundled browser: %s", local_chrome)
        return str(local_chrome), str(local_driver) if local_driver.exists() else ""

    browsers_dir = GPM_ROAMING / "Browsers"
    if not browsers_dir.exists():
        raise FileNotFoundError(
            f"Browser not found. Place chrome.exe in {LOCAL_BROWSER_DIR} "
            f"or install GPMLoginGlobal."
        )

    chrome_exe = ""
    driver_exe = ""
    for core_dir in sorted(browsers_dir.iterdir(), reverse=True):
        if not core_dir.is_dir() or "chromium" not in core_dir.name.lower():
            continue
        c = core_dir / "chrome.exe"
        d = core_dir / "gpmdriver.exe"
        if c.exists():
            chrome_exe = str(c)
        if d.exists():
            driver_exe = str(d)
        if chrome_exe:
            break

    if not chrome_exe:
        raise FileNotFoundError(f"chrome.exe not found in {browsers_dir}")
    return chrome_exe, driver_exe


_SCREEN_RESOLUTIONS = [
    (1920, 1080), (1366, 768), (1536, 864), (1440, 900), (1600, 900),
    (2560, 1440), (1680, 1050), (1280, 720), (1280, 800), (1280, 1024),
    (1360, 768), (1920, 1200), (2560, 1080), (3440, 1440), (3840, 2160),
]
_DEVICE_MEMORIES = [2, 4, 8, 16, 32]
_HW_CONCURRENCIES = [2, 4, 6, 8, 12, 16, 24]
_HEAP_SIZES = [2147483648, 4294705152, 4294967296]
_LANGUAGES = [
    "en-US", "en-GB", "vi-VN", "fr-FR", "de-DE", "ja-JP", "ko-KR",
    "zh-CN", "pt-BR", "es-ES", "it-IT", "ru-RU", "th-TH", "id-ID",
]
_PLATFORMS = ["Win32", "Win64"]
_GL_VENDORS = [
    "Google Inc. (NVIDIA)", "Google Inc. (AMD)", "Google Inc. (Intel)",
    "Google Inc.", "NVIDIA Corporation", "ATI Technologies Inc.",
    "Intel Inc.", "Mesa",
]
_GL_RENDERERS = [
    "ANGLE (NVIDIA, NVIDIA GeForce GTX 1660 SUPER Direct3D11 vs_5_0 ps_5_0, D3D11)",
    "ANGLE (NVIDIA, NVIDIA GeForce RTX 3060 Direct3D11 vs_5_0 ps_5_0, D3D11)",
    "ANGLE (NVIDIA, NVIDIA GeForce RTX 4070 Direct3D11 vs_5_0 ps_5_0, D3D11)",
    "ANGLE (AMD, AMD Radeon RX 580 Direct3D11 vs_5_0 ps_5_0, D3D11)",
    "ANGLE (AMD, AMD Radeon RX 6700 XT Direct3D11 vs_5_0 ps_5_0, D3D11)",
    "ANGLE (Intel, Intel(R) UHD Graphics 630 Direct3D11 vs_5_0 ps_5_0, D3D11)",
    "ANGLE (Intel, Intel(R) Iris(R) Xe Graphics Direct3D11 vs_5_0 ps_5_0, D3D11)",
    "ANGLE (NVIDIA, NVIDIA GeForce GTX 1080 Ti Direct3D11 vs_5_0 ps_5_0, D3D11)",
    "ANGLE (NVIDIA, NVIDIA GeForce RTX 2070 SUPER Direct3D11 vs_5_0 ps_5_0, D3D11)",
    "ANGLE (AMD, AMD Radeon RX 5700 XT Direct3D11 vs_5_0 ps_5_0, D3D11)",
]
_CHROME_VERSIONS = [
    "130.0.6723.117", "131.0.6778.86", "132.0.6834.110", "133.0.6900.91",
    "134.0.6945.67", "135.0.7023.55", "136.0.7100.42", "137.0.7151.68",
    "138.0.7204.93", "139.0.7258.44", "140.0.7312.65", "141.0.7390.81",
    "142.0.7444.163",
]


def _random_fingerprint() -> dict[str, Any]:
    """Generate a unique random fingerprint for each browser session."""
    import random as _rnd

    w, h = _rnd.choice(_SCREEN_RESOLUTIONS)
    hw_conc = _rnd.choice(_HW_CONCURRENCIES)
    dev_mem = _rnd.choice(_DEVICE_MEMORIES)
    heap_sz = _rnd.choice(_HEAP_SIZES)
    lang = _rnd.choice(_LANGUAGES)
    platform = _rnd.choice(_PLATFORMS)
    gl_vendor = _rnd.choice(_GL_VENDORS)
    gl_renderer = _rnd.choice(_GL_RENDERERS)
    dnt = _rnd.choice([True, False])

    chrome_ver = _rnd.choice(_CHROME_VERSIONS)
    major = chrome_ver.split(".")[0]
    ua = (
        f"Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        f"AppleWebKit/537.36 (KHTML, like Gecko) "
        f"Chrome/{major}.0.0.0 Safari/537.36"
    )

    color_depth = _rnd.choice([24, 32])
    pixel_ratio = _rnd.choice([1.0, 1.25, 1.5, 2.0])

    return {
        "taskbar_title": "",
        "navigator": {
            "allow_custom_user_agent": True,
            "user_agent": ua,
            "auto_language": False,
            "language": lang,
            "do_not_track": dnt,
            "hardware_concurrency": hw_conc,
            "device_memory": dev_mem,
            "js_heap_size_limit": heap_sz,
            "platform": platform,
        },
        "canvas": {"mode": _rnd.choice([1, 2])},
        "client_rect": {"mode": _rnd.choice([1, 2])},
        "webgl_image": {"mode": _rnd.choice([1, 2])},
        "webgl_metadata": {
            "mode": 1,
            "vendor": gl_vendor,
            "renderer": gl_renderer,
        },
        "audio_context": {"mode": _rnd.choice([1, 2])},
        "screen": {"mode": 1, "width": w, "height": h,
                    "color_depth": color_depth, "pixel_ratio": pixel_ratio},
        "font": {"mode": _rnd.choice([1, 2])},
        "media_devices": {"is_masked": True},
        "webrtc": {"mode": _rnd.choice([1, 2, 3])},
        "timezone": {"auto": True, "timezone": ""},
        "geolocation": {"mode": 0},
    }


def _normalize_language(code: str) -> str:
    code = (code or "").strip()
    if not code:
        return "en-US"
    if re.match(r"^[a-z]{2}-[A-Z]{2}$", code):
        return code
    if re.match(r"^[a-z]{2}$", code):
        return f"{code.lower()}-{code.upper()}"
    return "en-US"


def _build_fingerprint_js(fp: dict[str, Any]) -> str:
    """Build JavaScript to inject fingerprint overrides into the page."""
    nav = fp.get("navigator", {})
    screen = fp.get("screen", {})
    webgl = fp.get("webgl_metadata", {})

    ua = nav.get("user_agent", "")
    platform = nav.get("platform", "Win32")
    lang = nav.get("language", "en-US")
    hw = nav.get("hardware_concurrency", 8)
    mem = nav.get("device_memory", 8)
    dnt = "1" if nav.get("do_not_track") else "null"
    w = screen.get("width", 1920)
    h = screen.get("height", 1080)
    cd = screen.get("color_depth", 24)
    pr = screen.get("pixel_ratio", 1.0)
    gl_v = webgl.get("vendor", "")
    gl_r = webgl.get("renderer", "")

    return f"""(function() {{
  try {{
    Object.defineProperty(navigator, 'hardwareConcurrency', {{get: () => {hw}}});
    Object.defineProperty(navigator, 'deviceMemory', {{get: () => {mem}}});
    Object.defineProperty(navigator, 'platform', {{get: () => '{platform}'}});
    Object.defineProperty(navigator, 'language', {{get: () => '{lang}'}});
    Object.defineProperty(navigator, 'languages', {{get: () => ['{lang}', '{lang.split("-")[0]}']}});
    Object.defineProperty(navigator, 'doNotTrack', {{get: () => {dnt}}});
    Object.defineProperty(navigator, 'webdriver', {{get: () => undefined}});
    Object.defineProperty(screen, 'width', {{get: () => {w}}});
    Object.defineProperty(screen, 'height', {{get: () => {h}}});
    Object.defineProperty(screen, 'availWidth', {{get: () => {w}}});
    Object.defineProperty(screen, 'availHeight', {{get: () => {h - 40}}});
    Object.defineProperty(screen, 'colorDepth', {{get: () => {cd}}});
    Object.defineProperty(screen, 'pixelDepth', {{get: () => {cd}}});
    Object.defineProperty(window, 'devicePixelRatio', {{get: () => {pr}}});
    Object.defineProperty(window, 'outerWidth', {{get: () => {w}}});
    Object.defineProperty(window, 'outerHeight', {{get: () => {h}}});
  }} catch(e) {{}}
  try {{
    const getParam = WebGLRenderingContext.prototype.getParameter;
    WebGLRenderingContext.prototype.getParameter = function(p) {{
      if (p === 37445) return '{gl_v}';
      if (p === 37446) return '{gl_r}';
      return getParam.call(this, p);
    }};
    const getParam2 = WebGL2RenderingContext.prototype.getParameter;
    WebGL2RenderingContext.prototype.getParameter = function(p) {{
      if (p === 37445) return '{gl_v}';
      if (p === 37446) return '{gl_r}';
      return getParam2.call(this, p);
    }};
  }} catch(e) {{}}
}})();"""


def _get_default_dynamic_data() -> dict[str, Any]:
    return {
        "proxy": {"raw_proxy": "", "proxy_region": ""},
        "note": "",
        "color": None,
        "browser_type": 1,
        "browser_version": "142.0.7444.163",
        "os_type": 1,
    }


def _create_profile_in_db(
    profile_name: str = "",
    group_id: str = "",
) -> tuple[str, str, Path]:
    """Create a new profile in GPM's SQLite database.
    Returns (profile_id, profile_name, profile_user_data_dir).
    """
    db_path = _get_gpm_db_path()
    if not db_path.exists():
        raise FileNotFoundError(f"GPM database not found: {db_path}")

    storage_root = _get_gpm_storage_path()
    profile_id = str(uuid.uuid4())

    if not profile_name:
        suffix = secrets.token_hex(2).upper()
        now = datetime.now().strftime("%Y%m%d_%H%M%S")
        profile_name = f"AUTO_{now}_{suffix}"

    conn = sqlite3.connect(str(db_path))
    try:
        cur = conn.cursor()
        if not group_id:
            cur.execute("SELECT id FROM groups ORDER BY [order] ASC LIMIT 1")
            row = cur.fetchone()
            group_id = row[0] if row else str(uuid.uuid4())

        fingerprint = json.dumps(_random_fingerprint(), ensure_ascii=False)
        dynamic = json.dumps(_get_default_dynamic_data(), ensure_ascii=False)
        now_str = datetime.utcnow().isoformat()

        cur.execute(
            """INSERT INTO profiles
               (id, name, group_id, storage_path, fingerprint_data, dynamic_data,
                is_deleted, created_at, updated_at)
               VALUES (?, ?, ?, ?, ?, ?, 0, ?, ?)""",
            (profile_id, profile_name, group_id, profile_id, fingerprint, dynamic, now_str, now_str),
        )
        conn.commit()
    finally:
        conn.close()

    profile_dir = storage_root / profile_id
    profile_dir.mkdir(parents=True, exist_ok=True)

    _log.info("Created GPM profile: %s (%s) at %s", profile_name, profile_id, profile_dir)
    return profile_id, profile_name, profile_dir


def _list_profiles_from_db(limit: int = 10) -> list[dict[str, Any]]:
    db_path = _get_gpm_db_path()
    if not db_path.exists():
        return []
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    try:
        cur = conn.cursor()
        cur.execute("SELECT id, name, storage_path FROM profiles WHERE is_deleted=0 ORDER BY created_at DESC LIMIT ?", (limit,))
        return [dict(r) for r in cur.fetchall()]
    finally:
        conn.close()


# Mỗi Chrome 1 port debug. 19200–19499 = 300 port → tối đa ~300 luồng đồng thời (40–50 tab/máy 64GB chạy thoải mái).
DEBUG_PORT_START = 19200
DEBUG_PORT_END = 19500

import threading as _threading
_port_lock = _threading.Lock()
_browser_launch_lock = _threading.Lock()
_used_ports: set[int] = set()
_active_drivers: list = []
_drivers_lock = _threading.Lock()
_browser_scale_factor: float = 1.0
_tile_timer: "_threading.Timer | None" = None   # debounce timer để tile sau khi tất cả cửa sổ mở
_tile_timer_lock = _threading.Lock()

# Đảm bảo khoảng cách tối thiểu giữa 2 lần mở browser (để window trước kịp load)
_LAUNCH_INTERVAL: float = 4.0   # giây giữa 2 lần launch
_last_launch_time: float = 0.0

# ── Proxy management ──────────────────────────────────────────────────────────
_use_proxy: bool = True          # master on/off, đổi real-time từ GUI
_proxy_single: str = ""          # proxy đơn từ ô Settings
_proxy_list: list[str] = []      # list từ list_proxy.txt
_proxy_list_lock = _threading.Lock()

# ── Profile management ────────────────────────────────────────────────────────
_no_save_profile: bool = False   # True → xóa profile tạm sau mỗi session


def set_use_proxy(val: bool) -> None:
    global _use_proxy
    _use_proxy = bool(val)
    _log.info("PROXY: %s", "bật" if _use_proxy else "tắt")


def get_use_proxy() -> bool:
    return _use_proxy


def set_no_save_profile(val: bool) -> None:
    global _no_save_profile
    _no_save_profile = bool(val)
    _log.info("NO_SAVE_PROFILE: %s", "bật" if _no_save_profile else "tắt")


def get_no_save_profile() -> bool:
    return _no_save_profile


def set_single_proxy(proxy: str) -> None:
    """Cập nhật proxy đơn (từ ô Settings)."""
    global _proxy_single
    _proxy_single = (proxy or "").strip()


def load_proxy_list(path: str | None = None) -> int:
    """Đọc list_proxy.txt, trả về số proxy đã load.
    Mỗi dòng: host:port:user:pass  hoặc  host:port
    """
    global _proxy_list, _proxy_index
    if path is None:
        path = str(TOOL_DIR / "data" / "list_proxy.txt")
    try:
        lines = Path(path).read_text(encoding="utf-8").splitlines()
    except FileNotFoundError:
        lines = []
    except Exception as e:
        _log.warning("load_proxy_list: lỗi đọc %s: %s", path, e)
        lines = []
    cleaned = [ln.strip() for ln in lines if ln.strip() and not ln.strip().startswith("#")]
    with _proxy_list_lock:
        _proxy_list = cleaned
    _log.info("PROXY LIST: load %d proxy từ %s", len(cleaned), path)
    return len(cleaned)


def _pick_proxy() -> str:
    """Bốc ngẫu nhiên 1 proxy từ list (rotating).
    Chỉ gọi khi _use_proxy=True.
    """
    import random
    with _proxy_list_lock:
        if _proxy_list:
            proxy = random.choice(_proxy_list)
            _log.info("PROXY PICK: %s", proxy.split(":")[0] + ":" + proxy.split(":")[1] if ":" in proxy else proxy)
            return proxy
    return _proxy_single


def register_driver(driver):
    with _drivers_lock:
        _active_drivers.append(driver)
    # Tile ngay lập tức (cửa sổ mới xuất hiện đúng chỗ)
    tile_browser_windows()
    # Tile lại sau 2s (chờ tất cả cửa sổ còn lại mở xong)
    _schedule_tile(delay=2.0)


def _schedule_tile(delay: float = 2.0):
    """Debounce: tile sau `delay` giây kể từ lần đăng ký cuối cùng.
    Mỗi lần có cửa sổ mới, reset timer → tile 1 lần sau khi tất cả mở xong.
    """
    global _tile_timer
    with _tile_timer_lock:
        if _tile_timer is not None:
            _tile_timer.cancel()
        _tile_timer = _threading.Timer(delay, tile_browser_windows)
        _tile_timer.daemon = True
        _tile_timer.start()


def unregister_driver(driver):
    with _drivers_lock:
        try:
            _active_drivers.remove(driver)
        except ValueError:
            pass


def get_active_drivers() -> list:
    with _drivers_lock:
        return list(_active_drivers)


def _get_screen_size() -> tuple[int, int]:
    try:
        import ctypes
        user32 = ctypes.windll.user32
        user32.SetProcessDPIAware()
        return user32.GetSystemMetrics(0), user32.GetSystemMetrics(1)
    except Exception:
        return 1920, 1080


def tile_browser_windows(scale_factor: float | None = None):
    """Sắp xếp tất cả browser windows:
    - Xếp hàng ngang trước, khi hết hàng thì xuống dòng
    - Kích thước đồng đều, fill đầy ô (scale_factor = tỉ lệ thu nhỏ trong ô)
    - scale_factor: 0.1–1.0, None = giữ nguyên
    """
    global _browser_scale_factor
    if scale_factor is not None:
        _browser_scale_factor = max(0.1, min(1.0, float(scale_factor)))

    drivers = get_active_drivers()
    n = len(drivers)
    if n <= 0:
        return

    screen_w, screen_h = _get_screen_size()
    taskbar_h = 48
    usable_w = screen_w
    usable_h = screen_h - taskbar_h

    # Số cột tối ưu: xếp hàng ngang trước, ưu tiên ít hàng
    # Với n cửa sổ, tìm số cột sao cho chiều rộng ô >= 300px
    min_cell_w = 300
    max_cols = max(1, usable_w // min_cell_w)
    cols = min(n, max_cols)
    rows = (n + cols - 1) // cols

    # Kích thước ô đầy đủ
    cell_w = usable_w // cols
    cell_h = usable_h // rows

    # Kích thước cửa sổ trong ô (có thể thu nhỏ theo scale)
    win_w = max(280, int(cell_w * _browser_scale_factor))
    win_h = max(200, int(cell_h * _browser_scale_factor))

    # Căn giữa trong ô nếu scale < 1
    offset_x = (cell_w - win_w) // 2
    offset_y = (cell_h - win_h) // 2

    for i, drv in enumerate(drivers):
        try:
            col = i % cols
            row = i // cols
            x = col * cell_w + offset_x
            y = row * cell_h + offset_y
            drv.set_window_position(x, y)
            drv.set_window_size(win_w, win_h)
        except Exception:
            pass


def set_browser_scale(factor: float):
    tile_browser_windows(scale_factor=factor)


def get_browser_scale() -> float:
    return _browser_scale_factor


def _find_free_debug_port(start: int = DEBUG_PORT_START, end: int = DEBUG_PORT_END) -> int:
    import socket
    with _port_lock:
        for port in range(start, end):
            if port in _used_ports:
                continue
            try:
                with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                    s.settimeout(0.1)
                    s.bind(("127.0.0.1", port))
                    _used_ports.add(port)
                    return port
            except OSError:
                continue
        raise RuntimeError(f"No free port found in range {start}-{end}")


def _release_debug_port(port: int):
    with _port_lock:
        _used_ports.discard(port)


def _parse_proxy(raw: str) -> dict[str, str]:
    """Parse proxy string. Supports formats:
    - host:port:user:pass  (rotating proxy)
    - user:pass@host:port
    - host:port
    - http://user:pass@host:port
    Returns dict with keys: host, port, user, pass, scheme.
    """
    raw = (raw or "").strip()
    if not raw:
        return {}

    scheme = "http"
    if raw.startswith("socks5://"):
        scheme = "socks5"
        raw = raw[len("socks5://"):]
    elif raw.startswith("socks4://"):
        scheme = "socks4"
        raw = raw[len("socks4://"):]
    elif raw.startswith("http://"):
        raw = raw[len("http://"):]
    elif raw.startswith("https://"):
        raw = raw[len("https://"):]

    user = passwd = ""

    if "@" in raw:
        auth_part, host_part = raw.rsplit("@", 1)
        parts_hp = host_part.split(":")
        host = parts_hp[0]
        port = parts_hp[1] if len(parts_hp) > 1 else "80"
        auth_parts = auth_part.split(":", 1)
        user = auth_parts[0]
        passwd = auth_parts[1] if len(auth_parts) > 1 else ""
    else:
        parts = raw.split(":")
        if len(parts) == 4:
            host, port, user, passwd = parts
        elif len(parts) == 3:
            host, port, user = parts
        elif len(parts) == 2:
            host, port = parts
        else:
            host = parts[0]
            port = "80"

    return {"host": host, "port": port, "user": user, "pass": passwd, "scheme": scheme}


def _create_proxy_auth_extension(proxy_info: dict[str, str], ext_dir: str) -> str:
    """Create a Chrome extension that handles proxy auth automatically.
    Returns path to the extension directory.
    """
    ext_path = Path(ext_dir) / "_proxy_auth_ext"
    ext_path.mkdir(parents=True, exist_ok=True)

    host = proxy_info["host"]
    port = proxy_info["port"]
    user = proxy_info["user"]
    passwd = proxy_info["pass"]
    scheme = proxy_info.get("scheme", "http")

    manifest = {
        "version": "1.0.0",
        "manifest_version": 3,
        "name": "Proxy Auth",
        "permissions": ["proxy", "webRequest", "webRequestAuthProvider"],
        "host_permissions": ["<all_urls>"],
        "background": {"service_worker": "background.js"},
        "minimum_chrome_version": "108.0",
    }
    (ext_path / "manifest.json").write_text(
        json.dumps(manifest, indent=2), encoding="utf-8"
    )

    bg_js = f"""const config = {{
  mode: "fixed_servers",
  rules: {{
    singleProxy: {{
      scheme: "{scheme}",
      host: "{host}",
      port: {port}
    }},
    bypassList: ["localhost"]
  }}
}};
chrome.proxy.settings.set({{value: config, scope: "regular"}});
chrome.webRequest.onAuthRequired.addListener(
  (details, callbackFn) => {{
    callbackFn({{
      authCredentials: {{
        username: "{user}",
        password: "{passwd}"
      }}
    }});
  }},
  {{urls: ["<all_urls>"]}},
  ["asyncBlocking"]
);"""
    (ext_path / "background.js").write_text(bg_js, encoding="utf-8")

    _log.info("Proxy auth extension created: %s (proxy=%s:%s)", ext_path, host, port)
    return str(ext_path)


def _launch_gpm_chrome(
    profile_dir: str,
    chrome_exe: str,
    debug_port: int,
    startup_url: str = "https://accounts.google.com/",
    proxy: str = "",
    extra_args: list[str] | None = None,
) -> subprocess.Popen:
    """Launch Chromium binary with profile and remote debugging.
    Supports rotating proxy with user:pass via auto-generated auth extension.
    """
    proxy_info = _parse_proxy(proxy)
    has_proxy_auth = bool(proxy_info.get("user"))

    args = [
        chrome_exe,
        f"--user-data-dir={profile_dir}",
        f"--remote-debugging-port={debug_port}",
        "--no-first-run",
        "--no-default-browser-check",
        "--disable-background-networking",
        "--disable-sync",
        "--disable-plugins",
        "--disable-dev-shm-usage",
        "--disable-gpu",
        "--disable-software-rasterizer",
        "--disable-background-timer-throttling",
        "--disable-renderer-backgrounding",
        "--disable-backgrounding-occluded-windows",
        "--disable-features=TranslateUI",
        "--js-flags=--max-old-space-size=512",
    ]

    if proxy_info:
        host = proxy_info["host"]
        port = proxy_info["port"]
        scheme = proxy_info.get("scheme", "http")
        if has_proxy_auth:
            ext_path = _create_proxy_auth_extension(proxy_info, profile_dir)
            args.append(f"--load-extension={ext_path}")
        else:
            args.append(f"--proxy-server={scheme}://{host}:{port}")
            args.append("--disable-extensions")
    else:
        args.append("--disable-extensions")

    if extra_args:
        args.extend(extra_args)
    if startup_url:
        args.append(startup_url)

    _log.info("Launching Chrome: %s", " ".join(args[:5]) + " ...")
    if proxy_info:
        _log.info("Proxy: %s:%s (auth=%s)", proxy_info["host"], proxy_info["port"],
                   "yes" if has_proxy_auth else "no")
    proc = subprocess.Popen(
        args,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    return proc


def _wait_for_debugger(port: int, timeout: float = 15.0) -> bool:
    import socket
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.settimeout(1.0)
                s.connect(("127.0.0.1", port))
                return True
        except (ConnectionRefusedError, OSError):
            time.sleep(0.5)
    return False


def _attach_selenium(
    debug_port: int,
    driver_path: str = "",
    proxy: str = "",
):
    from selenium import webdriver
    from selenium.webdriver.chrome.options import Options
    from selenium.webdriver.chrome.service import Service

    debugger_address = f"127.0.0.1:{debug_port}"

    opts = Options()
    opts.add_experimental_option("debuggerAddress", debugger_address)

    service = None
    if driver_path and os.path.exists(driver_path):
        service = Service(driver_path)
    else:
        try:
            from webdriver_manager.chrome import ChromeDriverManager
            service = Service(ChromeDriverManager().install())
        except Exception:
            service = None

    if service is not None:
        driver = webdriver.Chrome(service=service, options=opts)
    else:
        driver = webdriver.Chrome(options=opts)

    # Tăng connection pool size để tránh "Connection pool is full" khi tool polling nhanh.
    try:
        driver.command_executor._conn.clear()  # type: ignore[attr-defined]
    except Exception:
        pass
    try:
        import urllib3
        adapter = driver.command_executor._client_config  # type: ignore[attr-defined]
    except Exception:
        pass
    try:
        sess = driver.command_executor._session  # type: ignore[attr-defined]
        if hasattr(sess, "adapters"):
            from requests.adapters import HTTPAdapter
            sess.mount("http://", HTTPAdapter(pool_connections=4, pool_maxsize=8))
    except Exception:
        pass

    _switch_to_main_tab(driver)
    return driver


def _switch_to_main_tab(driver, timeout: float = 10.0) -> None:
    """Switch away from extension background pages to the real browser tab."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        handles = driver.window_handles
        for h in handles:
            try:
                driver.switch_to.window(h)
                url = driver.current_url or ""
                if not url.startswith("chrome-extension://") and not url.startswith("chrome://"):
                    return
            except Exception:
                continue
        time.sleep(0.5)

    if driver.window_handles:
        driver.switch_to.window(driver.window_handles[-1])


def _inject_fingerprint(driver, fp: dict[str, Any]) -> None:
    """Inject fingerprint JS into browser via CDP (persists across navigations) and direct execution."""
    js = _build_fingerprint_js(fp)
    try:
        driver.execute_cdp_cmd("Page.addScriptToEvaluateOnNewDocument", {"source": js})
    except Exception:
        pass
    try:
        driver.execute_script(js)
    except Exception:
        pass

    nav = fp.get("navigator", {})
    ua = nav.get("user_agent", "")
    if ua:
        try:
            driver.execute_cdp_cmd("Network.setUserAgentOverride", {
                "userAgent": ua,
                "platform": nav.get("platform", "Win32"),
                "acceptLanguage": nav.get("language", "en-US"),
            })
        except Exception:
            pass


def _find_gologin_chrome(version_hint: str = "") -> tuple[str, str]:
    """Return (chrome_exe, chromedriver_exe) from GoLogin Orbita browser.
    version_hint: '139' or '143'. Empty = pick latest available.
    Tìm local (browser/gologin/) trước, fallback sang ~/.gologin/browser/.
    """
    search_dirs = []
    if LOCAL_GOLOGIN_BROWSER_DIR.exists():
        search_dirs.append(LOCAL_GOLOGIN_BROWSER_DIR)
    if GOLOGIN_BROWSER_DIR.exists():
        search_dirs.append(GOLOGIN_BROWSER_DIR)

    if not search_dirs:
        raise FileNotFoundError(
            f"GoLogin browser not found.\n"
            f"  Local: {LOCAL_GOLOGIN_BROWSER_DIR}\n"
            f"  User:  {GOLOGIN_BROWSER_DIR}\n"
            f"Copy orbita-browser-XXX vào browser/gologin/ hoặc cài GoLogin."
        )

    hint = version_hint.strip()

    def _pick(orbita_dir: Path) -> tuple[str, str] | None:
        chrome = orbita_dir / "chrome.exe"
        if not chrome.exists():
            return None
        driver = orbita_dir / "chromedriver.exe"
        driver_str = str(driver) if driver.exists() else ""
        return str(chrome), driver_str

    for base_dir in search_dirs:
        if hint and hint in _ORBITA_VERSIONS:
            result = _pick(base_dir / _ORBITA_VERSIONS[hint])
            if result:
                _log.info("Using GoLogin Orbita %s: %s (driver: %s)", hint, result[0], result[1] or "auto")
                return result

    for base_dir in search_dirs:
        for ver_key in sorted(_ORBITA_VERSIONS.keys(), reverse=True):
            result = _pick(base_dir / _ORBITA_VERSIONS[ver_key])
            if result:
                _log.info("Using GoLogin Orbita %s (fallback): %s (driver: %s)", ver_key, result[0], result[1] or "auto")
                return result

    for base_dir in search_dirs:
        for d in sorted(base_dir.iterdir(), reverse=True):
            if d.is_dir() and d.name.startswith("orbita-browser"):
                result = _pick(d)
                if result:
                    _log.info("Using GoLogin Orbita (auto-detected): %s (driver: %s)", result[0], result[1] or "auto")
                    return result

    raise FileNotFoundError(
        f"No Orbita chrome.exe found.\n"
        f"Searched: {', '.join(str(d) for d in search_dirs)}\n"
        f"Expected: {', '.join(_ORBITA_VERSIONS.values())}"
    )


def _register_temp_profile_cleanup(driver: Any, profile_dir: Path) -> None:
    """Đăng ký cleanup: xóa profile_dir sau khi driver.quit() được gọi."""
    import shutil

    orig_quit = driver.quit

    def _quit_and_cleanup(*a, **kw):
        try:
            orig_quit(*a, **kw)
        finally:
            try:
                if profile_dir.exists():
                    shutil.rmtree(str(profile_dir), ignore_errors=True)
                    _log.info("Đã xóa profile tạm: %s", profile_dir)
            except Exception as e:
                _log.debug("Không xóa được profile tạm %s: %s", profile_dir, e)

    driver.quit = _quit_and_cleanup


def _start_gologin_direct(
    cfg: dict[str, Any],
    proxy: str = "",
    orbita_version: str = "",
) -> Any:
    """Launch 1 Chrome (GoLogin Orbita) cho 1 acc.

    Dùng nhân GoLogin Orbita (anti-detect browser):
    - Profile tạm (tempdir) mỗi phiên
    - Fingerprint random inject qua CDP
    - Selenium attach qua remote debugging port
    """
    import tempfile

    chrome_exe, driver_exe = _find_gologin_chrome(orbita_version)

    profile_dir = Path(tempfile.mkdtemp(
        prefix="gologin_", suffix="_" + secrets.token_hex(2)
    ))
    profile_dir_str = str(profile_dir)
    _log.info("GoLogin profile temp: %s", profile_dir_str)

    debug_port = _find_free_debug_port()
    startup_url = str(
        cfg.get("gpm_startup_url", "https://accounts.google.com/") or ""
    ).strip()
    effective_proxy = proxy  # proxy đã được kiểm soát từ patched_create_driver (use_proxy toggle)

    launch_fp = _random_fingerprint()
    fixed_lang = _normalize_language(
        str(cfg.get("gpm_language", "en-US") or "en-US")
    )
    launch_fp["navigator"]["language"] = fixed_lang

    real_ver = _get_orbita_chrome_version(chrome_exe)
    if real_ver:
        major = real_ver.split(".")[0]
        launch_fp["navigator"]["user_agent"] = (
            f"Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            f"AppleWebKit/537.36 (KHTML, like Gecko) "
            f"Chrome/{major}.0.0.0 Safari/537.36"
        )

    launch_ua = launch_fp["navigator"]["user_agent"]
    launch_lang = fixed_lang
    sw = launch_fp["screen"]["width"]
    sh = launch_fp["screen"]["height"]

    extra_launch_args = [
        f"--user-agent={launch_ua}",
        f"--lang={launch_lang}",
        f"--window-size={sw},{sh}",
        "--disable-blink-features=AutomationControlled",
    ]

    with _browser_launch_lock:
        proc = _launch_gpm_chrome(
            profile_dir=profile_dir_str,
            chrome_exe=chrome_exe,
            debug_port=debug_port,
            startup_url=startup_url,
            proxy=effective_proxy,
            extra_args=extra_launch_args,
        )

        if not _wait_for_debugger(debug_port, timeout=20.0):
            _release_debug_port(debug_port)
            try:
                proc.kill()
            except Exception:
                pass
            raise RuntimeError(
                f"GoLogin Orbita did not start on port {debug_port} within 20s"
            )

        time.sleep(2.0)

        _log.info(
            "GoLogin debugger up on port %d, attaching Selenium (driver: %s)",
            debug_port, driver_exe or "auto",
        )
        driver = _attach_selenium(
            debug_port=debug_port,
            driver_path=driver_exe,
            proxy=effective_proxy,
        )

    _inject_fingerprint(driver, launch_fp)
    _log.info(
        "GoLogin fingerprint injected: screen=%dx%d, hw=%d, mem=%dGB, lang=%s, UA=%s",
        sw, sh,
        launch_fp["navigator"]["hardware_concurrency"],
        launch_fp["navigator"]["device_memory"],
        launch_lang,
        launch_ua[:60],
    )

    register_driver(driver)
    if _no_save_profile:
        _register_temp_profile_cleanup(driver, profile_dir)
    return driver


def _get_orbita_chrome_version(chrome_exe: str) -> str:
    """Read the version file next to chrome.exe to get Orbita's real Chrome version."""
    chrome_path = Path(chrome_exe)
    version_file = chrome_path.parent / "version"
    if version_file.exists():
        try:
            raw = version_file.read_text(encoding="utf-8", errors="replace").strip()
            parts = raw.split(".")
            if len(parts) >= 1 and parts[0].isdigit():
                return raw.split("-")[0]
        except Exception:
            pass
    for d in chrome_path.parent.iterdir():
        if d.is_dir() and re.match(r"\d+\.\d+\.\d+\.\d+", d.name):
            return d.name
    return ""


def _start_gpm_direct(
    cfg: dict[str, Any],
    proxy: str = "",
) -> Any:
    """Launch 1 Chrome (GPM) cho 1 acc. Cách tạo profile hiện nay:

    1) Nếu config có gpm_profile_id và thư mục tương ứng tồn tại:
       → Dùng profile cố định đó (storage_root / profile_id). Hiếm dùng.

    2) Ngược lại (mặc định):
       → Tạo profile TẠM: tempfile.mkdtemp(prefix="gpm_", suffix="_<random>").
       → Chrome chạy với --user-data-dir=<thư mục tạm đó>.
       → Hết phiên (driver.quit()) thì process tắt; thư mục tạm có thể bị OS dọn sau.
       → Không lưu vào GPM, không tạo 1 profile/acc. Mỗi lần mở browser = 1 profile tạm mới.

    3) Mỗi Chrome dùng 1 port debug riêng (19200–19499), fingerprint ngẫu nhiên (UA, lang, kích thước).
    """
    import tempfile
    chrome_exe, driver_exe = _find_gpm_chrome()

    use_temp = True
    profile_id = str(cfg.get("gpm_profile_id", "") or "").strip()
    if profile_id:
        storage_root = _get_gpm_storage_path()
        profile_dir = storage_root / profile_id
        if profile_dir.exists():
            use_temp = False
            profile_dir_str = str(profile_dir)
        else:
            _log.warning("Profile dir %s not found, dùng temp", profile_dir)
            profile_id = ""

    if use_temp or not profile_id:
        profile_dir = Path(tempfile.mkdtemp(prefix="gpm_", suffix="_" + secrets.token_hex(2)))
        profile_dir_str = str(profile_dir)
        _log.info("Profile temp (không lưu): %s", profile_dir_str)
    debug_port = _find_free_debug_port()
    startup_url = str(cfg.get("gpm_startup_url", "https://accounts.google.com/") or "").strip()
    effective_proxy = proxy  # proxy đã được kiểm soát từ patched_create_driver (use_proxy toggle)

    launch_fp = _random_fingerprint()
    # Khóa ngôn ngữ để tránh random locale gây lệch flow đăng nhập.
    fixed_lang = _normalize_language(str(cfg.get("gpm_language", "en-US") or "en-US"))
    launch_fp["navigator"]["language"] = fixed_lang
    launch_ua = launch_fp["navigator"]["user_agent"]
    launch_lang = fixed_lang
    sw = launch_fp["screen"]["width"]
    sh = launch_fp["screen"]["height"]

    extra_launch_args = [
        f"--user-agent={launch_ua}",
        f"--lang={launch_lang}",
        f"--window-size={sw},{sh}",
    ]

    with _browser_launch_lock:
        proc = _launch_gpm_chrome(
            profile_dir=profile_dir_str,
            chrome_exe=chrome_exe,
            debug_port=debug_port,
            startup_url=startup_url,
            proxy=effective_proxy,
            extra_args=extra_launch_args,
        )

        if not _wait_for_debugger(debug_port, timeout=15.0):
            _release_debug_port(debug_port)
            try:
                proc.kill()
            except Exception:
                pass
            raise RuntimeError(
                f"Chrome did not start remote debugging on port {debug_port} within 15s"
            )

        time.sleep(2.0)

        _log.info("Chrome debugger up on port %d, attaching Selenium (driver: %s)", debug_port, driver_exe or "auto")
        driver = _attach_selenium(
            debug_port=debug_port,
            driver_path=driver_exe,
            proxy=effective_proxy,
        )

    _inject_fingerprint(driver, launch_fp)
    _log.info(
        "Fingerprint injected: screen=%dx%d, hw=%d, mem=%dGB, lang=%s, UA=%s",
        sw, sh,
        launch_fp["navigator"]["hardware_concurrency"],
        launch_fp["navigator"]["device_memory"],
        launch_lang,
        launch_ua[:60],
    )

    register_driver(driver)
    if _no_save_profile and use_temp:
        _register_temp_profile_cleanup(driver, profile_dir)
    return driver


def apply_google_flow_patch() -> None:
    import google_flow

    if getattr(google_flow, "_gpm_patch_applied", False):
        return

    orig_create_driver = google_flow.create_driver

    def patched_create_driver(proxy: str = "", user_data_dir: str = ""):
        cfg = _load_tool_config_json()
        browser_mode = str(cfg.get("browser_mode", "") or "").strip().lower()

        try:
            from app_config import load_tool_config
            hcfg = load_tool_config()
            browser_opt = str(hcfg.get("browser_option", "")).strip().lower()
        except Exception:
            browser_opt = ""

        # ── Proxy: nguồn DUY NHẤT là _pick_proxy() / rỗng khi tắt ──────────
        if not _use_proxy:
            proxy = ""   # tuyệt đối không proxy, bỏ qua mọi giá trị khác
        else:
            proxy = _pick_proxy()  # round-robin list hoặc single; có thể ""

        # hconfig.ini (GUI) được ưu tiên vì user chọn browser ở đó
        effective = browser_opt or browser_mode

        gologin_map = {
            "gologin-139": "139",
            "gologin-143": "143",
            "gologin": "",
        }
        if effective in gologin_map:
            orbita_ver = gologin_map[effective]
            return _start_gologin_direct(
                cfg=cfg, proxy=proxy, orbita_version=orbita_ver,
            )

        gpm_values = {
            "gpm", "gpm_login", "gpm-login", "gpm_direct",
            "browser-gpm142", "gpmlogin115off",
        }
        if effective in gpm_values:
            return _start_gpm_direct(cfg=cfg, proxy=proxy)

        return orig_create_driver(proxy=proxy, user_data_dir=user_data_dir)

    google_flow._orig_create_driver = orig_create_driver
    google_flow.create_driver = patched_create_driver
    google_flow._gpm_patch_applied = True
