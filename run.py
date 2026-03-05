# -*- coding: utf-8 -*-
"""
Entrypoint mỏng cho Gmail Tool.
"""
from __future__ import annotations

import subprocess
import sys
from pathlib import Path

from gpm_mode_patch import apply_google_flow_patch
from logging_setup import setup_logging
import gui_app

BASE_DIR = Path(__file__).resolve().parent


def _kill_orphan_tool_processes():
    """Mỗi khi mở tool: dừng toàn bộ tiến trình cũ (Chrome/GPM/chromedriver + Python run.py cũ) rồi mới chạy."""
    if sys.platform != "win32":
        return
    import logging
    import os
    _log = logging.getLogger(__name__)
    tool_path = str(BASE_DIR)
    my_pid = os.getpid()
    env = os.environ.copy()
    env["GMAIL_TOOL_PATH"] = tool_path
    env["GMAIL_TOOL_EXCLUDE_PID"] = str(my_pid)
    ps_script = r"""$p = $env:GMAIL_TOOL_PATH
$excludePid = [int]$env:GMAIL_TOOL_EXCLUDE_PID
Get-CimInstance Win32_Process -EA 0 | Where-Object {
  $n = $_.Name; $c = $_.CommandLine
  if (-not $c) { return $false }
  $matchPath = $c.IndexOf($p) -ge 0
  if ($n -match '^(chrome|gpmdriver|chromedriver|orbita)\.exe$' -and $matchPath) { return $true }
  if ($n -eq 'python.exe' -and $c -match 'run\.py' -and $matchPath -and $_.ProcessId -ne $excludePid) { return $true }
  $false
} | ForEach-Object {
  try { Stop-Process -Id $_.ProcessId -Force -EA Stop; $_.ProcessId } catch {}
}"""
    try:
        r = subprocess.run(
            ["powershell", "-NoProfile", "-ExecutionPolicy", "Bypass", "-Command", ps_script],
            capture_output=True,
            text=True,
            timeout=20,
            cwd=str(BASE_DIR),
            env=env,
        )
        pids = [x.strip() for x in (r.stdout or "").splitlines() if x.strip() and x.strip().isdigit()]
        if pids:
            _log.info("Đã dừng %s process cũ (Chrome/GPM/Python): %s", len(pids), ", ".join(pids))
    except Exception as exc:
        _log.debug("Kill orphan processes: %s", exc)


if sys.platform == "win32":
    import io

    if hasattr(sys.stdout, "buffer"):
        sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
    if hasattr(sys.stderr, "buffer"):
        sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8", errors="replace")


def _install_dependencies():
    req_file = BASE_DIR / "requirements.txt"
    if not req_file.exists():
        return
    try:
        subprocess.run(
            [sys.executable, "-m", "pip", "install", "-r", str(req_file), "-q"],
            check=True,
            capture_output=True,
            timeout=120,
        )
    except (subprocess.CalledProcessError, FileNotFoundError, subprocess.TimeoutExpired):
        try:
            subprocess.run(
                [sys.executable, "-m", "pip", "install", "-r", str(req_file)],
                check=True,
                cwd=str(BASE_DIR),
            )
        except Exception:
            pass


def _apply_autosave_patch():
    """Patch gui_app.run_gui so that config auto-saves when GUI fields change."""
    import gui_app
    import tkinter as tk

    _orig_run_gui = gui_app.run_gui

    def _patched_run_gui():
        _orig_Tk_init = tk.Tk.__init__

        def _hooked_Tk_init(self, *args, **kwargs):
            _orig_Tk_init(self, *args, **kwargs)
            _install_autosave(self)
            self.after(600, lambda r=self: _replace_combobox_with_dropdown(r))

        tk.Tk.__init__ = _hooked_Tk_init
        try:
            _orig_run_gui()
        finally:
            tk.Tk.__init__ = _orig_Tk_init

    gui_app.run_gui = _patched_run_gui


def _install_autosave(root):
    """After the root window is created, periodically check for config changes and auto-save."""
    import app_config
    import logging

    _log = logging.getLogger("autosave")
    _last_snapshot: dict[str, str] = {}

    def _collect_entries(widget) -> dict[str, str]:
        """Walk widget tree and collect Entry/Combobox values keyed by variable name."""
        result = {}
        try:
            for child in widget.winfo_children():
                cls_name = child.winfo_class()
                if cls_name in ("TEntry", "TCombobox", "Entry", "Combobox"):
                    try:
                        var = child.cget("textvariable")
                        val = child.get() if hasattr(child, "get") else ""
                        key = str(var) if var else str(child)
                        result[key] = str(val)
                    except Exception:
                        pass
                result.update(_collect_entries(child))
        except Exception:
            pass
        return result

    def _check_and_save():
        nonlocal _last_snapshot
        try:
            current = _collect_entries(root)
            if _last_snapshot and current != _last_snapshot:
                _trigger_save(root)
                _log.debug("Auto-saved config (field changed)")
            _last_snapshot = current
        except Exception:
            pass
        try:
            root.after(3000, _check_and_save)
        except Exception:
            pass

    root.after(5000, _check_and_save)


def _trigger_save(root):
    """Find and call _save_config_from_gui if accessible, else fallback to reading widgets."""
    import app_config

    try:
        hconfig = app_config.load_hconfig()
    except Exception:
        hconfig = {}

    entries = {}
    _walk_entries(root, entries)

    mapping = {
        "text_numthread": "max_workers",
        "text_dialog_recaptcha_apikey": "ez_captcha_api_key",
        "hero_sms_api_key": "hero_sms_api_key",
        "hero_sms_service": "hero_sms_service",
        "hero_sms_get_number_retries": "hero_sms_get_number_retries",
        "text_optionproxy": "proxy_mode",
        "proxy_active": "proxy",
        "timeoutlogin": "timeoutlogin",
        "delay": "delay",
    }
    bool_mapping = {
        "check_changepass": "change_pass",
        "check_changeemailrecovery": "change_mail_kp",
        "check_deletephonerecovery": "check_deletephonerecovery",
        "bat2fa_new": "bat2fa_new",
        "bat2fa_new_deleteallphone": "bat2fa_new_deleteallphone",
        "create_password_app": "create_password_app",
    }

    data = {}
    for hkey, ckey in mapping.items():
        val = hconfig.get(hkey, "")
        data[hkey] = str(val) if val is not None else ""
    for hkey, ckey in bool_mapping.items():
        val = hconfig.get(hkey, False)
        data[hkey] = str(val)

    for key, widget_val in entries.items():
        data[key] = widget_val

    try:
        app_config.save_hconfig(data)
    except Exception:
        pass


def _walk_entries(widget, result: dict):
    """Walk widget tree and collect named Entry/Combobox values."""
    import tkinter as tk
    try:
        for child in widget.winfo_children():
            cls_name = child.winfo_class()
            if cls_name in ("TEntry", "TCombobox", "Entry", "Combobox"):
                try:
                    var_name = child.cget("textvariable")
                    if var_name:
                        val = child.tk.globalgetvar(var_name)
                        result[str(var_name)] = str(val)
                except Exception:
                    pass
            _walk_entries(child, result)
    except Exception:
        pass


def _patch_status_multi_select():
    """Replace filter_accounts with a version supporting comma-separated multi-status."""
    import logging

    _log = logging.getLogger("patch_status")
    _orig_filter = gui_app.filter_accounts

    def _multi_filter(items, search_text="", status_filter=""):
        if not status_filter or status_filter.strip().lower() == "all":
            return _orig_filter(items, search_text, "all")
        selected = {s.strip().lower() for s in status_filter.split(",") if s.strip()}
        if not selected or "all" in selected:
            return _orig_filter(items, search_text, "all")
        kw = (search_text or "").strip().lower()
        out = []
        for it in items:
            email = str(it.get("email", ""))
            if kw and kw not in email.lower():
                continue
            status = str(it.get("status", "not_run")).lower()
            if status in selected:
                out.append(it)
        return out

    gui_app.filter_accounts = _multi_filter
    _log.info("Patched filter_accounts for multi-select support")


def _replace_combobox_with_dropdown(root):
    """Find the status Combobox, replace with dropdown button + popup Listbox multi-select."""
    import tkinter as tk
    from tkinter import ttk
    import logging

    _log = logging.getLogger("patch_status")

    cb = _find_status_combobox(root)
    if cb is None:
        _log.warning("Status Combobox not found – multi-select patch skipped")
        return

    parent = cb.master
    var_name = None
    try:
        var_name = cb.cget("textvariable")
    except Exception:
        pass

    pack_info = {}
    try:
        pack_info = cb.pack_info()
    except Exception:
        try:
            pack_info = cb.grid_info()
        except Exception:
            pass

    btn = ttk.Button(parent, text="Status: all \u25BE", width=22, command=lambda: None)
    cb.pack_forget()
    if pack_info and "side" in pack_info:
        btn.pack(
            side=pack_info.get("side", tk.LEFT),
            padx=int(pack_info.get("padx", 5)),
            pady=int(pack_info.get("pady", 0)),
        )
    else:
        btn.pack(side=tk.LEFT, padx=5)

    _state = {
        "btn": btn,
        "var_name": var_name,
        "root": root,
        "last_statuses": set(),
        "popup": None,
    }

    def _apply_selection():
        try:
            lb = _state.get("listbox")
            if lb is None or not lb.winfo_exists():
                _close_popup()
                return
            sel = [lb.get(i) for i in lb.curselection()]
        except Exception:
            _close_popup()
            return
        if not sel or "all" in sel:
            combined = "all"
            label = "Status: all"
        else:
            combined = ",".join(sel)
            if len(sel) <= 2:
                label = "Status: " + ", ".join(sel)
            else:
                label = f"Status: {sel[0]}, +{len(sel)-1}"
        btn.config(text=label + " \u25BE")
        if var_name:
            try:
                root.tk.globalsetvar(var_name, combined)
            except Exception:
                pass
        _close_popup()

    def _close_popup():
        pop = _state.get("popup")
        if pop and pop.winfo_exists():
            try:
                pop.destroy()
            except Exception:
                pass
        _state["popup"] = None
        _state["listbox"] = None

    def _on_click(*_):
        try:
            _close_popup()
            pop = tk.Toplevel(root)
            pop.overrideredirect(True)
            pop.withdraw()
            f = ttk.Frame(pop, padding=2)
            f.pack(fill="both", expand=True)
            lb = tk.Listbox(f, selectmode=tk.EXTENDED, height=10, width=24, exportselection=False)
            scr = ttk.Scrollbar(f, orient=tk.VERTICAL, command=lb.yview)
            lb.configure(yscrollcommand=scr.set)
            lb.pack(side=tk.LEFT, fill="both", expand=True)
            scr.pack(side=tk.RIGHT, fill=tk.Y)
            items = ["all"] + sorted(_state.get("last_statuses", {"not_run"}), key=str.lower)
            for it in items:
                lb.insert(tk.END, it)
            cur = "all"
            if var_name:
                try:
                    cur = str(root.tk.globalgetvar(var_name) or "all")
                except Exception:
                    pass
            sel_set = {s.strip().lower() for s in cur.split(",") if s.strip()}
            if not sel_set or "all" in sel_set:
                lb.selection_set(0)
            else:
                for i, it in enumerate(items):
                    if it.lower() in sel_set:
                        lb.selection_set(i)
            ttk.Button(f, text="OK", command=_apply_selection).pack(pady=2)
            _state["popup"] = pop
            _state["listbox"] = lb

            lb.bind("<Double-1>", lambda _: _apply_selection())
            lb.bind("<Return>", lambda _: _apply_selection())
            pop.bind("<Escape>", lambda _: _close_popup())
            pop.protocol("WM_DELETE_WINDOW", _close_popup)

            btn_x = btn.winfo_rootx()
            btn_y = btn.winfo_rooty() + btn.winfo_height()
            pop.geometry(f"+{btn_x}+{btn_y}")
            pop.deiconify()
            pop.focus_set()
            lb.focus_set()
        except Exception as exc:
            _log = __import__("logging").getLogger("patch_status")
            _log.exception("Status popup error: %s", exc)
            _close_popup()

    btn.config(command=_on_click)

    def _rebuild_items(statuses: set[str]):
        _state["last_statuses"] = statuses

    _state["last_statuses"] = {"not_run"}
    root.after(2000, lambda: _periodic_status_update_dropdown(_state, _rebuild_items))
    _log.info("Status Combobox replaced with dropdown Listbox multi-select")


def _find_status_combobox(root):
    """Walk widget tree to find the Combobox whose values contain 'all' and 'not_run'."""
    from tkinter import ttk
    import tkinter as tk

    candidates = []
    _walk_for_combobox(root, candidates)

    for cb in candidates:
        try:
            vals = cb.cget("values")
            if isinstance(vals, str):
                vals = root.tk.splitlist(vals)
            vals_lower = [str(v).lower() for v in vals]
            if "all" in vals_lower and "not_run" in vals_lower:
                return cb
        except Exception:
            continue
    return None


def _walk_for_combobox(widget, result):
    """Recursively find all Combobox widgets."""
    cls = widget.winfo_class()
    if cls in ("TCombobox", "Combobox"):
        result.append(widget)
    try:
        for child in widget.winfo_children():
            _walk_for_combobox(child, result)
    except Exception:
        pass


def _find_search_var(root, filter_parent=None):
    """Find the textvariable name of the search Entry (ent_search).
    Its trace_add('write') triggers _refresh_table, so poking it triggers a refresh.
    Searches in filter_parent first (the frame containing the status combobox),
    then falls back to full tree."""
    search_in = filter_parent if filter_parent else root
    for child in _all_widgets(search_in):
        cls = child.winfo_class()
        if cls in ("TEntry", "Entry"):
            try:
                tv = child.cget("textvariable")
                if tv:
                    return tv
            except Exception:
                continue
    if filter_parent:
        return _find_search_var(root, None)
    return None


def _all_widgets(widget):
    """Yield all widgets in the tree."""
    yield widget
    try:
        for child in widget.winfo_children():
            yield from _all_widgets(child)
    except Exception:
        pass


def _periodic_status_update_dropdown(state, rebuild_fn):
    """Scan loaded accounts for distinct status values and update the dropdown list."""
    import logging

    _log = logging.getLogger("patch_status")
    root = state["root"]

    try:
        items = gui_app.load_accounts_db()
        current_statuses = set()
        for it in items:
            st = str(it.get("status", "not_run")).strip()
            if st:
                current_statuses.add(st)

        if not current_statuses:
            current_statuses = {"not_run"}

        if current_statuses != state["last_statuses"]:
            rebuild_fn(current_statuses)
            state["last_statuses"] = current_statuses

    except Exception as exc:
        _log.debug("Status update error: %s", exc)

    try:
        root.after(5000, lambda: _periodic_status_update_dropdown(state, rebuild_fn))
    except Exception:
        pass


def _patch_remove_hold_after_2fa():
    """Remove the infinite hold loop that blocks workers after 2FA completion."""
    import logging

    _log = logging.getLogger("patch_hold")
    _orig_process = gui_app._process_single_account

    def _patched_process(item, idx, total, tasks, status_cb, logger, on_db_changed):
        orig_get_evt = gui_app.get_worker_pause_event

        def _noop_get_evt(wid):
            evt = orig_get_evt(wid)
            evt.clear()
            return evt

        gui_app.get_worker_pause_event = _noop_get_evt
        try:
            return _orig_process(item, idx, total, tasks, status_cb, logger, on_db_changed)
        finally:
            gui_app.get_worker_pause_event = orig_get_evt

    gui_app._process_single_account = _patched_process
    _log.info("Patched _process_single_account: removed hold_after_2fa infinite loop")


def _auto_update():
    """Tự động tải bản mới nhất từ GitHub khi khởi động (không cần git)."""
    try:
        import update as _upd
        _upd.main()
    except Exception as e:
        print(f"Auto-update loi (bo qua): {e}")


if __name__ == "__main__":
    setup_logging()
    print("Đang kiểm tra bản cập nhật từ GitHub...")
    _auto_update()
    print("Đang dọn process cũ (Chrome/GPM)...")
    _kill_orphan_tool_processes()
    print("Đang kiểm tra / cài đặt thư viện...")
    try:
        _install_dependencies()
        apply_google_flow_patch()
        _patch_status_multi_select()
        _apply_autosave_patch()
        _patch_remove_hold_after_2fa()
        gui_app.run_gui()
    except Exception as e:
        import logging

        logging.getLogger(__name__).exception("Lỗi khi chạy: %s", e)
        raise
