from __future__ import annotations

import importlib.machinery
import importlib.util
import sys
from pathlib import Path


def load_from_pyc(module_name: str, module_file: str) -> None:
    pyc_path = Path(module_file).with_suffix(".pyc")
    if not pyc_path.exists():
        raise ImportError(f"Missing bytecode file: {pyc_path}")

    loader = importlib.machinery.SourcelessFileLoader(module_name, str(pyc_path))
    spec = importlib.util.spec_from_loader(module_name, loader)
    if spec is None:
        raise ImportError(f"Could not create import spec for {pyc_path}")

    module = importlib.util.module_from_spec(spec)
    loader.exec_module(module)
    sys.modules[module_name] = module
