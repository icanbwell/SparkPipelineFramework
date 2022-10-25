from importlib import import_module
from typing import Any


def load_module(module_path: str, module_name: str) -> Any:
    try:
        module: Any = import_module(name=module_path)
        return getattr(module, module_name)
    except ModuleNotFoundError:
        raise ModuleNotFoundError(f"A Module for {module_name} is not found!")
