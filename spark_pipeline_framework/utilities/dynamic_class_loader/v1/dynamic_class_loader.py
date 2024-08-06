import os
import inspect
import importlib.util
from pathlib import Path
from typing import Type, List, TypeVar, Generic

T = TypeVar("T")


class DynamicClassLoader(Generic[T]):
    def __init__(self, base_class: Type[T], folder_path: Path | str):
        self.base_class: Type[T] = base_class
        self.folder_path: Path | str = folder_path

    def find_subclasses(self) -> List[Type[T]]:
        subclasses = []

        # Iterate through files in the specified folder
        for filename in os.listdir(self.folder_path):
            if filename.endswith(".py") and filename != "__init__.py":
                module_name = filename[:-3]
                file_path = os.path.join(self.folder_path, filename)

                # Dynamically import the module
                spec = importlib.util.spec_from_file_location(module_name, file_path)
                assert spec
                module = importlib.util.module_from_spec(spec)
                assert module
                assert spec.loader
                spec.loader.exec_module(module)

                # Inspect classes in the module
                for name, obj in inspect.getmembers(module, inspect.isclass):
                    if issubclass(obj, self.base_class) and obj is not self.base_class:
                        subclasses.append(obj)

        return subclasses
