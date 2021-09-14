from collections import UserDict
from typing import Any


class ParameterDict(UserDict):  # type: ignore
    """
    This dictionary returns the value if the key exists otherwise if the key starts with view_
    then it returns the key minus view_
    """

    def __getitem__(self, key: str) -> Any:
        # check if key exists
        if key in super().keys():
            return super().__getitem__(key)
        elif key.startswith("view_"):
            return key[5:]
        else:
            return super().__getitem__(key)

    def copy(self) -> "ParameterDict":
        if self.__class__ is ParameterDict:
            return ParameterDict(self.data.copy())
        import copy

        data = self.data
        try:
            self.data = {}
            c = copy.copy(self)
        finally:
            self.data = data
        c.update(self)
        return c
