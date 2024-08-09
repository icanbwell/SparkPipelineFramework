from __future__ import annotations

from dataclasses import fields, MISSING
from typing import Any, Dict, Type, TypeVar, TYPE_CHECKING

if TYPE_CHECKING:
    # noinspection PyUnresolvedReferences
    from _typeshed import DataclassInstance

T = TypeVar("T", bound=DataclassInstance)


class DataClassLoader:
    @staticmethod
    def from_dict(data_class: Type[T], data: Dict[str, Any]) -> T:
        field_values = {}

        for field in fields(data_class):
            field_name = field.name
            field_type = field.type
            field_value = data.get(field_name, MISSING)

            if field_value is not MISSING:
                if isinstance(field_value, dict) and hasattr(
                    field_type, "__dataclass_fields__"
                ):
                    field_values[field_name] = DataClassLoader.from_dict(
                        field_type, field_value
                    )
                else:
                    field_values[field_name] = field_value

        return data_class(**field_values)
