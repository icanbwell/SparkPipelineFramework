from __future__ import annotations

from dataclasses import fields, MISSING
from typing import Any, Dict, Type, TypeVar, TYPE_CHECKING, get_origin, get_args

if TYPE_CHECKING:
    # noinspection PyUnresolvedReferences
    from _typeshed import DataclassInstance

TDataClass = TypeVar("TDataClass", bound="DataclassInstance")


class DataClassLoader:
    @staticmethod
    def from_dict(data_class: Type[TDataClass], data: Dict[str, Any]) -> TDataClass:
        field_values = {}

        for field in fields(data_class):
            field_name = field.name
            field_type = field.type
            field_value = data.get(field_name, MISSING)

            if field_value is not MISSING:
                # Check if the field is a list
                if get_origin(field_type) is list:
                    item_type = get_args(field_type)[0]
                    if hasattr(item_type, "__dataclass_fields__"):
                        # noinspection PyTypeChecker
                        field_values[field_name] = [
                            DataClassLoader.from_dict(item_type, item)
                            for item in field_value
                        ]
                    else:
                        field_values[field_name] = field_value

                # Check if the field is a nested dataclass
                elif isinstance(field_value, dict) and hasattr(
                    field_type, "__dataclass_fields__"
                ):
                    field_values[field_name] = DataClassLoader.from_dict(
                        field_type, field_value
                    )

                else:
                    field_values[field_name] = field_value

        return data_class(**field_values)
