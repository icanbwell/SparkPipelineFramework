import json


def remove_field_from_json(json_str: str, column_name: str) -> str:
    """
    UDF function which removes given field from JSON
    """
    json_dict = json.loads(json_str)
    del json_dict[column_name]
    return str(json_dict)
