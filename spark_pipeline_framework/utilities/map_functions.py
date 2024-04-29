import json
from typing import List


def remove_field_from_json(json_str: str, field_list: List[str]) -> str:
    """
    Map function which removes given field from JSON
    """
    json_dict = json.loads(json_str)
    for field in field_list:
        del json_dict[field]
    return str(json.dumps(json_dict))
