import json
from typing import Any, Dict, List, Union, cast
from datetime import datetime, date


def json_serial(obj: Any) -> str:
    """JSON serializer for objects not serializable by default json code"""

    # https://stackoverflow.com/questions/11875770/how-to-overcome-datetime-datetime-not-json-serializable
    if isinstance(obj, (datetime, date)):
        return obj.isoformat()
    return str(obj)


def remove_empty_elements(
    d: Union[List[Dict[str, Any]], Dict[str, Any]]
) -> Union[List[Dict[str, Any]], Dict[str, Any]]:
    """recursively remove empty lists, empty dicts, or None elements from a dictionary"""

    def empty(x: Any) -> bool:
        return x is None or x == {} or x == []

    if not isinstance(d, (dict, list)):
        return d
    elif isinstance(d, list):
        return [
            cast(Dict[str, Any], v)
            for v in (remove_empty_elements(v) for v in d)
            if not empty(v)
        ]
    else:
        return {
            k: v
            for k, v in ((k, remove_empty_elements(v)) for k, v in d.items())
            if not empty(v)
        }


def convert_dict_to_fhir_json(dict_: Dict[str, Any]) -> str:
    """
    Returns dictionary as json string


    :return:
    """
    instance_variables: Dict[str, Any] = cast(
        Dict[str, Any], remove_empty_elements(dict_)
    )

    instance_variables_text: str = json.dumps(instance_variables, default=json_serial)
    return instance_variables_text
