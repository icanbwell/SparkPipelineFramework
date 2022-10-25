import json
from typing import Any, Dict
from datetime import datetime, date


def convert_dict_to_str(dict_: Dict[str, Any]) -> str:
    """
    Returns dictionary as string


    :return:
    """
    instance_variables: Dict[str, Any] = dict_
    # instance_variables = {k: v for k, v in instance_variables.items() if isinstance(v, (int, float, bool, str))}
    instance_variables = {
        k: v
        for k, v in instance_variables.items()
        # if type(v) in [int, float, bool, str, datetime]
    }

    # instance_variables = {k: v for k, v in instance_variables.items() if not isinstance(v, object)}
    # if "_sort_fields" in dict_:
    #     instance_variables["sort_fields"] = [
    #         str(f) for f in dict_["_sort_fields"]
    #     ]
    # if "_filters" in dict_:
    #     instance_variables["filters"] = [f"{f}" for f in dict_["_filters"]]

    def json_serial(obj: Any) -> str:
        """JSON serializer for objects not serializable by default json code"""

        # https://stackoverflow.com/questions/11875770/how-to-overcome-datetime-datetime-not-json-serializable
        if isinstance(obj, (datetime, date)):
            return obj.isoformat()
        # if isinstance(obj, list):
        #     return f"[{[str(o) for o in obj]}]"
        return str(obj)

    instance_variables_text: str = json.dumps(instance_variables, default=json_serial)
    return instance_variables_text
