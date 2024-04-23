import json


def remove_field_from_json(json_str, column_name):
    json_dict = json.loads(json_str)
    del json_dict[column_name]
    return str(json_dict)
