import boto3
from typing import Dict, Any


def get_ssm_config(
    path: str = "/prod/databelt/",
    region: str = "us-east-1",
    truncate_keys: bool = False,
) -> Dict[str, Any]:
    ssm = boto3.client("ssm", region_name=region)
    param_list = []
    next_token = None
    done = False
    path_len = len(path)
    while not done:
        args = {
            "Path": path,
            "Recursive": True,
            "WithDecryption": True,
        }
        if next_token:
            args["NextToken"] = next_token
        param_response = ssm.get_parameters_by_path(**args)
        param_list.extend(param_response.get("Parameters", []))
        next_token = param_response.get("NextToken", None)
        if not next_token:
            done = True

    params = {}
    for param in param_list:
        if truncate_keys:
            key = param["Name"][path_len:]
        else:
            key = param["Name"]
        params[key] = param["Value"]

    return params
