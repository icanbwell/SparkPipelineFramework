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


def put_ssm_config(
    config_arguments: Dict[str, Any],
    region: str = "us-east-1",
) -> Dict[str, Any]:
    """
    Update ssm config in AWS

    :param config_arguments: Configurations sent to ssm client
    :param region: default region in aws
    :return refer https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/ssm.html#SSM.Client.put_parameter
    doc for return type
    """
    ssm = boto3.client("ssm", region_name=region)
    response: Dict[str, Any] = ssm.put_parameter(**config_arguments)
    return response
