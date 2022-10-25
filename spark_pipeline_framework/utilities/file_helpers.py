import os
import boto3
from botocore.exceptions import ClientError
from smart_open import s3  # type: ignore
from pathlib import Path
from typing import Union, List


def get_absolute_paths(file_path: Union[str, List[str], Path]) -> List[str]:
    """
    Abstracts handling of paths so we can use paths on both k8s in AWS and local

    :param file_path: the path or paths to format appropriately
    :return: a list of paths optimized for local or k8s usages
    """
    if not file_path:
        raise ValueError(f"file_path is empty: {file_path}")

    if isinstance(file_path, str):
        if file_path.__contains__(":"):
            return [file_path]
        else:
            data_dir = Path(__file__).parent.parent.joinpath("./")
            return [f"file://{data_dir.joinpath(file_path)}"]
    elif isinstance(file_path, Path):
        data_dir = Path(__file__).parent.parent.joinpath("./")
        return [f"file://{data_dir.joinpath(file_path)}"]
    elif isinstance(file_path, (list, tuple)):
        data_dir = Path(__file__).parent.parent.joinpath("./")
        absolute_paths = []
        for path in file_path:
            if ":" in path:
                absolute_paths.append(path)
            else:
                absolute_paths.append(f"file://{data_dir.joinpath(path)}")
        return absolute_paths
    else:
        raise TypeError(f"Unknown type '{type(file_path)}' for file_path {file_path}")


def isfile(path: str) -> bool:
    if path.startswith("s3://"):
        s3_uri = s3.parse_uri(path)
        bucket = s3_uri["bucket_id"]
        prefix = s3_uri["key_id"]
        s3_resource = boto3.resource("s3")
        try:
            s3_resource.Object(bucket, prefix).load()
            return True
        except ClientError as e:
            if e.response["Error"]["Code"] == "404":
                return False
            else:
                raise e

    return os.path.isfile(path)


def listdir(path: str) -> List[str]:
    query_files = []
    if path.startswith("s3://"):
        s3_uri = s3.parse_uri(path)
        bucket = s3_uri["bucket_id"]
        prefix = s3_uri["key_id"]
        for key, content in s3.iter_bucket(bucket, prefix=prefix):
            query_files.append(f"s3://{bucket}/{key}")
        return query_files
    paths = os.listdir(path)
    for sub_path in paths:
        query_files.append(os.path.join(path, sub_path))
    return query_files
