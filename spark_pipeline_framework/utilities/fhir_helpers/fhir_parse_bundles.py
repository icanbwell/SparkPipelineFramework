from typing import Any, Dict, Iterable, List, Union, Optional
import json

from pyspark.sql.types import Row
from spark_pipeline_framework.logger.yarn_logger import get_logger


def combine_bundles(contents: str) -> Dict[str, Any]:
    resources_or_bundles: List[Dict[str, Any]] = json.loads(contents)
    # create a bundle
    bundle: Dict[str, Union[str, List[Dict[str, Any]]]] = {
        "resourceType": "Bundle",
        "type": "collection",
        "entry": [],
    }
    # iterate over each bundle in resources_or_bundles.
    # resources_or_bundles can either be a normal resource or a bundle resource
    for resource_or_bundle in resources_or_bundles:
        if (
            not isinstance(resource_or_bundle, dict)
            or "resourceType" not in resource_or_bundle
        ):  # bad/corrupt entry
            continue
        if resource_or_bundle["resourceType"] != "Bundle":  # normal resource
            assert isinstance(bundle["entry"], list)
            bundle["entry"].append({"resource": resource_or_bundle})
        else:  # if it is a bundle then just add to bundle entries
            entry: Dict[str, Any]
            for entry in resource_or_bundle["entry"]:
                assert isinstance(bundle["entry"], list)
                if (
                    isinstance(entry, dict) and "resource" in entry
                ):  # it is a valid entry
                    bundle["entry"].append(entry)

    return bundle


def extract_resource_from_json(
    file_path: str, contents: str, resources_to_extract: Optional[List[str]] = None
) -> Iterable[Row]:
    """
    Returns a list of rows.  Each row contains file name, resourceType and resource
    :param file_path: location of file
    :param contents: a string consisting of a bundle
    :param resources_to_extract: optional list of resources to extract
    """
    logger = get_logger(__name__)
    file_name: str = file_path.split("/")[-1].replace(".gz", "")

    try:
        logger.info(
            f"Extracting resources from {file_path} and using file name {file_name}"
        )

        bundle: Dict[str, Any] = combine_bundles(contents=contents)

        # now create a row for each entry
        rows: List[Row] = [
            Row(
                file_name=str(file_name),
                resourceType=str(entry["resource"]["resourceType"]),
                resource=json.dumps(entry["resource"]),
            )
            for entry in bundle["entry"]
            if "resource" in entry
            and "resourceType" in entry["resource"]
            and entry["resource"]["resourceType"]
            != "OperationOutcome"  # these are just fluff
            and (
                not resources_to_extract
                or len(resources_to_extract) == 0
                or entry["resource"]["resourceType"] in resources_to_extract
            )
        ]

        logger.info(f"Finished extracting resources from {file_path}")
        # row = Row(**resources_dict)
        return rows
    except Exception as e:
        logger.exception(f"Error extracting resources from {file_path}: {str(e)}")
        return [
            Row(
                file_name=str(file_name),
                resourceType="OperationOutcome",
                resource={
                    "issue": [
                        {
                            "severity": "error",
                            "code": "invalid",
                            "details": {"text": f"{str(e)}"},
                            "diagnostics": f"{file_path}: {contents}",
                        }
                    ]
                },
            )
        ]
