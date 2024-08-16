from typing import Optional, Dict

import boto3


class InstanceHelper:
    # Cache dictionary to store instance type and memory size
    instance_memory_cache: Dict[str, Optional[str]] = {}

    @staticmethod
    def get_instance_memory(*, instance_type: str) -> Optional[str]:
        """
        Given an instance type, return the memory in bytes for that instance type.


        :param instance_type: The instance type for which to get the memory information.
        :type instance_type: str
        """

        # Check if the memory for the instance type is already in the cache
        if instance_type in InstanceHelper.instance_memory_cache:
            return InstanceHelper.instance_memory_cache[instance_type]

        ec2 = boto3.client("ec2")

        # Call the describe_instance_types API
        response = ec2.describe_instance_types(InstanceTypes=[instance_type])

        if not response["InstanceTypes"]:
            return None

        # Extract memory information from the response
        memory_info = response["InstanceTypes"][0]["MemoryInfo"]
        memory_in_gib = memory_info["SizeInMiB"] / 1024  # Convert MiB to GiB

        # Store the memory size in the cache
        InstanceHelper.instance_memory_cache[instance_type] = memory_in_gib

        return f"{memory_in_gib}g"
