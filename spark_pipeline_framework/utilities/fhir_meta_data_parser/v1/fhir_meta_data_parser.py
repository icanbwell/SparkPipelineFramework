from typing import Dict, Any, List, Set, Optional


class FhirMetaDataParser:
    """
    Parses FHIR metadata


    """

    @staticmethod
    def get_resource_types_from_metadata(metadata: Dict[str, Any]) -> List[str]:
        """
        Gets resource types present in the metadata


        :param metadata:
        """
        resource_types: Set[str] = set()
        rest_entry: Dict[str, Any]
        for rest_entry in metadata.get("rest", []):
            for resource in rest_entry.get("resource", []):
                resource_type: Optional[str] = resource.get("type")
                if resource_type is not None:
                    resource_types.add(resource_type)

        return list(resource_types)

    @staticmethod
    def get_fir_version_from_metadata(metadata: Dict[str, Any]) -> Optional[str]:
        """
        Gets FHIR version from metadata


        :param metadata:
        """
        fhir_version: Optional[str] = metadata.get("fhirVersion")
        return fhir_version
