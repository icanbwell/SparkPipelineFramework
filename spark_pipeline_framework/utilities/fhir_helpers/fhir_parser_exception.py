from typing import Optional

from spark_pipeline_framework.utilities.dictionary_writer.v1.dictionary_writer import (
    convert_dict_to_str,
)


class FhirParserException(Exception):
    def __init__(
        self,
        *,
        url: str,
        json_data: str,
        message: str,
        response_status_code: Optional[int],
    ) -> None:
        self.url: str = url
        self.data: str = json_data
        json = {
            "message": f"FHIR parse failed: {message}",
            "url": url,
            "status_code": response_status_code,
            "json_data": json_data,
        }

        super().__init__(convert_dict_to_str(json))
