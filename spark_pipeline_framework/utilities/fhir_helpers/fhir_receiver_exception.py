from typing import Optional

from spark_pipeline_framework.utilities.dictionary_writer.v1.dictionary_writer import (
    convert_dict_to_str,
)


class FhirReceiverException(Exception):
    def __init__(
        self,
        *,
        url: str,
        json_data: str,
        response_text: Optional[str],
        response_status_code: Optional[int],
        message: str,
        request_id: Optional[str],
    ) -> None:
        self.url: str = url
        self.data: str = json_data
        json = {
            "message": f"FHIR receive failed: {message}",
            "url": url,
            "status_code": response_status_code,
            "response_text": response_text,
            "json_data": json_data,
            "request_id": request_id,
        }

        super().__init__(convert_dict_to_str(json))
