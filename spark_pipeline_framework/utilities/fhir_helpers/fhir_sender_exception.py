from typing import Optional


class FhirSenderException(Exception):
    def __init__(
        self,
        exception: Exception,
        url: str,
        json_data: str,
        response_text: Optional[str],
        response_status_code: Optional[int],
        message: str,
    ) -> None:
        self.exception: Exception = exception
        self.url: str = url
        self.data: str = json_data
        super().__init__(
            f"FHIR send failed to {url} {response_status_code}: {json_data}.  {message} {response_text}"
        )
