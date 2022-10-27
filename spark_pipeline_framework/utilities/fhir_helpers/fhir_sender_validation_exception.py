class FhirSenderValidationException(Exception):
    def __init__(
        self,
        url: str,
        json_data: str,
    ) -> None:
        self.url: str = url
        self.data: str = json_data
        super().__init__(f"FHIR send validation failed to {url}: {json_data}.")
