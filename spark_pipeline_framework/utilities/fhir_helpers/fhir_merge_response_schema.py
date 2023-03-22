from pyspark.sql.types import (
    StructType,
    StructField,
    IntegerType,
    StringType,
    ArrayType,
)


class FhirMergeResponseSchema:
    """
    This class provides names for columns in FhirMergeResponse

    Should match to https://github.com/icanbwell/helix.fhir.client.sdk/blob/main/helix_fhir_client_sdk/responses/fhir_merge_response.py
    """

    request_id = "request_id"
    url = "url"
    responses = "responses"
    error = "error"
    access_token = "access_token"
    status = "status"
    data = "data"

    @staticmethod
    def get_schema() -> StructType:
        """
        Returns the schema of FhirMergeResponse

        Should match to https://github.com/icanbwell/helix.fhir.client.sdk/blob/main/helix_fhir_client_sdk/responses/fhir_merge_response.py
        """
        response_schema = StructType(
            [
                StructField("request_id", StringType(), nullable=True),
                StructField("url", StringType(), nullable=False),
                StructField("responses", ArrayType(StringType()), nullable=False),
                StructField("error", StringType(), nullable=True),
                StructField("access_token", StringType(), nullable=True),
                StructField("status", IntegerType(), nullable=False),
                StructField("data", StringType(), nullable=False),
            ]
        )
        return response_schema
