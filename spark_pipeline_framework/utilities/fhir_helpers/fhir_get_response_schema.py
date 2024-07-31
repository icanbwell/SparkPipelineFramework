from pyspark.sql.types import (
    StructType,
    StructField,
    IntegerType,
    StringType,
    ArrayType,
)


class FhirGetResponseSchema:
    """
    This class provides names for columns in FhirGetResponse

    Should match to
    https://github.com/icanbwell/helix.fhir.client.sdk/blob/main/helix_fhir_client_sdk/responses/fhir_get_response.py
    """

    partition_index = "partition_index"
    sent = "sent"
    received = "received"
    responses = "responses"
    first = "first"
    last = "last"
    error_text = "error_text"
    url = "url"
    status_code = "status_code"
    request_id = "request_id"
    access_token = "access_token"
    extra_context_to_return = "extra_context_to_return"

    @staticmethod
    def get_schema() -> StructType:
        """
        Returns the schema of FhirGetResponse

        Should match to
        https://github.com/icanbwell/helix.fhir.client.sdk/blob/main/helix_fhir_client_sdk/responses/fhir_get_response.py
        """
        response_schema = StructType(
            [
                StructField(
                    FhirGetResponseSchema.partition_index, IntegerType(), nullable=False
                ),
                StructField(FhirGetResponseSchema.sent, IntegerType(), nullable=False),
                StructField(
                    FhirGetResponseSchema.received, IntegerType(), nullable=False
                ),
                StructField(
                    FhirGetResponseSchema.responses,
                    ArrayType(StringType()),
                    nullable=False,
                ),
                StructField(FhirGetResponseSchema.first, StringType(), nullable=True),
                StructField(FhirGetResponseSchema.last, StringType(), nullable=True),
                StructField(
                    FhirGetResponseSchema.error_text, StringType(), nullable=True
                ),
                StructField(FhirGetResponseSchema.url, StringType(), nullable=True),
                StructField(
                    FhirGetResponseSchema.status_code, IntegerType(), nullable=True
                ),
                StructField(
                    FhirGetResponseSchema.request_id, StringType(), nullable=True
                ),
                StructField(
                    FhirGetResponseSchema.access_token, StringType(), nullable=True
                ),
                StructField(
                    FhirGetResponseSchema.extra_context_to_return,
                    StringType(),
                    nullable=True,
                ),
            ]
        )
        return response_schema
