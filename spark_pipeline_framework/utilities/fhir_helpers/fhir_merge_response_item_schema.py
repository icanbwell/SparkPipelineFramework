from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    BooleanType,
)


class FhirMergeResponseItemSchema:
    updated = "updated"
    created = "created"
    deleted = "deleted"
    issue = "issue"
    id_ = "id"
    uuid = "uuid"
    resourceType = "resourceType"
    sourceAssigningAuthority = "sourceAssigningAuthority"
    resource_version = "resource_version"
    message = "message"
    error = "error"
    token = "token"
    resource_json = "resource_json"

    @staticmethod
    def get_schema() -> StructType:
        """
        Returns the schema of FhirMergeResponse

        Should match to
        https://github.com/icanbwell/helix.fhir.client.sdk/blob/main/helix_fhir_client_sdk/responses/fhir_merge_response.py
        """
        response_schema = StructType(
            [
                StructField(
                    FhirMergeResponseItemSchema.created, BooleanType(), nullable=True
                ),
                StructField(
                    FhirMergeResponseItemSchema.updated, BooleanType(), nullable=True
                ),
                StructField(
                    FhirMergeResponseItemSchema.deleted, BooleanType(), nullable=True
                ),
                StructField(
                    FhirMergeResponseItemSchema.id_, StringType(), nullable=True
                ),
                StructField(
                    FhirMergeResponseItemSchema.uuid, StringType(), nullable=True
                ),
                StructField(
                    FhirMergeResponseItemSchema.resourceType,
                    StringType(),
                    nullable=True,
                ),
                StructField(
                    FhirMergeResponseItemSchema.sourceAssigningAuthority,
                    StringType(),
                    nullable=True,
                ),
                StructField(
                    FhirMergeResponseItemSchema.resource_version,
                    StringType(),
                    nullable=True,
                ),
                StructField(
                    FhirMergeResponseItemSchema.message, StringType(), nullable=True
                ),
                StructField(
                    FhirMergeResponseItemSchema.issue, StringType(), nullable=True
                ),
                StructField(
                    FhirMergeResponseItemSchema.error, StringType(), nullable=True
                ),
                StructField(
                    FhirMergeResponseItemSchema.token, StringType(), nullable=True
                ),
            ]
        )
        return response_schema
