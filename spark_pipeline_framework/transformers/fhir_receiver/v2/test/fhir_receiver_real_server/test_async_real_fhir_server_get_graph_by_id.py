import json
from os import environ, path, makedirs
from pathlib import Path
from shutil import rmtree
from typing import Any, List, Dict, Optional

import pytest

from helix_fhir_client_sdk.responses.fhir_merge_response import FhirMergeResponse
from helix_fhir_client_sdk.utilities.fhir_helper import FhirHelper
from helix_fhir_client_sdk.utilities.practitioner_generator import PractitionerGenerator
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, ArrayType, StringType
from spark_fhir_schemas.r4.resources.endpoint import EndpointSchema
from spark_fhir_schemas.r4.resources.group import GroupSchema
from spark_fhir_schemas.r4.resources.insuranceplan import InsurancePlanSchema
from spark_fhir_schemas.r4.resources.location import LocationSchema
from spark_fhir_schemas.r4.resources.measurereport import MeasureReportSchema
from spark_fhir_schemas.r4.resources.organization import OrganizationSchema
from spark_fhir_schemas.r4.resources.practitioner import PractitionerSchema
from spark_fhir_schemas.r4.resources.practitionerrole import PractitionerRoleSchema
from spark_fhir_schemas.r4.resources.schedule import ScheduleSchema

from spark_pipeline_framework.progress_logger.progress_logger import ProgressLogger
from spark_pipeline_framework.transformers.fhir_reader.v1.fhir_reader import FhirReader
from spark_pipeline_framework.transformers.fhir_receiver.v2.fhir_receiver import (
    FhirReceiver,
)
from spark_pipeline_framework.utilities.fhir_server_test_context.v1.fhir_server_test_context import (
    FhirServerTestContext,
)
from spark_pipeline_framework.utilities.spark_data_frame_helpers import (
    create_empty_dataframe,
)


@pytest.mark.parametrize("run_synchronously", [True, False])
@pytest.mark.parametrize("use_data_streaming", [True, False])
async def test_async_real_fhir_server_get_graph_by_id(
    spark_session: SparkSession, run_synchronously: bool, use_data_streaming: bool
) -> None:
    print()
    data_dir: Path = Path(__file__).parent.joinpath("./")

    temp_folder = data_dir.joinpath("../temp")
    if path.isdir(temp_folder):
        rmtree(temp_folder)
    makedirs(temp_folder)

    patient_json_path: Path = temp_folder.joinpath("patient.json")
    resource_type = "Practitioner"
    async with FhirServerTestContext(
        resource_type=resource_type
    ) as fhir_server_test_context:

        environ["LOGLEVEL"] = "DEBUG"

        fhir_server_url: str = fhir_server_test_context.fhir_server_url
        auth_client_id = fhir_server_test_context.auth_client_id
        auth_client_secret = fhir_server_test_context.auth_client_secret
        auth_well_known_url = fhir_server_test_context.auth_well_known_url

        fhir_client = await fhir_server_test_context.create_fhir_client_async()
        fhir_client = fhir_client.url(fhir_server_url).resource(resource_type)

        count: int = 2
        roles_per_practitioner: int = 2

        id_dict: Dict[str, List[str]] = PractitionerGenerator.get_ids(
            count=count, roles_per_practitioner=roles_per_practitioner
        )
        # delete Practitioner resources
        print(f"Deleting {count} {resource_type} resources: {id_dict[resource_type]}")
        await FhirHelper.delete_resources_by_ids_async(
            fhir_client=fhir_client,
            resource_type=resource_type,
            id_list=id_dict[resource_type],
        )
        # delete Organization resources
        print(f"Deleting {count} Organization resources: {id_dict['Organization']}")
        await FhirHelper.delete_resources_by_ids_async(
            fhir_client=fhir_client,
            resource_type="Organization",
            id_list=id_dict["Organization"],
        )
        # delete PractitionerRole resources
        print(
            f"Deleting {count} PractitionerRole resources: {id_dict['PractitionerRole']}"
        )
        await FhirHelper.delete_resources_by_ids_async(
            fhir_client=fhir_client,
            resource_type="PractitionerRole",
            id_list=id_dict["PractitionerRole"],
        )
        print("Finished deleting resources")

        fhir_client = await fhir_server_test_context.create_fhir_client_async()
        fhir_client = fhir_client.url(fhir_server_url).resource(resource_type)

        bundle: Dict[str, Any] = PractitionerGenerator.generate_resources_bundle(
            count=count, roles_per_practitioner=roles_per_practitioner
        )

        expected_resource_count = len(bundle["entry"])
        print("expected_resource_count", expected_resource_count)

        print(f"Merging bundle with {expected_resource_count} resources")
        merge_response: Optional[FhirMergeResponse] = (
            await FhirMergeResponse.from_async_generator(
                fhir_client.merge_async(json_data_list=[json.dumps(bundle)])
            )
        )
        assert merge_response is not None
        print(f"Merged {expected_resource_count} resources")
        print(json.dumps(merge_response.responses))
        assert merge_response.status == 200, merge_response.responses
        assert (
            len(merge_response.responses) == expected_resource_count
        ), merge_response.responses

        # assert merge_response.responses[0]["created"] is True, merge_response.responses

        def get_practitioner_graph(client_slug: str) -> Dict[str, Any]:
            return {
                "resourceType": "GraphDefinition",
                "id": "o",
                "name": "provider_everything",
                "status": "active",
                "start": "Practitioner",
                "link": [
                    {
                        "description": "Practitioner Roles for this Practitioner",
                        "target": [
                            {
                                "type": "PractitionerRole",
                                "params": f"practitioner={{ref}}&active:not=false&_security=https://www.icanbwell.com/access|{client_slug},https://www.icanbwell.com/access|connecthub",
                                "link": [
                                    {
                                        "path": "organization",
                                        "target": [
                                            {
                                                "type": "Organization",
                                                "link": [
                                                    {
                                                        "path": "endpoint[x]",
                                                        "target": [
                                                            {"type": "Endpoint"}
                                                        ],
                                                    }
                                                ],
                                            }
                                        ],
                                    },
                                    {
                                        "path": "location[x]",
                                        "target": [{"type": "Location"}],
                                    },
                                    {
                                        "path": "healthcareService[x]",
                                        "target": [{"type": "HealthcareService"}],
                                    },
                                    {
                                        "path": "extension.extension:url=plan",
                                        "target": [
                                            {
                                                "link": [
                                                    {
                                                        "path": "valueReference",
                                                        "target": [
                                                            {"type": "InsurancePlan"}
                                                        ],
                                                    }
                                                ]
                                            }
                                        ],
                                    },
                                    {
                                        "target": [
                                            {
                                                "type": "Schedule",
                                                "params": f"actor={{ref}}&_security=https://www.icanbwell.com/access|{client_slug}",
                                            }
                                        ]
                                    },
                                ],
                            }
                        ],
                    },
                    {
                        "description": "Group",
                        "target": [
                            {
                                "type": "Group",
                                "params": f"member={{ref}}&_security=https://www.icanbwell.com/access|{client_slug}",
                            }
                        ],
                    },
                    {
                        "description": "Review score for the practitioner",
                        "target": [
                            {
                                "type": "MeasureReport",
                                "params": f"subject={{ref}}&_security=https://www.icanbwell.com/access|{client_slug}",
                            }
                        ],
                    },
                ],
            }

        # act
        df: DataFrame = create_empty_dataframe(spark_session=spark_session)

        id_df = spark_session.createDataFrame(
            [(s,) for s in id_dict[resource_type]], ["id"]
        )

        id_df.createOrReplaceTempView("id_view")

        parameters = {"flow_name": "Test Pipeline V2", "team_name": "Data Operations"}

        with ProgressLogger() as progress_logger:
            await FhirReceiver(
                server_url=fhir_server_url,
                resource=resource_type,
                id_view="id_view",
                action="$graph",
                additional_parameters=["contained=true"],
                separate_bundle_resources=True,
                action_payload=get_practitioner_graph(client_slug="bwell"),
                file_path=patient_json_path,
                progress_logger=progress_logger,
                parameters=parameters,
                run_synchronously=run_synchronously,
                use_data_streaming=use_data_streaming,
                auth_well_known_url=auth_well_known_url,
                auth_client_id=auth_client_id,
                auth_client_secret=auth_client_secret,
            ).transform_async(df)

            # Assert
            json_df: DataFrame = df.sparkSession.read.text(str(patient_json_path))
            print(" ---- Result of $graph call ------")
            json_df.show(truncate=False)
            print(" ---- Schema of $graph call ------")
            json_df.printSchema()

            assert json_df.count() == count

            assert json_df.schema == StructType(
                [
                    StructField("value", StringType(), True),
                ]
            )

            # now try to read it
            await FhirReader(
                file_path=patient_json_path,
                view="practitioner_graphs",
                name="fhir_reader",
                progress_logger=progress_logger,
                schema=StructType(
                    [
                        StructField(
                            "practitioner",
                            ArrayType(
                                PractitionerSchema.get_schema(
                                    include_extension=True,
                                )
                            ),
                        ),
                        StructField(
                            "practitionerrole",
                            ArrayType(
                                PractitionerRoleSchema.get_schema(
                                    include_extension=True,
                                    extension_fields=[
                                        "valueCodeableConcept",
                                        "valueRange",
                                        "valueReference",
                                    ],
                                )
                            ),
                        ),
                        StructField(
                            "organization",
                            ArrayType(OrganizationSchema.get_schema()),
                        ),
                        StructField(
                            "location",
                            ArrayType(
                                LocationSchema.get_schema(
                                    include_extension=True,
                                    extension_fields=["valueString"],
                                )
                            ),
                        ),
                        StructField(
                            "insuranceplan",
                            ArrayType(InsurancePlanSchema.get_schema()),
                        ),
                        StructField(
                            "endpoint",
                            ArrayType(
                                EndpointSchema.get_schema(
                                    include_extension=False,
                                )
                            ),
                        ),
                        StructField(
                            "schedule",
                            ArrayType(
                                ScheduleSchema.get_schema(
                                    include_extension=True,
                                    extension_fields=[
                                        "valueCodeableConcept",
                                        "valueRange",
                                        "valueDecimal",
                                    ],
                                )
                            ),
                        ),
                        StructField("group", ArrayType(GroupSchema.get_schema())),
                        StructField(
                            "measurereport", ArrayType(MeasureReportSchema.get_schema())
                        ),
                    ]
                ),
            ).transform_async(df)

            # Assert
            practitioner_graphs_df: DataFrame = spark_session.table(
                "practitioner_graphs"
            )
            print(" ---- Result of FhirReader ------")
            practitioner_graphs_df.show(truncate=False)
            print(" ---- Schema of FhirReader ------")
            practitioner_graphs_df.printSchema()

            assert practitioner_graphs_df.count() == count

            rows: List[Dict[str, Any]] = [
                r.asDict(recursive=True) for r in practitioner_graphs_df.collect()
            ]

            rows = sorted(rows, key=lambda x: x["practitioner"][0]["id"])

            assert rows[0]["practitioner"][0]["id"] == id_dict[resource_type][0]
            assert rows[1]["practitioner"][0]["id"] == id_dict[resource_type][1]
