import json
import os
from typing import Iterable, Any, Dict, List

import pandas as pd
import requests
from pyspark.sql import DataFrame
from pyspark.sql.types import StructType, StructField, StringType, TimestampType


class IterableHelper:
    @staticmethod
    def send_user_profile_to_iterable(user_profile_df: DataFrame) -> None:
        print(f"UserProfile count:{user_profile_df.count()}")

        # Define function to call REST API
        def call_api_for_each_row(
            batch_iter: Iterable[pd.DataFrame],
        ) -> Iterable[pd.DataFrame]:
            # https://support.iterable.com/hc/en-us/articles/204780579-API-Overview-and-Sample-Payloads#post-api-users-update
            api_url = "https://api.iterable.com/api/users/update"  # Replace with your actual API URL
            iterable_api_key = os.getenv("ITERABLE_API_KEY")

            batch: pd.DataFrame
            for batch in batch_iter:
                responses = []

                pdf_json: str = batch.to_json(orient="records")
                input_rows: List[Dict[str, Any]] = json.loads(pdf_json)

                result: List[Dict[str, Any]] = []

                for row in input_rows:
                    master_person_id = row["master_person_id"]
                    client_person_id = row["client_person_id"]
                    organization_id = row["organization_id"]
                    client_slug = row["client_slug"]
                    created_date = row["created_date"]
                    last_updated_date = row["last_updated_date"]
                    all_fields: List[Dict[str, Any]] = row["all_fields"]
                    # print(f"all_fields:{all_fields}, type: {type(all_fields)}")

                    try:
                        headers = {
                            "Api-key": iterable_api_key,
                            "Content-Type": "application/json",
                        }

                        data_fields: Dict[str, Any] = {
                            field["field_name"]: field["field_value"]
                            for field in all_fields
                        }
                        data = {
                            "email": f"imran.qureshi+{master_person_id}@icanbwell.com",
                            "userId": f"{master_person_id}",
                            "dataFields": data_fields,
                            "preferUserId": True,
                            # "mergeNestedObjects": True,
                            "createNewFields": True,
                        }
                        print(f"data: {data}")
                        # Make the POST request
                        response = requests.post(api_url, headers=headers, json=data)
                        # Get the response text (or extract relevant info as needed)
                        responses.append(response.text)
                    except requests.exceptions.RequestException as e:
                        # Handle errors, you could log or return an error message
                        responses.append(f"Error: {str(e)}")

                # Add the responses to the DataFrame
                batch["api_response"] = responses

            return batch_iter

        # Define schema
        schema = StructType(
            [
                StructField("master_person_id", StringType(), False),
                StructField("client_person_id", StringType(), False),
                StructField("organization_id", StringType(), False),
                StructField("client_slug", StringType(), False),
                StructField("created_date", TimestampType(), False),
                StructField("last_updated_date", TimestampType(), False),
                StructField(
                    "api_response", StringType(), True
                ),  # Field to store API response
            ]
        )

        # Use mapInPandas to apply the API call to each partition of the DataFrame
        result_df = user_profile_df.mapInPandas(call_api_for_each_row, schema)

        # Show the resulting DataFrame
        result_df.show()

    @staticmethod
    def send_task_to_iterable(task_df: DataFrame) -> None:
        print(f"Task count:{task_df.count()}")

        # Define function to call REST API
        def call_api_for_each_row(
            batch_iter: Iterable[pd.DataFrame],
        ) -> Iterable[pd.DataFrame]:
            # https://support.iterable.com/hc/en-us/articles/204780579-API-Overview-and-Sample-Payloads#post-api-users-update
            api_url = "https://api.iterable.com/api/events/track"  # Replace with your actual API URL
            iterable_api_key = os.getenv("ITERABLE_API_KEY")

            batch: pd.DataFrame
            for batch in batch_iter:
                responses = []

                pdf_json: str = batch.to_json(orient="records")
                input_rows: List[Dict[str, Any]] = json.loads(pdf_json)

                result: List[Dict[str, Any]] = []

                for row in input_rows:
                    master_person_id = row["master_person_id"]
                    client_person_id = row["client_person_id"]
                    organization_id = row["organization_id"]
                    client_slug = row["client_slug"]
                    created_date = row["created_date"]
                    last_updated_date = row["last_updated_date"]
                    activity_definition_id = row["activity_definition_id"]
                    task_name = row["task_name"]
                    task_id = row["task_id"]
                    completed_date = row["completed_date"]
                    all_fields: List[Dict[str, Any]] = row["all_fields"]

                    try:
                        headers = {
                            "Api-key": iterable_api_key,
                            "Content-Type": "application/json",
                        }

                        data_fields: Dict[str, Any] = {
                            field["field_name"]: field["field_value"]
                            for field in all_fields
                        }
                        data_fields["task_id"] = task_id
                        data_fields["task_name"] = task_name
                        data_fields["activity_definition_id"] = activity_definition_id
                        data_fields["completed_date"] = completed_date

                        data = {
                            "userId": f"{master_person_id}",
                            "eventName": (
                                task_name
                                if not completed_date
                                else f"{task_name}_Completed"
                            ),
                            "id": task_id,
                            "createdAt": created_date,
                            "dataFields": data_fields,
                            "campaignId": 0,
                            "templateId": 0,
                            "createNewFields": True,
                        }
                        print(f"data: {data}")
                        # Make the POST request
                        response = requests.post(api_url, headers=headers, json=data)
                        # Get the response text (or extract relevant info as needed)
                        responses.append(response.text)
                    except requests.exceptions.RequestException as e:
                        # Handle errors, you could log or return an error message
                        responses.append(f"Error: {str(e)}")

                # Add the responses to the DataFrame
                batch["api_response"] = responses

            return batch_iter

        # Define schema
        schema = StructType(
            [
                StructField("master_person_id", StringType(), False),
                StructField("client_person_id", StringType(), False),
                StructField("organization_id", StringType(), False),
                StructField("client_slug", StringType(), False),
                StructField("created_date", TimestampType(), False),
                StructField("last_updated_date", TimestampType(), False),
                StructField(
                    "api_response", StringType(), True
                ),  # Field to store API response
            ]
        )

        # Use mapInPandas to apply the API call to each partition of the DataFrame
        result_df = task_df.mapInPandas(call_api_for_each_row, schema)

        # Show the resulting DataFrame
        result_df.show()
