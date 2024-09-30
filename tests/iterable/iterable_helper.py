import json
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
            api_url = (
                "https://example.com/api/endpoint"  # Replace with your actual API URL
            )

            batch: pd.DataFrame
            for batch in batch_iter:
                responses = []

                pdf_json: str = batch.to_json(orient="records")
                input_rows: List[Dict[str, Any]] = json.loads(pdf_json)

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
                        iterable_api_key = "79b444d11d8e4e97bc04c7d0c60ab27d"
                        headers = {
                            "Api-key": iterable_api_key,
                            "Content-Type": "application/json",
                        }

                        # Define the payload
                        # data_fields = {
                        #     "data_source": "John Muir Health",
                        #     "date_received": "2018-06-01",
                        # }
                        data_fields: Dict[str, Any] = {
                            field["field_name"]: field["field_value"]
                            for field in all_fields
                        }
                        data = {
                            "email": "imran.qureshi@icanbwell.com",
                            "userId": f"{master_person_id}",
                            "dataFields": data_fields,
                            "preferUserId": True,
                            "mergeNestedObjects": True,
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
