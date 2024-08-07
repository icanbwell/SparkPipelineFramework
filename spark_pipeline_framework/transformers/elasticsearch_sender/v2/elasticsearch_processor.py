import json
from typing import (
    Any,
    Dict,
    Iterable,
    List,
    Optional,
    Callable,
    Iterator,
    AsyncGenerator,
    cast,
)

import pandas as pd

from spark_pipeline_framework.logger.yarn_logger import get_logger
from spark_pipeline_framework.transformers.elasticsearch_sender.v2.elasticsearch_helpers import (
    send_json_bundle_to_elasticsearch,
)
from spark_pipeline_framework.transformers.elasticsearch_sender.v2.elasticsearch_result import (
    ElasticSearchResult,
)
from spark_pipeline_framework.transformers.elasticsearch_sender.v2.elasticsearch_sender_parameters import (
    ElasticSearchSenderParameters,
)
from spark_pipeline_framework.utilities.async_pandas_udf.v1.async_pandas_dataframe_udf import (
    AsyncPandasDataFrameUDF,
)
from spark_pipeline_framework.utilities.async_pandas_udf.v1.function_types import (
    HandlePandasBatchWithParametersFunction,
)


class ElasticSearchProcessor:
    @staticmethod
    async def process_partition(
        input_values: List[Dict[str, Any]],
        parameters: Optional[ElasticSearchSenderParameters],
    ) -> AsyncGenerator[Dict[str, Any], None]:
        assert parameters
        count: int = 0
        try:
            result: Iterable[Dict[str, Any] | None] = (
                ElasticSearchProcessor.send_partition_to_server(
                    partition_index=0, parameters=parameters, rows=input_values
                )
            )
            r: Dict[str, Any] | None
            for r in result:
                count += 1
                if r:
                    yield r
                else:
                    yield {
                        "error": "Failed to send data to ElasticSearch",
                        "partition_index": 0,
                        "url": parameters.index,
                        "success": 0,
                        "failed": 1,
                        "payload": json.dumps(input_values),
                    }
        except Exception as e:
            # if an exception is thrown then return an error for each row
            for input_value in input_values:
                count += 1
                yield {
                    "error": str(e),
                    "partition_index": 0,
                    "url": parameters.index,
                    "success": 0,
                    "failed": 1,
                    "payload": json.dumps(input_values),
                }
        # we actually want to error here since something strange happened
        assert count == len(input_values), f"count={count}, len={len(input_values)}"

    @staticmethod
    def get_process_batch_function(
        *, parameters: ElasticSearchSenderParameters
    ) -> Callable[[Iterable[pd.DataFrame]], Iterator[pd.DataFrame]]:
        """
        Returns a function that includes the passed parameters so that function
        can be used in a pandas_udf

        :param parameters: FhirSenderParameters
        :return: pandas_udf
        """

        return AsyncPandasDataFrameUDF(
            async_func=cast(
                HandlePandasBatchWithParametersFunction[ElasticSearchSenderParameters],
                ElasticSearchProcessor.process_partition,
            ),
            parameters=parameters,
        ).get_pandas_udf()

    @staticmethod
    def send_partition_to_server(
        *,
        partition_index: int,
        rows: Iterable[Dict[str, Any]],
        parameters: ElasticSearchSenderParameters,
    ) -> Iterable[Optional[Dict[str, Any]]]:
        assert parameters.index is not None
        assert isinstance(parameters.index, str)
        assert parameters.operation is not None
        assert isinstance(parameters.operation, str)
        assert parameters.doc_id_prefix is None or isinstance(
            parameters.doc_id_prefix, str
        )

        json_data_list: List[str] = [
            r["value"] for r in rows if "value" in r and r["value"] is not None
        ]
        assert isinstance(json_data_list, list)
        assert all(isinstance(j, str) for j in json_data_list)

        logger = get_logger(__name__)

        if len(json_data_list) > 0:
            logger.info(
                f"Sending batch {partition_index}/{parameters.desired_partitions} "
                f"containing {len(json_data_list)} rows "
                f"to ES Server/{parameters.index}. [{parameters.name}].."
            )
            # send to server
            response_json: ElasticSearchResult = send_json_bundle_to_elasticsearch(
                json_data_list=json_data_list,
                index=parameters.index,
                operation=parameters.operation,
                logger=logger,
                doc_id_prefix=parameters.doc_id_prefix,
            )
            response_json.partition_index = partition_index
            yield response_json.to_dict_flatten_payload()
        else:
            logger.info(
                f"Batch {partition_index}/{parameters.desired_partitions} is empty"
            )
            yield None
