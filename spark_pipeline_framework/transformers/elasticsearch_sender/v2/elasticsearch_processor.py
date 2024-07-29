import json
from typing import Any, Dict, Iterable, List, Optional, Callable

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


class ElasticSearchProcessor:
    @staticmethod
    def get_process_batch_function(
        *, parameters: ElasticSearchSenderParameters
    ) -> Callable[[Iterable[pd.DataFrame]], Iterable[pd.DataFrame]]:
        """
        Returns a function that includes the passed parameters so that function
        can be used in a pandas_udf

        :param parameters: FhirSenderParameters
        :return: pandas_udf
        """

        def process_batch(
            batch_iter: Iterable[pd.DataFrame],
        ) -> Iterable[pd.DataFrame]:
            """
            This function is passed a list of dataframes, each dataframe is a partition

            :param batch_iter: Iterable[pd.DataFrame]
            :return: Iterable[pd.DataFrame]
            """
            pdf: pd.DataFrame
            index: int = 0
            # print(f"batch type: {type(batch_iter)}")
            for pdf in batch_iter:
                # print(f"pdf type: {type(pdf)}")
                # convert the dataframe to a list of dictionaries
                pdf_json: str = pdf.to_json(orient="records")
                rows: List[Dict[str, Any]] = json.loads(pdf_json)
                # print(f"Processing partition {pdf.index} with {len(rows)} rows")
                # send the partition to the server
                result_list: Iterable[Dict[str, Any] | None] = (
                    ElasticSearchProcessor.send_partition_to_server(
                        partition_index=index, parameters=parameters, rows=rows
                    )
                )
                # remove any nulls
                result_list = [r for r in result_list if r is not None]
                index += 1
                # yield the result as a dataframe
                yield pd.DataFrame(result_list)

        return process_batch

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
            yield response_json.to_dict()
        else:
            logger.info(
                f"Batch {partition_index}/{parameters.desired_partitions} is empty"
            )
            yield None
