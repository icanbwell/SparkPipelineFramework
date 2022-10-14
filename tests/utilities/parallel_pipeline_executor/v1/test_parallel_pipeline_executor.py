import time

import pytest

from spark_pipeline_framework.transformers.framework_transformer.v1.framework_transformer import (
    FrameworkTransformer,
)
from spark_pipeline_framework.utilities.capture_parameters import capture_parameters
from pyspark.ml.base import Transformer
from pyspark.ml.feature import SQLTransformer
from pyspark.ml.param import Param
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.session import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

from spark_pipeline_framework.progress_logger.progress_logger import ProgressLogger
from spark_pipeline_framework.utilities.parallel_pipeline_executor.v1.parallel_pipeline_executor import (
    ParallelPipelineExecutor,
)


@pytest.mark.asyncio
async def test_can_run_pipelines_in_parallel(spark_session: SparkSession) -> None:
    my_schema = StructType(
        [
            StructField("Name", StringType(), False),
            StructField("Age", IntegerType(), False),
        ]
    )

    test_list = [
        ("Mike", 19),
        ("June", 18),
        ("Rachel", 16),
        ("Rob", 18),
        ("Scott", 17),
        ("Mike", 21),
        ("Mike", 15),
    ]

    df = spark_session.createDataFrame(test_list, schema=my_schema)

    executor = ParallelPipelineExecutor(max_tasks=2, progress_logger=ProgressLogger())
    executor.append(
        "Name",
        [
            SQLTransformer(statement="SELECT Name FROM __THIS__"),
            DummyDelayTransformer(delay=10),
        ],
    )
    executor.append(
        "age",
        [
            SQLTransformer(statement="SELECT Age FROM __THIS__"),
            DummyDelayTransformer(delay=10),
        ],
    )
    executor.append(
        "Name+Age",
        [
            SQLTransformer(statement="SELECT Name, Age FROM __THIS__"),
            DummyDelayTransformer(delay=5),
        ],
    )

    expected_count = 3

    actual_count = 0
    async for name, result_df in executor.transform_async(df, spark_session):
        actual_count += 1
        assert type(result_df) == DataFrame
        result_df.show()

    assert actual_count == expected_count


class DummyDelayTransformer(FrameworkTransformer):
    """
    dummy
    """

    # noinspection PyUnusedLocal
    @capture_parameters
    def __init__(self, delay: int) -> None:
        super().__init__()
        self.delay: Param[int] = Param(self, "delay", "")
        self._setDefault(delay=delay)

        kwargs = self._input_kwargs
        self.setParams(**kwargs)

    # noinspection PyMethodMayBeStatic
    def _transform(self, df: DataFrame) -> DataFrame:
        time.sleep(float(self.getDelay()))
        return df

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def setDelay(self, value: int) -> Transformer:
        self._paramMap[self.delay] = value
        return self

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def getDelay(self) -> int:
        return self.getOrDefault(self.delay)
