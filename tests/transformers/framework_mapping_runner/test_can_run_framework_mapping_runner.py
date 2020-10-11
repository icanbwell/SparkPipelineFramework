from pathlib import Path
from typing import Callable, Dict, Any

from pyspark.sql import SparkSession, DataFrame
from spark_auto_mapper.automapper_base import AutoMapperBase
from spark_pipeline_framework.progress_logger.progress_logger import ProgressLogger

from spark_pipeline_framework.proxy_generator.python_transformer_helpers import get_python_function_from_location
from spark_pipeline_framework.transformers.framework_mapping_runner import FrameworkMappingLoader


def test_can_run_framework_mapping_runner(spark_session: SparkSession) -> None:
    # Arrange
    data_dir: Path = Path(__file__).parent.parent.joinpath('./')

    spark_session.createDataFrame(
        [
            (1, 'Qureshi', 'Imran'),
            (2, 'Vidal', 'Michael'),
        ],
        ['member_id', 'last_name', 'first_name']
    ).createOrReplaceTempView("patients")

    source_df: DataFrame = spark_session.table("patients")

    df = source_df.select("member_id")
    df.createOrReplaceTempView("members")

    # Act
    mapping_function: Callable[[Dict[str, Any]], AutoMapperBase] = get_python_function_from_location(
        location=str(data_dir.joinpath("library/features/carriers_mapping/v1")),
        import_module_name='.mapping'
    )
    with ProgressLogger() as progress_logger:
        FrameworkMappingLoader(
            view="members",
            mapping_function=mapping_function,
            parameters={"foo": "bar"},
            progress_logger=progress_logger
        ).transform(df)

    # Assert
    result_df: DataFrame = spark_session.table("members")

    result_df.printSchema()
    result_df.show(truncate=False)

    assert len(result_df.columns) == 5
    assert result_df.where("member_id == 1").select("dst1").collect()[0][0] == "src1"
    assert result_df.where("member_id == 1").select("dst2").collect()[0][0][0] == "address1"

    assert result_df.where("member_id == 1").select("dst3").collect()[0][0][0] == "address1"
    assert result_df.where("member_id == 1").select("dst3").collect()[0][0][1] == "address2"

    assert result_df.where("member_id == 1").select("dst4").collect()[0][0][0][0] == "usual"
    assert result_df.where("member_id == 1").select("dst4").collect()[0][0][0][1] == "Qureshi"
