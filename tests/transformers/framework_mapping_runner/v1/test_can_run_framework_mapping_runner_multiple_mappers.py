from pathlib import Path
from typing import Callable, Dict, Any, Union, List

from pyspark.sql import SparkSession, DataFrame
from spark_auto_mapper.automappers.automapper_base import AutoMapperBase
from spark_pipeline_framework.progress_logger.progress_logger import ProgressLogger

from spark_pipeline_framework.proxy_generator.python_transformer_helpers import (
    get_python_function_from_location,
)
from spark_pipeline_framework.transformers.framework_mapping_runner.v1.framework_mapping_runner import (
    FrameworkMappingLoader,
)


def test_can_run_framework_mapping_runner_multiple_mappings(
    spark_session: SparkSession,
) -> None:
    # Arrange
    data_dir: Path = Path(__file__).parent.parent.joinpath("./")

    spark_session.createDataFrame(
        [(1, "Qureshi", "Imran"), (2, "Vidal", "Michael"),],  # noqa: E231
        ["member_id", "last_name", "first_name"],
    ).createOrReplaceTempView("patients")

    source_df: DataFrame = spark_session.table("patients")

    df = source_df.select("member_id")
    df.createOrReplaceTempView("members")

    # Act
    mapping_function: Callable[
        [Dict[str, Any]], Union[AutoMapperBase, List[AutoMapperBase]]
    ] = get_python_function_from_location(
        location=str(
            data_dir.joinpath("library/features/carriers_multiple_mappings/v1")
        ),
        import_module_name=".mapping",
        function_name="mapping",
    )
    with ProgressLogger() as progress_logger:
        FrameworkMappingLoader(
            view="members",
            mapping_function=mapping_function,
            parameters={"foo": "bar", "view2": "my_view_2"},
            progress_logger=progress_logger,
        ).transform(df)

    # Assert
    result_df: DataFrame = spark_session.table("members")

    result_df.printSchema()
    result_df.show(truncate=False)

    assert len(result_df.columns) == 6
    assert result_df.where("patient_id == 1").select("dst1").collect()[0][0] == "src1"
    assert (
        result_df.where("patient_id == 1").select("dst2").collect()[0][0][0]
        == "address1"
    )

    assert (
        result_df.where("patient_id == 1").select("dst3").collect()[0][0][0]
        == "address1"
    )
    assert (
        result_df.where("patient_id == 1").select("dst3").collect()[0][0][1]
        == "address2"
    )

    assert (
        result_df.where("patient_id == 1").select("dst4").collect()[0][0][0][0]
        == "usual"
    )
    assert (
        result_df.where("patient_id == 1").select("dst4").collect()[0][0][0][1]
        == "Qureshi"
    )

    result_df2: DataFrame = spark_session.table("my_view_2")

    result_df2.printSchema()
    result_df2.show(truncate=False)

    assert len(result_df2.columns) == 3
    assert result_df2.where("patient_id == 1").select("dst1").collect()[0][0] == "src2"
