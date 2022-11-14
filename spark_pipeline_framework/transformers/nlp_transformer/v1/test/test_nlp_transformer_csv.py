from pathlib import Path
from typing import Dict, Any

from pyspark.sql.dataframe import DataFrame
from pyspark.sql.session import SparkSession
from pyspark.sql.types import StructType
from spark_pipeline_framework.transformers.nlp_transformer.v1.nlp_transformer import (
    NlpTransformer,
)

from spark_pipeline_framework.transformers.framework_drop_duplicates_transformer.v1.framework_drop_duplicates_transformer import (
    FrameworkDropDuplicatesTransformer,
)

from spark_pipeline_framework.pipelines.framework_pipeline import FrameworkPipeline
from spark_pipeline_framework.progress_logger.progress_logger import ProgressLogger
from spark_pipeline_framework.transformers.framework_csv_loader.v1.framework_csv_loader import (
    FrameworkCsvLoader,
)


class MyPipeline(FrameworkPipeline):
    def __init__(self, parameters: Dict[str, Any], progress_logger: ProgressLogger):
        super(MyPipeline, self).__init__(
            parameters=parameters, progress_logger=progress_logger
        )
        self.transformers = self.create_steps(
            [  # type: ignore
                FrameworkCsvLoader(
                    view=parameters["view"], file_path=parameters["analysis_path"]
                ),
                FrameworkDropDuplicatesTransformer(
                    columns=[parameters["column"]], view=parameters["view"]
                ),
                NlpTransformer(
                    column=parameters["column"],
                    view=parameters["view"],
                    parameters=parameters,
                ),
            ]
        )


def test_can_run_framework_pipeline(spark_session: SparkSession) -> None:
    # Arrange
    data_dir: Path = Path(__file__).parent.joinpath("./")
    analysis_path: str = f"file://{data_dir.joinpath('challenge_info_small.csv')}"

    schema = StructType([])

    df: DataFrame = spark_session.createDataFrame(
        spark_session.sparkContext.emptyRDD(), schema
    )

    spark_session.sql("DROP TABLE IF EXISTS default.nlp_analysis")

    # Act
    parameters = {
        "analysis_path": analysis_path,
        "column": "challenge_name",
        "path_to_csv": analysis_path,
        "view": "nlp_analysis",
        "binarize_tokens": False,
        "table_name": "my_NLP_table",
        "file_path": analysis_path,
        "condense_output_columns": False,
        "perform_analysis": ["all"],
    }
    with ProgressLogger() as progress_logger:
        pipeline: MyPipeline = MyPipeline(
            parameters=parameters, progress_logger=progress_logger
        )
        transformer = pipeline.fit(df)
        transformer.transform(df)

    # Assert
    result_df: DataFrame = spark_session.sql("SELECT * FROM nlp_analysis")
    result_df.show()

    assert result_df.count() > 0


def do_nlp_test() -> None:
    print("Building Session")
    import time

    begin = time.time()

    spark_session = (
        SparkSession.builder.appName("Spark NLP")
        .config(
            "spark.jars.packages",
            "mysql:mysql-connector-java:8.0.24,com.johnsnowlabs.nlp:spark-nlp_2.12:4.2.1",
        )
        .getOrCreate()
    )

    test_can_run_framework_pipeline(spark_session)
    print("TIME ELAPSED: ")
    print(time.time() - begin)


if __name__ == "__main__":
    do_nlp_test()
