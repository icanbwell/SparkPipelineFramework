from pathlib import Path

from pyspark.sql import SparkSession, DataFrame
from spark_pipeline_framework.transformers.framework_csv_loader.v1.framework_csv_loader import (
    FrameworkCsvLoader,
)
from spark_pipeline_framework.utilities.spark_data_frame_helpers import (
    create_empty_dataframe,
)
from spark_pipeline_framework.transformers.framework_fill_na_transformer.v1.framework_fill_na_transformer import (
    FrameworkFillNaTransformer,
)


def test_framework_fill_na_transformer(spark_session: SparkSession) -> None:
    # create a dataframe with the test data
    data_dir: Path = Path(__file__).parent.joinpath("./")

    df: DataFrame = create_empty_dataframe(spark_session=spark_session)

    view: str = "primary_care_protocol"
    FrameworkCsvLoader(
        view=view,
        filepath=data_dir.joinpath("primary_care_protocol.csv"),
        clean_column_names=False,
    ).transform(df)

    # ensure we have all the rows even the ones we want to drop
    result_df: DataFrame = spark_session.table(view)
    result_df = result_df.withColumn(
        "Minimum Age", result_df["Minimum Age"].cast("float")
    )
    result_df.createOrReplaceTempView(view)
    assert 7 == result_df.count()

    # drop the rows with null NPI or null Last Name
    FrameworkFillNaTransformer(
        view=view, column_mapping={"Minimum Age": 1.0, "Maximum Age": "No Limit"}
    ).transform(df)

    # assert we get only the rows with a populated NPI
    result_df = spark_session.table(view)
    assert 7 == result_df.count()
    assert "No Limit" == result_df.select("Maximum Age").collect()[1].__getitem__(
        "Maximum Age"
    )
    assert 24.0 == result_df.agg({"Minimum Age": "sum"}).collect()[0][0]
