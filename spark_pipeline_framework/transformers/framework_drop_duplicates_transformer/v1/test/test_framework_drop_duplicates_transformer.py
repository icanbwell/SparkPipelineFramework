from pathlib import Path

from pyspark.sql import SparkSession, DataFrame

from spark_pipeline_framework.transformers.framework_csv_loader.v1.framework_csv_loader import (
    FrameworkCsvLoader,
)
from spark_pipeline_framework.transformers.framework_drop_duplicates_transformer.v1.framework_drop_duplicates_transformer import (
    FrameworkDropDuplicatesTransformer,
)
from spark_pipeline_framework.utilities.spark_data_frame_helpers import (
    create_empty_dataframe,
)


def test_framework_drop_duplicates_transformer(spark_session: SparkSession) -> None:
    # create a dataframe with the test data
    data_dir: Path = Path(__file__).parent.joinpath("./")

    df: DataFrame = create_empty_dataframe(spark_session=spark_session)

    view: str = "primary_care_protocol"
    FrameworkCsvLoader(
        view=view,
        file_path=data_dir.joinpath("primary_care_protocol.csv"),
        clean_column_names=False,
    ).transform(df)

    # ensure we have all the rows even the ones we want to drop
    result_df: DataFrame = spark_session.table(view)
    assert 3 == result_df.count()

    # drop the rows with null NPI or null Last Name
    FrameworkDropDuplicatesTransformer(columns=["NPI"], view=view).transform(df)

    # assert we get only the rows with a populated NPI
    result_df = spark_session.table(view)
    assert 2 == result_df.count()
