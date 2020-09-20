from pathlib import Path

from pyspark.sql.session import SparkSession


def test_simple_csv_loader_pipeline(spark_session: SparkSession) -> None:
    # Arrange
    data_dir: str = Path(__file__).parent.joinpath('./')
    flights_path: str = f"file://{data_dir.joinpath('flights.csv')}"

