from pyspark.sql.session import SparkSession

from spark_pipeline_framework.transformers.framework_jdbc_exporter.v1.framework_jdbc_exporter import \
    FrameworkJdbcExporter


def test_can_save_via_jdbc(spark_session: SparkSession) -> None:
    """
    Because testing against a database is a pain and most of the core transformation logic is in the base exporter,
    we're only testing that the options and format are correctly being exposed.
    """
    # Arrange
    view = "my_view"
    jdbc_url = "jdbc:mysql:user@password:host/db:port"
    table = "my_view_table"
    driver = "org.driver.FakeDriver"

    # Act
    exporter = FrameworkJdbcExporter(
        view=view,
        jdbc_url=jdbc_url,
        table=table,
        driver=driver,
        mode=FrameworkJdbcExporter.MODE_OVERWRITE,
    )

    # Assert
    options = exporter.getOptions()
    assert options["url"] == jdbc_url
    assert options["driver"] == driver
    assert exporter.getFormat() == "jdbc"
    assert exporter.getMode() == "overwrite"
