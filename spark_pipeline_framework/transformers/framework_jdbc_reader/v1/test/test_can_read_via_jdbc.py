from pyspark.sql.session import SparkSession

from spark_pipeline_framework.transformers.framework_jdbc_reader.v1.framework_jdbc_reader import (
    FrameworkJdbcReader,
)


def test_can_read_via_jdbc(spark_session: SparkSession) -> None:
    """
    Because testing against a database is a pain and most of the core transformation logic is in the base module,
    we're only testing that the options and format are correctly being exposed.
    """
    # Arrange
    view = "my_view"
    name = "read_my_data"
    jdbc_url = "jdbc:mysql:user@password:host/db:port"
    query = "(select * from questionnaire) questionnaire_alias"
    driver = "org.driver.FakeDriver"

    # Act
    reader = FrameworkJdbcReader(
        jdbc_url=jdbc_url, query=query, driver=driver, view=view, name=name
    )

    # Assert
    assert reader.getJdbcUrl() == jdbc_url
    assert reader.getDriver() == driver
    assert reader.getFormat() == "jdbc"
    assert reader.getQuery() == query
    assert reader.getView() == view
