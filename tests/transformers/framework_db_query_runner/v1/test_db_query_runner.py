from pyspark.sql.session import SparkSession

from spark_pipeline_framework.transformers.framework_db_query_runner.v1.framework_db_query_runner import \
    FrameworkDBQueryRunner


def test_db_query_runner(spark_session: SparkSession) -> None:
    """
    we're only testing that the db configs are correctly exposed.
    """
    # Arrange
    username = "username"
    password = "password"
    host = "localhost"
    port = 3306
    query = "CREATE TABLE ABC(a text)"
    db = "database"

    # Act
    query_runner = FrameworkDBQueryRunner(
        username=username,
        password=password,
        host=host,
        port=port,
        db_name=db,
        query=query
    )

    # Assert
    assert username == query_runner.getUsername()
    assert password == query_runner.getPassword()
    assert host == query_runner.getHost()
    assert port == query_runner.getPort()
    assert query == query_runner.getQuery()
    assert db == query_runner.getDb()
