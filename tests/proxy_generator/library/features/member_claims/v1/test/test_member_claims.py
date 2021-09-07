from pathlib import Path

from pyspark.sql import SparkSession

from library.test_runners.file_to_fhir_test_runner import FileToFhirTestRunner


def test_mock_member_claims(spark_session: SparkSession) -> None:
    data_dir: Path = Path(__file__).parent.joinpath("./")
    test_name = "test_mock_member_claims"

    FileToFhirTestRunner(
        spark_session=spark_session, data_dir=data_dir, test_name=test_name
    ).run_test()

    # Steps:
    # 1. Create a single row of sample data from production
    # 2. De-identify the data
    # 3. Save it as csv or json in input folder
    # 4. Run this test
    # 5. The test will detect that there is nothing in fhir_calls folder and will wroite
    #       the output of the test in that folder
    # 6. Review the output in fhir_calls folder and make sure it is right
    # 7. Commit everything to GitHub
