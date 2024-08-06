import pytest
from _pytest.monkeypatch import MonkeyPatch
from botocore.client import BaseClient

from spark_pipeline_framework.utilities.document_db_connection.v1.document_db_connection import (
    DocumentDbServerUrl,
)


@pytest.mark.parametrize(
    "env_name,url_part",
    [
        ("local", "mongodb://mongo:27017/"),
        ("dev", "fake_dev_password"),
        ("prod", "fake_prod_password"),
    ],
)
def test_can_get_server_url_for_environment(
    monkeypatch: MonkeyPatch, ssm_mock: BaseClient, env_name: str, url_part: str
) -> None:

    ssm_mock.put_parameter(  # type: ignore
        Name="/prod-ue1/integrationhub-service/mongo_password",
        Value="fake_prod_password",
        Type="SecureString",
    )

    ssm_mock.put_parameter(  # type: ignore
        Name="/dev-ue1/integrationhub-service/mongo_password",
        Value="fake_dev_password",
        Type="SecureString",
    )
    monkeypatch.setenv("ENV", env_name)
    server_url: str = DocumentDbServerUrl().get_server_url()
    print(server_url)
    assert server_url
    assert url_part in server_url
