import os
from typing import Tuple, Optional

from opensearchpy import OpenSearch

from furl import furl

from spark_pipeline_framework.utilities.aws.config import get_ssm_config


class ElasticSearchConnection:
    def get_elastic_search_client(self) -> OpenSearch:
        env_name = os.environ.get("ENV", "local")
        es_url = self._get_connection_string()
        client = OpenSearch(
            hosts=es_url,
            use_ssl=env_name != "local",
            verify_certs=env_name != "local",
        )
        return client

    @staticmethod
    def get_elastic_search_host() -> str:
        host: str = os.environ["ELASTICSEARCH_HOST"]
        return host

    @staticmethod
    def _get_ssm_path(env_name: str) -> str:
        return f"/{env_name}/helix/elasticsearch/"

    @staticmethod
    def _get_connection_string() -> str:
        env_name = os.environ.get("ENV", "local")
        host: furl = furl(ElasticSearchConnection.get_elastic_search_host())
        port: str = os.environ.get("ELASTICSEARCH_PORT", host.port)
        user, password = ElasticSearchConnection._get_user_and_password(
            env_name=env_name
        )
        host.set(username=user, password=password, port=port)
        return str(host.url)

    @staticmethod
    def _get_user_and_password(env_name: str) -> Tuple[Optional[str], Optional[str]]:
        if env_name == "local":
            return "admin", "admin"
        username: Optional[str] = os.environ.get("ELASTICSEARCH_USER")
        password: Optional[str] = os.environ.get("ELASTICSEARCH_PASSWORD")
        # if password is set an environment variable then use that
        if password:
            return username, password
        # else get from SSM
        path: str = ElasticSearchConnection._get_ssm_path(env_name=env_name)
        db_config = get_ssm_config(path=path)
        username = db_config[f"{path}username"]
        password = db_config[f"{path}password"]
        return username, password
