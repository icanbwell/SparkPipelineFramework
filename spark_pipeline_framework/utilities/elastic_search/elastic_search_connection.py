import os
from typing import Tuple, Optional

from opensearchpy import OpenSearch, AsyncOpenSearch

from furl import furl

from spark_pipeline_framework.utilities.aws.config import get_ssm_config
from botocore.exceptions import ClientError
from spark_pipeline_framework.logger.yarn_logger import get_logger


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

    def get_elastic_search_async_client(self, *, timeout: int = 60) -> AsyncOpenSearch:
        """
        Create an async OpenSearch client

        :param timeout: timeout in seconds for calls made by this client
        :return: async OpenSearch client
        """
        env_name = os.environ.get("ENV", "local")
        es_url = self._get_connection_string()
        client = AsyncOpenSearch(
            hosts=es_url,
            use_ssl=env_name != "local",
            verify_certs=env_name != "local",
            timeout=timeout,  # timeout in seconds
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
        logger = get_logger(__name__)
        if env_name == "local":
            return "admin", "admin"
        username: Optional[str] = os.environ.get("ELASTICSEARCH_USER")
        password: Optional[str] = os.environ.get("ELASTICSEARCH_PASSWORD")
        # if password is set an environment variable then use that
        if password:
            return username, password
        # else get from SSM
        path: str = ElasticSearchConnection._get_ssm_path(env_name=env_name)
        try:
            db_config = get_ssm_config(path=path)
        except ClientError:
            bwell_env = os.environ.get("BWELL_ENV", "local")
            bwell_path: str = ElasticSearchConnection._get_ssm_path(env_name=bwell_env)
            logger.info(
                f"Unable to find SSM path via ENV: {path}; trying to use BWELL_ENV instead: {bwell_path}"
            )
            db_config = get_ssm_config(path=bwell_path)
            path = bwell_path

        username = db_config[f"{path}username"]
        password = db_config[f"{path}password"]
        return username, password
