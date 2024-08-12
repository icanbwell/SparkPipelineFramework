import os
import urllib.parse

from spark_pipeline_framework.utilities.aws.config import get_ssm_config


class DocumentDbServerUrl:
    CERT_PATH = "/helix.pipelines/certs/rds-combined-ca-bundle.pem"

    def get_server_url(self) -> str:
        env_name = os.environ.get("ENV", "local").lower()
        server_url: str = ""

        if env_name in ["local"]:
            # document db is not available locally so just use the local mongo server
            server_url = "mongodb://mongo:27017/"
        elif env_name in ["dev"]:
            server_url = self._get_dev_connection_string()
        elif env_name in ["client-sandbox"]:
            server_url = self._get_client_sandbox_connection_string()
        elif env_name in ["staging"]:
            server_url = self._get_staging_connection_string()
        elif env_name in ["prod"]:
            server_url = self._get_prod_connection_string()
        else:
            raise Exception(f"Unexpected environment: {env_name}")

        return server_url

    def _get_dev_connection_string(self) -> str:
        path: str = os.environ.get(
            "MONGO_PASSWORD_SSM_PATH", "/dev-ue1/integrationhub-service/"
        )
        ssm_config = get_ssm_config(path=path)
        password = ssm_config[f"{path}mongo_password"]
        escape_password = urllib.parse.quote_plus(password)
        mongo_username = os.environ.get(
            "MONGO_USERNAME", "dev-svc-integration-hub-service"
        )
        mongo_host = os.environ.get(
            "MONGO_HOST",
            "mongodb+srv://cl-dev-services-pl-0.vpsmx.mongodb.net/?authSource=admin&retryWrites=false",
        )
        return self._get_connection_string(mongo_username, escape_password, mongo_host)

    def _get_client_sandbox_connection_string(self) -> str:
        path: str = os.environ.get(
            "MONGO_PASSWORD_SSM_PATH", "/client-sandbox-ue1/integrationhub-service/"
        )
        ssm_config = get_ssm_config(path=path)
        password = ssm_config[f"{path}mongo_password"]
        escape_password = urllib.parse.quote_plus(password)
        mongo_username = os.environ.get(
            "MONGO_USERNAME", "cs-svc-integration-hub-service"
        )
        mongo_host = os.environ.get(
            "MONGO_HOST",
            "mongodb+srv://cl-clients-sandbox-services-pl-1.bonmy.mongodb.net/?authSource=admin",
        )
        return self._get_connection_string(mongo_username, escape_password, mongo_host)

    def _get_staging_connection_string(self) -> str:
        path: str = os.environ.get(
            "MONGO_PASSWORD_SSM_PATH", "/staging/integrationhub-service/"
        )
        ssm_config = get_ssm_config(path=path)
        password = ssm_config[f"{path}mongo_password"]
        escape_password = urllib.parse.quote_plus(password)
        mongo_username = os.environ.get(
            "MONGO_USERNAME", "staging-svc-integration-hub-service"
        )
        mongo_host = os.environ.get(
            "MONGO_HOST",
            "mongodb+srv://cl-staging-services-pl-3.arsv0.mongodb.net/?authSource=admin&retryWrites=false",
        )
        return self._get_connection_string(mongo_username, escape_password, mongo_host)

    def _get_prod_connection_string(self) -> str:
        path: str = os.environ.get(
            "MONGO_PASSWORD_SSM_PATH", "/prod-ue1/integrationhub-service/"
        )
        ssm_config = get_ssm_config(path=path)
        password = ssm_config[f"{path}mongo_password"]
        escape_password = urllib.parse.quote_plus(password)
        mongo_username = os.environ.get("MONGO_USERNAME", "prod-svc-integration-hub")
        mongo_host = os.environ.get(
            "MONGO_HOST",
            "mongodb+srv://cl-prod-services-pl-0.21mit.mongodb.net/?authSource=admin&retryWrites=false",
        )
        return self._get_connection_string(mongo_username, escape_password, mongo_host)

    def _get_connection_string(self, user: str, password: str, host: str) -> str:
        split_host = host.split("//")
        print_safe_connection_string = f"mongodb+srv://{user}:******@{split_host[1]}"
        print(f"connecthub conn string: {print_safe_connection_string}")
        connection_string = f"mongodb+srv://{user}:{password}@{split_host[1]}"
        return connection_string
