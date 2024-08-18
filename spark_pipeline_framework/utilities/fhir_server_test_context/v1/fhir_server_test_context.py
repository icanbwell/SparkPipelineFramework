import logging
from os import environ
from typing import Optional, List, Dict, Any, Type, Union

from helix_fhir_client_sdk.fhir_client import FhirClient
from helix_fhir_client_sdk.utilities.fhir_server_helpers import FhirServerHelpers

from spark_pipeline_framework.utilities.fhir_helpers.fhir_get_access_token import (
    fhir_get_access_token_async,
)
from spark_pipeline_framework.utilities.fhir_helpers.token_helper import TokenHelper


class FhirServerTestContext:
    """
    This class cleans the FHIR server before/after running the tests.
    """

    def __init__(
        self, *, resource_type: str | List[str], owner_tag: str = "bwell"
    ) -> None:
        """
        This class cleans the FHIR server before/after running the tests.

        :param resource_type: The resource type to clean.
        :param owner_tag: The owner tag to clean.
        """
        assert resource_type is not None
        assert isinstance(resource_type, str) or isinstance(resource_type, list)
        self.resource_type: str | List[str] = resource_type
        self.owner_tag: str = owner_tag
        self.fhir_server_url: str = environ["FHIR_SERVER_URL"]
        self.auth_client_id = environ["FHIR_CLIENT_ID"]
        self.auth_client_secret = environ["FHIR_CLIENT_SECRET"]
        self.auth_well_known_url = environ["AUTH_CONFIGURATION_URI"]

    async def __aenter__(self) -> "FhirServerTestContext":
        """
        Implement the async enter method so that this class can be used as an async context manager.

        """
        if isinstance(self.resource_type, list):
            for resource_type in self.resource_type:
                await FhirServerHelpers.clean_fhir_server_async(
                    resource_type=resource_type, owner_tag=self.owner_tag
                )
        else:
            await FhirServerHelpers.clean_fhir_server_async(
                resource_type=self.resource_type, owner_tag=self.owner_tag
            )
        return self

    async def __aexit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc_val: Optional[BaseException],
        exc_tb: Optional[Union[Type[BaseException], None]],
    ) -> None:
        """
        Implement the async exit method so that this class can be used as an async context manager.

        :param exc_type: The exception type.
        :param exc_val: The exception value.
        :param exc_tb: The exception traceback.
        """
        if isinstance(self.resource_type, list):
            for resource_type in self.resource_type:
                await FhirServerHelpers.clean_fhir_server_async(
                    resource_type=resource_type, owner_tag=self.owner_tag
                )
        else:
            await FhirServerHelpers.clean_fhir_server_async(
                resource_type=self.resource_type, owner_tag=self.owner_tag
            )

    def get_authorization_header(self) -> Dict[str, Any]:
        """
        Create the authorization header using the client id and client secret.

        :return: The authorization header.
        """
        token_url: Optional[str] = TokenHelper.get_auth_server_url_from_well_known_url(
            well_known_url=self.auth_well_known_url
        )
        assert token_url
        return TokenHelper.get_authorization_header(
            client_id=self.auth_client_id,
            client_secret=self.auth_client_secret,
            token_url=token_url,
            # scope=f"access/{self.owner_tag}.* user/*.*",
            # scope=f"access/*.* user/*.*",
            scope=None,
        )

    def get_auth_server_url_from_well_known_url(self) -> Optional[str]:
        """
        Get the authorization server URL from the well known URL.

        :return: The authorization server URL.
        """
        return TokenHelper.get_auth_server_url_from_well_known_url(
            well_known_url=self.auth_well_known_url
        )

    def get_token_url(self) -> Optional[str]:
        """
        Get the token URL.

        :return: The token URL.
        """
        return self.get_auth_server_url_from_well_known_url()

    async def create_fhir_client_async(self) -> FhirClient:
        """
        Create the FHIR client.

        :return: The FHIR client.
        """
        fhir_client = FhirClient()
        fhir_client = fhir_client.url(self.fhir_server_url)
        fhir_client = fhir_client.client_credentials(
            client_id=self.auth_client_id, client_secret=self.auth_client_secret
        )
        fhir_client = fhir_client.auth_wellknown_url(self.auth_well_known_url)
        fhir_client = fhir_client.set_access_token(await self.get_access_token_async())
        return fhir_client

    async def get_access_token_async(self) -> Optional[str]:
        """
        Get the access token.

        :return: The access token.
        """
        return await fhir_get_access_token_async(
            logger=logging.getLogger(__name__),
            server_url=self.fhir_server_url,
            log_level="DEBUG",
            auth_client_id=self.auth_client_id,
            auth_client_secret=self.auth_client_secret,
            auth_well_known_url=self.auth_well_known_url,
        )
