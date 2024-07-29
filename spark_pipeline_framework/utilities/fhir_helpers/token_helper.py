from os import environ
from typing import Optional, cast, Any, Dict

import requests
from requests.auth import HTTPBasicAuth


class TokenHelper:
    @staticmethod
    def get_oauth_token(
        *, client_id: str, client_secret: str, token_url: str, scope: Optional[str]
    ) -> Optional[str]:
        # Prepare the headers and body for the request
        headers = {"Content-Type": "application/x-www-form-urlencoded"}
        data = {"grant_type": "client_credentials"}
        if scope:
            data["scope"] = scope

        # Make the POST request to the token endpoint
        response = requests.post(
            token_url,
            headers=headers,
            data=data,
            auth=HTTPBasicAuth(client_id, client_secret),
        )

        # Check if the request was successful
        if response.status_code == 200:
            token_data = response.json()
            return cast(Optional[str], token_data.get("access_token"))
        else:
            raise Exception(
                f"Failed to get token: {response.status_code}, {response.text}"
            )

    @staticmethod
    def get_authorization_header(
        *, client_id: str, client_secret: str, token_url: str, scope: Optional[str]
    ) -> Dict[str, Any]:
        access_token: Optional[str] = TokenHelper.get_oauth_token(
            client_id=client_id,
            client_secret=client_secret,
            token_url=token_url,
            scope=scope,
        )
        assert access_token
        return {"Authorization": f"Bearer {access_token}"}

    @staticmethod
    def get_authorization_header_from_environment() -> Dict[str, Any]:
        auth_client_id = environ["FHIR_CLIENT_ID"]
        auth_client_secret = environ["FHIR_CLIENT_SECRET"]
        auth_well_known_url = environ["AUTH_CONFIGURATION_URI"]

        token_url: Optional[str] = TokenHelper.get_auth_server_url_from_well_known_url(
            well_known_url=auth_well_known_url
        )
        assert token_url
        return TokenHelper.get_authorization_header(
            client_id=auth_client_id,
            client_secret=auth_client_secret,
            token_url=token_url,
            scope=None,
        )

    @staticmethod
    def get_auth_server_url_from_well_known_url(
        *, well_known_url: str
    ) -> Optional[str]:
        try:
            well_known_response = requests.get(well_known_url)
            # Get token endpoint
            well_known_info = well_known_response.json()
            token_url: Optional[str] = well_known_info.get("token_endpoint")
            return token_url
        except Exception as e:
            return None
