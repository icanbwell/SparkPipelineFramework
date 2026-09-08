from typing import Optional, cast, Any, Dict
from urllib.parse import urlparse

import requests
from requests.auth import HTTPBasicAuth

# Seconds to wait for an auth server before giving up.  Neither request below
# previously passed a timeout, so a hung auth server blocked the calling Spark
# task forever rather than failing it.
DEFAULT_AUTH_TIMEOUT_SECONDS = 30

# Only these URL schemes may be fetched.  `requests` will happily dispatch
# non-HTTP schemes to an installed adapter, so without this check a `token_url`
# that reaches this function from configuration could point somewhere that
# leaks the client credentials passed below.
_ALLOWED_URL_SCHEMES = frozenset({"http", "https"})


def _validate_url_scheme(url: str, *, parameter_name: str) -> None:
    """Reject URLs that are not plain HTTP(S).

    `http` is deliberately still permitted: local development and the
    SparkPipelineFramework.Testing mock FHIR server both use plain HTTP.
    """
    scheme = urlparse(url).scheme.lower()
    if scheme not in _ALLOWED_URL_SCHEMES:
        raise ValueError(
            f"{parameter_name} must be an http or https URL, got scheme"
            f" {scheme!r}. Refusing to send a request to {parameter_name}."
        )


class TokenHelper:
    @staticmethod
    def get_oauth_token(
        *,
        client_id: str,
        client_secret: str,
        token_url: str,
        scope: Optional[str],
        timeout_seconds: float = DEFAULT_AUTH_TIMEOUT_SECONDS,
    ) -> Optional[str]:
        # `token_url` arrives from configuration.  Validate it before attaching
        # the client credentials, so a malformed or hostile value cannot cause
        # them to be sent somewhere unintended.
        _validate_url_scheme(token_url, parameter_name="token_url")

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
            timeout=timeout_seconds,
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
        *,
        client_id: str,
        client_secret: str,
        token_url: str,
        scope: Optional[str],
        timeout_seconds: float = DEFAULT_AUTH_TIMEOUT_SECONDS,
    ) -> Dict[str, Any]:
        access_token: Optional[str] = TokenHelper.get_oauth_token(
            client_id=client_id,
            client_secret=client_secret,
            token_url=token_url,
            scope=scope,
            timeout_seconds=timeout_seconds,
        )
        assert access_token
        return {"Authorization": f"Bearer {access_token}"}

    @staticmethod
    def get_auth_server_url_from_well_known_url(
        *,
        well_known_url: str,
        timeout_seconds: float = DEFAULT_AUTH_TIMEOUT_SECONDS,
    ) -> Optional[str]:
        try:
            _validate_url_scheme(well_known_url, parameter_name="well_known_url")
            well_known_response = requests.get(well_known_url, timeout=timeout_seconds)
            # Get token endpoint
            well_known_info = well_known_response.json()
            token_url: Optional[str] = well_known_info.get("token_endpoint")
            return token_url
        except Exception:
            return None
