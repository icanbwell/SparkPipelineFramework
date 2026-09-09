from typing import Optional, cast, Any, Dict
from urllib.parse import urlparse

import requests
from requests.auth import HTTPBasicAuth

from spark_pipeline_framework.logger.yarn_logger import get_logger

# Seconds to wait for an auth server before giving up.  Neither request below
# previously passed a timeout, so a hung auth server blocked the calling Spark
# task forever rather than failing it.
DEFAULT_AUTH_TIMEOUT_SECONDS = 30

# Only these URL schemes may be fetched.  `requests` will happily dispatch
# non-HTTP schemes to an installed adapter, so without this check a `token_url`
# that reaches this function from configuration could point somewhere that
# leaks the client credentials passed below.
_ALLOWED_URL_SCHEMES = frozenset({"http", "https"})

# Both requests below pass `allow_redirects=False`.  `requests` follows
# redirects by default, which means the configured auth-server URL is only the
# *first* hop: the server on the other end chooses the rest.  An OIDC discovery
# document and an OAuth2 token endpoint are both terminal resources, so a
# redirect here is never something we want to follow silently.
#
# What this does and does not buy, per call site:
#   * The discovery GET is the one that gains real protection.  `requests` does
#     nothing for it (no credentials are attached), and its response body picks
#     `token_endpoint`, which then receives the client credentials -- so whoever
#     answers that request chooses where the credentials go.
#   * The token POST gains no *credential* protection: `requests` already strips
#     `Authorization` on every cross-host redirect (Session.rebuild_auth), and
#     301/302/303 drop the body too.  What it gains is blocking a read
#     primitive -- `get_oauth_token` raises
#     f"Failed to get token: {status_code}, {response.text}", so a followed
#     redirect would copy the redirect target's body into the Spark logs.
#
# Deliberately NOT applied, because both would break this repo rather than
# secure it -- Aikido's generic remediation text suggests them, but:
#   * "block requests to private IP addresses" -- the auth server IS on a
#     private address in the environments this code runs in.  The test compose
#     stack points AUTH_CONFIGURATION_URI at `http://keycloak:8080/...`, which
#     resolves to an RFC1918 address, so this would fail closed on our own
#     stack.
#   * "only allow requests to allowlisted domains" -- this is a library whose
#     callers legitimately point it at per-customer FHIR/auth hosts, so there
#     is no allowlist this module could hardcode.  A caller that wants to
#     restrict destinations has to do it where the config is produced.
_FOLLOW_REDIRECTS = False


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

        # Make the POST request to the token endpoint.
        # `allow_redirects=False` matters most here: this is the one request
        # that carries the client credentials.  A 3xx from the configured host
        # surfaces below as a non-200 with its status and body, rather than
        # being chased to a destination the configuration never named.
        response = requests.post(
            token_url,
            headers=headers,
            data=data,
            auth=HTTPBasicAuth(client_id, client_secret),
            timeout=timeout_seconds,
            allow_redirects=_FOLLOW_REDIRECTS,
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
            well_known_response = requests.get(
                well_known_url,
                timeout=timeout_seconds,
                allow_redirects=_FOLLOW_REDIRECTS,
            )
            # Get token endpoint.  Note `token_endpoint` is read straight out of
            # this response body and is later handed to get_oauth_token(), which
            # sends the client credentials to it.  So the host answering here
            # effectively chooses where those credentials go -- which is why the
            # response must come from the configured URL itself and not from
            # wherever a redirect pointed.
            well_known_info = well_known_response.json()
            token_url: Optional[str] = well_known_info.get("token_endpoint")
            return token_url
        except Exception as e:
            # This deliberately stays non-fatal (callers treat None as "no
            # well-known endpoint configured"), but log it: without this, a
            # redirecting or unreachable discovery URL is indistinguishable
            # from one that is simply absent.
            #
            # Logger is fetched here rather than at module scope because
            # get_logger() installs a handler as a side effect; the repo does
            # the same in fhir_parse_bundles.py.
            get_logger(__name__).warning(
                "Could not read token_endpoint from well_known_url"
                f" {well_known_url!r}: {type(e).__name__}: {e}"
            )
            return None
