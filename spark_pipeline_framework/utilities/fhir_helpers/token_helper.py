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

# Default for `allow_redirects`.  `requests` follows redirects by default, which
# makes the configured URL only the *first* hop, and both endpoints here are
# terminal resources.  Two of Aikido's other suggestions are deliberately not
# applied: blocking private IPs would fail closed on our own stack (the auth
# server is `http://keycloak:8080/...`), and a static domain allowlist is
# impossible for a library targeting per-customer hosts -- see
# `require_same_origin_token_endpoint` for the config-derived equivalent.
_FOLLOW_REDIRECTS = False


_DEFAULT_PORTS = {"http": 80, "https": 443}


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


def _origin(url: str) -> Optional[tuple[str, str, int]]:
    """(scheme, host, port) for `url`, or None if it is not an absolute HTTP URL.

    Ports are normalised so `https://h` and `https://h:443` compare equal.
    """
    parts = urlparse(url)
    scheme = parts.scheme.lower()
    if scheme not in _ALLOWED_URL_SCHEMES or not parts.hostname:
        return None
    return scheme, parts.hostname.lower(), parts.port or _DEFAULT_PORTS[scheme]


def _same_origin(a: str, b: str) -> bool:
    """True when both are absolute HTTP(S) URLs sharing scheme, host and port."""
    origin_a = _origin(a)
    return origin_a is not None and origin_a == _origin(b)


class TokenHelper:
    @staticmethod
    def get_oauth_token(
        *,
        client_id: str,
        client_secret: str,
        token_url: str,
        scope: Optional[str],
        timeout_seconds: float = DEFAULT_AUTH_TIMEOUT_SECONDS,
        allow_redirects: bool = _FOLLOW_REDIRECTS,
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

        # A 3xx surfaces below as a non-200 rather than being chased to a
        # destination the configuration never named.
        response = requests.post(
            token_url,
            headers=headers,
            data=data,
            auth=HTTPBasicAuth(client_id, client_secret),
            timeout=timeout_seconds,
            allow_redirects=allow_redirects,
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
        allow_redirects: bool = _FOLLOW_REDIRECTS,
    ) -> Dict[str, Any]:
        access_token: Optional[str] = TokenHelper.get_oauth_token(
            client_id=client_id,
            client_secret=client_secret,
            token_url=token_url,
            scope=scope,
            timeout_seconds=timeout_seconds,
            allow_redirects=allow_redirects,
        )
        assert access_token
        return {"Authorization": f"Bearer {access_token}"}

    @staticmethod
    def get_auth_server_url_from_well_known_url(
        *,
        well_known_url: str,
        timeout_seconds: float = DEFAULT_AUTH_TIMEOUT_SECONDS,
        allow_redirects: bool = _FOLLOW_REDIRECTS,
        require_same_origin_token_endpoint: bool = True,
    ) -> Optional[str]:
        """Resolve `token_endpoint` from an OIDC discovery document.

        Returns None on any failure (callers treat that as "not configured"),
        but always logs why -- otherwise a broken discovery URL is
        indistinguishable from an absent one.

        `require_same_origin_token_endpoint` is the SSRF control: the returned
        `token_endpoint` is handed to get_oauth_token(), which sends the client
        credentials to it, so whoever answers here would otherwise choose their
        destination. Pass False only if your provider genuinely serves its token
        endpoint from another origin -- RFC 8414 permits it, but it is unusual.
        """
        # get_logger() installs a handler as a side effect, so call it here
        # rather than at module scope; same as fhir_parse_bundles.py.
        logger = get_logger(__name__)
        try:
            _validate_url_scheme(well_known_url, parameter_name="well_known_url")
            well_known_response = requests.get(
                well_known_url,
                timeout=timeout_seconds,
                allow_redirects=allow_redirects,
            )
            if well_known_response.status_code != 200:
                # Checked explicitly: a 3xx *with* a JSON body would otherwise
                # parse fine and return None with nothing logged.
                redirect_hint = (
                    f" (unfollowed redirect to"
                    f" {well_known_response.headers.get('Location')!r})"
                    if well_known_response.is_redirect
                    else ""
                )
                logger.warning(
                    f"well_known_url {well_known_url!r} returned HTTP"
                    f" {well_known_response.status_code}{redirect_hint};"
                    " cannot resolve token_endpoint"
                )
                return None

            well_known_info = well_known_response.json()
            token_url: Optional[str] = well_known_info.get("token_endpoint")
            if not token_url:
                logger.warning(
                    f"well_known_url {well_known_url!r} returned a document with"
                    " no 'token_endpoint'"
                )
                return None

            if require_same_origin_token_endpoint and not _same_origin(
                well_known_url, token_url
            ):
                logger.warning(
                    f"Refusing token_endpoint {token_url!r} from well_known_url"
                    f" {well_known_url!r}: different origin. The client"
                    " credentials would be sent to a host the configuration did"
                    " not name. Pass"
                    " require_same_origin_token_endpoint=False if this provider"
                    " legitimately uses a separate origin."
                )
                return None

            return token_url
        except Exception as e:
            logger.warning(
                "Could not read token_endpoint from well_known_url"
                f" {well_known_url!r}: {type(e).__name__}: {e}"
            )
            return None
