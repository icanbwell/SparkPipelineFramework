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
#   * "only allow requests to allowlisted domains" -- not as a *static* list:
#     this is a library whose callers legitimately point it at per-customer
#     FHIR/auth hosts, so there is no allowlist this module could hardcode.
#     What it does instead is derive the constraint from the configuration --
#     see `require_same_origin_token_endpoint`, which pins the discovered
#     token_endpoint to the origin of the discovery URL the caller supplied.
#     That is the destination restriction that actually matters here, because
#     token_endpoint is what receives the client credentials.
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

        Returns None on any failure -- callers treat that as "no well-known
        endpoint configured" -- but every such path now logs why, which is the
        only way to tell an absent discovery URL from a broken one.

        `require_same_origin_token_endpoint` is the real SSRF control here.  The
        returned `token_endpoint` is handed to get_oauth_token(), which sends
        the client credentials to it, so whoever answers this request otherwise
        chooses where those credentials go.  Requiring it to share the discovery
        URL's origin bounds that to the host the configuration actually named.
        Pass False only if your provider legitimately serves its token endpoint
        from another origin; RFC 8414 permits it, though it is unusual.
        """
        # Logger is fetched inside the function rather than at module scope
        # because get_logger() installs a handler as a side effect; the repo
        # does the same in fhir_parse_bundles.py.
        logger = get_logger(__name__)
        try:
            _validate_url_scheme(well_known_url, parameter_name="well_known_url")
            well_known_response = requests.get(
                well_known_url,
                timeout=timeout_seconds,
                allow_redirects=allow_redirects,
            )
            if well_known_response.status_code != 200:
                # Named explicitly rather than left to .json() to fail, because
                # a 3xx *with* a JSON body would otherwise parse fine, yield no
                # token_endpoint and return None with nothing logged.
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
