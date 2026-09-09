"""Tests for TokenHelper's SSRF hardening.

These stand up real loopback HTTP servers rather than mocking `requests`.  The
behaviour under test *is* how `requests` treats redirects, so a mock that stubs
`requests.get` would assert nothing: it would pass whether or not
`allow_redirects=False` were actually passed through.  Each test that involves a
redirect asserts the redirect target recorded **zero** requests.
"""

import json
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer
from typing import Any, Dict, Generator, List, Optional, Tuple

import pytest

from spark_pipeline_framework.utilities.fhir_helpers.token_helper import (
    TokenHelper,
    _origin,
    _same_origin,
)

# Requests that reached the redirect target.  A non-empty list means a redirect
# was followed, i.e. the hardening regressed.
_SINK: List[Tuple[str, str, Optional[str]]] = []

# Where the auth server should send redirects.  Set once the sink server binds.
_SINK_BASE = ""

# What the discovery document should advertise as its token_endpoint.  Tests
# rewrite this to point on- or off-origin.
_TOKEN_ENDPOINT = ""


class _SinkHandler(BaseHTTPRequestHandler):
    """Stands in for an attacker-controlled redirect target."""

    def _record(self) -> None:
        _SINK.append((self.command, self.path, self.headers.get("Authorization")))
        body = b"{}"
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    do_GET = _record
    do_POST = _record

    def log_message(self, format: str, *args: Any) -> None:
        pass


class _AuthHandler(BaseHTTPRequestHandler):
    """A stand-in auth server with one route per scenario."""

    def _json(self, payload: Dict[str, Any], status: int = 200) -> None:
        body = json.dumps(payload).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _redirect(self, status: int = 302, with_body: bool = False) -> None:
        body = json.dumps({"token_endpoint": f"{_SINK_BASE}/token"}).encode()
        self.send_response(status)
        self.send_header("Location", f"{_SINK_BASE}/redirected")
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)) if with_body else "0")
        self.end_headers()
        if with_body:
            self.wfile.write(body)

    def do_GET(self) -> None:
        if self.path == "/.well-known":
            self._json({"token_endpoint": _TOKEN_ENDPOINT})
        elif self.path == "/.well-known-no-endpoint":
            self._json({"issuer": "somebody"})
        elif self.path == "/.well-known-redirect":
            self._redirect()
        elif self.path == "/.well-known-redirect-with-body":
            self._redirect(with_body=True)
        elif self.path == "/.well-known-500":
            self._json({"error": "boom"}, status=500)
        else:
            self._json({"error": "not found"}, status=404)

    def do_POST(self) -> None:
        if self.path == "/token":
            self._json({"access_token": "a-token"})
        elif self.path == "/token-redirect":
            self._redirect(status=307)
        else:
            self._json({"error": "not found"}, status=404)

    def log_message(self, format: str, *args: Any) -> None:
        pass


@pytest.fixture(scope="module")
def servers() -> Generator[Tuple[str, str], None, None]:
    """Yield (auth_base, sink_base) for two loopback servers."""
    global _SINK_BASE
    auth = HTTPServer(("127.0.0.1", 0), _AuthHandler)
    sink = HTTPServer(("127.0.0.1", 0), _SinkHandler)
    for server in (auth, sink):
        threading.Thread(target=server.serve_forever, daemon=True).start()
    auth_base = f"http://127.0.0.1:{auth.server_port}"
    _SINK_BASE = f"http://127.0.0.1:{sink.server_port}"
    try:
        yield auth_base, _SINK_BASE
    finally:
        auth.shutdown()
        sink.shutdown()


@pytest.fixture(autouse=True)
def reset_sink() -> Generator[None, None, None]:
    _SINK.clear()
    yield


# --------------------------------------------------------------------------
# origin helpers
# --------------------------------------------------------------------------


def test_origin_normalises_default_ports() -> None:
    assert _origin("https://host/path") == ("https", "host", 443)
    assert _origin("https://host:443/other") == ("https", "host", 443)
    assert _origin("http://host") == ("http", "host", 80)


def test_origin_rejects_non_http_and_relative() -> None:
    assert _origin("file:///etc/passwd") is None
    assert _origin("/relative/path") is None
    assert _origin("not a url") is None


def test_same_origin() -> None:
    assert _same_origin("https://h/a", "https://h:443/b") is True
    assert _same_origin("http://h/a", "https://h/a") is False  # scheme
    assert _same_origin("https://h/a", "https://other/a") is False  # host
    assert _same_origin("https://h/a", "https://h:8443/a") is False  # port
    assert _same_origin("https://h/a", "/relative") is False
    assert _same_origin("/relative", "https://h/a") is False


# --------------------------------------------------------------------------
# discovery: token_endpoint resolution
# --------------------------------------------------------------------------


def test_resolves_same_origin_token_endpoint(servers: Tuple[str, str]) -> None:
    global _TOKEN_ENDPOINT
    auth_base, _ = servers
    _TOKEN_ENDPOINT = f"{auth_base}/token"
    assert (
        TokenHelper.get_auth_server_url_from_well_known_url(
            well_known_url=f"{auth_base}/.well-known", timeout_seconds=5
        )
        == _TOKEN_ENDPOINT
    )


def test_rejects_cross_origin_token_endpoint(servers: Tuple[str, str]) -> None:
    """The core SSRF control: the credentials must not follow the response body."""
    global _TOKEN_ENDPOINT
    auth_base, sink_base = servers
    _TOKEN_ENDPOINT = f"{sink_base}/token"
    assert (
        TokenHelper.get_auth_server_url_from_well_known_url(
            well_known_url=f"{auth_base}/.well-known", timeout_seconds=5
        )
        is None
    )


def test_cross_origin_token_endpoint_allowed_when_opted_out(
    servers: Tuple[str, str],
) -> None:
    global _TOKEN_ENDPOINT
    auth_base, sink_base = servers
    _TOKEN_ENDPOINT = f"{sink_base}/token"
    assert (
        TokenHelper.get_auth_server_url_from_well_known_url(
            well_known_url=f"{auth_base}/.well-known",
            timeout_seconds=5,
            require_same_origin_token_endpoint=False,
        )
        == _TOKEN_ENDPOINT
    )


def test_discovery_redirect_is_not_followed(servers: Tuple[str, str]) -> None:
    auth_base, _ = servers
    assert (
        TokenHelper.get_auth_server_url_from_well_known_url(
            well_known_url=f"{auth_base}/.well-known-redirect", timeout_seconds=5
        )
        is None
    )
    assert _SINK == [], f"redirect was followed: {_SINK}"


def test_discovery_redirect_with_json_body_is_not_followed(
    servers: Tuple[str, str],
) -> None:
    """A 3xx carrying a JSON body used to parse fine and return None silently."""
    auth_base, _ = servers
    assert (
        TokenHelper.get_auth_server_url_from_well_known_url(
            well_known_url=f"{auth_base}/.well-known-redirect-with-body",
            timeout_seconds=5,
        )
        is None
    )
    assert _SINK == [], f"redirect was followed: {_SINK}"


def test_discovery_missing_token_endpoint(servers: Tuple[str, str]) -> None:
    auth_base, _ = servers
    assert (
        TokenHelper.get_auth_server_url_from_well_known_url(
            well_known_url=f"{auth_base}/.well-known-no-endpoint", timeout_seconds=5
        )
        is None
    )


def test_discovery_server_error(servers: Tuple[str, str]) -> None:
    auth_base, _ = servers
    assert (
        TokenHelper.get_auth_server_url_from_well_known_url(
            well_known_url=f"{auth_base}/.well-known-500", timeout_seconds=5
        )
        is None
    )


def test_discovery_rejects_non_http_scheme() -> None:
    """The scheme guard raises, and this method swallows it into None."""
    assert (
        TokenHelper.get_auth_server_url_from_well_known_url(
            well_known_url="file:///etc/passwd", timeout_seconds=5
        )
        is None
    )


# --------------------------------------------------------------------------
# token endpoint
# --------------------------------------------------------------------------


def test_get_oauth_token(servers: Tuple[str, str]) -> None:
    auth_base, _ = servers
    assert (
        TokenHelper.get_oauth_token(
            client_id="id",
            client_secret="secret",
            token_url=f"{auth_base}/token",
            scope=None,
            timeout_seconds=5,
        )
        == "a-token"
    )


def test_token_redirect_is_not_followed(servers: Tuple[str, str]) -> None:
    """307 preserves method and body, so this is the leakiest redirect status."""
    auth_base, _ = servers
    with pytest.raises(Exception, match="Failed to get token: 307"):
        TokenHelper.get_oauth_token(
            client_id="id",
            client_secret="secret",
            token_url=f"{auth_base}/token-redirect",
            scope=None,
            timeout_seconds=5,
        )
    assert _SINK == [], f"credentialed request followed a redirect: {_SINK}"


def test_token_redirect_followed_when_opted_in(servers: Tuple[str, str]) -> None:
    """The escape hatch works -- and shows why it defaults to off."""
    auth_base, _ = servers
    TokenHelper.get_oauth_token(
        client_id="id",
        client_secret="secret",
        token_url=f"{auth_base}/token-redirect",
        scope=None,
        timeout_seconds=5,
        allow_redirects=True,
    )
    assert len(_SINK) == 1, f"expected the redirect to be followed, got {_SINK}"
    # requests strips Authorization across origins even when following.
    assert _SINK[0][2] is None


def test_token_rejects_non_http_scheme() -> None:
    with pytest.raises(ValueError, match="must be an http or https URL"):
        TokenHelper.get_oauth_token(
            client_id="id",
            client_secret="secret",
            token_url="file:///etc/passwd",
            scope=None,
        )


def test_get_authorization_header(servers: Tuple[str, str]) -> None:
    auth_base, _ = servers
    assert TokenHelper.get_authorization_header(
        client_id="id",
        client_secret="secret",
        token_url=f"{auth_base}/token",
        scope=None,
        timeout_seconds=5,
    ) == {"Authorization": "Bearer a-token"}
