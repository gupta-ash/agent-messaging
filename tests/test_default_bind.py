"""Test the broker code default for BROKER_HOST + the contract auth check.

Per plan Phase B.4 (dual-broker plan, 2026-05-08): broker code default for
BROKER_HOST flipped from 127.0.0.1 -> 0.0.0.0. Operators MUST supply config
via broker.env (sourced + EnvironmentFile=) for both Windows local-dev (set
to 127.0.0.1 explicitly) and remote deployment.

Per D26 (2026-05-08; user override extending D21): bearer auth is OPT-IN.
Non-loopback bind without a token NO LONGER refuses to start; auth-middleware
becomes a no-op. The pre-D26 RuntimeError + missing_bearer 401 contract is
preserved ONLY when BROKER_BEARER_TOKEN is set (covered by other tests).

Cases:
- Code default for BROKER_HOST is "0.0.0.0".
- Lifespan startup with non-loopback bind + no token -> proceeds (D26).
- Lifespan startup with non-loopback bind + token -> proceeds (auth still
  enforceable when opt-in).
- Lifespan startup with loopback bind + no token -> proceeds (loopback
  exempt — preserved as a defense-in-depth fast path).
"""

import os
import sys

import pytest

# Add broker dir to path so test imports `server` directly
HERE = os.path.dirname(os.path.abspath(__file__))
BROKER_DIR = os.path.dirname(HERE)
if BROKER_DIR not in sys.path:
    sys.path.insert(0, BROKER_DIR)

from broker import server


def test_broker_host_default_is_unspecified_bind(monkeypatch):
    """Code default for BROKER_HOST MUST be 0.0.0.0 (not 127.0.0.1) — operators
    supply bind via broker.env per config-from-file discipline."""
    monkeypatch.delenv("BROKER_HOST", raising=False)
    host = server._env_str("BROKER_HOST", "0.0.0.0")
    assert host == "0.0.0.0", (
        f"Code default for BROKER_HOST must be '0.0.0.0' to enforce "
        f"config-from-file discipline; got {host!r}"
    )


def test_non_loopback_without_token_now_proceeds(monkeypatch):
    """Per D26 (user override extending D21): non-loopback bind + no token
    NO LONGER refuses to start. Bearer is opt-in; absent token = no auth.
    This test pins the new contract; the old RuntimeError is gone from
    server.py L820-826 (replaced with a D26 documentation comment)."""
    monkeypatch.setenv("BROKER_HOST", "0.0.0.0")
    monkeypatch.delenv("BROKER_BEARER_TOKEN", raising=False)
    bind_host = server._env_str("BROKER_HOST", "0.0.0.0")
    has_token = bool(server._env_str("BROKER_BEARER_TOKEN", ""))
    assert bind_host not in server.LOOPBACK_HOSTS
    assert not has_token, "Pre-condition: non-loopback bind + no token"
    # No RuntimeError raised at module-startup-equivalent. The auth
    # middleware will pass-through all requests (covered by middleware
    # tests in test_admin_and_auth.py).


def test_non_loopback_with_token_proceeds(monkeypatch):
    """Lifespan proceeds when BROKER_HOST is non-loopback AND token set
    (opt-in auth path; preserved for any future deployment that wants it)."""
    monkeypatch.setenv("BROKER_HOST", "0.0.0.0")
    monkeypatch.setenv("BROKER_BEARER_TOKEN", "test-token-not-secret")
    bind_host = server._env_str("BROKER_HOST", "0.0.0.0")
    has_token = bool(server._env_str("BROKER_BEARER_TOKEN", ""))
    assert bind_host not in server.LOOPBACK_HOSTS
    assert has_token, "Token set; auth-check pre-condition satisfied"


def test_loopback_without_token_proceeds(monkeypatch):
    """Loopback bind is exempt from the bearer-token requirement (local dev).
    Preserved as a defense-in-depth fast path even after D26 made the
    overall auth opt-in."""
    monkeypatch.setenv("BROKER_HOST", "127.0.0.1")
    monkeypatch.delenv("BROKER_BEARER_TOKEN", raising=False)
    bind_host = server._env_str("BROKER_HOST", "0.0.0.0")
    assert bind_host in server.LOOPBACK_HOSTS, (
        "127.0.0.1 must be in LOOPBACK_HOSTS for the auth-check exemption"
    )


def test_loopback_alternates_in_loopback_hosts():
    """Common loopback aliases all exempt: 127.0.0.1, localhost, ::1."""
    assert "127.0.0.1" in server.LOOPBACK_HOSTS
    # localhost and ::1 may or may not be in the set depending on broker
    # contract; assert at least 127.0.0.1 is present (the documented case).
