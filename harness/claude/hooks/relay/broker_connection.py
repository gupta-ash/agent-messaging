"""Resolve the explicit environment or installed, nonsecret Claude broker URL."""

from __future__ import annotations

import json
import os
import re
from pathlib import Path
from typing import Any

_PORTABLE_BROKER_URL = "http://127.0.0.1:8787"
_INSTALLED_URL_RE = re.compile(r"http://127\.0\.0\.1:([1-9][0-9]{0,4})\Z")


def _unique_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    fields: dict[str, Any] = {}
    for key, value in pairs:
        if key in fields:
            raise ValueError("duplicate connection field")
        fields[key] = value
    return fields


def resolve_broker_base_url() -> str:
    """Environment wins; only an absent installed selection permits port 8787.

    Resolve from the invoking user's home, never a service/install directory or
    cwd. An existing unreadable or malformed selection is a configuration error,
    not permission to contact an older endpoint.
    """
    if "BROKER_BASE_URL" in os.environ:
        return os.environ["BROKER_BASE_URL"].rstrip("/")

    path = Path.home() / ".agents" / "installations" / "agent-messaging" / "connection.json"
    try:
        text = path.read_text(encoding="utf-8")
    except FileNotFoundError as exc:
        if path.is_symlink():
            raise RuntimeError(f"Invalid installed broker connection: {path}") from exc
        return _PORTABLE_BROKER_URL
    except (OSError, UnicodeError) as exc:
        raise RuntimeError(f"Cannot read installed broker connection: {path}") from exc

    try:
        fields = json.loads(text, object_pairs_hook=_unique_object)
        if not isinstance(fields, dict) or set(fields) != {"schema_version", "broker_url"}:
            raise ValueError("expected exactly schema_version and broker_url")
        if type(fields["schema_version"]) is not int or fields["schema_version"] != 1:
            raise ValueError("unsupported connection schema")
        url = fields["broker_url"]
        match = _INSTALLED_URL_RE.fullmatch(url) if isinstance(url, str) else None
        if match is None or int(match.group(1)) > 65535:
            raise ValueError("expected a canonical loopback HTTP URL with a valid port")
        return url
    except (ValueError, TypeError) as exc:
        raise RuntimeError(f"Invalid installed broker connection: {path}: {exc}") from exc
