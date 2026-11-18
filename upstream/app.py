"""Demo artifact upstream with controllable readiness, auth and slow downloads.

Run two copies (blue/green).  Every response identifies itself via
``X-Upstream-Name`` so the relay demo makes routing visible.

Origin credentials: when ``UPSTREAM_REQUIRE_AUTH=1`` (the compose default) both
the readiness endpoint (``/ready``) and downloads (``/artifact``) require an
``Authorization`` header matching the currently configured key.  The key can be
rotated at runtime through ``/_control/key`` so the relay's credential rotation
API can be demonstrated end to end.  Key material is never returned by any
endpoint; only a short fingerprint is exposed.
"""

from __future__ import annotations

import asyncio
import hashlib
import hmac
import os
import time
from typing import AsyncIterator

from fastapi import Depends, FastAPI, Header, HTTPException, Request
from fastapi.responses import JSONResponse, StreamingResponse

NAME = os.environ.get("UPSTREAM_NAME", "upstream")
PROBE_DEFAULT_DELAY = float(os.environ.get("UPSTREAM_PROBE_DELAY", "0"))
PROBE_DEFAULT_STATUS = int(os.environ.get("UPSTREAM_PROBE_STATUS", "200"))
REQUIRE_AUTH = os.environ.get("UPSTREAM_REQUIRE_AUTH", "0") == "1"
# Initial key.  Empty means "auth enabled but no key set" -> every request is
# rejected until a key is installed via the control endpoint.
CURRENT_KEY = os.environ.get("UPSTREAM_AUTH_KEY", "")
MAX_PROBE_DELAY = 120.0

app = FastAPI(title=f"Artifact upstream {NAME}")
state = {
    "probe_delay": PROBE_DEFAULT_DELAY,
    "probe_status": PROBE_DEFAULT_STATUS,
    "require_auth": REQUIRE_AUTH,
    "auth_key": CURRENT_KEY,
    "key_version": 0,
    # Last 64 accepted/rejected auth attempts, for demo observability.
    "auth_events": [],
}


def _fingerprint(key: str) -> str:
    """Non-reversible short digest so operators can compare keys safely."""
    return hashlib.sha256(key.encode()).hexdigest()[:10] if key else "none"


def require_origin_key(authorization: str | None = Header(default=None)) -> None:
    """Enforce the origin credential on /ready and downloads."""
    if not state["require_auth"]:
        return
    accepted = bool(
        authorization
        and state["auth_key"]
        and hmac.compare_digest(authorization, state["auth_key"])
    )
    event = {
        "at": time.time(),
        "result": "accepted" if accepted else "rejected",
        "presented_present": authorization is not None,
        "key_version": state["key_version"],
    }
    state["auth_events"].append(event)
    del state["auth_events"][:-64]
    if not accepted:
        # Never echo the presented value back.
        raise HTTPException(status_code=401, detail="invalid or missing origin credential")


@app.get("/ready", dependencies=[Depends(require_origin_key)])
async def ready():
    """Readiness endpoint (the relay's default probe path)."""
    return await _readiness()


@app.get("/healthz")
async def healthz():
    """Liveness alias. Intentionally unauthenticated for container healthchecks;
    the credential probe path used by the relay is /ready."""
    return await _readiness()


async def _readiness():
    """Readiness endpoint. A long delay mimics a slow control plane."""
    delay = state["probe_delay"]
    started = time.monotonic()
    if delay:
        await asyncio.sleep(delay)
    status = state["probe_status"]
    return JSONResponse(
        {"upstream": NAME, "status": "ok" if status == 200 else "not_ready"},
        status_code=status,
        headers={"x-upstream": NAME, "x-probe-delay": str(round(time.monotonic() - started, 3))},
    )


@app.post("/_control/probe")
async def control_probe(delay: float = 0.0, status: int = 200):
    """Set readiness latency/status, e.g. POST /_control/probe?delay=4&status=503."""
    if delay < 0 or delay > MAX_PROBE_DELAY:
        return JSONResponse({"error": "delay out of range"}, status_code=400)
    state["probe_delay"] = delay
    state["probe_status"] = status
    return {"upstream": NAME, "probe_delay": delay, "probe_status": status}


@app.get("/_control/probe")
async def control_probe_get():
    # Never include the key itself in any response.
    return {
        "upstream": NAME,
        "probe_delay": state["probe_delay"],
        "probe_status": state["probe_status"],
        "require_auth": state["require_auth"],
        "key_version": state["key_version"],
        "key_fingerprint": _fingerprint(state["auth_key"]),
    }


@app.post("/_control/key")
async def control_key(key: str):
    """Install a new origin key at runtime (the "ops rotates the secret" step).

    The new key takes effect immediately for subsequent requests; in-flight
    downloads are not re-authenticated.  Pass an empty string to lock the
    service (every credentialed request then fails until a new key is set).
    """
    state["auth_key"] = key
    state["key_version"] += 1
    state["auth_events"].clear()
    return {
        "upstream": NAME,
        "key_version": state["key_version"],
        "key_fingerprint": _fingerprint(key),
    }


@app.get("/_control/key")
async def control_key_get():
    """Report key metadata only — the secret itself is never returned."""
    return {
        "upstream": NAME,
        "require_auth": state["require_auth"],
        "key_version": state["key_version"],
        "key_fingerprint": _fingerprint(state["auth_key"]),
        "auth_events": state["auth_events"],
    }


@app.get("/artifact", dependencies=[Depends(require_origin_key)])
async def artifact(
    request: Request,
    bytes: int = 1024 * 1024,
    chunk: int = 65536,
    delay: float = 0.0,
):
    """Stream ``bytes`` bytes with ``delay`` seconds between chunks."""
    bytes = max(0, min(bytes, 512 * 1024 * 1024))
    chunk = max(1, min(chunk, 1024 * 1024))
    delay = max(0.0, min(delay, 10.0))

    def generate() -> AsyncIterator[bytes]:
        async def gen() -> AsyncIterator[bytes]:
            remaining = bytes
            seq = 0
            while remaining > 0:
                size = min(chunk, remaining)
                seed = f"{NAME}:{seq}:".encode()
                block = (seed * (size // len(seed) + 1))[:size]
                yield block
                remaining -= size
                seq += 1
                if delay:
                    await asyncio.sleep(delay)

        return gen()

    headers = {
        "x-upstream": NAME,
        "x-artifact-bytes": str(bytes),
        "x-artifact-delay": str(delay),
        "x-upstream-key-version": str(state["key_version"]),
        # No Content-Length: exercises chunk-by-chunk relaying.
    }
    return StreamingResponse(generate(), media_type="application/octet-stream", headers=headers)


@app.get("/")
async def index():
    return {
        "upstream": NAME,
        "probe_delay": state["probe_delay"],
        "probe_status": state["probe_status"],
        "require_auth": state["require_auth"],
        "key_version": state["key_version"],
        "key_fingerprint": _fingerprint(state["auth_key"]),
    }
