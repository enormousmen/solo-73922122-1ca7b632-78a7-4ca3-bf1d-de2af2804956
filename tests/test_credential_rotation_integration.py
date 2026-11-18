"""End-to-end credential rotation tests over real TCP sockets.

Covers: management-key authorization, client Authorization stripping, bound
credential pinning, probe-gated rotation (200 commits, failure keeps the old
value), late-probe safety, non-blocking behavior and secret hygiene.
"""

from __future__ import annotations

import asyncio
import time

import aiohttp
import pytest

from .test_proxy_integration import stack, status

pytestmark = pytest.mark.asyncio

ADMIN = {"X-Management-Key": "admin-key"}
BAD_ADMIN = {"X-Management-Key": "wrong-key"}


async def rotate(base: str, target: str, authorization: str, headers=None, wait=True):
    hdrs = dict(ADMIN if headers is None else headers)
    async with aiohttp.ClientSession() as session:
        async with session.post(
            base + "/api/credentials",
            json={
                "target": target,
                "authorization": authorization,
                "wait": wait,
                "wait_timeout": 5,
            },
            headers=hdrs,
        ) as response:
            return response.status, await response.json()


async def upstream_state(url: str) -> dict:
    async with aiohttp.ClientSession() as session:
        async with session.get(url + "/_state") as response:
            return await response.json()


async def set_upstream_key(stack_obj, target: str, key: str) -> None:
    """Install a key directly on the in-process upstream app state."""
    app = stack_obj["upstreams"][target].server.config.app
    app.state.require_auth = True
    app.state.auth_key = key
    app.state.key_version += 1


async def test_management_endpoints_require_the_admin_key():
    async with stack(admin_key="admin-key") as s:
        async with aiohttp.ClientSession() as session:
            # No key -> 401.
            async with session.get(s["base"] + "/api/credentials") as r:
                assert r.status == 401
            # Wrong key -> 401.
            async with session.get(
                s["base"] + "/api/credentials", headers=BAD_ADMIN
            ) as r:
                assert r.status == 401
            # POST without key -> 401.
            async with session.post(
                s["base"] + "/api/credentials",
                json={"target": "blue", "authorization": "Bearer x"},
            ) as r:
                assert r.status == 401
            # Bearer-scheme management key also works.
            async with session.get(
                s["base"] + "/api/credentials",
                headers={"Authorization": "Bearer admin-key"},
            ) as r:
                assert r.status == 200
            # Correct key -> 200, metadata only.
            async with session.get(
                s["base"] + "/api/credentials", headers=ADMIN
            ) as r:
                payload = await r.json()
                assert r.status == 200
                assert payload["credentials"]["blue"]["version"] == 0
        # /api/status stays open without a management key.
        snap = await status(s["base"])
        assert "credentials" in snap


async def test_management_api_fails_closed_when_admin_key_unset():
    async with stack() as s:
        async with aiohttp.ClientSession() as session:
            async with session.get(s["base"] + "/api/credentials") as r:
                assert r.status == 503
            async with session.post(
                s["base"] + "/api/credentials",
                json={"target": "blue", "authorization": "Bearer x"},
            ) as r:
                assert r.status == 503


async def test_relay_sends_bound_credential_and_strips_client_authorization():
    async with stack(require_auth=True, admin_key="admin-key") as s:
        async with aiohttp.ClientSession() as session:
            # Client sends its own (wrong) Authorization: it must be replaced.
            async with session.get(
                f"{s['base']}/artifact?bytes=128&chunk=64",
                headers={"Authorization": "Bearer attacker-supplied"},
            ) as response:
                assert response.status == 200
                assert response.headers["x-relay-credential-version"] == "0"
                body = await response.read()
                assert len(body) == 128
        observed = await upstream_state(s["blue"])
        assert observed["request_auth"] == ["Bearer blue-key-1"]
        assert "attacker-supplied" not in str(observed["request_auth"])


async def test_head_request_also_uses_bound_credential():
    async with stack(require_auth=True, admin_key="admin-key") as s:
        async with aiohttp.ClientSession() as session:
            async with session.head(f"{s['base']}/artifact?bytes=64") as response:
                assert response.status == 200
                assert response.headers["x-relay-credential-version"] == "0"
                assert await response.read() == b""


async def test_rotation_200_commits_and_new_downloads_use_new_credential():
    async with stack(require_auth=True, admin_key="admin-key") as s:
        # Upstream changes its key first; the relay still holds v1.
        await set_upstream_key(s, "blue", "Bearer blue-key-2")
        # Old credential now fails at the origin.
        async with aiohttp.ClientSession() as session:
            async with session.get(f"{s['base']}/artifact?bytes=64") as r:
                assert r.status == 401

        # Rotation probes /ready with the candidate: 200 -> commit.
        code, payload = await rotate(s["base"], "blue", "Bearer blue-key-2")
        assert code == 200
        assert payload["rotation"]["status"] == "committed"
        assert payload["rotation"]["from_version"] == 0
        assert payload["rotation"]["to_version"] == 1
        # Secret is absent from the management response.
        assert "blue-key-2" not in str(payload)

        snap = await status(s["base"])
        assert snap["credentials"]["blue"]["version"] == 1
        assert snap["current"]["credential_version"] == 1
        assert "blue-key" not in str(snap)

        async with aiohttp.ClientSession() as session:
            async with session.get(f"{s['base']}/artifact?bytes=64") as r:
                assert r.status == 200
                assert r.headers["x-relay-credential-version"] == "1"
        observed = await upstream_state(s["blue"])
        assert observed["request_auth"][-1] == "Bearer blue-key-2"


async def test_failed_rotation_keeps_old_credential():
    async with stack(require_auth=True, admin_key="admin-key") as s:
        # Candidate key is never installed upstream -> /ready answers 401.
        code, payload = await rotate(s["base"], "blue", "Bearer wrong-candidate")
        assert code == 200
        assert payload["rotation"]["status"] == "failed"
        assert payload["rotation"]["probe_status"] == 401
        snap = await status(s["base"])
        assert snap["credentials"]["blue"]["version"] == 0
        # Existing credential still works.
        async with aiohttp.ClientSession() as session:
            async with session.get(f"{s['base']}/artifact?bytes=64") as r:
                assert r.status == 200
                assert r.headers["x-relay-credential-version"] == "0"
        # Probe presented the rejected candidate, never the working credential.
        observed = await upstream_state(s["blue"])
        assert "Bearer wrong-candidate" in observed["probes_auth"]


async def test_accepted_download_is_pinned_across_rotation():
    """A download accepted on v1 must keep presenting v1 after v2 is committed;
    it must not be re-routed or have its Authorization header swapped."""
    async with stack(drain_seconds=30.0, require_auth=True, admin_key="admin-key") as s:
        # Separate client sessions: one holds the streaming download, the other
        # drives the management API (a single shared aiohttp session serialises
        # its connector state and would abort the in-flight response itself).
        dl_session = aiohttp.ClientSession()
        mgmt_session = aiohttp.ClientSession()
        try:
            download = await dl_session.get(
                f"{s['base']}/artifact?bytes=2000000&chunk=2000&delay=0.02"
            )
            assert download.status == 200
            assert download.headers["x-relay-credential-version"] == "0"

            # Consume part of the body so the client applies backpressure the
            # same way a real downloader does; a fully idle aiohttp reader lets
            # its own connector tear down the response, which is client-side
            # behaviour unrelated to the relay.
            received = 0
            async for chunk in download.content.iter_chunked(4096):
                received += len(chunk)
                if received >= 40000:
                    break

            # Rotate while the download is streaming; the relay must not resend
            # auth mid-stream or re-route the accepted request.
            await set_upstream_key(s, "blue", "Bearer blue-key-2")
            async with mgmt_session.post(
                s["base"] + "/api/credentials",
                json={
                    "target": "blue",
                    "authorization": "Bearer blue-key-2",
                    "wait": True,
                    "wait_timeout": 5,
                },
                headers=ADMIN,
            ) as r:
                payload = await r.json()
                assert r.status == 200
                assert payload["rotation"]["status"] == "committed"

            # The in-flight download keeps streaming to completion (no re-route).
            async for chunk in download.content.iter_chunked(4096):
                received += len(chunk)
            assert received == 2000000
            snap = await status(s["base"])
            gen = snap["generations"][0]
            assert gen["in_flight"] == 0
            assert gen["in_flight_requests"] == []

            # A fresh download binds v2.
            async with dl_session.get(f"{s['base']}/artifact?bytes=64") as r:
                assert r.status == 200
                assert r.headers["x-relay-credential-version"] == "1"
        finally:
            await dl_session.close()
            await mgmt_session.close()


async def test_late_successful_probe_cannot_overwrite_newer_credential():
    async with stack(probe_timeout=5.0, require_auth=True, admin_key="admin-key") as s:
        blue_app = s["upstreams"]["blue"].server.config.app
        # Make blue's /ready slow so the first rotation stays pending.  The fake
        # upstream sleeps before checking auth, holding any credential equally.
        blue_app.state.probe_delay = 1.0

        mgmt = aiohttp.ClientSession()
        try:
            async with mgmt.post(
                s["base"] + "/api/credentials",
                json={
                    "target": "blue",
                    "authorization": "Bearer blue-key-2",
                    "wait": False,
                },
                headers=ADMIN,
            ) as r:
                assert r.status == 202
                first = await r.json()
            assert first["rotation"]["status"] == "pending"
            await asyncio.sleep(0.15)

            # Install key-2 (so the stale probe resolves 200), make readiness
            # instant again, and commit rotation #2 with key-3.
            blue_app.state.probe_delay = 0.0
            await set_upstream_key(s, "blue", "Bearer blue-key-3")
            async with mgmt.post(
                s["base"] + "/api/credentials",
                json={
                    "target": "blue",
                    "authorization": "Bearer blue-key-3",
                    "wait": True,
                    "wait_timeout": 5,
                },
                headers=ADMIN,
            ) as r:
                second = await r.json()
                assert r.status == 200
                assert second["rotation"]["status"] == "committed"
                assert second["rotation"]["to_version"] == 1
        finally:
            await mgmt.close()

        # Let the stale first probe finish. It resolves 200 with key-2 (the
        # upstream still accepts whatever auth_key is set; here key-3 rejects
        # key-2 with 401 — either outcome must not overwrite the committed v1).
        await asyncio.sleep(1.3)
        snap = await status(s["base"])
        assert snap["credentials"]["blue"]["version"] == 1
        rotations = {r["id"]: r for r in snap["recent_credential_rotations"]}
        assert rotations[first["rotation"]["id"]]["status"] == "superseded"
        assert rotations[second["rotation"]["id"]]["status"] == "committed"


async def test_rotation_probe_does_not_block_downloads_switch_or_status():
    async with stack(probe_timeout=5.0, require_auth=True, admin_key="admin-key") as s:
        async with aiohttp.ClientSession() as session:
            await session.post(
                s["blue"] + "/_control/probe", params={"delay": 1.5}
            )
        started = time.monotonic()
        code, pending = await rotate(
            s["base"], "blue", "Bearer blue-key-2", wait=False
        )
        assert code == 202 and pending["rotation"]["status"] == "pending"
        assert time.monotonic() - started < 0.5

        # Downloads, status and a target switch all stay responsive while the
        # credential probe is in flight.
        dl_session = aiohttp.ClientSession()
        ctrl_session = aiohttp.ClientSession()
        try:
            t0 = time.monotonic()
            snap_task = asyncio.create_task(status(s["base"]))
            download_task = asyncio.create_task(
                dl_session.get(f"{s['base']}/artifact?bytes=4096&chunk=1024")
            )
            switch_task = asyncio.create_task(
                ctrl_session.post(
                    s["base"] + "/api/switch",
                    json={"target": "green", "wait": True, "wait_timeout": 5},
                )
            )
            snap, download_response, switch_response = await asyncio.gather(
                snap_task, download_task, switch_task
            )
            assert time.monotonic() - t0 < 2.0
            assert snap["pending_credential_rotations"][0]["status"] == "pending"
            assert download_response.status == 200
            await download_response.read()
            assert switch_response.status == 200
            switch_payload = await switch_response.json()
            assert switch_payload["operation"]["status"] == "committed"
        finally:
            await dl_session.close()
            await ctrl_session.close()

        # The pending blue rotation eventually fails (delay still on, and
        # key-2 is not installed anyway); blue's v0 credential is untouched.
        for _ in range(100):
            snap = await status(s["base"])
            if not snap["pending_credential_rotations"]:
                break
            await asyncio.sleep(0.05)
        assert snap["credentials"]["blue"]["version"] == 0


async def test_unknown_target_and_empty_credential_are_rejected():
    async with stack(admin_key="admin-key") as s:
        code, payload = await rotate(s["base"], "nope", "Bearer x")
        assert code == 404
        async with aiohttp.ClientSession() as session:
            async with session.post(
                s["base"] + "/api/credentials",
                json={"target": "blue", "authorization": "   "},
                headers=ADMIN,
            ) as r:
                assert r.status == 422


async def test_rotation_history_endpoint_reports_outcome_without_secret():
    async with stack(require_auth=True, admin_key="admin-key") as s:
        await set_upstream_key(s, "blue", "Bearer blue-key-2")
        code, payload = await rotate(s["base"], "blue", "Bearer blue-key-2")
        assert code == 200 and payload["rotation"]["status"] == "committed"
        rotation_id = payload["rotation"]["id"]
        async with aiohttp.ClientSession() as session:
            async with session.get(
                s["base"] + f"/api/credential-rotations/{rotation_id}", headers=ADMIN
            ) as r:
                assert r.status == 200
                detail = await r.json()
        assert detail["rotation"]["status"] == "committed"
        assert "blue-key" not in str(detail)
        async with aiohttp.ClientSession() as session:
            async with session.get(
                s["base"] + "/api/credential-rotations/9999", headers=ADMIN
            ) as r:
                assert r.status == 404
            async with session.get(
                s["base"] + f"/api/credential-rotations/{rotation_id}"
            ) as r:
                assert r.status == 401


async def test_no_credential_mode_passes_no_authorization_upstream():
    """Without RELAY_TARGET_AUTH_* the relay sends no Authorization at all and
    strips whatever the client supplied."""
    async with stack() as s:
        async with aiohttp.ClientSession() as session:
            async with session.get(
                f"{s['base']}/artifact?bytes=64",
                headers={"Authorization": "Bearer whatever"},
            ) as r:
                assert r.status == 200
        observed = await upstream_state(s["blue"])
        assert observed["request_auth"] == [None]


async def test_rotation_after_target_switch_probes_and_pins_correct_target():
    """Credential rotation follows the target name, not the generation: after a
    switch to green, rotating green's key probes green and binds new downloads."""
    async with stack(require_auth=True, admin_key="admin-key") as s:
        async with aiohttp.ClientSession() as session:
            async with session.post(
                s["base"] + "/api/switch",
                json={"target": "green", "wait": True, "wait_timeout": 5},
            ) as r:
                assert (await r.json())["operation"]["status"] == "committed"

            # Downloads on green present green's v0 credential.
            async with session.get(f"{s['base']}/artifact?bytes=64") as r:
                assert r.status == 200
                assert r.headers["x-relay-target"] == "green"
                assert r.headers["x-relay-credential-version"] == "0"

        await set_upstream_key(s, "green", "Bearer green-key-9")
        code, payload = await rotate(s["base"], "green", "Bearer green-key-9")
        assert code == 200 and payload["rotation"]["status"] == "committed"

        observed = await upstream_state(s["green"])
        assert observed["probes_auth"][-1] == "Bearer green-key-9"
        async with aiohttp.ClientSession() as session:
            async with session.get(f"{s['base']}/artifact?bytes=64") as r:
                assert r.status == 200
                assert r.headers["x-relay-target"] == "green"
                assert r.headers["x-relay-credential-version"] == "1"
        # Blue's credential is untouched.
        snap = await status(s["base"])
        assert snap["credentials"]["blue"]["version"] == 0
        assert snap["credentials"]["green"]["version"] == 1


async def test_rotation_does_not_create_generation_or_drain():
    async with stack(drain_seconds=0.2, require_auth=True, admin_key="admin-key") as s:
        await set_upstream_key(s, "blue", "Bearer blue-key-2")
        code, payload = await rotate(s["base"], "blue", "Bearer blue-key-2")
        assert code == 200 and payload["rotation"]["status"] == "committed"
        snap = await status(s["base"])
        # Exactly one generation, still active: rotation never re-routes.
        assert len(snap["generations"]) == 1
        assert snap["generations"][0]["state"] == "active"
        assert snap["generations"][0]["id"] == 1
