"""Credential rotation state-machine tests without real network I/O."""

import asyncio

import pytest

from relay.config import load_config
from relay.generations import RelayState


class FakeResponse:
    def __init__(self, status: int):
        self.status = status

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return False


class FakeProbeSession:
    """Programmable probe session keyed by (target, presented authorization).

    Records every probe's URL and headers so tests can assert which credential
    was presented.  ``hook`` is an async callable returning (status, delay).
    """

    def __init__(self):
        self.calls = []
        self.hook = None

    def get(self, url, *, timeout, allow_redirects, headers=None):
        record = {"url": url, "headers": dict(headers or {})}
        self.calls.append(record)

        class Ctx:
            async def __aenter__(inner):
                target = url.split("//")[1].split(".")[0]
                auth = (headers or {}).get("Authorization")
                status, delay = await self.hook(target, auth)
                if delay:
                    await asyncio.sleep(delay)
                return FakeResponse(status)

            async def __aexit__(inner, *exc):
                return False

        return Ctx()


def make_state(**env):
    base = {
        "RELAY_TARGETS": "a=http://a.local,b=http://b.local",
        "RELAY_DRAIN_SECONDS": "0.3",
        "RELAY_TARGET_AUTH_a": "Bearer alpha-1",
        "RELAY_TARGET_AUTH_b": "Bearer beta-1",
        "RELAY_ADMIN_KEY": "admin-secret",
    }
    base.update(env)
    cfg = load_config(base)
    state = RelayState(cfg)
    state.probe_session = FakeProbeSession()
    return state


@pytest.mark.asyncio
async def test_initial_credentials_come_from_config():
    state = make_state()
    assert state.credentials["a"].version == 0
    assert state.credentials["a"].auth_header == "Bearer alpha-1"
    # Snapshot must never contain the secret value.
    snap = state.snapshot()
    dumped = repr(snap)
    assert "alpha-1" not in dumped and "beta-1" not in dumped
    assert snap["credentials"]["a"] == {"version": 0, "configured": True, "rotated_at": None}


@pytest.mark.asyncio
async def test_successful_rotation_commits_new_version():
    state = make_state()
    state.probe_session.hook = lambda t, auth: asyncio.sleep(0, result=(200, 0))
    rotation = await state.begin_rotation("a", "Bearer alpha-2")
    await asyncio.wait_for(rotation.changed.wait(), 2)
    assert rotation.status == "committed"
    assert rotation.from_version == 0 and rotation.to_version == 1
    entry = state.credentials["a"]
    assert entry.version == 1 and entry.auth_header == "Bearer alpha-2"
    assert entry.rotated_at is not None
    # The probe presented exactly the candidate credential.
    assert state.probe_session.calls[-1]["headers"] == {"Authorization": "Bearer alpha-2"}
    # No generation is created and traffic is untouched by a credential rotation.
    assert [g.id for g in state.generations] == [1]
    assert state.current.target_name == "a"


@pytest.mark.asyncio
async def test_failed_probe_keeps_old_credential():
    state = make_state()

    async def hook(target, auth):
        return 401 if auth == "Bearer alpha-2" else 200, 0

    state.probe_session.hook = hook
    rotation = await state.begin_rotation("a", "Bearer alpha-2")
    await asyncio.wait_for(rotation.changed.wait(), 2)
    assert rotation.status == "failed"
    assert rotation.probe_status == 401
    entry = state.credentials["a"]
    assert entry.version == 0 and entry.auth_header == "Bearer alpha-1"
    assert state.pending_rotations == {}


@pytest.mark.asyncio
async def test_probe_exception_keeps_old_credential():
    state = make_state()

    async def hook(target, auth):
        raise TimeoutError("probe timed out")

    state.probe_session.hook = hook
    rotation = await state.begin_rotation("b", "Bearer beta-2")
    await asyncio.wait_for(rotation.changed.wait(), 2)
    assert rotation.status == "failed"
    assert "TimeoutError" in rotation.reason
    assert state.credentials["b"].auth_header == "Bearer beta-1"


@pytest.mark.asyncio
async def test_newer_rotation_supersedes_unfinished_one():
    state = make_state()

    async def hook(target, auth):
        # First candidate is slow; the second one answers immediately.
        return (200, 0.3 if auth == "Bearer alpha-2" else 0)

    state.probe_session.hook = hook
    first = await state.begin_rotation("a", "Bearer alpha-2")
    await asyncio.sleep(0.05)
    second = await state.begin_rotation("a", "Bearer alpha-3")
    await asyncio.wait_for(second.changed.wait(), 2)
    assert first.status == "superseded"
    assert first.superseded_by == second.id
    assert second.status == "committed" and second.to_version == 1
    assert state.credentials["a"].auth_header == "Bearer alpha-3"
    # Give the stale probe task time to resolve and prove it does nothing.
    await asyncio.sleep(0.4)
    assert state.credentials["a"].version == 1
    assert state.credentials["a"].auth_header == "Bearer alpha-3"


@pytest.mark.asyncio
async def test_late_successful_probe_after_commit_cannot_overwrite():
    """The headline race: rotation #1's probe returns 200 only after rotation #2
    has committed.  The stale 200 must not revert the credential."""
    state = make_state()
    gate = asyncio.Event()

    async def hook(target, auth):
        if auth == "Bearer alpha-2":
            await gate.wait()
        return 200, 0

    state.probe_session.hook = hook
    first = await state.begin_rotation("a", "Bearer alpha-2")
    await asyncio.sleep(0.05)
    second = await state.begin_rotation("a", "Bearer alpha-3")
    await asyncio.wait_for(second.changed.wait(), 2)
    assert state.credentials["a"].auth_header == "Bearer alpha-3"
    assert first.status == "superseded"
    gate.set()  # stale probe now resolves 200 with the old candidate
    await asyncio.sleep(0.2)
    assert state.credentials["a"].version == 1
    assert state.credentials["a"].auth_header == "Bearer alpha-3"
    assert first.status == "superseded"


@pytest.mark.asyncio
async def test_rotation_is_per_target_and_does_not_touch_other_target():
    state = make_state()
    state.probe_session.hook = lambda t, auth: asyncio.sleep(0, result=(200, 0))
    rotation = await state.begin_rotation("b", "Bearer beta-9")
    await asyncio.wait_for(rotation.changed.wait(), 2)
    assert state.credentials["b"].auth_header == "Bearer beta-9"
    assert state.credentials["a"].auth_header == "Bearer alpha-1"
    # Pending slots are independent.
    assert state.pending_rotations == {}


@pytest.mark.asyncio
async def test_bound_credential_reflects_accept_time_only():
    state = make_state()
    before = state.bound_credential("a")
    assert before.version == 0 and before.auth_header == "Bearer alpha-1"
    state.probe_session.hook = lambda t, auth: asyncio.sleep(0, result=(200, 0))
    rotation = await state.begin_rotation("a", "Bearer alpha-2")
    await asyncio.wait_for(rotation.changed.wait(), 2)
    after = state.bound_credential("a")
    assert after.version == 1 and after.auth_header == "Bearer alpha-2"
    # The earlier bound object is immutable: accepted downloads keep old creds.
    assert before.version == 0 and before.auth_header == "Bearer alpha-1"


@pytest.mark.asyncio
async def test_rotation_does_not_block_status_snapshot():
    state = make_state()

    async def slow_hook(target, auth):
        return 200, 1.0

    state.probe_session.hook = slow_hook
    await state.begin_rotation("a", "Bearer alpha-2")
    snap = state.snapshot()  # synchronous, must not await
    pending = snap["pending_credential_rotations"]
    assert len(pending) == 1 and pending[0]["target"] == "a"
    assert "alpha-2" not in repr(snap)
    await asyncio.sleep(1.2)
    assert state.credentials["a"].version == 1


@pytest.mark.asyncio
async def test_switch_probe_uses_current_target_credential():
    state = make_state()
    state.probe_session.hook = lambda t, auth: asyncio.sleep(0, result=(200, 0))
    op = await state.begin_switch("b")
    await asyncio.wait_for(op.changed.wait(), 2)
    assert op.status == "committed"
    call = state.probe_session.calls[-1]
    assert call["url"].endswith("/ready")
    assert call["headers"] == {"Authorization": "Bearer beta-1"}


@pytest.mark.asyncio
async def test_no_credential_configured_means_no_auth_header_on_probe():
    cfg = load_config(
        {
            "RELAY_TARGETS": "a=http://a.local,b=http://b.local",
        }
    )
    state = RelayState(cfg)
    state.probe_session = FakeProbeSession()
    state.probe_session.hook = lambda t, auth: asyncio.sleep(0, result=(200, 0))
    op = await state.begin_switch("b")
    await asyncio.wait_for(op.changed.wait(), 2)
    assert state.probe_session.calls[-1]["headers"] == {}
    rotation = await state.begin_rotation("a", None)
    await asyncio.wait_for(rotation.changed.wait(), 2)
    assert rotation.status == "committed"
    assert state.credentials["a"].configured is False


@pytest.mark.asyncio
async def test_aclose_cancels_pending_rotation():
    state = make_state()
    gate = asyncio.Event()

    async def hook(target, auth):
        await gate.wait()
        return 200, 0

    state.probe_session.hook = hook
    rotation = await state.begin_rotation("a", "Bearer alpha-2")
    await asyncio.sleep(0.05)
    await state.aclose()
    gate.set()
    await asyncio.sleep(0.1)
    # Cancelled before committing: old credential remains.
    assert state.credentials["a"].auth_header == "Bearer alpha-1"
    assert rotation.status == "pending"
