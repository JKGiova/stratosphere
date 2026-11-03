"""Frozen authored v1/v2 evidence remains readable after removing its producer."""

import hashlib
import json
import shutil
import tarfile
from pathlib import Path

import pytest

from stratosphere.development.service import StrategyDevelopment
from stratosphere.registry.workspace import WorkspaceDatabase
from stratosphere.research.store import ResearchError, ResearchStore
from stratosphere.sources.service import SourceService
from stratosphere.sources.store import SourceStore
from stratosphere.workflow.discovery import StrategyDiscovery

FIXTURES = Path(__file__).parent / "fixtures/development-history"


@pytest.fixture
def historical(tmp_path):
    with tarfile.open(FIXTURES / "campaigns.tar.gz") as archive:
        archive.extractall(tmp_path, filter="data")

    async def forbidden(*args, **kwargs):
        raise AssertionError("A historical reader attempted acquisition, inference or execution")

    discovery = StrategyDiscovery(
        SourceService(SourceStore(tmp_path / "sources"), online_check=forbidden)
    )
    discovery.store = ResearchStore(tmp_path / "intake")
    service = StrategyDevelopment(
        tmp_path / "development",
        discovery,
        WorkspaceDatabase(tmp_path / "library.sqlite3"),
        runner=forbidden,
        online_check=forbidden,
    )
    return service, json.loads((FIXTURES / "ids.json").read_text())


def evidence_bytes(root):
    return {
        str(p.relative_to(root)): hashlib.sha256(p.read_bytes()).hexdigest()
        for p in root.rglob("*")
        if p.is_file() and not p.name.endswith(".lock")
    }


@pytest.mark.parametrize("version", ["v1", "v2"])
async def test_frozen_reports_reopen_without_rewriting_or_executing(historical, version):
    service, ids = historical
    identity = ids[version]
    before = evidence_bytes(service.store.root)
    original = service.store.read(identity, "report.json")
    assert service.report(identity) == original
    assert "Native starts:" in service.show(identity)
    assert service.evidence_for(original["owner"])
    with pytest.raises(ResearchError, match="read-only"):
        await service.run(identity, owner=original["owner"])
    assert evidence_bytes(service.store.root) == before


def test_v2_action_chain_tampering_is_rejected(historical):
    service, ids = historical
    turn = service.history.turn_id(ids["v2"], 0)
    action = service.history.turns.read(turn, "action.json")
    path = service.history.turns.directory(turn) / "action.json"
    path.write_text(json.dumps(action | {"reason": "tampered"}))
    with pytest.raises(ResearchError, match="action changed"):
        service.report(ids["v2"])


def test_pending_native_reservation_stays_consumed_and_cannot_resume(historical):
    service, ids = historical
    identity = ids["v2"]
    reader = service.history
    # Simulate an interrupted second turn after native reservation, before its outcome receipt.
    shutil.rmtree(reader.turns.directory(reader.turn_id(identity, 2)))
    turn = reader.turns.directory(reader.turn_id(identity, 1))
    (turn / "outcome.json").unlink()
    (turn / "seal.json").unlink()
    service.store.event(identity, "paused")
    state = reader.state(identity)
    assert state["native_reserved"] == 2 and state["pending"]
    assert "Read-only checkpoint" in service.show(identity)
    assert not hasattr(reader, "run") and not hasattr(reader, "execute")
