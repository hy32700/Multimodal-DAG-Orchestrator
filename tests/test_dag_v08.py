import pytest
from pathlib import Path
from core.orchestrator_v08 import (
    BudgetGuard,
    DAGNodeV08,
    DiskBackedArtifactStore,
    MultimodalDAGOrchestratorV08,
    PooledMultiProtocolConnector,
    SecretSanitizer,
    SemanticDAGCache,
)
from core.skill_registry_v08 import VersionedSkillRegistry


def test_cycle_detection():
    nodes = {
        "a": DAGNodeV08("a", "A", "planning", "p", deps=["b"]),
        "b": DAGNodeV08("b", "B", "planning", "p", deps=["a"]),
    }
    orch = MultimodalDAGOrchestratorV08(
        "t1", nodes, PooledMultiProtocolConnector(), lambda c: {},
        DiskBackedArtifactStore(Path("./workspaces"), "t1"),
        BudgetGuard(), SemanticDAGCache(Path("./workspaces/test_cache.json"))
    )
    with pytest.raises(ValueError, match="Cycle detected"):
        orch.validate_acyclic()


def test_secret_sanitizer():
    raw = "Error calling https://api.example.com with sk-1234567890abcdef"
    masked = SecretSanitizer.mask_text(raw)
    assert "1234567890" not in masked
    assert "sk-123****cdef" in masked


def test_skill_version_rollback(tmp_path: Path):
    reg = VersionedSkillRegistry(tmp_path)
    v1 = reg.publish_version("video_pipeline", {"steps": ["s1"]})
    v2 = reg.publish_version("video_pipeline", {"steps": ["s1", "s2_broken"]})
    assert reg.get_active("video_pipeline")["version"] == v2
    rolled = reg.rollback_previous("video_pipeline")
    assert rolled == v1
    assert reg.get_active("video_pipeline")["version"] == v1
