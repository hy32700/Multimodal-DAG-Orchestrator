import json
import time
from pathlib import Path
from typing import Any, Dict, Optional


class VersionedSkillRegistry:
    def __init__(self, registry_dir: Path):
        self.registry_dir = registry_dir
        self.registry_dir.mkdir(parents=True, exist_ok=True)

    def publish_version(self, skill_name: str, skill_payload: Dict[str, Any]) -> str:
        skill_dir = self.registry_dir / skill_name
        skill_dir.mkdir(parents=True, exist_ok=True)
        version_id = f"v_{int(time.time() * 1000)}"
        record = {"version": version_id, "created_at": time.time(), "failures": 0, "payload": skill_payload}
        (skill_dir / f"{version_id}.json").write_text(json.dumps(record, ensure_ascii=False, indent=2), encoding="utf-8")
        (skill_dir / "active.txt").write_text(version_id, encoding="utf-8")
        return version_id

    def get_active(self, skill_name: str) -> Optional[Dict[str, Any]]:
        skill_dir = self.registry_dir / skill_name
        active_file = skill_dir / "active.txt"
        if not active_file.exists():
            return None
        ver = active_file.read_text(encoding="utf-8").strip()
        target = skill_dir / f"{ver}.json"
        return json.loads(target.read_text(encoding="utf-8")) if target.exists() else None

    def rollback_previous(self, skill_name: str) -> Optional[str]:
        skill_dir = self.registry_dir / skill_name
        versions = sorted([p.stem for p in skill_dir.glob("v_*.json")])
        if len(versions) < 2:
            return None
        prev_ver = versions[-2]
        (skill_dir / "active.txt").write_text(prev_ver, encoding="utf-8")
        return prev_ver
