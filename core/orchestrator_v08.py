import asyncio
import hashlib
import json
import random
import re
import shutil
import time
import uuid
from collections import deque
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Set, Tuple
import httpx
from pydantic import BaseModel, Field, ValidationError


class PreflightChecker:
    @staticmethod
    def run_checks(workspace_root: Path) -> Dict[str, Any]:
        workspace_root.mkdir(parents=True, exist_ok=True)
        disk = shutil.disk_usage(workspace_root)
        free_gb = round(disk.free / (1024 ** 3), 2)
        has_ffmpeg = shutil.which("ffmpeg") is not None
        has_npx = shutil.which("npx") is not None

        warnings = []
        if not has_ffmpeg:
            warnings.append("Missing 'ffmpeg': audio/video stitching nodes will auto-degrade to storyboard specs.")
        if not has_npx:
            warnings.append("Missing 'npx': Node-based MCP servers will be bypassed.")
        if free_gb < 2.0:
            warnings.append(f"Low disk space ({free_gb} GB free): aggressive artifact GC enabled.")

        return {
            "status": "ready" if not warnings else "degraded_ready",
            "free_disk_gb": free_gb,
            "capabilities": {"ffmpeg": has_ffmpeg, "mcp_npx": has_npx},
            "warnings": warnings,
        }


class SecretSanitizer:
    KEY_PATTERN = re.compile(r"(sk-[a-zA-Z0-9_-]{8,}|Bearer\s+[a-zA-Z0-9._-]{10,})")

    @classmethod
    def mask_text(cls, text: str) -> str:
        def _repl(match: re.Match) -> str:
            val = match.group(0)
            return f"{val[:6]}****{val[-4:]}" if len(val) > 12 else "****"
        return cls.KEY_PATTERN.sub(_repl, text)

    @classmethod
    def export_safe_config(cls, config_dict: Dict[str, Any]) -> Dict[str, Any]:
        raw = json.dumps(config_dict, ensure_ascii=False)
        return json.loads(cls.mask_text(raw))


@dataclass
class ArtifactRef:
    artifact_id: str
    node_id: str
    rel_path: str
    mime_type: str
    size_bytes: int
    sha256: str


class DiskBackedArtifactStore:
    def __init__(self, workspace_root: Path, run_id: str):
        self.run_dir = workspace_root / run_id / "artifacts"
        self.run_dir.mkdir(parents=True, exist_ok=True)
        self._write_lock = asyncio.Lock()

    async def save_bytes(self, node_id: str, filename: str, data: bytes, mime_type: str) -> ArtifactRef:
        safe_name = Path(filename).name
        async with self._write_lock:
            target = self.run_dir / safe_name
            if target.exists():
                target = self.run_dir / f"{node_id}_{safe_name}"
            target.write_bytes(data)

        digest = hashlib.sha256(data).hexdigest()
        return ArtifactRef(
            artifact_id=uuid.uuid4().hex[:12],
            node_id=node_id,
            rel_path=str(target.relative_to(self.run_dir.parent)),
            mime_type=mime_type,
            size_bytes=len(data),
            sha256=digest,
        )

    @staticmethod
    def garbage_collect(workspace_root: Path, max_age_hours: int = 72):
        now = time.time()
        if not workspace_root.exists():
            return
        for entry in workspace_root.iterdir():
            if entry.is_dir() and (now - entry.stat().st_mtime) > max_age_hours * 3600:
                shutil.rmtree(entry, ignore_errors=True)


class BudgetExceededError(Exception):
    pass


class BudgetGuard:
    def __init__(self, max_budget_usd: float = 5.0):
        self.max_budget_usd = max_budget_usd
        self.spent_usd = 0.0
        self.total_prompt_tokens = 0
        self.total_completion_tokens = 0
        self._lock = asyncio.Lock()

    async def record_usage(self, prompt_tokens: int, completion_tokens: int, price_per_1k_in: float = 0.001, price_per_1k_out: float = 0.002):
        async with self._lock:
            cost = (prompt_tokens / 1000.0) * price_per_1k_in + (completion_tokens / 1000.0) * price_per_1k_out
            self.spent_usd += cost
            self.total_prompt_tokens += prompt_tokens
            self.total_completion_tokens += completion_tokens
            if self.spent_usd > self.max_budget_usd:
                raise BudgetExceededError(f"Budget exceeded: ${self.spent_usd:.4f} > ${self.max_budget_usd:.2f}")


class SemanticDAGCache:
    def __init__(self, cache_file: Path):
        self.cache_file = cache_file
        self._store: Dict[str, Dict[str, Any]] = {}
        if cache_file.exists():
            try:
                self._store = json.loads(cache_file.read_text(encoding="utf-8"))
            except Exception:
                self._store = {}

    @staticmethod
    def make_key(capability: str, prompt: str, upstream_hash: str) -> str:
        raw = f"{capability}::{prompt.strip()}::{upstream_hash}"
        return hashlib.sha256(raw.encode("utf-8")).hexdigest()

    def get(self, key: str) -> Optional[Dict[str, Any]]:
        return self._store.get(key)

    def put(self, key: str, value: Dict[str, Any]):
        self._store[key] = value
        self.cache_file.parent.mkdir(parents=True, exist_ok=True)
        self.cache_file.write_text(json.dumps(self._store, ensure_ascii=False), encoding="utf-8")


class MCPSandbox:
    ALLOWED_COMMANDS = {"ffmpeg", "python", "node", "npx"}

    def __init__(self, workspace_dir: Path):
        self.workspace_dir = workspace_dir.resolve()

    def validate_path(self, target_path: str) -> Path:
        resolved = (self.workspace_dir / target_path).resolve()
        if not str(resolved).startswith(str(self.workspace_dir)):
            raise PermissionError(f"Sandbox path traversal blocked: {target_path}")
        return resolved

    async def run_safe_tool(self, cmd: List[str], timeout: float = 30.0) -> str:
        if not cmd or Path(cmd[0]).name not in self.ALLOWED_COMMANDS:
            raise PermissionError(f"Command not allowed in MCP sandbox: {cmd[0] if cmd else 'EMPTY'}")
        proc = await asyncio.create_subprocess_exec(
            *cmd,
            cwd=str(self.workspace_dir),
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        try:
            stdout, stderr = await asyncio.wait_for(proc.communicate(), timeout=timeout)
        except asyncio.TimeoutError:
            proc.kill()
            raise TimeoutError(f"MCP tool timed out after {timeout}s and was killed.")
        if proc.returncode != 0:
            raise RuntimeError(f"MCP tool failed ({proc.returncode}): {stderr.decode('utf-8', 'ignore')[:300]}")
        return stdout.decode("utf-8", "ignore")


class PooledMultiProtocolConnector:
    def __init__(self, global_concurrency: int = 20):
        self.global_sem = asyncio.Semaphore(global_concurrency)
        self.client = httpx.AsyncClient(
            limits=httpx.Limits(max_connections=100, max_keepalive_connections=30),
            timeout=httpx.Timeout(60.0, connect=10.0),
        )

    async def close(self):
        await self.client.aclose()

    async def invoke(
        self,
        base_url: str,
        api_key: str,
        model: str,
        messages: List[Dict[str, Any]],
        protocol: str = "openai_compatible",
        max_retries: int = 4,
    ) -> Tuple[str, int, int]:
        if protocol == "local_no_system" and messages and messages[0]["role"] == "system":
            sys_txt = messages[0]["content"]
            rest = messages[1:]
            if rest:
                rest[0]["content"] = f"[System Instruction]\n{sys_txt}\n\n{rest[0]['content']}"
            messages = rest

        async with self.global_sem:
            for attempt in range(max_retries + 1):
                try:
                    resp = await self.client.post(
                        f"{base_url.rstrip('/')}/chat/completions",
                        headers={"Authorization": f"Bearer {api_key}"},
                        json={"model": model, "messages": messages, "temperature": 0.25},
                    )
                    if resp.status_code == 200:
                        body = resp.json()
                        text = body["choices"][0]["message"]["content"]
                        usage = body.get("usage", {})
                        return text, usage.get("prompt_tokens", 500), usage.get("completion_tokens", 500)
                    if resp.status_code in (429, 502, 503, 504) and attempt < max_retries:
                        await asyncio.sleep(random.uniform(0.5, 1.5 * (2 ** attempt)))
                        continue
                    resp.raise_for_status()
                except (httpx.TimeoutException, httpx.NetworkError):
                    if attempt < max_retries:
                        await asyncio.sleep(random.uniform(0.8, 2.0 * (2 ** attempt)))
                        continue
                    raise


class GlobalStyleBible(BaseModel):
    visual_palette: str = "Clean modern technical aesthetic"
    target_duration_sec: int = 30
    character_or_brand_anchor: str = ""
    shared_terminology: Dict[str, str] = Field(default_factory=dict)


class SubtaskContract(BaseModel):
    summary: str
    key_artifacts: Dict[str, Any]
    full_content: str
    artifact_refs: List[Dict[str, Any]] = Field(default_factory=list)


@dataclass
class DAGNodeV08:
    node_id: str
    title: str
    capability: str
    prompt: str
    deps: List[str] = field(default_factory=list)
    requires_human_approval: bool = False
    status: str = "pending"
    output: Optional[SubtaskContract] = None
    trace_metrics: Dict[str, Any] = field(default_factory=dict)


class MultimodalDAGOrchestratorV08:
    def __init__(
        self,
        run_id: str,
        nodes: Dict[str, DAGNodeV08],
        connector: PooledMultiProtocolConnector,
        provider_resolver: Callable[[str], Dict[str, Any]],
        artifact_store: DiskBackedArtifactStore,
        budget_guard: BudgetGuard,
        semantic_cache: SemanticDAGCache,
        style_bible: Optional[GlobalStyleBible] = None,
    ):
        self.run_id = run_id
        self.nodes = nodes
        self.connector = connector
        self.provider_resolver = provider_resolver
        self.artifact_store = artifact_store
        self.budget_guard = budget_guard
        self.semantic_cache = semantic_cache
        self.style_bible = style_bible or GlobalStyleBible()

        self._pause_event = asyncio.Event()
        self._pause_event.set()
        self._approval_events: Dict[str, asyncio.Event] = {}
        self.event_log: List[Dict[str, Any]] = []
        self._event_cond = asyncio.Condition()

    def validate_acyclic(self):
        in_deg = {nid: len(n.deps) for nid, n in self.nodes.items()}
        adj: Dict[str, List[str]] = {nid: [] for nid in self.nodes}
        for nid, n in self.nodes.items():
            for d in n.deps:
                if d not in self.nodes:
                    raise ValueError(f"Node [{nid}] depends on non-existent node [{d}]")
                adj[d].append(nid)

        queue = [nid for nid, deg in in_deg.items() if deg == 0]
        visited = 0
        while queue:
            curr = queue.pop(0)
            visited += 1
            for nxt in adj[curr]:
                in_deg[nxt] -= 1
                if in_deg[nxt] == 0:
                    queue.append(nxt)
        if visited != len(self.nodes):
            raise ValueError("Cycle detected in DAG topology! Execution aborted to prevent deadlock.")

    def recover_from_crash(self):
        for node in self.nodes.values():
            if node.status == "running":
                node.status = "pending"

    def _compute_critical_path_weights(self) -> Dict[str, int]:
        memo: Dict[str, int] = {}
        children: Dict[str, List[str]] = {nid: [] for nid in self.nodes}
        for nid, n in self.nodes.items():
            for d in n.deps:
                children[d].append(nid)

        def _depth(nid: str) -> int:
            if nid in memo:
                return memo[nid]
            memo[nid] = 1 if not children[nid] else 1 + max(_depth(c) for c in children[nid])
            return memo[nid]

        for nid in self.nodes:
            _depth(nid)
        return memo

    def pause_pipeline(self):
        self._pause_event.clear()

    def resume_pipeline(self):
        self._pause_event.set()

    def approve_node(self, node_id: str, edited_prompt: Optional[str] = None):
        if edited_prompt:
            self.nodes[node_id].prompt = edited_prompt
        self.nodes[node_id].status = "approved_ready"
        if node_id in self._approval_events:
            self._approval_events[node_id].set()

    def skip_node(self, node_id: str):
        node = self.nodes[node_id]
        node.status = "skipped"
        node.output = SubtaskContract(summary="Skipped by user.", key_artifacts={"skipped": True}, full_content="")

    async def _emit(self, ev_type: str, data: Dict[str, Any]):
        async with self._event_cond:
            seq = len(self.event_log) + 1
            self.event_log.append({"seq_id": seq, "ts": time.time(), "type": ev_type, "data": data})
            self._event_cond.notify_all()

    async def _execute_single(self, node_id: str):
        await self._pause_event.wait()
        node = self.nodes[node_id]
        t0 = time.monotonic()

        if node.requires_human_approval and node.status != "approved_ready":
            node.status = "waiting_approval"
            ev = asyncio.Event()
            self._approval_events[node_id] = ev
            await self._emit("approval_required", {"node_id": node_id, "title": node.title, "prompt": node.prompt})
            await ev.wait()

        node.status = "running"
        await self._emit("node_started", {"node_id": node_id, "title": node.title})

        upstream_blocks = []
        for d in node.deps:
            dep_node = self.nodes[d]
            if dep_node.output:
                upstream_blocks.append(f"[{d}:{dep_node.title}] 摘要={dep_node.output.summary} | 契约={dep_node.output.key_artifacts}")
        upstream_str = "\n".join(upstream_blocks) or "无"
        upstream_hash = hashlib.sha256(upstream_str.encode("utf-8")).hexdigest()[:16]

        cache_key = self.semantic_cache.make_key(node.capability, node.prompt, upstream_hash)
        cached = self.semantic_cache.get(cache_key)
        if cached:
            node.output = SubtaskContract.model_validate(cached)
            node.status = "completed"
            node.trace_metrics = {"duration_ms": round((time.monotonic() - t0) * 1000, 1), "cache_hit": True}
            await self._emit("node_completed", {"node_id": node_id, "cache_hit": True})
            return

        try:
            prov = self.provider_resolver(node.capability)
            sys_msg = (
                f"你是 Multimodal-DAG v0.8 协同节点。全局一致性规范：{self.style_bible.model_dump_json()}。\n"
                '必须输出合法 JSON：{"summary": "<=200字摘要", "key_artifacts": {}, "full_content": "完整产物"}'
            )
            user_msg = f"子任务：{node.prompt}\n\n上游依赖上下文：\n{upstream_str}"

            raw_text, in_tok, out_tok = await self.connector.invoke(
                base_url=prov["base_url"],
                api_key=prov["api_key"],
                model=prov["model"],
                messages=[{"role": "system", "content": sys_msg}, {"role": "user", "content": user_msg}],
                protocol=prov.get("protocol", "openai_compatible"),
            )
            await self.budget_guard.record_usage(in_tok, out_tok)

            cleaned = re.sub(r"^```(?:json)?|```$", "", raw_text.strip(), flags=re.MULTILINE).strip()
            try:
                contract = SubtaskContract.model_validate_json(cleaned)
            except ValidationError:
                contract = SubtaskContract(summary=raw_text[:240], key_artifacts={"auto_wrapped": True}, full_content=raw_text)

            if len(contract.full_content) > 2048:
                ref = await self.artifact_store.save_bytes(
                    node_id, f"{node_id}_output.txt", contract.full_content.encode("utf-8"), "text/plain"
                )
                contract.artifact_refs.append(ref.__dict__)

            node.output = contract
            node.status = "completed"
            self.semantic_cache.put(cache_key, contract.model_dump())
            node.trace_metrics = {
                "duration_ms": round((time.monotonic() - t0) * 1000, 1),
                "cache_hit": False,
                "spent_usd_cumulative": round(self.budget_guard.spent_usd, 4),
            }
            await self._emit("node_completed", {"node_id": node_id, "metrics": node.trace_metrics})

        except BudgetExceededError as be:
            node.status = "failed"
            self.pause_pipeline()
            await self._emit("budget_halted", {"node_id": node_id, "reason": str(be)})
        except Exception as exc:
            node.status = "failed"
            node.trace_metrics = {"duration_ms": round((time.monotonic() - t0) * 1000, 1), "error": SecretSanitizer.mask_text(str(exc))}
            await self._emit("node_failed", {"node_id": node_id, "error": node.trace_metrics["error"]})

    def _get_downstream_subgraph(self, root_id: str) -> Set[str]:
        visited = set()
        queue = deque([root_id])
        while queue:
            curr = queue.popleft()
            for nid, node in self.nodes.items():
                if curr in node.deps and nid not in visited:
                    visited.add(nid)
                    queue.append(nid)
        return visited

    async def rerun_subtask(self, node_id: str, new_prompt: Optional[str] = None, cascade_mode: str = "auto"):
        node = self.nodes[node_id]
        if new_prompt:
            node.prompt = new_prompt
        node.status = "pending"
        downstream = self._get_downstream_subgraph(node_id)
        if cascade_mode in ("auto", "mark_dirty"):
            for d_id in downstream:
                self.nodes[d_id].status = "dirty"
            await self._emit("downstream_marked_dirty", {"root": node_id, "affected": list(downstream)})
        if cascade_mode == "auto":
            await self.execute_all(target_ids={node_id, *downstream})
        else:
            await self.execute_all(target_ids={node_id})

    async def execute_all(self, target_ids: Optional[Set[str]] = None):
        self.validate_acyclic()
        self.recover_from_crash()
        weights = self._compute_critical_path_weights()
        scope = target_ids or set(self.nodes.keys())

        while True:
            await self._pause_event.wait()
            ready = [
                nid for nid in scope
                if self.nodes[nid].status in ("pending", "dirty", "approved_ready")
                and all(self.nodes[d].status in ("completed", "skipped") for d in self.nodes[nid].deps)
            ]
            if not ready:
                break
            ready.sort(key=lambda nid: weights.get(nid, 1), reverse=True)
            await asyncio.gather(*(self._execute_single(nid) for nid in ready))

        await self._emit("pipeline_finished", {
            "run_id": self.run_id,
            "total_spent_usd": round(self.budget_guard.spent_usd, 4),
        })
