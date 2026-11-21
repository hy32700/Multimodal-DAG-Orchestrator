import asyncio
import io
import json
import os
import zipfile
from pathlib import Path
from typing import Any, Dict, Optional

from fastapi import FastAPI, Header, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, StreamingResponse
from pydantic import BaseModel

from core.orchestrator_v08 import (
    BudgetGuard,
    DAGNodeV08,
    DiskBackedArtifactStore,
    GlobalStyleBible,
    MultimodalDAGOrchestratorV08,
    PooledMultiProtocolConnector,
    PreflightChecker,
    SemanticDAGCache,
)

app = FastAPI(title="Multimodal-DAG Orchestrator v0.8", version="0.8.0")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

WORKSPACE_ROOT = Path("./workspaces")
ADMIN_TOKEN = os.getenv("ORCHESTRATOR_TOKEN", "")
ACTIVE_RUNS: Dict[str, MultimodalDAGOrchestratorV08] = {}
CONNECTOR = PooledMultiProtocolConnector(global_concurrency=20)


def _verify_token(token: Optional[str]):
    if ADMIN_TOKEN and token != ADMIN_TOKEN:
        raise HTTPException(status_code=401, detail="Invalid X-Orchestrator-Token")


class CreatePipelineRequest(BaseModel):
    run_id: str
    max_budget_usd: float = 5.0
    style_bible: Optional[GlobalStyleBible] = None
    nodes: list[Dict[str, Any]]


class ApproveNodeRequest(BaseModel):
    edited_prompt: Optional[str] = None


class RerunNodeRequest(BaseModel):
    new_prompt: Optional[str] = None
    cascade_mode: str = "auto"


@app.on_event("startup")
async def on_startup():
    DiskBackedArtifactStore.garbage_collect(WORKSPACE_ROOT, max_age_hours=72)
    report = PreflightChecker.run_checks(WORKSPACE_ROOT)
    print(f"[Multimodal-DAG v0.8] Preflight Report: {json.dumps(report, ensure_ascii=False)}")


@app.on_event("shutdown")
async def on_shutdown():
    await CONNECTOR.close()


@app.get("/")
async def serve_workbench():
    return FileResponse(Path(__file__).parent.parent / "frontend" / "index.html")


@app.get("/api/v1/preflight")
async def get_preflight():
    return PreflightChecker.run_checks(WORKSPACE_ROOT)


@app.post("/api/v1/pipeline/start")
async def start_pipeline(req: CreatePipelineRequest, x_orchestrator_token: Optional[str] = Header(None)):
    _verify_token(x_orchestrator_token)
    nodes = {
        n["node_id"]: DAGNodeV08(
            node_id=n["node_id"],
            title=n["title"],
            capability=n.get("capability", "planning"),
            prompt=n["prompt"],
            deps=n.get("deps", []),
            requires_human_approval=n.get("requires_human_approval", False),
        )
        for n in req.nodes
    }

    def _default_resolver(cap: str) -> Dict[str, Any]:
        return {
            "base_url": os.getenv("PRIMARY_BASE_URL", "https://api.deepseek.com/v1"),
            "api_key": os.getenv("PRIMARY_API_KEY", ""),
            "model": os.getenv("PRIMARY_MODEL", "deepseek-chat"),
            "protocol": os.getenv("PRIMARY_PROTOCOL", "openai_compatible"),
        }

    orch = MultimodalDAGOrchestratorV08(
        run_id=req.run_id,
        nodes=nodes,
        connector=CONNECTOR,
        provider_resolver=_default_resolver,
        artifact_store=DiskBackedArtifactStore(WORKSPACE_ROOT, req.run_id),
        budget_guard=BudgetGuard(max_budget_usd=req.max_budget_usd),
        semantic_cache=SemanticDAGCache(WORKSPACE_ROOT / ".semantic_cache.json"),
        style_bible=req.style_bible,
    )
    orch.validate_acyclic()
    ACTIVE_RUNS[req.run_id] = orch
    asyncio.create_task(orch.execute_all())
    return {"status": "started", "run_id": req.run_id, "critical_weights": orch._compute_critical_path_weights()}


@app.get("/api/v1/pipeline/{run_id}/stream")
async def stream_events(run_id: str, last_seq_id: int = Query(0, ge=0)):
    orch = ACTIVE_RUNS.get(run_id)
    if not orch:
        raise HTTPException(status_code=404, detail="Run not found")

    async def _gen():
        cursor = last_seq_id
        while True:
            async with orch._event_cond:
                while cursor >= len(orch.event_log):
                    await orch._event_cond.wait()
                batch = orch.event_log[cursor:]
                cursor = len(orch.event_log)
            for ev in batch:
                yield f"id: {ev['seq_id']}\nevent: {ev['type']}\ndata: {json.dumps(ev, ensure_ascii=False)}\n\n"
                if ev["type"] in ("pipeline_finished", "budget_halted"):
                    return

    return StreamingResponse(_gen(), media_type="text/event-stream")


@app.post("/api/v1/pipeline/{run_id}/control/{action}")
async def control_pipeline(run_id: str, action: str):
    orch = ACTIVE_RUNS.get(run_id)
    if not orch:
        raise HTTPException(status_code=404, detail="Run not found")
    if action == "pause":
        orch.pause_pipeline()
    elif action == "resume":
        orch.resume_pipeline()
    else:
        raise HTTPException(status_code=400, detail="Unknown action")
    return {"run_id": run_id, "action": action}


@app.post("/api/v1/pipeline/{run_id}/nodes/{node_id}/approve")
async def approve_node(run_id: str, node_id: str, body: ApproveNodeRequest):
    orch = ACTIVE_RUNS.get(run_id)
    if not orch or node_id not in orch.nodes:
        raise HTTPException(status_code=404, detail="Node not found")
    orch.approve_node(node_id, edited_prompt=body.edited_prompt)
    return {"run_id": run_id, "node_id": node_id, "status": "approved"}


@app.post("/api/v1/pipeline/{run_id}/nodes/{node_id}/skip")
async def skip_node(run_id: str, node_id: str):
    orch = ACTIVE_RUNS.get(run_id)
    if not orch or node_id not in orch.nodes:
        raise HTTPException(status_code=404, detail="Node not found")
    orch.skip_node(node_id)
    return {"run_id": run_id, "node_id": node_id, "status": "skipped"}


@app.post("/api/v1/pipeline/{run_id}/nodes/{node_id}/rerun")
async def rerun_node(run_id: str, node_id: str, body: RerunNodeRequest):
    orch = ACTIVE_RUNS.get(run_id)
    if not orch or node_id not in orch.nodes:
        raise HTTPException(status_code=404, detail="Node not found")
    asyncio.create_task(orch.rerun_subtask(node_id, new_prompt=body.new_prompt, cascade_mode=body.cascade_mode))
    return {"run_id": run_id, "node_id": node_id, "status": "rerunning", "cascade_mode": body.cascade_mode}


@app.get("/api/v1/pipeline/{run_id}/export-zip")
async def export_zip(run_id: str):
    run_dir = WORKSPACE_ROOT / run_id
    if not run_dir.exists():
        raise HTTPException(status_code=404, detail="Workspace not found")

    def _zip_generator():
        buf = io.BytesIO()
        with zipfile.ZipFile(buf, mode="w", compression=zipfile.ZIP_DEFLATED) as zf:
            for file_path in run_dir.rglob("*"):
                if file_path.is_file():
                    zf.write(file_path, arcname=str(file_path.relative_to(run_dir)))
                    buf.seek(0)
                    chunk = buf.read()
                    if chunk:
                        yield chunk
                    buf.seek(0)
                    buf.truncate(0)
        buf.seek(0)
        rem = buf.read()
        if rem:
            yield rem

    return StreamingResponse(
        _zip_generator(),
        media_type="application/zip",
        headers={"Content-Disposition": f"attachment; filename={run_id}_v08_bundle.zip"},
    )
