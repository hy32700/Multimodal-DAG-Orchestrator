import os
import webbrowser
from pathlib import Path
import uvicorn
from core.orchestrator_v08 import PreflightChecker


def main():
    print("=== Multimodal-DAG Orchestrator v0.8 QuickStart ===")
    report = PreflightChecker.run_checks(Path("./workspaces"))
    for w in report["warnings"]:
        print(f"[提示] {w}")

    if not os.getenv("PRIMARY_API_KEY"):
        print("[警告] 未检测到 PRIMARY_API_KEY 环境变量，请先执行: export PRIMARY_API_KEY='sk-...'")

    url = "http://127.0.0.1:8088"
    print(f"正在启动控制台: {url}")
    webbrowser.open(url)
    uvicorn.run("server.app:app", host="127.0.0.1", port=8088, reload=False)


if __name__ == "__main__":
    main()
