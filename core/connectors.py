import asyncio
import random
import time
import httpx
from typing import Any, Dict, Optional


class CircuitBreakerOpenError(Exception):
    pass


class ProviderCircuitBreaker:
    def __init__(self, failure_threshold: int = 5, recovery_timeout: float = 30.0):
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self.consecutive_failures = 0
        self.opened_at: Optional[float] = None

    def allow_request(self) -> bool:
        if self.opened_at is None:
            return True
        if time.monotonic() - self.opened_at >= self.recovery_timeout:
            self.opened_at = None
            self.consecutive_failures = 0
            return True
        return False

    def record_success(self):
        self.consecutive_failures = 0
        self.opened_at = None

    def record_failure(self):
        self.consecutive_failures += 1
        if self.consecutive_failures >= self.failure_threshold:
            self.opened_at = time.monotonic()


class ResilientLLMConnector:
    def __init__(self, global_max_concurrency: int = 16):
        self.global_sem = asyncio.Semaphore(global_max_concurrency)
        self.provider_sems: Dict[str, asyncio.Semaphore] = {}
        self.breakers: Dict[str, ProviderCircuitBreaker] = {}

    def _get_provider_controls(self, provider_name: str, max_conc: int):
        if provider_name not in self.provider_sems:
            self.provider_sems[provider_name] = asyncio.Semaphore(max_conc)
            self.breakers[provider_name] = ProviderCircuitBreaker()
        return self.provider_sems[provider_name], self.breakers[provider_name]

    async def chat_completion(
        self,
        provider_name: str,
        base_url: str,
        api_key: str,
        model: str,
        messages: list,
        max_concurrency: int = 5,
        node_timeout: float = 45.0,
        max_retries: int = 4,
        is_degraded: bool = False,
    ) -> Dict[str, Any]:
        p_sem, breaker = self._get_provider_controls(provider_name, max_concurrency)
        if not breaker.allow_request():
            raise CircuitBreakerOpenError(f"Provider [{provider_name}] circuit breaker is OPEN.")

        if is_degraded:
            messages = [
                {
                    "role": "system",
                    "content": "[Degraded-Mode] 当前未挂载原生多模态/专用模型，请输出高度结构化的文本规格书、分镜参数表或可直接渲染的源码骨架。",
                },
                *messages,
            ]

        async with self.global_sem, p_sem:
            for attempt in range(max_retries + 1):
                try:
                    async with httpx.AsyncClient(timeout=node_timeout) as client:
                        resp = await client.post(
                            f"{base_url.rstrip('/')}/chat/completions",
                            headers={"Authorization": f"Bearer {api_key}"},
                            json={"model": model, "messages": messages, "temperature": 0.3},
                        )
                        if resp.status_code == 200:
                            breaker.record_success()
                            return resp.json()["choices"][0]["message"]["content"]

                        if resp.status_code in (429, 500, 502, 503, 504) and attempt < max_retries:
                            retry_after = resp.headers.get("Retry-After")
                            base_delay = float(retry_after) if retry_after else (1.5 * (2 ** attempt))
                            await asyncio.sleep(random.uniform(0.5, base_delay))
                            continue

                        resp.raise_for_status()
                except (httpx.TimeoutException, httpx.NetworkError) as exc:
                    if attempt < max_retries:
                        await asyncio.sleep(random.uniform(0.8, 2.0 * (2 ** attempt)))
                        continue
                    breaker.record_failure()
                    raise RuntimeError(f"Node timeout/network failure after {max_retries} retries: {exc}") from exc
                except Exception:
                    breaker.record_failure()
                    raise
