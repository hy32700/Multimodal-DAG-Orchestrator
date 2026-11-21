import asyncio
import httpx
from dataclasses import dataclass, field
from typing import Dict, List, Tuple


@dataclass
class ProviderConfig:
    name: str
    enabled: bool
    base_url: str
    api_key: str
    model: str
    capabilities: List[str] = field(default_factory=list)
    max_concurrency: int = 5
    healthy: bool = False


class V3ConfigStore:
    def __init__(self, providers: Dict[str, ProviderConfig], fallback_chain: Dict[str, List[str]]):
        self.providers = providers
        self.fallback_chain = fallback_chain

    async def health_check_all(self, timeout: float = 6.0) -> Dict[str, bool]:
        async def _probe(p: ProviderConfig) -> Tuple[str, bool]:
            if not p.enabled or not p.api_key or p.api_key.startswith("${"):
                p.healthy = False
                return p.name, False
            try:
                async with httpx.AsyncClient(timeout=timeout) as client:
                    resp = await client.get(
                        f"{p.base_url.rstrip('/')}/models",
                        headers={"Authorization": f"Bearer {p.api_key}"},
                    )
                    p.healthy = resp.status_code != 401
            except Exception:
                p.healthy = False
            return p.name, p.healthy

        results = await asyncio.gather(*[_probe(p) for p in self.providers.values()])
        return dict(results)

    def resolve_provider(self, required_capability: str) -> Tuple[ProviderConfig, bool]:
        candidates = self.fallback_chain.get(required_capability, [])
        for idx, name in enumerate(candidates):
            provider = self.providers.get(name)
            if provider and provider.healthy:
                has_native_cap = required_capability in provider.capabilities
                return provider, (idx > 0 or not has_native_cap)

        for provider in self.providers.values():
            if provider.healthy:
                return provider, True

        raise RuntimeError("No healthy API provider available. Please configure at least one valid API Key.")
