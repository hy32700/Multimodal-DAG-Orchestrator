import re
from dataclasses import dataclass
from typing import Any, Callable, Dict, List


@dataclass
class CompiledStep:
    step_id: str
    mode: str
    tool_or_cap: str
    template: str
    output_assertion: str


class ParametricSkillRunner:
    def __init__(self, skill_name: str, required_slots: List[str], steps: List[CompiledStep]):
        self.skill_name = skill_name
        self.required_slots = required_slots
        self.steps = steps

    def validate_inputs(self, inputs: Dict[str, Any]) -> bool:
        return all(slot in inputs and bool(str(inputs[slot]).strip()) for slot in self.required_slots)

    async def execute_with_fallback(
        self,
        inputs: Dict[str, Any],
        mcp_executor: Callable,
        llm_connector: Any,
        cloud_replanner: Callable,
    ) -> Dict[str, Any]:
        if not self.validate_inputs(inputs):
            return await cloud_replanner(reason="Missing or out-of-bounds skill slots", inputs=inputs)

        context_vars = dict(inputs)
        results = {}

        for step in self.steps:
            rendered = step.template
            for k, v in context_vars.items():
                rendered = rendered.replace(f"{{{{{k}}}}}", str(v))

            try:
                if step.mode == "deterministic_tool":
                    out = await mcp_executor(step.tool_or_cap, rendered)
                else:
                    out = await llm_connector.quick_fill(step.tool_or_cap, rendered)

                if step.output_assertion and not re.search(step.output_assertion, str(out)):
                    raise ValueError(f"Assertion failed on step [{step.step_id}]")

                results[step.step_id] = out
                context_vars[f"{step.step_id}_out"] = out
            except Exception as exc:
                return await cloud_replanner(
                    reason=f"Skill [{self.skill_name}] failed at [{step.step_id}]: {exc}",
                    inputs=inputs,
                    partial_results=results,
                )

        return {"status": "skill_cache_hit", "skill": self.skill_name, "results": results}
