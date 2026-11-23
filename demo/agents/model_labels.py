"""Names for each model call that say what the agent loop is doing, not which model it uses.

Each agent loop calls the model several times: once to plan, then again after each batch of tool
results. By default the Agent Kit UI labels every one of those calls with the model id, and the
Temporal UI with the agent's name. Here a call is named from the conversation it is about to send:
the task (planning), the results of the tools it just called, or a nudge to finish its outputs.

- `LabelHooks.on_llm_start` runs right before each model call (in workflow code, so it's
  deterministic) and leaves the label on the runner;
- `labelled_stream_to_provider` hands that label to the Agent Kit UI in place of the model id;
- `LabelSummary` gives the Temporal UI's activity summary the same words.
"""

# No `from __future__ import annotations`: the Agent Kit reads annotations concretely.

from typing import Any

from temporalio import workflow

with workflow.unsafe.imports_passed_through():
    from agents import Agent, AgentHooks
    from temporal_agent_kit.ai_sdks.openai_agents import stream_to_provider
    from temporal_agent_kit.ai_sdks.openai_agents._model_parameters import ModelSummaryProvider

_LABEL_ATTR = "_demo_model_call_label"

PLANNING = {
    "Extractor": "planning the extraction",
    "Contract reviewer": "planning the review against the playbook",
    "Box Relay": "working out how to answer the question",
}

AFTER_TOOL = {
    "run_python_on_box_folder": "checking what its script did in /box",
    "read_pdf_text_from_box": "working through the contract text",
    "read_handed_off_file_from_box": "working through the handed-off extraction",
    "get_approved_clause_language": "drafting redlines from the approved clause language",
    "score_contract_risk": "writing up the memo with the risk score",
    "read_contract_in_extractor_sandbox": "answering from the contract",
    "read_file_in_reviewer_sandbox": "answering from the Reviewer's files",
}


def _get(item: Any, key: str) -> Any:
    return item.get(key) if isinstance(item, dict) else getattr(item, key, None)


def describe(agent_name: str, items: Any) -> str:
    """What this model call is for, judged from the conversation it is about to send."""
    items = items if isinstance(items, list) else [{"role": "user", "content": str(items)}]
    last = items[-1] if items else None
    doing = PLANNING.get(agent_name, "planning")
    if last is not None and _get(last, "type") == "function_call_output":
        names = {_get(i, "call_id"): _get(i, "name") for i in items if _get(i, "type") == "function_call"}
        tool = names.get(_get(last, "call_id")) or ""
        doing = AFTER_TOOL.get(tool, f"working with the result of {tool.replace('_', ' ')}" if tool else "deciding what to do next")
    elif last is not None and _get(last, "role") == "user" and str(_get(last, "content") or "").startswith("You have not written"):
        doing = "writing the outputs it hasn't written yet"
    return f"{agent_name} · {doing}"


class LabelHooks(AgentHooks):
    """Names each model call just before it goes out (the run context is the Agent Kit runner)."""

    async def on_llm_start(self, context: Any, agent: Agent, system_prompt: Any, input_items: Any) -> None:
        runner = getattr(context, "context", None)
        if runner is not None:
            setattr(runner, _LABEL_ATTR, describe(agent.name, input_items))


def labelled_stream_to_provider(model: str | None, run_context: Any):
    """The Agent Kit's provider, with the call's label where the UI would show the model id."""
    token = stream_to_provider(model, run_context)
    label = getattr(run_context, _LABEL_ATTR, None)
    return token.model_copy(update={"model": label}) if token is not None and label else token


class LabelSummary(ModelSummaryProvider):
    """The same label for the model activity's summary in the Temporal UI."""

    def provide(self, agent: Agent | None, instructions: str | None, input: Any) -> str:
        return describe(agent.name if agent else "Agent", input)
