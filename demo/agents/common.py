"""Shared plumbing for the agents: one OpenAI Agents SDK turn, and the specialist handoff loop."""

# No `from __future__ import annotations`: the harness reads tool and handler annotations
# concretely, and stringized ones fail to resolve.

from typing import Any, Awaitable, Callable, Mapping

from temporalio import workflow

with workflow.unsafe.imports_passed_through():
    from agents import Agent as OpenAIAgent
    from agents import ModelSettings, Runner, TResponseInputItem
    from openai.types.shared import Reasoning

    from temporal_agent_harness.ai_sdks.openai_agents_harness import as_openai_agent_tool
    from temporal_agent_harness.harness.agent_workflow import AgentWorkflowRunner

    from ..config import MODEL, REASONING_EFFORT
    from ..quiet_streams import STREAM_MODEL_CALLS
    from ..handoff import await_handoff, fingerprint
    from ..models import CaseResult, CaseTask

Tool = Callable[..., Awaitable[Any]]
# A tool, or (tool, injections) for tools with Injected[...] parameters.
ToolSpec = Tool | tuple[Tool, Mapping[str, Any]]


def _adapt(runner: AgentWorkflowRunner, spec: ToolSpec):
    if isinstance(spec, tuple):
        return as_openai_agent_tool(runner, spec[0], injections=spec[1])
    return as_openai_agent_tool(runner, spec)


async def run_model_turn(
    runner: AgentWorkflowRunner,
    *,
    name: str,
    instructions: str,
    tools: list[ToolSpec],
    conversation: list[TResponseInputItem],
    max_turns: int = 30,
) -> tuple[str, list[TResponseInputItem]]:
    """Run the model until it replies; return the reply and the updated conversation."""
    sdk_agent = OpenAIAgent(
        name=name,
        instructions=instructions,
        model=MODEL,
        tools=[_adapt(runner, spec) for spec in tools],
        model_settings=ModelSettings(
            parallel_tool_calls=True,
            reasoning=Reasoning(effort=REASONING_EFFORT) if REASONING_EFFORT else None,
        ),
    )
    if STREAM_MODEL_CALLS:
        result = Runner.run_streamed(sdk_agent, input=conversation, context=runner, max_turns=max_turns)
        async for _event in result.stream_events():
            pass
    else:
        # DEMO_STREAM_MODE=off: one plain model activity per call, no text deltas streamed.
        result = await Runner.run(sdk_agent, input=conversation, context=runner, max_turns=max_turns)
    return str(result.final_output), result.to_input_list()


def _call_id() -> str:
    return str(workflow.uuid4())


async def run_specialist(
    runner: AgentWorkflowRunner,
    *,
    agent_name: str,
    instructions: str,
    tools: list[Tool],
    task: CaseTask,
    outputs: list[str],
    file_exists: Callable[[str], Awaitable[bool]],
    prefetch: list[tuple[Tool, dict[str, Any]]] = (),
) -> CaseResult:
    """A specialist's whole job, deterministic around one model turn:

    1. wait for each handed-off input to sync into this sandbox through Box Mount;
    2. run any tool calls the job always needs (e.g. reading the contract) and give the model
       their results, rather than trusting it not to skip them;
    3. let the model do the work (it reads and writes /box with run_python_on_box_folder);
    4. check it wrote each output (one nudge if not), and fingerprint them for the next agent.
    """
    def status(text: str) -> None:  # the 'current details' line in the Temporal UI
        workflow.set_current_details(f"**{agent_name}** · {text}")

    status(f"waiting for {len(task.inputs)} handed-off file(s) to sync into this sandbox through Box")
    received = [await runner.run_tool(_call_id(), await_handoff, ref=ref) for ref in task.inputs]
    status("working: " + task.instructions[:120])
    fetched = [(tool, kwargs, await runner.run_tool(_call_id(), tool, **kwargs)) for tool, kwargs in prefetch]

    lines = [task.instructions, ""]
    if task.documents:
        lines += ["Source documents (paths relative to /box):", *[f"- {d}" for d in task.documents], ""]
    if received:
        lines += ["Handed-off inputs, already synced into your sandbox:"]
        lines += [f"- /box/{r.path} (from {r.written_in})" for r in received]
        lines.append("")
    for tool, kwargs, result in fetched:
        args = ", ".join(f"{k}={v!r}" for k, v in kwargs.items())
        body = result.model_dump_json(indent=2) if hasattr(result, "model_dump_json") else str(result)
        lines += [f"Already called for you: {tool.__name__}({args}) returned:", body, ""]
    lines += ["You must write:", *[f"- /box/{o}" for o in outputs]]
    conversation: list[TResponseInputItem] = [{"role": "user", "content": "\n".join(lines)}]

    summary, conversation = await run_model_turn(
        runner, name=agent_name, instructions=instructions, tools=tools, conversation=conversation
    )
    missing = [o for o in outputs if not await file_exists(o)]
    if missing:
        conversation.append({"role": "user", "content": "You have not written: "
                             + ", ".join(f"/box/{m}" for m in missing) + ". Write them now with run_python_on_box_folder."})
        summary, conversation = await run_model_turn(
            runner, name=agent_name, instructions=instructions, tools=tools, conversation=conversation
        )

    refs = [await runner.run_tool(_call_id(), fingerprint, path=o) for o in outputs]
    status("done: wrote " + ", ".join(f"{r.path} ({r.sha256[:8]})" for r in refs))
    sandbox = refs[0].written_in if refs else (received[0].received_in if received else "")
    return CaseResult(agent=agent_name, sandbox=sandbox, summary=summary, received=received, outputs=refs)
