"""
LLM Node - Handles LLM invocations with optional search, tools, and evaluator.
Supports both standard execution and chat mode with history.

Uses LangChain-compatible AIXPChatModel for LangGraph native streaming.
Nodes always return Dict - streaming is handled by LangGraph via stream_mode="messages".
"""

import json
import logging
import time
from typing import Any, Callable, Dict, List, Optional, Tuple

from langchain_core.messages import AIMessage, HumanMessage, SystemMessage, ToolMessage

from app.services.workflow_engine.nodes.base import NodeConfig
from app.services.workflow_engine.state import WorkflowState
from app.services.workflow_engine.variable_resolver import VariableResolver
from app.services.exceptions import ToolsUnavailableError

logger = logging.getLogger(__name__)


# Evaluator response format for structured output
EVALUATOR_RESPONSE_FORMAT = {
    "type": "json_schema",
    "json_schema": {
        "name": "evaluation_result",
        "strict": True,
        "schema": {
            "type": "object",
            "properties": {
                "score": {
                    "type": "integer",
                    "description": "Score from 1 to 10 rating the quality of the output",
                },
                "feedback": {
                    "type": "string",
                    "description": "Specific feedback explaining the score and suggestions for improvement",
                },
            },
            "required": ["score", "feedback"],
            "additionalProperties": False,
        },
    },
}


def check_search_condition(has_search, search_config):
    """
    Checks if search is true or not.
    Args:
    has_search: Search subcomponent from orch_config
    search_config: The values of search subcomponent from orch_config
    """
    if has_search and search_config:
        return True
    return False


def create_llm_node(config: NodeConfig) -> Callable[[WorkflowState], Dict[str, Any]]:
    """
    Factory: Create LLM node function from configuration.

    Args:
        config: Node configuration from workflow_json

    Returns:
        Function that takes WorkflowState and returns state update dict
    """
    # Extract configs from properties
    llm_config = config.properties.get("llm_configurations", {})
    prompt_config = config.properties.get("prompt", {})

    # Check sub-components
    has_search = config.has_sub_component("Search")
    has_tools = config.has_sub_component("Tools")
    has_evaluator = config.has_sub_component("Evaluator")

    search_config = config.get_sub_component("Search") if has_search else None
    tools_config = config.get_sub_component("Tools") if has_tools else None
    evaluator_config = _get_evaluator_config(config) if has_evaluator else None

    resolver = VariableResolver()

    logger.info(
        f"LLM Node {config.node_id} - has_search: {has_search}, has_tools: {has_tools}, "
        f"has_evaluator: {has_evaluator}"
    )

    def llm_node(state: WorkflowState) -> Dict[str, Any]:
        """
        Execute LLM node.

        Always returns Dict - streaming is handled by LangGraph via stream_mode="messages".
        When streaming is enabled in llm_config, the AIXPChatModel will emit tokens
        through LangChain callbacks, which LangGraph captures automatically.
        """
        start_time = time.time()
        search_execution_info = {}
        guardrails_execution_info = {}
        search_context = {}
        resolved_prompt = ""
        tools_execution_info = {}

        try:
            # Check if chat mode is enabled
            is_chat_enabled = _check_if_chat_enabled(state)

            if is_chat_enabled:
                # Chat mode execution (evaluator not supported in chat mode yet)
                return _execute_chat_mode(
                    state,
                    config,
                    llm_config,
                    prompt_config,
                    has_search,
                    search_config,
                    resolver,
                    has_tools,
                    tools_config
                )

            # Standard execution flow (non-chat)
            # 1. Perform search if enabled (PRE-PROCESSING - runs once before retry loop)
            is_search = check_search_condition(has_search, search_config)
            if is_search:
                search_result = _execute_search(state, config.node_id)
                search_context = search_result.get("data", {})
                search_execution_info = search_result.get("search_execution_info", {})

            # 2. Resolve prompt variables
            prompt_template = prompt_config.get("text", "")
            resolved_prompt = resolver.resolve(
                prompt_template,
                state["workflow_input"],
                state.get("node_outputs", {}),
                search_context,
                state,
            )

            # 3. Execute based on mode (with or without evaluator)
            annotation = ""

            if has_evaluator and evaluator_config:
                # Execute with evaluator retry loop
                logger.info(f"LLM Node {config.node_id} - Executing with evaluator")
                evaluator_template = evaluator_config.get("prompt", {}).get("evaluator_prompt", "")
                resolved_evaluator_prompt = resolver.resolve(
                        evaluator_template,
                        state["workflow_input"],
                        state.get("node_outputs", {}),
                        search_context,
                        state,
                    )
                evaluator_config["prompt"]["evaluator_prompt"] = resolved_evaluator_prompt
                (
                    output,
                    guardrails_execution_info,
                    annotation,
                    resolved_prompt,
                    tools_execution_info # Formatted resolved_input with all attempts
                ) = _execute_with_evaluator(
                    state=state,
                    original_prompt=resolved_prompt,
                    llm_config=llm_config,
                    prompt_config=prompt_config,
                    node_id=config.node_id,
                    evaluator_config=evaluator_config,
                    has_tools=has_tools,
                    tools_config=tools_config,
                )
            elif has_tools and tools_config:
                # Execute with tools (no evaluator)
                output, guardrails_execution_info, annotation = _execute_with_tools(
                    state,
                    resolved_prompt,
                    llm_config,
                    tools_config,
                    prompt_config,
                    tools_execution_info,
                    config.node_id,
                )
            else:
                # Standard LLM execution (no tools, no evaluator)
                output, guardrails_execution_info, annotation = _execute_llm(
                    state, resolved_prompt, llm_config, prompt_config, config.node_id
                )

            execution_time = time.time() - start_time
            execution_order = state.get("execution_counter", 0) + 1

            # Return state update with annotation
            return {
                "node_outputs": {
                    config.node_id: {
                        "output": output,
                        "display_name": config.display_name,
                        "execution_time": execution_time,
                        "execution_order": execution_order,
                        "status": "COMPLETED",
                        "resolved_input": resolved_prompt,
                        "search_context": search_context,
                        "logs": {
                            "search_execution_info": search_execution_info,
                            "guardrails_execution_info": guardrails_execution_info,
                            "tools_execution_info" : tools_execution_info
                        },
                    }
                },
                "execution_order": [config.node_id],
                "execution_counter": execution_order,
                "annotation": annotation,  # Update annotation in state
            }

        except Exception as e:
            execution_time = time.time() - start_time
            execution_order = state.get("execution_counter", 0) + 1

            logger.exception(f"LLM node {config.node_id} failed")

            return {
                "node_outputs": {
                    config.node_id: {
                        "output": None,
                        "display_name": config.display_name,
                        "execution_time": execution_time,
                        "execution_order": execution_order,
                        "status": "FAILED",
                        "error": str(e),
                        "resolved_input": resolved_prompt,
                        "logs": {
                            "search_execution_info": search_execution_info,
                            "guardrails_execution_info": guardrails_execution_info,
                            "tools_execution_info" : tools_execution_info
                        },
                    }
                },
                "execution_order": [config.node_id],
                "execution_counter": execution_order,
                "error": f"Node {config.node_id} failed: {str(e)}",
                "failed_node": config.node_id,
            }

    return llm_node


def _get_evaluator_config(config: NodeConfig) -> Dict[str, Any]:
    """
    Get the full evaluator subcomponent configuration.

    Args:
        config: Node configuration

    Returns:
        Evaluator subcomponent configuration dict including properties and llm_configurations
    """
    for sub in config.sub_components:
        if sub.get("component_type") == "Evaluator":
            return sub.get("properties", {})
    return {}


def _parse_stream_value(value: Any) -> bool:
    """
    Parse stream parameter value to boolean.
    Handles string values like "true", "True", "false", "False" etc.
    """
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.lower() in ("true", "1", "yes")
    return False


def _check_if_chat_enabled(state: WorkflowState) -> bool:
    """Check if chat is enabled from cache based on environment."""
    try:
        is_chat = state.get("is_chat")

        if is_chat.lower() == "true":
            is_chat_enabled = True
        else:
            is_chat_enabled = False

        logger.info(f"Chat enabled status: {is_chat_enabled}")
        return is_chat_enabled
    except Exception as e:
        logger.error(f"Error checking if chat is enabled: {str(e)}")
        return False


def _extract_chat_inputs(state: WorkflowState) -> tuple[str, List[Dict[str, Any]]]:
    """Extract user_query and history from workflow_input[0]."""
    workflow_input = state.get("workflow_input", [])
    if not workflow_input:
        raise ValueError("No input for chat mode")

    chat_input = (
        workflow_input[0] if isinstance(workflow_input, list) else workflow_input
    )
    logger.info(f"chat_input is {chat_input}")

    user_query = chat_input.get("user_query", "")
    history = chat_input.get("history_list", [])

    return user_query, history


def _perform_chat_search(
    state: WorkflowState, user_query: str, node_id: str
) -> tuple[Dict[str, Any], Dict[str, Any]]:
    """Perform search with chat=True and user_query."""
    from app.services.external_clients import SearchServiceClient

    search_context = {}
    search_execution_info = {}

    try:
        agent_id = state.get("agent_id")
        search_payload = {
            "usecase_code": state.get("usecase_code"),
            "agent_id": int(agent_id),
            "node_id": node_id,
            "transaction_id": state.get("transaction_id"),
            "user_query": user_query,
        }

        search_client = SearchServiceClient(use_mock=False)
        jwt_token = state.get("jwt_token")
        environment = state.get("environment")

        search_results = search_client.search(
            search_payload,
            jwt_token=jwt_token,
            environment=environment,
            context=dict(state),
            chat=True,
        )

        logger.info(f"Chat search results: {search_results}")

        if isinstance(search_results, dict):
            search_execution_info = search_results.get("search_execution_info", {})
            search_context = search_results.get("data", {})

        return search_context, search_execution_info

    except Exception as e:
        logger.error(f"Chat search failed: {str(e)}")
        return {"error": str(e)}, {}


def _extract_text_content(content_list: Any) -> str:
    """Extract text content from message content field."""
    if isinstance(content_list, list):
        for item in content_list:
            if isinstance(item, dict) and item.get("type") == "text":
                return item.get("text", "")
        return ""
    return content_list if isinstance(content_list, str) else ""


def _create_message_from_role(role: str, content: str):
    """Create appropriate LangChain message based on role."""
    role_lower = role.lower()
    if role_lower == "user":
        return HumanMessage(content=content)
    elif role_lower == "assistant":
        return AIMessage(content=content)
    elif role_lower == "system":
        return SystemMessage(content=content)
    return None


def _build_langchain_messages(
    prompt: str,
    system_prompt: Optional[str] = None,
    history: Optional[List[Dict[str, Any]]] = None,
) -> List:
    """Build LangChain message objects from prompt, system prompt, and history."""
    messages = []

    if system_prompt:
        messages.append(SystemMessage(content=system_prompt))

    if history:
        for msg in history:
            text_content = _extract_text_content(msg.get("content", []))
            message = _create_message_from_role(msg.get("role", ""), text_content)
            if message:
                messages.append(message)

    messages.append(HumanMessage(content=prompt))
    return messages


def _execute_chat_mode(
    state: WorkflowState,
    config: NodeConfig,
    llm_config: Dict[str, Any],
    prompt_config: Dict[str, Any],
    has_search: bool,
    search_config: Any,
    resolver: VariableResolver,
    has_tools: bool = False,
    tools_config: Optional[Dict[str, Any]] = None
) -> Dict[str, Any]:
    """
    Execute in chat mode with history support.

    Uses AIXPLLMGatewayChat which supports LangGraph native streaming via callbacks.
    """
    from app.services.workflow_engine.adapters import AIXPLLMGatewayChat

    start_time = time.time()
    search_execution_info = {}
    guardrail_execution_info = {}
    search_context = {}
    tools_execution_info = {}

    try:
        # 1. Extract chat inputs
        user_query, history = _extract_chat_inputs(state)
        logger.info(f"Chat mode - user_query: {user_query}")
        logger.info(f"Chat mode - history length: {len(history)}")

        # 2. Perform chat search if search component enabled
        if has_search and search_config:
            search_context, search_execution_info = _perform_chat_search(
                state, user_query, config.node_id
            )

        # 3. Resolve prompt variables
        prompt_template = prompt_config.get("text", "")
        resolved_prompt = resolver.resolve(
            prompt_template,
            state["workflow_input"],
            state.get("node_outputs", {}),
            search_context,
            state,
        )

        # 4. Combine resolved prompt with user query
        if resolved_prompt:
            combined_prompt = f"{resolved_prompt}\n{user_query}"
        else:
            combined_prompt = user_query

        logger.info("Combined Prompt: %s", combined_prompt)

        # 5. Build LangChain messages with history
        system_prompt = state.get("system_prompt")
        messages = _build_langchain_messages(combined_prompt, system_prompt, history)
        #Chat history without system prompt as system prompt will be be injected into ReAct agent separetely
        chat_history = _build_langchain_messages(combined_prompt, history)
        logger.info(f"Built {len(messages)} LangChain messages for chat mode")

        # 6. Create AIXPLLMGatewayChat with node_id and agent_version for proper key transformation
        is_streaming = _parse_stream_value(llm_config.get("stream", "false"))
        logger.info(f'is_streaming is {is_streaming}')

        if has_tools and tools_config:
            logger.info(f"Chat Tools Execution Flow : {tools_config}")
            output, guardrail_execution_info, annotation  = _execute_with_tools(
                state, combined_prompt, llm_config, tools_config, prompt_config, tools_execution_info, config.node_id, chat_history
            )
        else:
            logger.info("Chat Normal Execution Flow (No Tools)")
            output, guardrail_execution_info, annotation = _execute_chat_llm(
                state, messages, config.node_id
            )

        execution_time = time.time() - start_time
        execution_order = state.get("execution_counter", 0) + 1

        # 8. Return success response with annotation
        return {
            "node_outputs": {
                config.node_id: {
                    "output": output,
                    "display_name": config.display_name,
                    "execution_time": execution_time,
                    "execution_order": execution_order,
                    "status": "COMPLETED",
                    "resolved_input": combined_prompt,
                    "search_context": search_context,
                    "chat_enabled": True,
                    "logs": {
                        "search_execution_info": search_execution_info,
                        "guardrails_execution_info": guardrail_execution_info,
                        "tools_execution_info": tools_execution_info
                    },
                }
            },
            "execution_order": [config.node_id],
            "execution_counter": execution_order,
            "annotation": annotation,  # Update annotation in state
        }

    except Exception as e:
        execution_time = time.time() - start_time
        execution_order = state.get("execution_counter", 0) + 1
        logger.exception("Error in chat mode execution")

        return {
            "node_outputs": {
                config.node_id: {
                    "output": None,
                    "display_name": config.display_name,
                    "execution_time": execution_time,
                    "execution_order": execution_order,
                    "status": "FAILED",
                    "error": str(e),
                    "chat_enabled": True,
                    "logs": {
                        "search_execution_info": search_execution_info,
                        "guardrails_execution_info": guardrail_execution_info,
                        "tools_execution_info": tools_execution_info
                    },
                }
            },
            "execution_order": [config.node_id],
            "execution_counter": execution_order,
            "error": f"Node {config.node_id} failed: {str(e)}",
            "failed_node": config.node_id,
        }

def _execute_chat_llm(
    state: WorkflowState,
    messages: list,
    node_id: str = "",
) -> tuple[Any, Dict[str, Any], str]:

    from app.services.workflow_engine.adapters import AIXPLLMGatewayChat
    
    model = AIXPLLMGatewayChat(
        usecase_code=state["usecase_code"],
        transaction_id=str(state["transaction_id"]),
        agent_id=str(state["agent_id"]),
        node_id=node_id,
        jwt_token=state.get("jwt_token", ""),
        environment=state.get("environment", "cre"),
        agent_version=state.get("agent_version"),
        response_format={},  # Chat mode doesn't use structured response format
        context=dict(state),  # Pass state as context for node_id transformation
        )

    # Invoke model - LangGraph handles streaming via callbacks if stream_mode="messages"
    response = model.invoke(messages)
    output = response.content

    # Extract annotation from response additional_kwargs
    annotation = response.additional_kwargs.get("annotation", "")

    # Extract guardrails execution info from response additional_kwargs
    guardrail_execution_info = response.additional_kwargs.get(
        "guardrails_execution_info", {}
    )
    logger.info(
        f"Chat mode - Extracted guardrails execution info: {guardrail_execution_info}"
    )

    return output, guardrail_execution_info, annotation

def _parse_response_format(
    prompt_config: Dict[str, Any], agent_type: str
) -> Dict[str, Any]:
    """Parse response format, return empty dict if empty string."""
    response_format_str = prompt_config.get("response_format", "")

    if not response_format_str:
        return {}

    try:
        parsed = json.loads(response_format_str)
        response_format = _convert_to_json_schema(parsed, agent_type)
        logger.info(f"Parsed response format: {json.dumps(response_format, indent=2)}")
        return response_format
    except Exception as e:
        logger.warning(f"Failed to parse response_format: {e}")
        return {}


def _execute_search(state: WorkflowState, node_id: str) -> Dict[str, Any]:
    """Execute search sub-component."""
    from app.services.external_clients import SearchServiceClient

    client = SearchServiceClient(use_mock=False)
    payload = {
        "usecase_code": state["usecase_code"],
        "agent_id": state["agent_id"],
        "node_id": node_id,
        "transaction_id": state["transaction_id"],
    }

    result = client.search(
        payload,
        jwt_token=state.get("jwt_token"),
        environment=state.get("environment"),
        context=dict(state),
    )

    return result


def _execute_llm(
    state: WorkflowState,
    prompt: str,
    llm_config: Dict[str, Any],
    prompt_config: Dict[str, Any],
    node_id: str,
) -> tuple[Any, Dict[str, Any], str]:
    """
    Execute standard LLM call using AIXPLLMGatewayChat.

    Uses LangChain adapter which supports LangGraph native streaming.
    When streaming is enabled and graph uses stream_mode="messages",
    tokens are automatically streamed via LangChain callbacks.

    Returns:
        tuple: (output, guardrails_info, annotation)
    """
    from app.services.workflow_engine.adapters import AIXPLLMGatewayChat

    logger.info(f"llm config is {llm_config}")
    # Get system prompt
    system_prompt = state.get("system_prompt")

    # Build LangChain messages
    messages = _build_langchain_messages(prompt, system_prompt)

    # Parse response format from prompt_config
    response_format = _parse_response_format(prompt_config, state["agent_type"])

    # Create AIXPLLMGatewayChat with node_id and agent_version for proper key transformation
    model = AIXPLLMGatewayChat(
        usecase_code=state["usecase_code"],
        transaction_id=str(state["transaction_id"]),
        agent_id=str(state["agent_id"]),
        node_id=node_id,
        jwt_token=state.get("jwt_token", ""),
        environment=state.get("environment", "cre"),
        agent_version=state.get("agent_version"),
        response_format=response_format,  # Pass parsed response format
        context=dict(state),  # Pass state as context for node_id transformation
    )

    # Invoke model - LangGraph handles streaming automatically via callbacks
    response = model.invoke(messages)
    output_str = response.content
    logger.info(f"output from llm is {output_str}")

    # Extract annotation from response additional_kwargs
    annotation = response.additional_kwargs.get("annotation", "")

    # Extract guardrails execution info from response additional_kwargs
    guardrails_info = response.additional_kwargs.get("guardrails_execution_info", {})
    logger.info(f"Extracted guardrails execution info: {guardrails_info}")

    # Parse JSON output if response_format was specified
    output = _parse_llm_output(
        output_str if isinstance(output_str, str) else str(output_str)
    )

    return output, guardrails_info, annotation


def _execute_with_tools(
    state: WorkflowState,
    prompt: str,
    llm_config: Dict[str, Any],
    tools_config: Dict[str, Any],
    prompt_config: Dict[str, Any],
    tools_execution_info: dict,
    node_id: str = "",
    history: list|None = None,
) -> tuple[Any, Dict[str, Any], str]:
    """
    Execute LLM with tools using ReAct agent.

    Returns:
        tuple: (output, guardrails_info, annotation)
    """
    import asyncio

    # Get or create event loop
    try:
        loop = asyncio.get_event_loop()
        if loop.is_closed():
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
    except RuntimeError:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)

    return loop.run_until_complete(
        _execute_with_tools_async(
            state, prompt, llm_config, tools_config, prompt_config, tools_execution_info, node_id, history
        )
    )


def _prepare_mcp_config_list(state: WorkflowState) -> List[Dict[str, Any]]:
    """
    Validate and normalize mcp_config from state.

    Args:
        state: Workflow state containing mcp_config

    Returns:
        Normalized list of MCP server configurations
    """
    mcp_config_list = state.get("mcp_config", [])

    if mcp_config_list is None:
        mcp_config_list = []
    elif isinstance(mcp_config_list, dict):
        logger.warning("mcp_config is a dict, expected list. Wrapping in list.")
        mcp_config_list = [mcp_config_list]

    logger.info(
        f"MCP config list contains {len(mcp_config_list)} server configurations"
    )
    return mcp_config_list


async def _initialize_tools(
    mcp_config_list: List[Dict[str, Any]],
    select_mcp_tools: Dict[str, List[str]],
) -> Tuple[Any, List[Any]]:
    """
    Initialize MCPToolManager and get filtered tools.

    Args:
        mcp_config_list: List of MCP server configurations
        select_mcp_tools: Dict mapping MCP names to selected tool lists

    Returns:
        Tuple of (tool_manager, tools)

    Raises:
        Exception: If no tools are available
    """
    from app.services.workflow_engine.mcp_manager import MCPToolManager

    tool_manager = MCPToolManager(
        mcp_config_list=mcp_config_list,
        select_mcp_tools=select_mcp_tools,
    )

    server_info = tool_manager.get_server_info()
    logger.info(f"MCP Tool Manager server info: {server_info}")

    tools = await tool_manager.get_tools()
    if not tools:
        logger.warning("No tools loaded, falling back to standard LLM")
        await tool_manager.close()
        raise ToolsUnavailableError(
        select_mcp_tools=select_mcp_tools,
        available_configs=tool_manager.mcp_configs.keys(),
    )

    logger.info(f"Loaded {len(tools)} tools for ReAct agent: {[t.name for t in tools]}")
    return tool_manager, tools


def _create_react_agent_components(
    state: WorkflowState,
    prompt_config: Dict[str, Any],
    node_id: str,
    tools: List[Any],
) -> Tuple[Any, Any]:
    """
    Create LLM adapter and ReAct agent.

    Args:
        state: Workflow state
        prompt_config: Prompt configuration containing response_format
        node_id: Current node identifier
        tools: List of tools for the agent

    Returns:
        Tuple of (llm, agent)
    """
    from langgraph.prebuilt import create_react_agent

    from app.services.workflow_engine.adapters import AIXPLLMGatewayChat

    response_format = _parse_response_format(prompt_config, state["agent_type"])

    llm = AIXPLLMGatewayChat(
        usecase_code=state["usecase_code"],
        transaction_id=str(state["transaction_id"]),
        agent_id=str(state["agent_id"]),
        node_id=node_id,
        jwt_token=state.get("jwt_token") or "",
        environment=state.get("environment", "cre"),
        agent_version=state.get("agent_version"),
        response_format=response_format,
        context=dict(state),
    )

    system_prompt = state.get("system_prompt", "You are a helpful assistant.")
    agent = create_react_agent(model=llm, tools=tools, prompt=system_prompt)

    logger.info("=" * 80)
    logger.info("REACT AGENT CONFIGURATION")
    logger.info("=" * 80)
    logger.info(f"System Prompt: {system_prompt}")
    logger.info(f"Available Tools: {[t.name for t in tools]}")
    logger.info("=" * 80)

    return llm, agent


async def _stream_agent_execution(agent: Any, input_messages: List[Any], tools_execution_info: dict) -> List[Any]:
    """
    Execute agent with streaming and log intermediate steps.

    Args:
        agent: The ReAct agent to execute
        prompt: User prompt to send to the agent

    Returns:
        List of messages from execution

    Raises:
        Exception: Re-raises any exception after logging captured messages
    """
    from langchain_core.messages import AIMessage, HumanMessage, ToolMessage

    logger.info(f"Executing ReAct agent with {len(input_messages)} messages...")
    messages_so_far = []

    try:
        logger.info("=" * 60)
        logger.info("REACT AGENT INTERMEDIATE STEPS (REAL-TIME)")
        logger.info("=" * 60)

        step_count = 0
        previous_message_count = 0

        async for chunk in agent.astream(
            {"messages": input_messages}, stream_mode="values"
        ):
            logger.info(f"Tool Chunk is {chunk}")
            messages_so_far = chunk.get("messages", [])
            current_message_count = len(messages_so_far)

            if messages_so_far and current_message_count > previous_message_count:
                latest_message = messages_so_far[-1]
                step_count += 1

                _log_full_message_context(
                    messages_so_far,
                    f"[Step {step_count}] - After receiving new message",
                )
                _log_step_message(latest_message, step_count)
                previous_message_count = current_message_count

        logger.info("=" * 60)
        logger.info(f"REACT AGENT COMPLETED - Total steps: {step_count}")
        logger.info("=" * 60)

        _log_full_message_context(
            messages_so_far, "[FINAL] - Complete conversation history sent to LLM"
        )
        
        build_execution_info(messages_so_far, tools_execution_info)
        logger.info(f"Tools Execution Info : {tools_execution_info}")

        return messages_so_far

    except Exception as e:
        logger.error("=" * 60)
        logger.error(
            f"REACT AGENT FAILED - Captured {len(messages_so_far)} messages before failure"
        )
        logger.error(f"Error: {str(e)}")
        logger.error("=" * 60)

        if messages_so_far:
            _log_agent_intermediate_steps(messages_so_far)
            build_execution_info(messages_so_far, tools_execution_info, str(e))
        raise

def build_execution_info(messages: list, tools_execution_info: dict, error: str | None = None) -> dict:
    """Parse ReAct agent messages into structured execution info."""
    tool_data = _extract_tool_data(messages)
    raw_steps = _build_raw_steps(messages)
    grouped = _merge_steps(raw_steps, tool_data, error)
    tools_execution_info["steps"] =  grouped

def _extract_tool_data(messages: list) -> dict:
    """Extract tool call info and results keyed by tool_call_id."""
    data = {}
    for msg in messages:
        if isinstance(msg, AIMessage) and msg.tool_calls:
            for tc in msg.tool_calls:
                data[tc["id"]] = {
                    "tool_name": tc["name"],
                    "tool_id": tc["id"],
                    "tool_args": tc["args"],
                    "type": "tool_call",
                }
        elif isinstance(msg, ToolMessage):
            entry = data.get(msg.tool_call_id, {})
            entry["output"] = _extract_tool_output(msg.content)
            data[msg.tool_call_id] = entry
    return data

def _extract_tool_output(content) -> str:
    """Extract text from tool message content."""
    if isinstance(content, str):
        return content
    if isinstance(content, list):
        return " ".join(
            block["text"] for block in content
            if isinstance(block, dict) and block.get("type") == "text"
        )
    return str(content) 

def _build_raw_steps(messages: list) -> list:
    """Build flat list of step descriptors from messages."""
    steps = []
    for msg in messages:
        if isinstance(msg, HumanMessage):
            steps.append({"type": "human", "details": {"text": msg.content}})
        elif isinstance(msg, AIMessage) and msg.tool_calls:
            for tc in msg.tool_calls:
                steps.append({"type": "tool_call", "tool_call_id": tc["id"]})
        elif isinstance(msg, AIMessage):
            steps.append({"type": "final", "details": {"text": msg.content}})
    return steps

def _merge_steps(raw_steps: list, tool_data: dict, error: str | None = None) -> list:
    """Assign step numbers and build final grouped output."""
    title_map = {
        "human": "Received user message",
        "final": "Final LLM response",
    }
    grouped = []
    for step in raw_steps:
        step_num = len(grouped) + 1
        if step["type"] == "tool_call":
            call_id = step["tool_call_id"]
            info = tool_data.get(call_id, {})
            if "output" not in info:
                info["output"] = ""
                if error:
                    info["error"] = error
            grouped.append({
                "step": step_num,
                "title": f"Tool Call: {info.get('tool_name', 'unknown')} ({call_id})",
                "details": info,
            })
        else:
            grouped.append({
                "step": step_num,
                "title": title_map[step["type"]],
                "details": step["details"],
            })
    return grouped


def _log_step_message(message: Any, step_count: int) -> None:
    """
    Log a single step message during agent execution.

    Args:
        message: The message to log
        step_count: Current step number
    """
    from langchain_core.messages import AIMessage, ToolMessage

    if hasattr(message, "tool_calls") and message.tool_calls:
        logger.info(f"[Step {step_count}] LLM DECIDED TO CALL TOOLS:")
        for tc_idx, tc in enumerate(message.tool_calls):
            logger.info(f"  Tool [{tc_idx + 1}]: {tc.get('name', 'unknown')}")
            logger.info(f"  Tool Call ID: {tc.get('id', 'N/A')}")
            logger.info(f"  Parameters: {json.dumps(tc.get('args', {}), indent=2)}")
        if hasattr(message, "content") and message.content:
            logger.info(f"  Reasoning: {str(message.content)}")

    elif isinstance(message, ToolMessage):
        logger.info(f"[Step {step_count}] TOOL RESPONSE RECEIVED:")
        logger.info(f"  Tool Name: {getattr(message, 'name', 'unknown')}")
        logger.info(f"  Tool Call ID: {getattr(message, 'tool_call_id', 'N/A')}")
        logger.info(f"  Response: {str(message.content) if message.content else ''}...")

    elif isinstance(message, AIMessage) and not (
        hasattr(message, "tool_calls") and message.tool_calls
    ):
        content_preview = str(message.content) if message.content else ""
        logger.info(f"[Step {step_count}] AI RESPONSE (potential final):")
        logger.info(f"  Content: {content_preview}...")


def _extract_tool_execution_result(
    messages: List[Any],
) -> Tuple[Any, Dict[str, Any], str]:
    """
    Extract output, guardrails_info, and annotation from agent messages.

    Args:
        messages: List of messages from agent execution

    Returns:
        Tuple of (output, guardrails_info, annotation)
    """
    logger.info(f"result is {{'messages': {messages}}}")

    final_message = messages[-1]
    output_str = final_message.content
    output = _parse_llm_output(
        output_str if isinstance(output_str, str) else str(output_str)
    )

    logger.info(f"ReAct agent completed. Response length: {len(str(output_str))}")
    logger.info(f"output_type is {type(output)}")
    logger.info(f"type of op from tool is {type(output)}")
    logger.info(f"output from tool is {output}")

    guardrails_info = {}
    annotation = ""
    if hasattr(final_message, "additional_kwargs"):
        guardrails_info = final_message.additional_kwargs.get(
            "guardrails_execution_info", {}
        )
        annotation = final_message.additional_kwargs.get("annotation", "")
        logger.info(
            f"Tools execution - Extracted guardrails execution info: {guardrails_info}"
        )

    return output, guardrails_info, annotation


async def _execute_with_tools_async(
    state: WorkflowState,
    prompt: str,
    llm_config: Dict[str, Any],
    tools_config: Dict[str, Any],
    prompt_config: Dict[str, Any],
    tools_execution_info: dict,
    node_id: str = "",
    history: list|None = None,
) -> Tuple[Any, Dict[str, Any], str]:
    """
    Async execution with tools using LangChain create_react_agent.

    Args:
        state: Workflow state containing mcp_config and other context
        prompt: The resolved prompt to send to the agent
        llm_config: LLM configuration from node properties
        tools_config: Tools sub-component properties containing select_mcp_tools
        prompt_config: Prompt configuration containing response_format
        node_id: Current node identifier

    Returns:
        tuple: (output, guardrails_info, annotation)
    """
    select_mcp_tools = tools_config.get("select_mcp_tools", {})
    logger.info(f'llm_config is {llm_config}')
    logger.info(f"Tools config received: {tools_config}")
    logger.info(f"Extracted select_mcp_tools: {select_mcp_tools}")
    logger.info(f"User Prompt (first 500 chars): {prompt[:500]}...")

    mcp_config_list = _prepare_mcp_config_list(state)
    tool_manager, tools = await _initialize_tools(mcp_config_list, select_mcp_tools)

    try:
        _, agent = _create_react_agent_components(state, prompt_config, node_id, tools)
        if history:
            input_messages = history
        else:
            input_messages = [HumanMessage(content=prompt)]
        messages = await _stream_agent_execution(agent, input_messages, tools_execution_info)
        return _extract_tool_execution_result(messages)
    finally:
        await tool_manager.close()


def _log_system_message(idx: int, msg: Any) -> None:
    """Log a SystemMessage."""
    content_preview = str(msg.content)[:500] if msg.content else ""
    logger.info(f"  [{idx}] SYSTEM MESSAGE:")
    logger.info(f"       Content: {content_preview}...")


def _log_human_message(idx: int, msg: Any) -> None:
    """Log a HumanMessage."""
    content_preview = str(msg.content)[:500] if msg.content else ""
    logger.info(f"  [{idx}] HUMAN MESSAGE (User Prompt):")
    logger.info(f"       Content: {content_preview}...")


def _log_ai_message_tool_calls(idx: int, msg: Any) -> None:
    """Log an AIMessage with tool calls."""
    logger.info(f"  [{idx}] AI MESSAGE (Tool Call Decision):")
    for tc_idx, tc in enumerate(msg.tool_calls):
        tool_name = tc.get("name", "unknown")
        tool_args = tc.get("args", {})
        tool_id = tc.get("id", "N/A")
        logger.info(f"       Tool [{tc_idx + 1}]: {tool_name}")
        logger.info(f"       Tool Call ID: {tool_id}")
        logger.info(f"       Arguments: {json.dumps(tool_args, indent=2)}")
    if msg.content:
        content_preview = str(msg.content)[:300]
        logger.info(f"       Reasoning: {content_preview}")


def _log_ai_message_response(idx: int, msg: Any) -> None:
    """Log an AIMessage response (without tool calls)."""
    content_preview = str(msg.content)[:500] if msg.content else ""
    logger.info(f"  [{idx}] AI MESSAGE (Response):")
    logger.info(f"       Content: {content_preview}...")


def _log_ai_message(idx: int, msg: Any) -> None:
    """Log an AIMessage, dispatching to appropriate handler."""
    if hasattr(msg, "tool_calls") and msg.tool_calls:
        _log_ai_message_tool_calls(idx, msg)
    else:
        _log_ai_message_response(idx, msg)


def _log_tool_message(idx: int, msg: Any) -> None:
    """Log a ToolMessage."""
    tool_name = getattr(msg, "name", "unknown")
    tool_call_id = getattr(msg, "tool_call_id", "N/A")
    content_preview = str(msg.content)[:500] if msg.content else ""
    logger.info(f"  [{idx}] TOOL MESSAGE (Tool Response):")
    logger.info(f"       Tool Name: {tool_name}")
    logger.info(f"       Tool Call ID: {tool_call_id}")
    logger.info(f"       Response: {content_preview}...")


def _log_unknown_message(idx: int, msg: Any) -> None:
    """Log an unknown message type."""
    msg_type = type(msg).__name__
    content = getattr(msg, "content", str(msg))
    content_preview = str(content)[:300] if content else ""
    logger.info(f"  [{idx}] {msg_type}:")
    logger.info(f"       Content: {content_preview}")


def _log_message_by_type(idx: int, msg: Any) -> None:
    """Dispatch message logging to appropriate handler based on type."""
    from langchain_core.messages import (
        AIMessage,
        HumanMessage,
        SystemMessage,
        ToolMessage,
    )

    if isinstance(msg, SystemMessage):
        _log_system_message(idx, msg)
    elif isinstance(msg, HumanMessage):
        _log_human_message(idx, msg)
    elif isinstance(msg, AIMessage):
        _log_ai_message(idx, msg)
    elif isinstance(msg, ToolMessage):
        _log_tool_message(idx, msg)
    else:
        _log_unknown_message(idx, msg)


def _log_full_message_context(messages: List, step_label: str = "") -> None:
    """
    Log the complete message context being sent to the LLM.

    This shows the full accumulated prompt/messages that the LLM sees
    at each step of the ReAct agent execution.

    Args:
        messages: List of all messages in the conversation so far
        step_label: Label to identify which step this is (e.g., "Before Tool Call", "After Tool Response")
    """
    logger.info("=" * 80)
    logger.info(f"FULL MESSAGE CONTEXT {step_label}")
    logger.info(f"Total messages in context: {len(messages)}")
    logger.info("=" * 80)

    for idx, msg in enumerate(messages):
        _log_message_by_type(idx, msg)

    logger.info("=" * 80)
    logger.info(f"END OF MESSAGE CONTEXT {step_label}")
    logger.info("=" * 80)


def _log_step_human_message(step_idx: int, msg: Any) -> None:
    """Log a HumanMessage for intermediate steps."""
    msg_type = type(msg).__name__
    content_preview = str(msg.content)[:300] if msg.content else ""
    logger.info(f"[Step {step_idx}] USER INPUT ({msg_type}):")
    logger.info(f"  Content: {content_preview}...")


def _log_step_ai_tool_call(step_idx: int, msg: Any) -> None:
    """Log an AIMessage with tool calls for intermediate steps."""
    msg_type = type(msg).__name__
    logger.info(f"[Step {step_idx}] LLM DECISION - TOOL CALL ({msg_type}):")
    for tc_idx, tc in enumerate(msg.tool_calls):
        tool_name = tc.get("name", "unknown")
        tool_args = tc.get("args", {})
        tool_id = tc.get("id", "N/A")
        logger.info(f"  Tool [{tc_idx + 1}]: {tool_name}")
        logger.info(f"  Tool Call ID: {tool_id}")
        logger.info(f"  Parameters: {tool_args}")
    if msg.content:
        content_preview = str(msg.content)[:200]
        logger.info(f"  Reasoning: {content_preview}")


def _log_step_ai_response(step_idx: int, msg: Any) -> None:
    """Log an AIMessage final response (without tool calls) for intermediate steps."""
    msg_type = type(msg).__name__
    content_preview = str(msg.content)[:500] if msg.content else ""
    logger.info(f"[Step {step_idx}] FINAL RESPONSE ({msg_type}):")
    logger.info(f"  Content: {content_preview}...")


def _log_step_ai_message(step_idx: int, msg: Any) -> None:
    """Log an AIMessage, dispatching to appropriate handler for intermediate steps."""
    if hasattr(msg, "tool_calls") and msg.tool_calls:
        _log_step_ai_tool_call(step_idx, msg)
    else:
        _log_step_ai_response(step_idx, msg)


def _log_step_tool_response(step_idx: int, msg: Any) -> None:
    """Log a ToolMessage for intermediate steps."""
    msg_type = type(msg).__name__
    tool_name = getattr(msg, "name", "unknown")
    tool_call_id = getattr(msg, "tool_call_id", "N/A")
    content_preview = str(msg.content)[:500] if msg.content else ""
    logger.info(f"[Step {step_idx}] TOOL RESPONSE ({msg_type}):")
    logger.info(f"  Tool Name: {tool_name}")
    logger.info(f"  Tool Call ID: {tool_call_id}")
    logger.info(f"  Response: {content_preview}...")


def _log_step_other_message(step_idx: int, msg: Any) -> None:
    """Log an unknown message type for intermediate steps."""
    msg_type = type(msg).__name__
    content = getattr(msg, "content", str(msg))
    content_preview = str(content)[:200] if content else ""
    logger.info(f"[Step {step_idx}] OTHER MESSAGE ({msg_type}):")
    logger.info(f"  Content: {content_preview}")


def _log_step_by_type(step_idx: int, msg: Any) -> None:
    """Dispatch step logging to appropriate handler based on message type."""
    from langchain_core.messages import AIMessage, HumanMessage, ToolMessage

    if isinstance(msg, HumanMessage):
        _log_step_human_message(step_idx, msg)
    elif isinstance(msg, AIMessage):
        _log_step_ai_message(step_idx, msg)
    elif isinstance(msg, ToolMessage):
        _log_step_tool_response(step_idx, msg)
    else:
        _log_step_other_message(step_idx, msg)


def _log_agent_intermediate_steps(messages: List) -> None:
    """
    Log all intermediate steps from ReAct agent execution.

    This function iterates through all messages in the agent result and logs:
    - User input (HumanMessage)
    - Tool selections and parameters (AIMessage with tool_calls)
    - Tool responses (ToolMessage)
    - Final response (AIMessage without tool_calls)

    Args:
        messages: List of messages from the agent execution result
    """
    logger.info("=" * 60)
    logger.info("REACT AGENT INTERMEDIATE STEPS")
    logger.info("=" * 60)

    for i, msg in enumerate(messages):
        _log_step_by_type(i, msg)

    logger.info("=" * 60)
    logger.info(f"Total steps in agent execution: {len(messages)}")
    logger.info("=" * 60)


def _convert_to_json_schema(data: Any, agent_type: str) -> Dict[str, Any]:
    """Convert hierarchical structure to OpenAI JSON schema."""
    if not data or not isinstance(data, list):
        return {}

    # Build schema for each group
    item_schemas = []
    for group in data:
        if isinstance(group, list) and group:
            group_schema = _build_group_schema(group, agent_type)
            item_schemas.append(group_schema)

    if not item_schemas:
        return {}

    group_count = len(item_schemas)

    return {
        "type": "json_schema",
        "json_schema": {
            "name": "response_schema",
            "strict": True,
            "schema": {
                "type": "array",
                "minItems": group_count,
                "maxItems": group_count,
                "items": item_schemas,
            },
        },
    }


def _build_group_schema(nodes: list, agent_type: str) -> Dict[str, Any]:
    """Build schema for a group of nodes."""
    properties = {}
    required = []

    for node in nodes:
        node_name = node.get("name", "")
        if not node_name:
            continue

        prop_schema = _build_node_property_schema(node, agent_type)
        if prop_schema:
            properties[node_name] = prop_schema
            required.append(node_name)

    return {
        "type": "object",
        "properties": properties,
        "required": required,
        "additionalProperties": False,
    }


def _build_node_property_schema(
    node: Dict[str, Any], agent_type: str
) -> Dict[str, Any]:
    """Build property schema for a single node."""
    node_type = node.get("dataType", "")
    is_multiple = node.get("isMultiple", False)
    children = node.get("child", [])

    # Build the base schema
    if children and node_type == "OBJECT":
        obj_properties = {}
        obj_required = []
        for child in children:
            child_name = child.get("name", "")
            if child_name:
                child_schema = _build_node_property_schema(child, agent_type)
                obj_properties[child_name] = child_schema
                obj_required.append(child_name)

        base_schema = {
            "type": "object",
            "properties": obj_properties,
            "required": obj_required,
            "additionalProperties": False,
        }
    elif agent_type == "DOCUMENT_SEQA" and not children and node_type != "OBJECT":
        schema = _build_doc_seqa_node_property_schema(node_type, is_multiple)
        base_schema = {
            "type": "object",
            "properties": schema,
            "required": list(schema.keys()),
            "additionalProperties": False,
        }

    else:
        type_mapping = {
            "NUMBER": "number",
            "STRING": "string",
            "BOOLEAN": "boolean",
            "OBJECT": "object",
            "ARRAY": "array",
        }
        base_schema = {"type": type_mapping.get(node_type, "string")}

    # Wrap in array if isMultiple
    if is_multiple and agent_type!='DOCUMENT_SEQA':
        return {"type": "array", "items": base_schema}

    return base_schema


def _build_doc_seqa_node_property_schema(node_type, is_multiple: bool):
    type_mapping = {
        "NUMBER": "number",
        "STRING": "string",
        "BOOLEAN": "boolean",
        "OBJECT": "object",
        "ARRAY": "array",
    }

    value_schema = {"type": type_mapping.get(node_type, "string")}

    if is_multiple:
        value_schema = {
            "type": "array",
            "items": value_schema
        }

    return {
        "value": value_schema,
        "confidence_score": {
            "type": "number",
            "description": "Confidence score between 0 and 1"
        }
    }


def _parse_llm_output(output_str: str) -> Any:
    """Parse LLM output, handling JSON and markdown code blocks."""
    logger.info("going inside llm output parsing")
    if not output_str:
        return output_str

    # Clean markdown code blocks
    cleaned = _clean_markdown_json(output_str)

    try:
        return json.loads(cleaned)
    except json.JSONDecodeError:
        logger.info("json decode error happened.")
        return output_str


def _clean_markdown_json(text: str) -> str:
    """Clean markdown code blocks from LLM output."""
    import re

    text = text.strip()

    # Remove ```json ... ``` blocks
    patterns = [
        (r"^```json\s*\n?(.*?)\n?```$", r"\1"),
        (r"^```\s*\n?(.*?)\n?```$", r"\1"),
        (r"^`(.*?)`$", r"\1"),
    ]

    for pattern, replacement in patterns:
        match = re.match(pattern, text, re.DOTALL | re.MULTILINE)
        if match:
            text = re.sub(pattern, replacement, text, flags=re.DOTALL | re.MULTILINE)
            break

    return text.strip()


# =============================================================================
# EVALUATOR SUBCOMPONENT FUNCTIONS
# =============================================================================


def _evaluate_output(
    state: WorkflowState,
    output: Any,
    evaluator_prompt: str,
    eval_llm_config: Dict[str, Any],
    node_id: str,
) -> Tuple[int, str]:
    """
    Evaluate LLM output using the evaluator LLM.

    Args:
        state: Workflow state
        output: The LLM output to evaluate
        evaluator_prompt: Evaluation criteria from config
        eval_llm_config: LLM configuration for the evaluator
        node_id: Node ID (same as parent LLM node)

    Returns:
        Tuple of (score: 1-10, feedback: str)
    """
    from app.services.workflow_engine.adapters import AIXPLLMGatewayChat
    logger.info(f'eval_llm_config is {eval_llm_config}')
    # Build evaluation prompt
    eval_system_prompt = """You are an output evaluator. Your task is to evaluate the given output based on the provided criteria.
You MUST respond with a JSON object containing:
- "score": An integer from 1 to 10 (1 being worst, 10 being best)
- "feedback": Specific feedback explaining the score and suggestions for improvement

Be strict but fair in your evaluation."""

    # Format output for evaluation
    output_str = (
        json.dumps(output, indent=2)
        if isinstance(output, (dict, list))
        else str(output)
    )

    eval_prompt = f"""## Output to Evaluate:
{output_str}

## Evaluation Criteria:
{evaluator_prompt}

Evaluate the output based on the criteria above. Provide a score from 1-10 and specific feedback."""

    messages = _build_langchain_messages(eval_prompt, eval_system_prompt)

    # Create evaluator LLM model with structured response format
    model = AIXPLLMGatewayChat(
        usecase_code=state["usecase_code"],
        transaction_id=str(state["transaction_id"]),
        agent_id=str(state["agent_id"]),
        node_id=node_id,
        jwt_token=state.get("jwt_token", ""),
        environment=state.get("environment", "cre"),
        agent_version=state.get("agent_version"),
        response_format=EVALUATOR_RESPONSE_FORMAT,
        context=dict(state),
    )

    try:
        response = model.invoke(messages)
        response_content = response.content

        logger.info(f"Evaluator response: {response_content}")

        # Parse evaluation response
        eval_result = _parse_llm_output(response_content)

        if isinstance(eval_result, dict):
            score = int(eval_result.get("score", 5))
            feedback = str(eval_result.get("feedback", "No feedback provided"))
        else:
            # If parsing failed, try to extract from string
            logger.warning(
                f"Evaluator response not in expected format: {response_content}"
            )
            score = 5
            feedback = str(response_content)

        # Ensure score is within bounds
        score = max(1, min(10, score))

        logger.info(
            f"Evaluation result - Score: {score}, Feedback: {feedback[:100]}..."
        )
        return score, feedback

    except Exception as e:
        logger.error(f"Error during evaluation: {str(e)}")
        # Return a neutral score on error, allowing retry
        return 5, f"Evaluation error: {str(e)}"


def _build_retry_prompt(
    original_prompt: str,
    previous_output: Any,
    previous_feedback: str,
) -> str:
    """
    Build a retry prompt that includes the original prompt plus feedback from previous attempt.

    Args:
        original_prompt: The original LLM prompt
        previous_output: Output from the previous attempt
        previous_feedback: Feedback from the evaluator

    Returns:
        Enhanced prompt with feedback context
    """
    output_str = (
        json.dumps(previous_output, indent=2)
        if isinstance(previous_output, (dict, list))
        else str(previous_output)
    )

    retry_prompt = f"""{original_prompt}

[Previous attempt output: {output_str}]
[Previous attempt feedback: {previous_feedback}]

Please improve your response based on the feedback above."""

    return retry_prompt


def _format_resolved_input(attempts: List[Dict[str, Any]]) -> str:
    """
    Format all attempts into a single string for resolved_input storage.

    Args:
        attempts: List of attempt dictionaries containing prompt, score, feedback

    Returns:
        Formatted string with all attempts
    """
    parts = []
    for attempt in attempts:
        attempt_num = attempt.get("attempt_number", 0)
        prompt = attempt.get("prompt", "")
        score = attempt.get("score", "N/A")
        feedback = attempt.get("feedback", "N/A")

        part = f"attempt_{attempt_num}: {prompt}"
        if attempt_num < len(attempts):  # Not the last attempt
            part += f"\n[Score: {score}]"
            part += f"\n[Feedback: {feedback}]"

        parts.append(part)

    return "\n---\n".join(parts)


def _execute_with_evaluator(
    state: WorkflowState,
    original_prompt: str,
    llm_config: Dict[str, Any],
    prompt_config: Dict[str, Any],
    node_id: str,
    evaluator_config: Dict[str, Any],
    has_tools: bool = False,
    tools_config: Optional[Dict[str, Any]] = None,
) -> Tuple[Any, Dict[str, Any], str, str, Dict[str, Any] ]:
    """
    Execute LLM with evaluator retry loop.

    This wrapper function adds evaluation and retry logic around the core LLM execution.
    It works with both standard LLM execution and tool-enabled execution.

    Args:
        state: Workflow state
        original_prompt: The resolved prompt (after variable resolution)
        llm_config: LLM configuration from node properties
        prompt_config: Prompt configuration
        node_id: Node ID
        evaluator_config: Evaluator subcomponent configuration
        has_tools: Whether tools are enabled
        tools_config: Tools subcomponent configuration (if has_tools is True)

    Returns:
        Tuple of (output, guardrails_info, annotation, formatted_resolved_input)
    """
    # Extract evaluator settings from config
    eval_prompt_config = evaluator_config.get("prompt", {})
    threshold = eval_prompt_config.get("threshold", 7)
    max_retries = eval_prompt_config.get("max_retries", 3)
    on_failure = str(eval_prompt_config.get("on_failure", "last")).lower()
    evaluator_prompt = eval_prompt_config.get("evaluator_prompt", "")
    eval_llm_config = evaluator_config.get("llm_configurations", llm_config)

    logger.debug("RESOLVED EVALUATOR PROMPT - %s", evaluator_prompt)
    logger.info(
        f"Evaluator config - threshold: {threshold}, max_retries: {max_retries}, "
        f"on_failure: {on_failure}"
    )

    attempts: List[Dict[str, Any]] = []
    best_attempt: Dict[str, Any] = {}
    current_prompt = original_prompt
    last_guardrails_info = {}
    last_annotation = ""
    last_tools_info = {}

    for attempt_num in range(1, max_retries + 1):
        logger.info(f"Evaluator - Starting attempt {attempt_num}/{max_retries}")

        # Execute LLM (with or without tools)
        if has_tools and tools_config:
            output, guardrails_info, annotation = _execute_with_tools(
                state, current_prompt, llm_config, tools_config, prompt_config, last_tools_info, node_id
            )
        else:
            output, guardrails_info, annotation = _execute_llm(
                state, current_prompt, llm_config, prompt_config, node_id
            )

        last_guardrails_info = guardrails_info
        last_annotation = annotation

        # Evaluate the output
        score, feedback = _evaluate_output(
            state, output, evaluator_prompt, eval_llm_config, node_id
        )

        # Store attempt
        attempt = {
            "attempt_number": attempt_num,
            "prompt": current_prompt,
            "output": output,
            "score": score,
            "feedback": feedback,
        }
        attempts.append(attempt)

        logger.info(
            f"Evaluator - Attempt {attempt_num}: Score={score}, Threshold={threshold}"
        )

        # Track best attempt
        if not best_attempt or score > best_attempt.get("score", 0):
            best_attempt = attempt

        # Check if passed threshold
        if score >= threshold:
            logger.info(
                f"Evaluator - Passed threshold on attempt {attempt_num} with score {score}"
            )
            formatted_resolved_input = _format_resolved_input(attempts)
            return output, guardrails_info, annotation, formatted_resolved_input, last_tools_info

        # If not passed and not last attempt, build retry prompt
        if attempt_num < max_retries:
            current_prompt = _build_retry_prompt(original_prompt, output, feedback)
            logger.info(
                f"Evaluator - Building retry prompt for attempt {attempt_num + 1}"
            )

    # Failed to meet threshold after max_retries
    logger.warning(
        f"Evaluator - Failed to meet threshold after {max_retries} attempts. "
        f"Best score: {best_attempt['score']}, on_failure: {on_failure}"
    )

    formatted_resolved_input = _format_resolved_input(attempts)

    if on_failure == "best":
        logger.info(
            f"Evaluator - Returning best attempt (attempt {best_attempt['attempt_number']}, "
            f"score {best_attempt['score']})"
        )
        return (
            best_attempt["output"],
            last_guardrails_info,
            last_annotation,
            formatted_resolved_input,
            last_tools_info
        )
    elif on_failure == "fail":
        logger.info("Evaluator - Returning failure result")
        return (
            {
                "status": "EVALUATION_FAILED",
                "message": f"Failed to meet evaluation threshold ({threshold}) after {max_retries} attempts",
                "best_score": best_attempt["score"],
            },
            last_guardrails_info,
            last_annotation,
            formatted_resolved_input,
            last_tools_info
        )
    else:  # "last" (default)
        logger.info(
            f"Evaluator - Returning last attempt (attempt {attempts[-1]['attempt_number']}, "
            f"score {attempts[-1]['score']})"
        )
        return (
            attempts[-1]["output"],
            last_guardrails_info,
            last_annotation,
            formatted_resolved_input,
            last_tools_info
        )
