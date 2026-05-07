"""
LLM client factory.

All agents import from here rather than instantiating SDK clients directly.
Auth is resolved from CODEX_AUTH_JSON in the environment:

  CODEX_AUTH_JSON='{"api_key": "...", "base_url": "https://..."}'

The client exposes a single method that mirrors the Anthropic messages.create
interface so call sites are unchanged:

  from llm.client import chat
  response = chat(model=..., system=..., messages=[...], max_tokens=...)
  text = response.content   # always a plain string
"""

from __future__ import annotations

import json
import logging
import os
from dataclasses import dataclass

logger = logging.getLogger(__name__)


@dataclass
class ChatResponse:
    content: str
    model: str
    input_tokens: int
    output_tokens: int


def chat(
    *,
    model: str,
    system: str,
    messages: list[dict],
    max_tokens: int = 4096,
) -> ChatResponse:
    """
    Sends a chat completion request via the Codex (OpenAI-compatible) API.

    Reads auth from CODEX_AUTH_JSON environment variable.
    Falls back to OPENAI_API_KEY if CODEX_AUTH_JSON is not set (test convenience).

    Args:
        model:      model identifier string
        system:     system prompt
        messages:   list of {"role": ..., "content": ...} dicts
        max_tokens: maximum tokens in the response

    Returns:
        ChatResponse with .content (str), .model, .input_tokens, .output_tokens
    """
    client = _build_client()

    # Prepend system message in OpenAI format
    full_messages = [{"role": "system", "content": system}] + messages

    logger.debug("Calling Codex model=%s max_tokens=%d", model, max_tokens)

    response = client.chat.completions.create(
        model=model,
        messages=full_messages,
        max_tokens=max_tokens,
    )

    choice = response.choices[0]
    usage = response.usage

    return ChatResponse(
        content=choice.message.content,
        model=response.model,
        input_tokens=usage.prompt_tokens if usage else 0,
        output_tokens=usage.completion_tokens if usage else 0,
    )


def _build_client():
    """Builds an OpenAI client from CODEX_AUTH_JSON or OPENAI_API_KEY."""
    from openai import OpenAI

    raw = os.environ.get("CODEX_AUTH_JSON")
    if raw:
        try:
            auth = json.loads(raw)
        except json.JSONDecodeError as exc:
            raise ValueError(
                "CODEX_AUTH_JSON is not valid JSON. "
                "Expected format: '{\"api_key\": \"...\", \"base_url\": \"https://...\"}'"
            ) from exc

        api_key = auth.get("api_key")
        base_url = auth.get("base_url")  # optional; defaults to OpenAI

        if not api_key:
            raise ValueError("CODEX_AUTH_JSON must contain an 'api_key' field.")

        return OpenAI(api_key=api_key, base_url=base_url) if base_url else OpenAI(api_key=api_key)

    # Fallback: standard OPENAI_API_KEY (useful in CI without CODEX_AUTH_JSON)
    api_key = os.environ.get("OPENAI_API_KEY")
    if api_key:
        logger.warning("CODEX_AUTH_JSON not set; falling back to OPENAI_API_KEY.")
        return OpenAI(api_key=api_key)

    raise EnvironmentError(
        "No LLM credentials found. Set CODEX_AUTH_JSON or OPENAI_API_KEY."
    )


# ── Model aliases ─────────────────────────────────────────────────────────────
# Keep model names in one place so agents don't hard-code them.

CODEX_LARGE  = "gpt-4.1"        # heavy reasoning: deviation detector, anomaly remediations
CODEX_MEDIUM = "gpt-4.1-mini"   # classification and summarisation: classifier, changelog
