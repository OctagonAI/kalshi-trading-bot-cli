"""
Utilities for working with OpenAI Responses API (GPT-5 compatible).

This module provides helpers to:
- Create responses with message-style input
- Extract the completed assistant message (handles reasoning-first outputs)
- Parse structured outputs into Pydantic models using Responses API
"""

from typing import Any, Dict, List, Optional, Sequence, Tuple, Type, TypeVar, Union, cast
import json

from pydantic import BaseModel


T = TypeVar("T", bound=BaseModel)


def _normalize_messages_input(messages: Sequence[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Convert chat-style messages into Responses API input format (list of role/content dicts).

    Accepts already-correct structures and passes them through unchanged.
    """
    normalized: List[Dict[str, Any]] = []
    for msg in messages:
        # If content is already a list of parts, keep as-is; otherwise wrap text
        content = msg.get("content")
        if isinstance(content, list):
            normalized.append({"role": msg.get("role", "user"), "content": content})
        else:
            normalized.append({"role": msg.get("role", "user"), "content": str(content) if content is not None else ""})
    return normalized


def extract_completed_message_text(response: Any) -> str:
    """
    Extract plain text from the completed assistant message in a Responses API result.

    Handles outputs that include a reasoning step before the completed message.
    Returns an empty string if nothing is found.
    """
    text_chunks: List[str] = []

    try:
        output_items = getattr(response, "output", None) or response.get("output", [])  # type: ignore[attr-defined]
    except Exception:
        output_items = []

    for item in output_items or []:
        # SDK objects expose attributes; JSON dicts expose keys – support both
        item_type = getattr(item, "type", None) or (isinstance(item, dict) and item.get("type"))
        item_status = getattr(item, "status", None) or (isinstance(item, dict) and item.get("status"))
        if item_type == "message" and item_status == "completed":
            content = getattr(item, "content", None) or (isinstance(item, dict) and item.get("content")) or []
            for part in content or []:
                part_type = getattr(part, "type", None) or (isinstance(part, dict) and part.get("type"))
                if part_type == "output_text":
                    # SDK: part.text; JSON: part["text"]
                    text_value = getattr(part, "text", None) or (isinstance(part, dict) and part.get("text"))
                    if isinstance(text_value, str) and text_value:
                        text_chunks.append(text_value)
            break

    return "".join(text_chunks).strip()


def extract_completed_message_with_annotations(response: Any) -> Tuple[str, List[Dict[str, Any]]]:
    """
    Extract text AND annotations from a completed assistant message in a Responses API result.
    
    Octagon Deep Research returns annotations with source citations:
    - annotation.order: Citation number (1, 2, 3...)
    - annotation.name: Source name (e.g., "Federal Reserve Press Release, Jan 2026")
    - annotation.url: Direct URL to the source
    
    Returns:
        Tuple of (text, annotations) where annotations is a list of dicts with order, name, url
    """
    text_chunks: List[str] = []
    annotations: List[Dict[str, Any]] = []

    try:
        output_items = getattr(response, "output", None) or response.get("output", [])  # type: ignore[attr-defined]
    except Exception:
        output_items = []

    for item in output_items or []:
        # SDK objects expose attributes; JSON dicts expose keys – support both
        item_type = getattr(item, "type", None) or (isinstance(item, dict) and item.get("type"))
        item_status = getattr(item, "status", None) or (isinstance(item, dict) and item.get("status"))
        if item_type == "message" and item_status == "completed":
            content = getattr(item, "content", None) or (isinstance(item, dict) and item.get("content")) or []
            for part in content or []:
                part_type = getattr(part, "type", None) or (isinstance(part, dict) and part.get("type"))
                if part_type == "output_text":
                    # Extract text
                    text_value = getattr(part, "text", None) or (isinstance(part, dict) and part.get("text"))
                    if isinstance(text_value, str) and text_value:
                        text_chunks.append(text_value)
                    
                    # Extract annotations
                    part_annotations = getattr(part, "annotations", None) or (isinstance(part, dict) and part.get("annotations")) or []
                    for ann in part_annotations:
                        try:
                            # Handle SDK objects
                            order = getattr(ann, "order", None) or (isinstance(ann, dict) and ann.get("order"))
                            name = getattr(ann, "name", None) or (isinstance(ann, dict) and ann.get("name"))
                            url = getattr(ann, "url", None) or (isinstance(ann, dict) and ann.get("url"))
                            
                            if order is not None and url:
                                annotations.append({
                                    "order": int(order),
                                    "name": str(name) if name else f"Source {order}",
                                    "url": str(url)
                                })
                        except Exception:
                            # Skip malformed annotations
                            pass
            break

    # Sort annotations by order
    annotations.sort(key=lambda x: x["order"])
    
    return "".join(text_chunks).strip(), annotations


async def responses_create_text(
    client: Any,
    *,
    model: str,
    messages: Sequence[Dict[str, Any]],
    reasoning_effort: str = "low",
    text_verbosity: str = "medium",
) -> str:
    """
    Create a Responses API call for free-form text and extract the assistant text.

    Falls back to an empty string if no completed message is present.
    """
    normalized = _normalize_messages_input(messages)
    response = await client.responses.create(
        model=model,
        input=normalized,
        reasoning={"effort": reasoning_effort},
        text={"verbosity": text_verbosity},
    )

    return extract_completed_message_text(response)


async def responses_parse_pydantic(
    client: Any,
    *,
    model: str,
    messages: Sequence[Dict[str, Any]],
    response_format: Type[T],
    reasoning_effort: str = "low",
    text_verbosity: str = "medium",
) -> T:
    """
    Parse structured output using Responses API into the provided Pydantic model type.

    Supports SDK objects exposing `output_parsed` or dicts with the same key.
    As a fallback, will attempt to locate a completed message and read a `.parsed` field
    if present on that message (for compatibility with interim SDK behaviors).
    """
    normalized = _normalize_messages_input(messages)

    # Build JSON schema from the Pydantic model
    try:
        schema = response_format.model_json_schema()
    except Exception:
        # Pydantic v1 fallback
        schema = response_format.schema()  # type: ignore[attr-defined]

    # Inject a strict schema instruction to ensure JSON-only output
    schema_str = json.dumps(schema)
    schema_instruction = {
        "role": "system",
        "content": (
            "You must respond with ONLY a single JSON object that validates against the following JSON Schema. "
            "Do not include any prose, code fences, or additional text. If a field is optional, omit it instead of writing null.\n\n"
            f"JSON Schema: {schema_str}"
        ),
    }

    # Prepend schema instruction
    normalized_with_schema = [schema_instruction] + list(normalized)

    resp = await client.responses.create(
        model=model,
        input=normalized_with_schema,
        reasoning={"effort": reasoning_effort},
        text={"verbosity": text_verbosity},
    )

    # Parse JSON from the completed message text
    text_value = extract_completed_message_text(resp)
    if text_value:
        try:
            data = json.loads(text_value)
            try:
                return cast(T, response_format.model_validate(data))
            except Exception:
                return cast(T, response_format.parse_obj(data))  # type: ignore[attr-defined]
        except Exception as exc:
            raise RuntimeError(f"Structured output parsing failed: invalid JSON in model output: {exc}")

    raise RuntimeError("Structured output parsing failed: no output_parsed or JSON text found in Responses API result.")


