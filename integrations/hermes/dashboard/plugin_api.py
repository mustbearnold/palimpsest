"""FastAPI backend router for the Palimpsest Hermes plugin.

Mounted by the Hermes web/gateway server at ``/api/plugins/palimpsest`` and
reached from the desktop app via ``ctx.rest`` (and the web dashboard via
``fetch``). Loaded as a top-level module (``hermes_dashboard_plugin_*``), so
sibling plugin modules are loaded explicitly by file path with unique names —
never by relative import.

Endpoints are content-free for status and scoped to explicit user intent for
recall and remember. The bearer token is never returned.
"""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path
from typing import Any

_PLUGIN_ROOT = Path(__file__).resolve().parent.parent


def _load_sibling(name: str) -> Any:
    module_name = f"palimpsest_hermes_{name}"
    cached = sys.modules.get(module_name)
    if cached is not None:
        return cached
    spec = importlib.util.spec_from_file_location(
        module_name, _PLUGIN_ROOT / f"{name}.py"
    )
    if spec is None or spec.loader is None:
        raise ImportError(f"cannot load palimpsest plugin module {name}")
    mod = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = mod
    spec.loader.exec_module(mod)
    return mod


_client_mod = _load_sibling("client")
PalimpsestClient = _client_mod.PalimpsestClient
PalimpsestConfig = _client_mod.PalimpsestConfig
PalimpsestError = _client_mod.PalimpsestError

from fastapi import APIRouter
from pydantic import BaseModel, Field

router = APIRouter()


def _hermes_home() -> str:
    try:
        from hermes_constants import get_hermes_home

        return str(get_hermes_home())
    except Exception:  # noqa: BLE001 - import fallback must never raise
        import os

        return os.environ.get("HERMES_HOME") or str(Path.home() / ".hermes")


def _config() -> PalimpsestConfig:
    return PalimpsestConfig.load(_hermes_home())


class RecallBody(BaseModel):
    query: str = Field(min_length=1, max_length=4096)
    top_k: int = Field(default=8, ge=1, le=50)


class RememberBody(BaseModel):
    content: str = Field(min_length=1, max_length=65536)
    key: str | None = Field(default=None, max_length=512)
    metadata: dict = Field(default_factory=dict)


@router.get("/status")
def status() -> dict:
    """Endpoint, scope, and reachability. Content-free, token never exposed."""
    config = _config()
    client = PalimpsestClient(config)
    return {**config.public_dict(), "reachable": client.health()}


@router.post("/recall")
def recall(body: RecallBody) -> dict:
    """Authorized current retrieval; returns trimmed receipt items."""
    try:
        receipt = PalimpsestClient(_config()).recall(body.query, page_size=body.top_k)
    except PalimpsestError as exc:
        return {"error": str(exc)}
    items = receipt.get("items")
    trimmed = []
    if isinstance(items, list):
        for item in items:
            if not isinstance(item, dict):
                continue
            trimmed.append(
                {
                    "memory_kind": item.get("memory_kind"),
                    "fact_id": item.get("fact_id"),
                    "namespace": item.get("namespace"),
                    "key": item.get("key"),
                    "value": item.get("value"),
                }
            )
    return {
        "retrieval_id": receipt.get("retrieval_id"),
        "status": receipt.get("status"),
        "items": trimmed,
    }


@router.post("/remember")
def remember(body: RememberBody) -> dict:
    """Explicit user-approved write: episode plus governed direct-evidence fact."""
    client = PalimpsestClient(_config())
    try:
        observed = client.remember(
            body.content,
            key=body.key,
            metadata=body.metadata,
            kind="hermes_desktop",
            source_type="hermes.desktop",
            namespace=_config().namespace,
        )
    except PalimpsestError as exc:
        return {"error": str(exc)}
    episode = observed["episode"]
    fact = observed["fact"]
    return {
        "status": "saved",
        "episode_id": episode.get("episode_id") or episode.get("id"),
        "fact_id": fact.get("fact_id") or fact.get("id") or fact.get("revision_id"),
    }
