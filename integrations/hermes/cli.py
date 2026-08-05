"""``hermes palimpsest`` CLI subcommands.

Registered through the convention-based memory-plugin discovery
(``discover_plugin_cli_commands``): commands appear only while ``palimpsest``
is the active memory provider. Importable standalone for tests (the
``.client`` relative import falls back to a top-level import).

Commands: ``status``, ``config``, ``recall <query>``, ``remember <content>``.
Never prints the bearer token or raw private memory beyond requested recall
results.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Any

try:  # loaded as a Hermes memory plugin submodule
    from .client import (
        RECALL_TOP_K_MAX,
        RECALL_TOP_K_MIN,
        PalimpsestClient,
        PalimpsestConfig,
        PalimpsestError,
        _episode_id,
        _fact_id,
        format_receipt,
        resolve_hermes_home,
    )
except ImportError:  # standalone import (tests, direct execution)
    from client import (
        RECALL_TOP_K_MAX,
        RECALL_TOP_K_MIN,
        PalimpsestClient,
        PalimpsestConfig,
        PalimpsestError,
        _episode_id,
        _fact_id,
        format_receipt,
        resolve_hermes_home,
    )

_TOP_K_DEFAULT = 8


def _print_json(value: Any) -> None:
    print(json.dumps(value, indent=2, sort_keys=True))


def _status() -> None:
    config = PalimpsestConfig.load(resolve_hermes_home())
    client = PalimpsestClient(config)
    payload = {**config.public_dict(), "reachable": client.health()}
    print("Palimpsest memory provider")
    print(
        f"  endpoint:  {config.base_url}  ({'reachable' if payload['reachable'] else 'UNREACHABLE'})"
    )
    print(f"  tenant:    {config.tenant_id}")
    print(f"  subject:   {config.subject_id}")
    print(f"  case:      {config.case_id}")
    print(f"  namespace: {config.namespace}")
    print(f"  token:     {'configured' if config.bearer_token else 'missing'}")


def _config() -> None:
    home = resolve_hermes_home()
    config = PalimpsestConfig.load(home)
    _print_json(
        {**config.public_dict(), "config_file": str(Path(home) / "palimpsest.json")}
    )


def _recall(query: str, top_k: int) -> None:
    config = PalimpsestConfig.load(resolve_hermes_home())
    client = PalimpsestClient(config)
    receipt = client.recall(query, page_size=top_k)
    print(format_receipt(receipt) or "[Palimpsest Memory]\n- (no results)")
    print(f"\nretrieval_id: {receipt.get('retrieval_id')}")


def _remember(content: str, key: str | None) -> None:
    config = PalimpsestConfig.load(resolve_hermes_home())
    client = PalimpsestClient(config)
    observed = client.remember(
        content,
        key=key,
        kind="hermes_cli",
        source_type="hermes.cli",
        namespace=config.namespace,
    )
    episode_id = _episode_id(observed["episode"])
    fact_id = _fact_id(observed["fact"])
    print(f"saved episode {episode_id}, fact {fact_id}")


def palimpsest_command(args: Any) -> None:
    """Handler dispatched by argparse (convention: ``<provider>_command``)."""
    sub = getattr(args, "palimpsest_subcommand", None)
    try:
        if sub == "status":
            _status()
        elif sub == "config":
            _config()
        elif sub == "recall":
            query = getattr(args, "query", "")
            top_k = int(getattr(args, "top_k", _TOP_K_DEFAULT) or _TOP_K_DEFAULT)
            top_k = max(RECALL_TOP_K_MIN, min(top_k, RECALL_TOP_K_MAX))
            _recall(query, top_k)
        elif sub == "remember":
            content = getattr(args, "content", "")
            _remember(content, getattr(args, "key", None))
        else:
            print(
                "Usage: hermes palimpsest <status|config|recall|remember>",
                file=sys.stderr,
            )
    except PalimpsestError as exc:
        print(f"palimpsest error: {exc}", file=sys.stderr)
        sys.exit(1)


def register_cli(subparser: Any) -> None:
    """Build the ``hermes palimpsest`` argparse tree (convention-based)."""
    subs = subparser.add_subparsers(dest="palimpsest_subcommand")
    subs.add_parser("status", help="Show endpoint, scope, and reachability")
    subs.add_parser("config", help="Show resolved configuration (token redacted)")
    recall = subs.add_parser("recall", help="Search saved memory")
    recall.add_argument("query", help="What to search for")
    recall.add_argument(
        "--top-k",
        type=int,
        default=_TOP_K_DEFAULT,
        help=f"Max results (default: {_TOP_K_DEFAULT})",
    )
    remember = subs.add_parser("remember", help="Save an explicit memory")
    remember.add_argument("content", help="The memory to save")
    remember.add_argument("--key", default=None, help="Optional fact key")
    subparser.set_defaults(func=palimpsest_command)
