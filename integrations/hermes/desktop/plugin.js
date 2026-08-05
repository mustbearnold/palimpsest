/**
 * Palimpsest memory pane for Hermes Desktop.
 *
 * Install: copy/symlink this file to
 *   <hermes home>/desktop-plugins/palimpsest/plugin.js
 * then run "Reload desktop plugins" from ⌘K.
 *
 * Talks to the plugin backend (dashboard/plugin_api.py) via ctx.rest, which
 * is scoped to /api/plugins/palimpsest by construction. The backend runs in
 * the Hermes gateway process; the pane never sees the bearer token.
 *
 * Plain ESM, loaded uncompiled — UI is jsx() calls, not JSX syntax. Only
 * @hermes/plugin-sdk, react, and react/jsx-runtime resolve.
 */

import { cn, haptic } from "@hermes/plugin-sdk";
import { useEffect, useState } from "react";
import { jsx, jsxs } from "react/jsx-runtime";

const ID = "palimpsest";

function StatusRow({ status }) {
  if (!status) {
    return jsx("div", {
      className: "text-(--ui-text-quaternary)",
      children: "status unavailable — is the gateway running?",
    });
  }
  const dot = status.reachable
    ? "bg-(--ui-accent)"
    : "bg-(--ui-stroke-secondary)";
  return jsxs("div", {
    className: "flex flex-col gap-1 text-xs",
    children: [
      jsxs("div", {
        className: "flex items-center gap-2",
        children: [
          jsx("span", {
            className: cn("inline-block size-2 rounded-full", dot),
          }),
          jsx("span", {
            className: "truncate font-mono",
            children: status.base_url,
          }),
          jsx("span", {
            className: "text-(--ui-text-tertiary)",
            children: status.reachable ? "reachable" : "unreachable",
          }),
        ],
      }),
      jsx("div", {
        className: "text-(--ui-text-tertiary)",
        children: `tenant ${String(status.tenant_id ?? "").slice(0, 8)}… · subject ${String(status.subject_id ?? "").slice(0, 8)}… · ns ${status.namespace ?? ""}`,
      }),
    ],
  });
}

function RecallResults({ items }) {
  if (!items || items.length === 0) {
    return jsx("div", {
      className: "text-(--ui-text-quaternary) text-xs",
      children: "no results",
    });
  }
  return jsx("div", {
    className: "flex flex-col gap-2",
    children: items.map((item, i) => {
      const value = item.value;
      const text = value && typeof value === "object" ? value.content : value;
      const caption = item.key || item.fact_id || "";
      return jsxs("div", {
        key: i,
        className: "rounded-md border border-(--ui-stroke-secondary) p-2",
        children: [
          jsx("div", {
            className: "truncate text-[0.6875rem] text-(--ui-text-tertiary)",
            children: caption,
          }),
          jsx("div", {
            className:
              "mt-0.5 line-clamp-3 break-words whitespace-pre-wrap text-xs",
            children: typeof text === "string" ? text : JSON.stringify(text),
          }),
        ],
      });
    }),
  });
}

function PalimpsestPane({ rest }) {
  const [status, setStatus] = useState(null);
  const [statusError, setStatusError] = useState("");
  const [query, setQuery] = useState("");
  const [items, setItems] = useState(null);
  const [recalling, setRecalling] = useState(false);
  const [recallError, setRecallError] = useState("");
  const [content, setContent] = useState("");
  const [saved, setSaved] = useState("");
  const [saving, setSaving] = useState(false);
  const [saveError, setSaveError] = useState("");

  useEffect(() => {
    let cancelled = false;
    rest("/status", { method: "GET", timeoutMs: 5000 })
      .then((res) => {
        if (!cancelled) setStatus(res);
      })
      .catch((err) => {
        if (!cancelled) {
          setStatus(null);
          setStatusError(String(err?.message ?? err));
        }
      });
    return () => {
      cancelled = true;
    };
  }, [rest]);

  const doRecall = async () => {
    if (!query.trim()) return;
    setRecalling(true);
    setRecallError("");
    setItems(null);
    try {
      const res = await rest("/recall", {
        method: "POST",
        body: { query: query.trim(), top_k: 8 },
        timeoutMs: 20000,
      });
      setItems(res.items ?? []);
    } catch (err) {
      setItems([]);
      setRecallError(String(err?.message ?? err));
    } finally {
      setRecalling(false);
    }
  };

  const doRemember = async () => {
    if (!content.trim()) return;
    setSaving(true);
    setSaveError("");
    setSaved("");
    try {
      const res = await rest("/remember", {
        method: "POST",
        body: { content: content.trim() },
        timeoutMs: 20000,
      });
      if (res.error) {
        setSaveError(res.error);
      } else {
        setSaved(
          `saved — episode ${String(res.episode_id ?? "").slice(0, 8)}… fact ${String(res.fact_id ?? "").slice(0, 8)}…`,
        );
        setContent("");
      }
    } catch (err) {
      setSaveError(String(err?.message ?? err));
    } finally {
      setSaving(false);
    }
  };

  return jsxs("div", {
    className: "flex h-full flex-col gap-3 overflow-y-auto p-3 text-sm",
    children: [
      jsxs("div", {
        className: "flex items-center justify-between gap-2",
        children: [
          jsx("div", {
            className: "font-medium",
            children: "Palimpsest Memory",
          }),
          jsx("button", {
            className:
              "text-xs text-(--ui-text-tertiary) hover:text-foreground",
            type: "button",
            onClick: () => {
              haptic("tap");
              rest("/status", { method: "GET", timeoutMs: 5000 })
                .then(setStatus)
                .catch((err) => setStatusError(String(err?.message ?? err)));
            },
            children: "refresh",
          }),
        ],
      }),
      statusError
        ? jsx("div", {
            className: "text-xs text-(--ui-text-secondary)",
            children: `⚠ ${statusError}`,
          })
        : jsx(StatusRow, { status }),
      jsx("div", { className: "border-t border-(--ui-stroke-secondary)" }),
      jsxs("div", {
        className: "flex gap-2",
        children: [
          jsx("input", {
            className:
              "min-w-0 flex-1 rounded-md border border-(--ui-stroke-secondary) bg-transparent px-2 py-1 text-xs outline-none",
            placeholder: "Recall — search saved memory…",
            value: query,
            onKeyDown: (e) => {
              if (e.key === "Enter") doRecall();
            },
            onChange: (e) => setQuery(e.target.value),
          }),
          jsx("button", {
            className:
              "rounded-md bg-(--ui-accent) px-2 py-1 text-xs text-white disabled:opacity-50",
            type: "button",
            disabled: recalling || !query.trim(),
            onClick: doRecall,
            children: recalling ? "…" : "Recall",
          }),
        ],
      }),
      recallError
        ? jsx("div", {
            className: "text-xs text-(--ui-text-secondary)",
            children: `⚠ ${recallError}`,
          })
        : jsx(RecallResults, { items }),
      jsx("div", { className: "border-t border-(--ui-stroke-secondary)" }),
      jsx("textarea", {
        className:
          "min-h-20 rounded-md border border-(--ui-stroke-secondary) bg-transparent px-2 py-1 text-xs outline-none",
        placeholder:
          "Remember — save an explicit memory (only when the user asks)…",
        value: content,
        onChange: (e) => setContent(e.target.value),
      }),
      jsxs("div", {
        className: "flex items-center gap-2",
        children: [
          jsx("button", {
            className:
              "rounded-md bg-(--ui-accent) px-2 py-1 text-xs text-white disabled:opacity-50",
            type: "button",
            disabled: saving || !content.trim(),
            onClick: doRemember,
            children: saving ? "…" : "Remember",
          }),
          saved
            ? jsx("span", {
                className: "text-xs text-(--ui-text-tertiary)",
                children: saved,
              })
            : null,
          saveError
            ? jsx("span", {
                className: "text-xs text-(--ui-text-secondary)",
                children: `⚠ ${saveError}`,
              })
            : null,
        ],
      }),
      jsx("div", {
        className: "mt-auto text-[0.6875rem] text-(--ui-text-quaternary)",
        children:
          "Palimpsest is general-purpose memory infrastructure — this pane talks to the same HTTP service as the Codex MCP adapter.",
      }),
    ],
  });
}

export default {
  id: ID,
  name: "Palimpsest Memory",
  register(ctx) {
    ctx.register({
      id: "palimpsest-pane",
      area: "panes",
      title: "Palimpsest",
      data: { placement: "right", width: "380px" },
      render: () => jsx(PalimpsestPane, { rest: ctx.rest }),
    });
  },
};
