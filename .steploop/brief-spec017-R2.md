# Review brief: spec 017 wiki workspace (R2)

You are the SAME independent reviewer as Round 1 (session
20260809_184355_7863f8). Round 1 verdict: FAIL 80/100. The author fixed
every finding. Verify each fix against the live file, then re-score.

## Prior verdict (R1, verbatim)

Verdict: FAIL. Score: 80/100.

Required changes:
1. spec.md:43 — Rewrite R5. The write-back API writes canonical facts. It
   does not write vault files. State: the write-back API is the only
   inbound path for external edits. Renderer output must not flow back
   into canonical memory except through attributable writes.
2. spec.md:38 or 177 — Name the rejection mechanism in R4 or [V-1].
   Push-only sync, no inbound merge path, rebuild discards foreign file
   state.
3. spec.md:159 — Cite ADR-0008 for the NDJSON format claim. 004 R2 does
   not name a format.
4. spec.md:57 — Pair 011 R2 with 001 R9 in R8, as issue #46 does.

Adversarial check (AC3): the [V-1] deferral is honest for the transport,
not for the direction. The spec must name the mechanism now: push-only
sync, no inbound merge, rebuild discards non-renderer file state.

## Tweak checklist (R2)

| R1 finding | Status | Where (verify live) |
| --- | --- | --- |
| 1. R5 inbound-path rewrite | claimed addressed | spec.md R5: "the only inbound path for edits into canonical memory" |
| 2. Rejection mechanism named | claimed addressed | spec.md R4 (push-only, no inbound merge path, rebuild discards non-renderer state) + AC3 |
| 3. ADR-0008 citation for NDJSON | claimed addressed | spec.md Resolved questions 1 |
| 4. R8 pairs 011 R2 with 001 R9 | claimed addressed | spec.md R8 |

Verify each row yourself. Do not trust the "claimed addressed" column.
Re-read the full spec at `specs/017-wiki-workspace/spec.md`.

## Re-verify (run these)

1. Re-read the full spec. Confirm the R4/R5 contradiction is gone: R4
   owns the outbound path (renderer → vault, push-only); R5 owns the
   inbound path (edits → canonical memory, write-back API only).
2. Confirm AC3 is now observable (push-only + no inbound merge path).
3. Confirm the ADR-0008 citation: `grep -n "NDJSON" docs/decisions/0008-durable-export-and-scoped-deletion.md`.
4. Confirm the R8 citation pair matches issue #46: `gh issue view 46`.
5. Check the fixes introduced no new contradiction (e.g. R5 wording vs
   R4 wording, AC3 vs the out-of-scope bullet on direct file edits).
6. STE100: sentences at most 20 words, no "etc.", no gerund forms.
7. Invite NEW findings: the fix round may have introduced new defects.

## Verdict contract

Write `.steploop/REVIEW-spec017-R2.md` with verdict PASS or FAIL, score
/100 (90-100 landable), verified-against file list, strengths (2-4
bullets), weaknesses (2-4 bullets), required changes (numbered). End
your reply with exactly `VERDICT: PASS` or `VERDICT: FAIL`. Reply cap
3 KB, verdict file cap 4 KB. READ-ONLY: write only the verdict file.
