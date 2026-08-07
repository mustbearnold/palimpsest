# Conventions

Formatting and style law. Conflicts resolve after the constitution and before
specs: constitution → conventions → specs → code comments.

## Documents (Markdown)

- ATX headings (`#`), exactly one H1 per file (the title), sentence case.
- Blank line before and after headings, lists, and code fences.
- Code fences always declare a language.
- No hard line-wrapping inside paragraphs; let editors soft-wrap.
- Tables only for genuinely tabular data; otherwise lists or prose.
- Relative links only within the repo. Kebab-case filenames:
  `spec-extraction.md`.
- Spec files follow this template: Status, Owner, Purpose, Requirements
  (RFC 2119 MUST/SHOULD/MAY), Acceptance criteria (Given/When/Then), Out of
  scope, Open questions, Links. Inferred requirements are tagged `[inferred]`.

## Simplified Technical English (STE-100)

Founder directive (2026-08-07): all prose in this repo uses ASD-STE100
Simplified Technical English. Scope: specs, ADRs, runbooks, READMEs, commit
messages, issue comments, and agent replies about Palimpsest work.

Core rules:

- Use only the approved STE vocabulary. One word has one meaning.
- A sentence has at most 20 words. A sentence has one idea.
- Use active voice. Procedures use the imperative.
- Use the simple present tense.
- No gerunds, no synonyms, no "etc.", no "e.g.", no "i.e.".
- Use "a", "an", and "the".
- Technical names stay unchanged.

Code is exempt: formatter output is law, and identifiers, test names,
verbatim strings, and direct quotes keep their exact form.

## Code

Formatter output is law. Never hand-format; never argue with the formatter.

| Language | Formatter | Linter |
| --- | --- | --- |
| Rust | cargo fmt | clippy (`-D warnings`) |
| Python | ruff format | ruff check |
| TypeScript / JavaScript | Prettier | ESLint |
| Shell | shfmt | shellcheck |
| JSON / YAML | Prettier | — |

Configs are committed. An `.editorconfig` at root sets charset/indent/EOL for
everything else (already present: UTF-8, LF, final newline, two-space indent,
four-space for Rust, trailing whitespace trimmed).

## Commits

`type(scope): imperative summary` — types: feat, fix, refactor, docs, test,
chore, sdd. One logical change per commit. Formatting commits contain no
logic. Migration commits use `sdd(phase-N): <summary>`.

## Naming

Docs and spec slugs: kebab-case. Code identifiers: the language's standard.
Spec/ADR numbers: zero-padded, never reused.
