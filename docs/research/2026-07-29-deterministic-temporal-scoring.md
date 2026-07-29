# Deterministic temporal scoring through 2027

Status: research recommendation
Research date: 2026-07-29
Resolves: [GitHub issue #21](https://github.com/mustbearnold/palimpsest/issues/21)

## Decision

Define temporal scoring in a versioned pure-Rust domain module that uses checked
integer arithmetic and one explicit round-to-nearest, ties-to-even operation.
PostgreSQL remains the authority for bitemporal selection, authorization-first
eligibility, channel ranks, and durable `numeric(20, 12)` score storage, but it
must not be the normative rounding or exponential-math implementation.

For `active-case-30d-v1`, define age as an integer count of elapsed
microseconds on the request's valid-time axis and use the continuous factor

```text
age_us = max(0, valid_at_us - recency_anchor_at_us)
half_life_us = 30 * 86_400 * 1_000_000 = 2_592_000_000_000
raw_decay = 2 ^ (-age_us / half_life_us)
temporal_factor = max(policy_floor, raw_decay)
```

`stable-v1` returns exactly `1.000000000000`. The active profile should evaluate
the formula with a policy-owned Q63 lookup algorithm described below, then
quantize once to exactly 12 decimal places with half-even rounding. The policy
document and digest must own the time unit, half-life, floor, recency-anchor
rule, internal arithmetic version, constants, operation order, rounding points,
score scale, and every ordering direction and null placement.

This is the strongest 2026-2027 posture because its result does not depend on a
host C math library, floating-point environment, PostgreSQL transcendental
implementation, database collation, query plan, or implicit rounding rule.

## Research method and confidence

This note uses PostgreSQL 18 documentation, Rust 1.97.1 standard-library
documentation, first-party crate documentation, GNU MPFR documentation, and the
IUPAC definition of half-life. Statements labeled **Source fact** describe the
source. Statements labeled **Recommendation** are Palimpsest decisions derived
from those facts and the repository's accepted temporal and retrieval
invariants.

Confidence is high for PostgreSQL rounding, timestamp precision, Rust integer
behavior, and the ordering requirements. Confidence is high for the proposed
integer contract's portability. Its exact constants and approximation bound
must still be generated, reviewed, and frozen as implementation fixtures; this
note intentionally does not invent those policy values.

## PostgreSQL arithmetic facts

**Source facts.** PostgreSQL `numeric` is an exact, selectable-precision decimal
type. Addition, subtraction, and multiplication produce exact results where
possible. A constrained `numeric(p, s)` column coerces input to its declared
scale, and PostgreSQL rounds when an input has more fractional digits than that
scale ([PostgreSQL 18 numeric types](https://www.postgresql.org/docs/18/datatype-numeric.html)).

PostgreSQL's rounding rule is not the rule issue #21 requires:

- `round(numeric)` and `round(numeric, scale)` break ties away from zero;
- `numeric(p, s)` coercion uses PostgreSQL numeric rounding, so a cast or insert
  must not be treated as a half-even quantizer;
- `double precision` usually rounds ties to even, but PostgreSQL explicitly
  describes its tie behavior as platform-dependent; and
- most PostgreSQL `double precision` mathematical functions use the host C
  library, so accuracy and boundary behavior can vary by host.

These behaviors are explicit in the PostgreSQL 18
[mathematical-function reference](https://www.postgresql.org/docs/18/functions-math.html)
and [numeric-type comparison](https://www.postgresql.org/docs/18/datatype-numeric.html).
For example, positive `0.0000000000025` at scale 12 becomes
`0.000000000003` under PostgreSQL numeric rounding but must become
`0.000000000002` under half-even rounding.

PostgreSQL exposes `power(numeric, numeric)`, but its public contract does not
state a correctly-rounded or cross-version-identical result for irrational
values ([PostgreSQL 18 mathematical functions](https://www.postgresql.org/docs/18/functions-math.html)).
It is acceptable as a test oracle with tolerances, not as the receipt's
normative exponential algorithm.

**Recommendation.** Persist only values that the domain scorer has already
quantized. Add database constraints or trigger validation that reject values
whose scale/canonical text is not the policy's 12-place representation. If a
SQL half-even helper is ever needed, implement the rule explicitly from integer
quotient, remainder, and quotient parity; do not wrap PostgreSQL `round()`.

## Exact half-even contract

For a non-negative rational numerator `n` and positive denominator `d`, compute
`q = n / d` and `r = n % d`. Return `q + 1` when `r > d - r`, or when
`r == d - r` and `q` is odd; otherwise return `q`. Comparing `r` with `d - r`
avoids the overflow risk of calculating `2 * r`. Apply the sign only after
rounding magnitude, and canonicalize negative zero to zero.

This is round-to-nearest, ties-to-even: an exact midpoint is rounded to the
representable result whose least-significant retained digit is even. GNU's
rounding reference describes that rule and its lower average bias
([GNU rounding](https://www.gnu.org/software/c-intro-and-ref/manual/html_node/Rounding.html)).
Rust Decimal calls the same rule `MidpointNearestEven`
([rust_decimal rounding strategies](https://docs.rs/rust_decimal/1.42.1/rust_decimal/enum.RoundingStrategy.html)).

Every public score string should have exactly 12 digits after the decimal point,
including zero: `0.000000000000`. The immutable policy must specify where
rounding occurs. A suitable v1 operation graph is:

1. Half-even each exact rational RRF channel contribution into integer
   `10^-12` units.
2. Add those integer units exactly to form the fused score.
3. Compute temporal, confidence, and importance factors from trusted metadata,
   then multiply in a fixed documented order, half-even back to `10^-12` units
   at each named public component boundary.
4. Apply the exact-identity bonus at the policy-specified point, using integer
   units, and reject checked overflow rather than saturating or wrapping.
5. Serialize the signed integer units with exactly 12 fractional digits and
   hash that canonical representation into the receipt.

Do not reassociate the expression, round only at final JSON formatting, or
derive a persisted score from a formatted binary float. Those changes can alter
ties and therefore ordering.

## Rust implementation options compatible with this workspace

The workspace pins Rust 1.97.1 and already uses `time` 0.3.54, but has no direct
fixed-decimal or big-number dependency. `num-traits` is only transitive and
should not become an undeclared domain dependency.

| Option | Compatibility and evidence | Decision |
| --- | --- | --- |
| A small `ScoreUnits(i128)` plus `DecayQ63(u128)` domain implementation | Rust 1.97.1 provides checked integer multiplication and Euclidean quotient/remainder operations ([`i128`](https://doc.rust-lang.org/1.97.1/std/primitive.i128.html)). With values bounded by policy, two Q63 factors multiply below `2^126`, so the intermediate fits `u128`. | **Recommend.** Small, auditable, no new dependency, exact public decimal units, and deterministic on every Rust target. |
| `fixed` 1.31 | Provides 128-bit binary fixed-point types and checked ties-even operations; its documented MSRV is 1.93, below the workspace's 1.97.1 ([fixed crate](https://docs.rs/fixed/1.31.0/fixed/)). | Compatible fallback. It still needs frozen exponential constants and a Palimpsest operation-order contract, so adopting it does not remove the main design work. |
| `rust_decimal` 1.42 | Uses a 96-bit coefficient and scales 0 through 28, implements `MidpointNearestEven`, and documents MSRV 1.67.1 ([representation](https://docs.rs/rust_decimal/1.42.1/rust_decimal/), [MSRV](https://docs.rs/crate/rust_decimal/1.42.1/source/README.md)). | Suitable for ordinary decimal factors, but not the best normative continuous-decay engine. Its decimal width and rounding API do not by themselves specify a correctly-rounded `2^x` algorithm. |
| Rust `f64::powf` or PostgreSQL `double precision` | Rust documents `powf` precision as non-deterministic across platforms, Rust versions, and even calls; PostgreSQL math functions commonly delegate to the host C library ([Rust `f64`](https://doc.rust-lang.org/1.97.1/std/primitive.f64.html#method.powf), [PostgreSQL math](https://www.postgresql.org/docs/18/functions-math.html)). | Reject for durable score and receipt-digest authority. |
| PostgreSQL `numeric` arithmetic and `power` | Exact for rational basic arithmetic where possible, durable and already used by the schema, but built-in rounding is half-away and the public docs do not freeze transcendental precision across versions. | Keep for storage, constraints, and independent comparison; do not make it the normative scorer. |

`time` 0.3.54 represents signed duration independently of floating point and
exposes whole-unit accessors; the database boundary is nevertheless the correct
place to quantize to PostgreSQL's microsecond resolution. PostgreSQL timestamps
and intervals have one-microsecond resolution, while `timestamptz` instants are
stored internally in UTC
([PostgreSQL 18 date/time types](https://www.postgresql.org/docs/18/datatype-datetime.html)).

## Portable continuous 30-day decay

IUPAC defines half-life as the time required for a quantity to reach one half
of its initial value in the applicable first-order case
([IUPAC Gold Book](https://goldbook.iupac.org/terms/view/H02716)). The continuous
Palimpsest factor `2^(-age / half_life)` therefore equals exactly one at age
zero, one half after 30 exact elapsed days, and one quarter after 60 exact
elapsed days before the policy floor is applied.

### Time coordinate

Use exact elapsed SI-style microseconds between two `timestamptz` instants. Do
not use calendar months, `age()`, local-midnight day counts, `date_part`, or add
`interval '1 day'` repeatedly. PostgreSQL stores intervals as separate months,
days, and microseconds because calendar months vary and civil days can be 23 or
25 hours across daylight-saving changes
([PostgreSQL 18 date/time types](https://www.postgresql.org/docs/18/datatype-datetime.html)).
`extract(epoch from interval)` returns exact `numeric` seconds, whereas
`date_part` returns `double precision` and can lose precision
([PostgreSQL 18 date/time functions](https://www.postgresql.org/docs/18/functions-datetime.html)).

The repository can therefore derive an integer age without floating point as
the exact microsecond difference of the two database instants. `30 days` in the
policy means exactly `2_592_000_000_000` elapsed microseconds, independent of
session `TimeZone` and daylight-saving rules.

### Recency anchor and bitemporal behavior

**Recommendation.** Persist an attributable `recency_anchor_at` beside the
trusted `recency_profile_id` and importance metadata. Its derivation rule must
be in the write/index policy; callers cannot set or override it as a retrieval
knob. For active case evidence, v1 should derive it from the source-domain
observation/effective-time rule selected by that policy, not `recorded_at` and
not receipt `evaluated_at`.

Then apply these rules:

- The effective revision is first selected at both receipt coordinates:
  `valid_during @> request.valid_at` and `recorded_at <= request.recorded_at`.
  Authorization, sensitivity, lifecycle, retention, and deletion filtering
  still precede every candidate and score operation.
- Temporal age uses only `request.valid_at - recency_anchor_at`. A current
  request captures one transaction-stable valid-time coordinate; an as-of
  request uses its explicit historical valid-time coordinate. PostgreSQL's
  `CURRENT_TIMESTAMP` is stable for the transaction, unlike
  `clock_timestamp()`
  ([PostgreSQL current time](https://www.postgresql.org/docs/18/functions-datetime.html#FUNCTIONS-DATETIME-CURRENT)).
- `recorded_at` decides what Palimpsest knew at the recorded-time cutoff; it
  does not make late-arriving old evidence artificially fresh. Receipt
  `evaluated_at` is audit time and must not change an as-of temporal score.
- Clamp negative age to zero. This prevents a future recency anchor from
  creating a factor greater than one when later-recorded evidence is evaluated
  against an earlier valid-time coordinate.
- Apply `max(policy_floor, raw_decay)` before public 12-place quantization.
  Once the raw curve is below the floor, older evidence remains at the exact
  floor; it is never made eligible by that floor.
- `stable-v1` returns one regardless of anchor or age. Confidence, importance,
  and temporal factors operate only on already-authorized candidates and can
  never affect bitemporal selection or eligibility.

This separation preserves bitemporal meaning: changing the recorded-time
coordinate can change which attributable revision was known, while changing the
valid-time coordinate can change both the effective revision and the temporal
age. Replaying an immutable receipt returns its stored dispositions and scores;
reauthorization may hide content but must not recompute the historical ranking.

### Normative Q63 algorithm

Avoid calculating one imprecise per-microsecond base and raising it to a large
power; base error would be amplified billions of times. Instead:

1. Divide `age_us` by `half_life_us` into whole half-lives `q` and remainder
   `r` using integer Euclidean division.
2. Apply `2^-q` exactly as a power-of-two scaling operation.
3. Obtain 63 binary fractional bits of `r / half_life_us` by repeated integer
   doubling and subtraction.
4. For every set bit `i`, multiply by the policy-owned Q63 integer constant
   `C[i] = half_even(2^(-2^-i) * 2^63)`, half-even back to Q63 after each
   multiplication in increasing `i` order.
5. Apply the policy floor in Q63, then convert Q63 to `10^-12` integer units
   with the exact quotient/remainder half-even rule.

Freeze the 63 constants, their generation tool/version, and their digest in the
policy artifact. Generate them offline with a pinned correctly-rounded
high-precision implementation such as GNU MPFR; MPFR describes itself as a
multiple-precision library with correct rounding
([GNU MPFR](https://www.mpfr.org/)). The committed integers, not MPFR or its
runtime availability, are the production authority.

The implementation must publish and test an error bound between this finite
Q63 curve and the mathematical expression. It must also prove policy-specific
integer range bounds before registration. A future higher-precision algorithm
gets a new arithmetic identifier and policy digest; it must not reinterpret
old receipts.

## Stable ordering requirements

PostgreSQL guarantees row order only through an explicit `ORDER BY`; later sort
expressions break ties left by earlier expressions, and null placement must be
specified when defaults are not part of the contract
([PostgreSQL 18 ordering](https://www.postgresql.org/docs/18/queries-order.html)).

The immutable temporal policy should own a complete tuple, with every direction
and null rule written out. The v1 result order should retain exact-qualified
identity precedence, then compare the quantized integer final score, then the
existing deterministic channel ranks, and finally end in unique identifiers:

```text
exact_identity_rank ASC NULLS LAST,
final_score_units DESC,
exact_rank ASC NULLS LAST,
lexical_rank ASC NULLS LAST,
vector_rank ASC NULLS LAST,
case_id ASC,
fact_id ASC,
revision_id ASC
```

The exact tuple may include additional explanatory components only if the
policy fixes them before the unique identifier suffix. Use the same tuple for
`row_number()`, result materialization, cursor position, manifest order, and the
top-level response. Each lexical, vector, and exact channel also needs its own
complete rank order ending in the same stable IDs. Never rely on CTE output
order, insertion order, UUIDv7 time meaning, heap order, or planner behavior.

Avoid locale-dependent text as a final tie-break. If a versioned policy must
sort text, pin the collation explicitly and include its identity/version in the
policy digest; stable UUID and integer keys are preferable.

## Required issue #21 decision and conformance fixtures

Before issue #21 is implementation-complete, lock and test:

1. The exact final-score expression, factor domains, multiplication order,
   intermediate rounding points, overflow bounds, floor value, and canonical
   12-place serialization.
2. Half-even vectors on both signs and parity boundaries, including values just
   below, at, and just above a half-unit at decimal place 12; PostgreSQL
   half-away counterexamples must be included.
3. Decay vectors at negative age, zero, one microsecond, 15 days, exactly 30
   days, 60 days, immediately around the floor crossing, and maximum supported
   timestamp age.
4. Current and two-axis as-of cases with late evidence, supersession, future
   validity, expiry, deletion, stable versus active profiles, and a proof that
   recorded/evaluated time does not leak into valid-time age.
5. Ten-repeat, projection-rebuild, process-restart, and legacy-upgrade
   comparisons with byte-identical ordering, score strings, manifest digests,
   and receipt digests. Add adversarial equal-score order-key vectors to prove
   the unique tie suffix.

Database backup/restore, cross-architecture, and additional supported
PostgreSQL-version comparisons are a separate release matrix. They are required
before claiming backup/restore recovery or cross-host compatibility, but are not
evidence for issue #21's bounded deterministic scoring seam. Until that matrix
passes, reports must name the tested architecture and database versions and make
no broader compatibility or restore claim.

## Recommended implementation boundary

Keep PostgreSQL's authorization-first materialized relation and candidate-rank
query. Return bounded, already-authorized candidate rows plus exact temporal
metadata to the adapter. Convert timestamp differences to integer microseconds
at the database seam, pass only integers and immutable policy data into a pure
domain scorer, and persist its canonical score units in the same repeatable-read
receipt transaction.

This boundary follows Palimpsest's existing direction that Rust owns the
deterministic domain while PostgreSQL owns durable temporal truth. It also gives
focused domain tests a small scoring surface, leaves SQL plans inspectable for
authorization and candidate generation, and makes a 2027 database or CPU change
incapable of silently rewriting ranking math.
