# Plan: Per-answer Sieve attribution for multiple-choice markets

## 1. Problem

One Sieve rule per market covers every answer of a multiple-choice market.
For a MEGA market with ~50 answers this has three costs:

1. **Cost.** Every Sieve match sends all N answers (texts + prices) to the
   decision model. A weather-bot false positive pays for the full state.
2. **Confusion.** The choice question is a forced pick over N answers, many
   of which are logically nested (`Colom wins by 2000 / 3500 / 5000 / …`).
   The abstain alternative (already shipped) prevents impulses onto
   unrelated answers, but the model still sees N answers per call.
3. **No caused-by.** `EngineRegistry::matching_markets` returns market IDs
   only. Nothing downstream knows *why* a rule matched, so triage state
   cannot be narrowed.

## 2. Proposal

Let the onboarding agent emit **one Sieve rule per tracked answer** plus one
market-level rule inside the market's program:

```
match Market_<id> { ... }        // unchanged: market-level keywords
match Answer_<market_id>_<answer_id> { ... }   // new: per-answer keywords
```

Conventions: rules named `Answer_<market_id>_<answer_id>` are scoped to the
market; rule names are `snake_case`-safe tokens. A post matching
`Answer_X` AND `Market_X` attributes the match to answer X; matching only
`Market_X` is an unattributed match (triage gets the full answer set and
decides, with the abstain alternative as backstop).

Rewrite the fold semantics after attribution:

```
direction = (p_target - 1/N_attributed) / (1 - 1/N_attributed)
```

where `N_attributed` counts the answers whose rules matched this post
(minimum 2, else no impulse). Nested answers within one attributed set are
unavoidable — the rule generator is instructed to give each answer rule its
own discriminative vocabulary. The generator prompt gains a section:
"one rule per answer; use the discriminative words of that answer only"
(percent thresholds for margin answers, dates for call-deadline answers,
total counts for seat counts).

## 3. Components

- `sieve/`: no engine change. `CompiledSieve::matches` already returns rule
  names; the registry only uses market rule names today.
- `src/sieve.rs` (`EngineRegistry`):
  - `matching_markets(text) -> Vec<MarketId>` stays.
  - New `matching_answers(text) -> HashMap<MarketId, BTreeSet<AnswerId>>`
    built by grouping rule names with the `Answer_<market>_<answer>` shape.
  - `SieveSource` (user-provided hint) unchanged; the onboarding agent now
    writes per-answer rules.
- `src/onboarding.rs` / `src/onboarding_agent.rs`:
  - `SIEVE_SYSTEM_PROMPT` gains the per-answer rule section.
  - `submit_sieve` validates that every tracked answer has an `Answer_`
    rule and that no rule references an untracked answer id; the error
    feeds the agent's retry loop.
  - `dry_run_sieve` additionally returns which rules matched, so the agent
    can see per-answer attribution before submitting.
- `src/pipeline.rs`:
  - `TriageJob` carries `attributed_answers: BTreeSet<AnswerId>`.
  - `PipelineEvent::PostMatched` / `Signal` gain an `answers_narrowed: usize`
    field for the `/status` and embed surfaces.
- `src/triage.rs`:
  - `TriageClient::evaluate` takes an optional subset of answers to place
    in `state.answers`. The choice question is built from the subset; a
    subset of size 0 (unattributed match) uses all tracked answers.
- `src/db.rs`: no schema change (answers already live in `tracked_answers`).
- `docs/SPEC.md` §2 (pipeline diagram), §6.4 (state contract), §6.7
  (agent contract) update together with the implementation.

## 4. Sequencing

1. Registry: rule-name groupings + `matching_answers`, unit tests.
2. Onboarding: stretched prompt sections + `submit_sieve` validation + the
   dry-run per-rule report.
3. Pipeline/triage: subset threading + ledger column `answers_considered`.
4. SPEC update; then a small Discord embed tweak (`[SIGNAL]` shows the
   attributed answers).

## 5. Non-goals

- No claim clustering, no cross-market dedupe (separate plan).
- No belief updates to non-attributed answers from an attributed scan
  (counter-impulses from the full MCQ distribution remain future work).
