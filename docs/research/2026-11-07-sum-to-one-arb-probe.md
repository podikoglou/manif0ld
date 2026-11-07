# S4 sum-to-one arbitrage — live probe (2026-11-07)

**Status:** research artifact. This is **not** a specification and it does not
define bot behaviour. The decision that follows from it is recorded in
`docs/SPEC.md` §6.12 (no-go). The proposal it tests is `docs/ALGO.md` §S4.

**Environment:** live Manifold public API (`/v0/...` and the broadcast
websocket), observed 2026-11-07, plus a temporary shallow clone of
`manifoldmarkets/manifold` used for backend-source citations (paths below are
`file:line` into that tree).

**Raw artifacts:** the JSON/JSONL captures and harnesses stayed in a temporary
scratch directory (`/tmp/opencode/arb_probe/`, not durable) and are **not**
part of this repo. Their names are listed at the end so the work can be
identified if it is ever repeated; do not treat those paths as links.

Tags: **[live]**, **[backend-source]**, **[not-possible]**.

## 1. Verdict

**[not-possible]** S4 as designed cannot be traded.

1. **The window does not exist.** `Σp = 1` to within `4e-14` across all 926
   open sum-to-one markets, in two snapshots 90 s apart. The invariant is
   re-established *inside the same transaction* as every bet, answer creation,
   and liquidity change, and `cpmm-multi-2` rejects any trade that would break
   it. There is nothing to buy below 1.
2. **A basket is blocked on most of the set.** `POST /v0/multi-bet` over more
   than one answer is hard-rejected on `cpmm-multi-2` — 752 of 926 markets
   (81%). Only 174 small markets remain eligible.
3. **The economics are negligible even in the hypothetical.** Profit scales as
   `δ² · depth`; a median market (8 answers, ~1 mana of usable depth) at a 1%
   deficit is worth ≈0.07–0.1 mana gross. No 1% deficit was ever observed.
4. **The piece that is cheap is worthless alone.** `rebalance` is real, free,
   and accurate as described, but it only collapses an existing YES/NO basket.
   With no basket to buy, there is nothing to collapse.

## 2. `contract/{id}/updated-answers` is not the bet feed

**[live]** The topic exists and acks cleanly on the first-frame protocol
(`{"type":"subscribe","txid":1,...}` → `{"type":"ack",...}`). The ack does not
validate topics: a bogus topic also acks `success:true`.

**[backend-source]** `backend/shared/src/websockets/helpers.ts:155`
`broadcastUpdatedAnswers` emits `{ answers: [{id, poolYes, poolNo, prob, volume}], broadcastTime }`.
It is called only from answer-row writes (`supabase/answers.ts:100,113`),
answer creation, and multi-bet groups (`place-bet.ts:703`, inside
`if (isMultiBet)`).

**[live] It does not fire on ordinary bets.** 12 hot `cpmm-multi-1` markets,
37 topics, 240 s: `contract/<id>/new-bet` matched bet events exactly
(14/14, 4/4, 7/7, 15/15), while `updated-answers` and `contract/<id>` delivered
**0 frames for those 40 bet events**. Across ~500 market-minutes of
`updated-answers` subscription it produced **one** frame, on the same
millisecond as a `contract/<id>` update (an answer-row write). Bet-driven pool
writes land in the contract row's `answers` JSONB and are published via
`broadcastUpdatedContract` (`helpers.ts:115`) on `contract/<id>` and
`global/updated-contract`.

**Consequence:** use `contract/{id}/new-bet` as the trigger and take the pool
snapshot from REST. `updated-answers` is the wrong signal for S4.

## 3. The invariant holds exactly, everywhere

**[live]** The REST answer shape is now
`{ id, p, pool: {YES, NO}, probability, totalLiquidity, ... }` — there is no
`poolYes`/`poolNo`/`prob` on the answer. The market has
`shouldAnswersSumToOne`.

Snapshot of all 926 open sum-to-one markets, two snapshots 90 s apart
(926/926 fetched both times):

| metric | value |
|---|---|
| markets | 926 (`cpmm-multi-2` 752, `cpmm-multi-1` 174) |
| `Σp − 1`, `cpmm-multi-1` | min 0, max 0 (all 174, both snapshots) |
| `Σp − 1`, `cpmm-multi-2` | min `−4.4e-12` … max 0 |
| markets with `\|1−Σp\| > 1e-9` | 0 |
| max positive deficit (`Σp < 1`) | 0 |

Mechanism census **[live]**: 752/926 (81%) are `cpmm-multi-2`. On
`cpmm-multi-1`: median 8 answers (p10 3, p90 26, max 100), median liquidity
1000, 43% have an answer below 0.5%, 0 markets with a zero pool side, 0 answers
with `p ≠ 0.5`.

**Why it never deviates [backend-source]:**

- Every bet on a sum-to-one market buys the target answer YES **and** buys NO
  in all other answers in the same transaction
  (`calculateCpmmMultiArbitrageBet` → `buyNoSharesUntilAnswersSumToOne`,
  `common/src/calculate-cpmm-arbitrage.ts:1745`), which pins `Σp = 1`.
- A new answer's insert, NO re-arb, and liquidity add run in one DB transaction
  (`backend/api/src/create-answer-cpmm.ts:500,626` via
  `getBetDownToOneMultiBetInfo`).
- `cpmm-multi-2` additionally **refuses** any trade that would leave
  `|Σp−1| > 1e-6` (`cpmmMultiTradeMissesSumToOne`,
  `calculate-cpmm-arbitrage.ts:93`; enforced at `place-multi-bet.ts:116` and in
  `place-bet`).

**[backend-source replay]** A real 100-mana YES bet replayed through the real
solver on a live 9-answer market: `sumBefore 0.9999999999999919 →
sumAfter 0.9999999999999919, delta 0`, with NO legs written to the 8 other
answers. Same result for `cpmm-multi-2` (delta 0, 2 other NO legs on 3-answer
markets).

## 4. Capacity if a deficit were injected

Bisecting the real basket solver for the amount that drives `Σp` back to 1,
with the deficit injected artificially:

| market (n, avg NO pool) | δ | optimal amount | profit | ROI |
|---|---|---|---|---|
| ZRSP0ZAd0q (2, 3142) | 0.1% | 282 | 0.08 | 0.03% |
| ZRSP0ZAd0q (2, 3142) | 1% | 2027 | 9.6 | 0.47% |
| ZRSP0ZAd0q (2, 3142) | 5% | 14 138 | 192.7 | 1.36% |
| vgw5ckzp3z (19, 1242) | 1% | 114 | 0.57 | 0.50% |
| dsPLUSSCRz (10, 238) | 1% | 14.4 | **0.07** | 0.50% |
| 920ug0REz9 (16, 34) | 1% | 6.6 | **0.03** | 0.50% |

Law: `ROI ≈ δ` and `profit ≈ δ·A*`, with `A* ≈ δ × pool scale`, so
**`profit ≈ δ² × depth`**. A median market at a 1% deficit is worth ≈0.1 mana.
The ALGO claim that `ARB_MARGIN`-covering windows exist is **[not-possible]**:
0/926 twice, and 0/40 bet events streamed.

## 5. `POST /v0/multi-bet`

**[backend-source]** `common/src/api/schema.ts:521`

```ts
'multi-bet': { method:'POST', authed:true,
  returns: [] as (CandidateBet & { betId: string })[],
  props: z.object({
    contractId: z.string(),
    amount: z.number().gte(1),              // TOTAL mana, not per answer
    limitProb: z.number().gte(0).lte(1).optional(),
    expiresAt: z.number().lt(MAX_EXPIRES_AT).optional(),
    answerIds: z.array(z.string()).min(1),
    deterministic: z.boolean().optional(),
  }).strict() }
```

- **No `dryRun`.** `.strict()` turns any extra field into a 400. Only
  `/v0/bet` has `dryRun`.
- **Atomic:** one `runTransactionWithRetries` with
  `select 1 from contracts where id=$1 for update` before any read
  (`place-multi-bet.ts:41-55`); all legs are written in one transaction.
- **Minimum:** total `amount ≥ 1` mana.
- **`limitProb`** must be a whole percentage (`getRoundedLimitProb`,
  `helpers/bets.ts:579`), else `400 limitProb must be in increments of 0.01`.
- **Response:** an array with one `CandidateBet` per answer plus `betId` and
  `betGroupId` (`place-multi-bet.ts:170`); each carries `amount, shares,
  orderAmount, fills[], probBefore, probAfter, isFilled`. The internal arb NO
  legs are **not** in the response; they are inserted as redemption bets.
- **Auth [live]:** unauthenticated POST → `401 {"message":"Missing
  Authorization header."}`.
- **YES only:** the outcome is forced to `'YES'` (`place-multi-bet.ts:58`).
- **Requires sum-to-one:** `shouldAnswersSumToOne` must be set, else
  `getNewMultiCpmmBetsInfo` throws `400 Not yet implemented`
  (`backend/api/src/new-bet.ts:263`).
- **BLOCKER [backend-source]** `place-multi-bet.ts:85`:

```ts
if (contract.mechanism === 'cpmm-multi-2' && betOnAnswers.length > 1)
  throw new APIError(400, "Buying several answers at once isn't available on this market yet.")
```

⇒ a basket is impossible on 752 of 926 markets. `cpmm-multi-2` has no
in-process dry run either (the solve takes ~70 ms at 10 answers and fails its
own post-check on ~1% of fuzzed baskets).

## 6. Basket sizing (kept for any future revisit)

### 6.1 The correct basket

**Equal YES *shares* per answer — not equal mana, not pool-proportional.**

**[backend-source]** `calculate-cpmm-arbitrage.ts:496`

```ts
while (amountToBet > 0.01) {
  const answersToBuy = updatedAnswers.filter(...)
  // buy equal number of shares in each answer
  const yesSharePriceSum = sumBy(answersToBuy, 'prob')
  const maxYesShares = amountToBet / yesSharePriceSum
  binarySearch(0, maxYesShares, (yesShares) => {
    yesAmounts = answersToBuy.map(({id, poolYes, poolNo, p = 0.5}) =>
      calculateAmountToBuySharesFixedP({pool:{YES:poolYes, NO:poolNo}, p, collectedFees},
        yesShares, 'YES', unfilledBetsByAnswer[id] ?? [], balanceByUserId))
    return sum(yesAmounts) - amountToBet
  })
}
```

`amount` is the **total** spent; the server solves for one share count `g` such
that `Σ cost_i(g) = amount` (`new-bet.ts:341` `multiBuyTakerAmount` sums the
legs and becomes both `amount` and `orderAmount`).

### 6.2 Exact post-trade probability

**[backend-source]** `common/src/calculate-cpmm.ts:56`

```ts
export function getCpmmProbability(pool: {YES:number, NO:number}, p: number) {
  const { YES, NO } = pool
  return (p * NO) / ((1 - p) * YES + p * NO)      // p = 0.5 on cpmm-multi-1
}
```

Buying `b` mana of YES moves the pool (`calculate-cpmm.ts:92`,
`calculateCpmmShares`, invariant `Y^p·N^(1-p) = k`):

```
k = Y^p · N^(1-p)
shares s = Y + b − (k · (b+N)^(p−1))^(1/p)
Y' = Y − s + b ,  N' = N + b
```

At `p = 0.5` the closed form in the other direction (shares → cost) is

```
b(g)     = ( −(N − g + Y) + sqrt((N − g + Y)² + 4·N·g) ) / 2
prob'(g) = (N + b(g)) / (Y + N + 2·b(g) − g)
```

Validated against `calculateAmountToBuySharesFixedP` to `4.6e-5` relative worst
case over 1988 points; that residual is the server's own bisection tolerance,
not the formula.

### 6.3 Search on shares, not amount

`g ↦ Σp(g)` is strictly increasing, so there is exactly one `g*` with `Σp = 1`,
the profit maximum (since `d(profit)/dg = 1 − Σp(g)`). Bisect on **shares `g`**,
not on `amount`: `g` needs no nested search and costs one sqrt per answer per
probe. Then send `amount = Σ b_i(g*)`. The ALGO's "binary-search `amount` so
post-trade `Σ ≈ 1`" is equivalent but needs an inner server-side solve.

**Corollary:** post-trade `Σp ≈ 1` is not an observable success test for a
multi-bet, because the server's own arb legs force `Σp = 1` at the end of
*any* basket regardless of `amount`. The only observable is `charged vs
shares` (`isFilled`/`amount` in the response): passing too much buys sets above
1.

**Fees:** `TAKER_FEE_CONSTANT = 0` and `FLAT_TRADE_FEE = 0`
(`common/src/fees.ts`) ⇒ zero fees. `ARB_MARGIN` would only need to cover the
deficit already being gone — which it always is.

### 6.4 Pseudocode

```
subscribe contract/{id}/new-bet for the 174 cpmm-multi-1 ids   # 1 topic each
on frame:
  if none of the bets touches a pool the bot cares about: return
  GET /v0/market/{id}                                     # 1 REST call, fresh pools
  answers = unresolved answers
  S = Σ  p_i*N_i / ((1-p_i)*Y_i + p_i*N_i)                 # p_i = 0.5 on multi-1
  if S >= 1 - ARB_MARGIN: return                          # in practice always true
  # exact cost of g YES shares in one answer (p = 0.5):
  b(y,n,g) = ( -(n - g + y) + sqrt((n - g + y)^2 + 4*n*g) ) / 2
  # post-trade prob after buying g shares in that answer:
  q(y,n,g) = (n + b(y,n,g)) / (y + n + 2*b(y,n,g) - g)
  # one-dimensional monotone search: the profit maximum is where marginal Σp = 1
  g* = bisect [0, ∞) on  Σ_i q(Y_i, N_i, g) = 1             # 60 iterations
  amount = Σ_i b(Y_i, N_i, g*)                              # total mana to send
  if amount < 1 or g* - amount < ARB_MARGIN * amount: return
  POST /v0/multi-bet { contractId, answerIds: all unresolved ids,
                       amount, deterministic: true }        # no limitProb: sweep
  # optional: POST /v0/market/{id}/rebalance to cash out the NO hedge
```

Do not bisect on the displayed probabilities alone, do not proportion amounts
to pools, and do not read the response's `Σp` as a profitability signal — check
`shares` vs `amount`.

## 7. `POST /v0/market/{id}/rebalance`

**[backend-source]** `schema.ts:616`

```ts
'market/:contractId/rebalance': { method:'POST', authed:true,
  props: z.object({ contractId: z.string() }).strict(),
  returns: {} as { cashRedeemed:number; minShares:number; betCount:number; loanPaid:number } }
```

`backend/api/src/rebalance-position.ts:30` calls `getContract`, then requires
`isMultiCpmm && shouldAnswersSumToOne`, else
`400 Rebalance is only supported on sum-to-one multi-choice markets.`; `403`
if resolved; the job is enqueued per `[userId, contractId]`. Effect
(`common/src/rebalance.ts:3`):

> Share identity: 1 NO in outcome i is equivalent to 1 YES in every outcome
> j != i … redeem the minimum-across-outcomes of YES shares at $1 each … This is
> pure accounting — no AMM interaction, no probability movement, no fees.

Synthetic bets are written with `isRedemption: true, isRebalance: true` and
`probBefore === probAfter`. A no-op returns `{cashRedeemed:0, minShares:0,
betCount:0, loanPaid:0}`. There is no dry run and no extra rate cost beyond the
POST; auth is required **[live]:** `401 {"message":"Missing Authorization
header."}`.

So the ALGO description of `rebalance` is accurate — but it is **not** a
substitute for capturing `Σp < 1`, and it is worthless without a basket: there
is nothing to collapse unless the account already holds YES and NO.

## 8. Discovery query

**[live]**
`GET /v0/search-markets?contractType=DEPENDENT_MULTIPLE_CHOICE&sort=24-hour-vol&filter=open&limit=1000`
returns `200` with all **926** markets in **one** call (~0.5 s). `limit` is
capped at **1000**, not 200 (`market-search-types.ts:72`), so the hourly sweep
is one request with no pagination.

`contractType=DEPENDENT_MULTIPLE_CHOICE` maps to
`outcome_type='MULTIPLE_CHOICE' AND coalesce(data->>'shouldAnswersSumToOne', true)=true`
(`backend/shared/src/supabase/search-contracts.ts:623`) — exactly the S4 set.
The response is `LiteMarket[]` (`id, mechanism, outcomeType, volume24Hours,
totalLiquidity, lastBetTime, …`) with **no answers** unless
`includeLiteAnswers=true` is added, which returns
`answers: [{id, text, probability}]` — probabilities only, no pools. Pools
still need `/v0/market/{id}`.

## 9. Subscription scale

**[live]** 201 topics (100 markets × `contract/<id>` and
`contract/<id>/updated-answers`, plus `global/new-bet`) on **one** connection:
ack `success:true`, alive for 200 s, 448 `global/new-bet` frames. There is no
documented or observed hard cap, but **delivery was not reliable at that
fan-out**: the 12 watched markets with 1–51 bet events produced only one
per-contract frame. At 37 topics (12 markets × 3) `contract/<id>/new-bet` was
exactly 1:1 with bet events (40/40). Practical reading: keep per-market
subscriptions in the tens per connection and shard if hundreds are needed;
`global/new-bet` alone is free and complete (~1 msg/s).

## 10. Why ALGO §S4 is infeasible as written

1. **[not-possible] The window does not exist.** `Σp = 1` to `4e-14` on all
   926 markets, both mechanisms, at all sampled times. Every bet, answer
   creation, and liquidity add re-arbs `Σ` back to 1 inside the same
   transaction, and `cpmm-multi-2` refuses trades that would break it.
2. **[not-possible] A multi-answer `multi-bet` is rejected on `cpmm-multi-2`**
   (`place-multi-bet.ts:85`) — 81% of the set. Only 174 markets are eligible,
   and they are the small ones (median liquidity 1000).
3. **`updated-answers` is the wrong topic.** It is not bet-driven (0/40
   events); use `new-bet` plus a REST snapshot.
4. **Economics.** Even an injected 1% deficit yields ≈0.1 mana on a median
   market (`profit ≈ δ²·depth`), and a 1% deficit has never been observed.
5. **Minor:** the market object no longer carries `poolYes/poolNo/prob` (it is
   `pool.{YES,NO}`, `p`, `probability`), and "sum-to-one" must be computed over
   *unresolved* answers.

## Appendix: raw artifact names (temporary, not in the repo)

The probe kept its captures and harnesses in a scratch directory
(`/tmp/opencode/arb_probe/`, deleted with the machine and not durable):

| file | what |
|---|---|
| `discover_variants.json`, `discovery_sum2one.json` | discovery params + 926 markets |
| `market_samples.json`, `market_summary.json` | first 40 full market objects |
| `multi1_markets.json`, `multi1_stats.json` | all 174 cpmm-multi-1 objects + stats |
| `sump_snapshots.json` | `Σp` for all 926, two snapshots 90 s apart |
| `capacity.json` | optimal amount / profit vs injected deficit |
| `sim_result.json`, `sim_perturbed2.json` | real backend solver on real/perturbed pools |
| `frames_answers.jsonl` | 26-topic `updated-answers` probe (0 bet frames) |
| `frames_corr.jsonl`, `corr_summary.json` | 12 markets × 3 topics; `new-bet` 1:1 |
| `frames_scale.jsonl`, `scale_summary.json` | 201 topics on one connection |
| `frames_focus.jsonl`, `frames_decisive.jsonl` | single-market topic comparison |
| `frames_contract_topic.jsonl` | 20 `contract/<id>` topics |
| `sim/run_backend.ts`, `run_single_bet.ts`, `run_capacity.ts`, `run_closed_form.ts`, `run_multi2.ts` | harnesses importing the real `common/` sources via tsconfig paths |
