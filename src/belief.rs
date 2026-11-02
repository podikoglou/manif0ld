//! Log-odds belief state with exponential decay toward the market price.
//!
//! The bot keeps its own probability for each market in log-odds space. Each
//! triaged post adds a signed impulse, scaled by the direction and evidence
//! strength. Between updates the belief decays toward the current market price,
//! so stale opinions do not trade forever.
//!
//! Everything in this module is pure: callers own the persisted state
//! (`p_internal_log_odds`, `belief_updated_at`) and pass it in. `now` and
//! `belief_updated_at` are Unix seconds.
//!
//! Decay is applied **before** the new impulse. A post arrives at time `now`;
//! the belief is first pulled toward the market by the elapsed time since the
//! previous update, then the post moves it. Decaying after the impulse would
//! immediately discount fresh evidence.

use crate::config::Config;
use crate::types::Direction;

/// Maximum absolute log-odds value, approximately `logit(0.99)`.
pub const LOG_ODDS_CLAMP: f64 = 4.595;

/// Minimum probability used when converting between probability and log-odds.
///
/// The maximum probability is `1.0 - PROB_CLAMP`; both bounds keep `logit` away
/// from infinities.
pub const PROB_CLAMP: f64 = 0.01;

/// Belief-engine parameters, usually sourced from [`Config`].
#[derive(Debug, Clone, Copy)]
pub struct BeliefParams {
    pub kappa: f64,
    pub halflife_hours: f64,
}

impl BeliefParams {
    /// Reads the belief parameters from process configuration.
    ///
    /// Defaults are `BELIEF_KAPPA=1.1` and `BELIEF_HALFLIFE_HOURS=4.0`.
    pub fn from_config(config: &Config) -> Self {
        Self {
            kappa: config.belief_kappa,
            halflife_hours: config.belief_halflife_hours,
        }
    }
}

/// Converts a probability to log-odds, clamping it to `[0.01, 0.99]` first.
pub fn logit(p: f64) -> f64 {
    let p = p.clamp(PROB_CLAMP, 1.0 - PROB_CLAMP);
    (p / (1.0 - p)).ln()
}

/// Converts log-odds to a probability, clamping the result to `[0.01, 0.99]`.
pub fn sigmoid(l: f64) -> f64 {
    let p = 1.0 / (1.0 + (-l).exp());
    p.clamp(PROB_CLAMP, 1.0 - PROB_CLAMP)
}

/// Fraction of a belief difference that survives `dt_hours`.
///
/// Returns `1.0` for a non-positive elapsed time (no decay) and `0.0` when the
/// half-life is non-positive, which guards against division by zero.
pub fn decay_factor(dt_hours: f64, halflife_hours: f64) -> f64 {
    if dt_hours <= 0.0 {
        return 1.0;
    }
    if halflife_hours <= 0.0 {
        return 0.0;
    }
    0.5_f64.powf(dt_hours / halflife_hours)
}

/// Decays stored log-odds toward the current market log-odds.
///
/// The result is clamped to `±LOG_ODDS_CLAMP`.
pub fn decayed_log_odds(
    l_stored: f64,
    belief_updated_at: i64,
    p_market_now: f64,
    now: i64,
    halflife_hours: f64,
) -> f64 {
    let l_market = logit(p_market_now);
    let elapsed_hours = now.saturating_sub(belief_updated_at) as f64 / 3600.0;
    let decay = decay_factor(elapsed_hours, halflife_hours);
    (l_market + (l_stored - l_market) * decay).clamp(-LOG_ODDS_CLAMP, LOG_ODDS_CLAMP)
}

/// Current internal probability after decay, without applying a new post.
pub fn current_p_internal(
    l_stored: f64,
    belief_updated_at: i64,
    p_market_now: f64,
    now: i64,
    halflife_hours: f64,
) -> f64 {
    sigmoid(decayed_log_odds(
        l_stored,
        belief_updated_at,
        p_market_now,
        now,
        halflife_hours,
    ))
}

/// Nominal log-odds impulse for one post, before clamping.
///
/// `strength` is clamped to `3`; a neutral direction yields zero.
pub fn nominal_impulse(direction: Direction, strength: u8, kappa: f64) -> f64 {
    let strength = f64::from(strength.min(3)) / 3.0;
    kappa * direction.polarity() * strength
}

/// Applies one post to the belief state.
///
/// Decays the stored log-odds toward `p_market_now` using the elapsed time
/// since `*belief_updated_at`, then adds [`nominal_impulse`], clamps, and
/// records `now` as the new update time.
///
/// Returns the impulse actually applied, `new_l - decayed_l`. This differs from
/// the nominal impulse when the log-odds clamp is reached, and callers should
/// use the returned value for logging and the signal ledger.
#[allow(clippy::too_many_arguments)]
pub fn apply_post(
    l_stored: &mut f64,
    belief_updated_at: &mut i64,
    direction: Direction,
    strength: u8,
    p_market_now: f64,
    now: i64,
    kappa: f64,
    halflife_hours: f64,
) -> f64 {
    let decayed = decayed_log_odds(
        *l_stored,
        *belief_updated_at,
        p_market_now,
        now,
        halflife_hours,
    );
    let impulse = nominal_impulse(direction, strength, kappa);
    let new_l = (decayed + impulse).clamp(-LOG_ODDS_CLAMP, LOG_ODDS_CLAMP);

    *l_stored = new_l;
    *belief_updated_at = now;

    new_l - decayed
}

/// Applies one post using [`BeliefParams`].
#[allow(clippy::too_many_arguments)]
pub fn apply_post_with_params(
    l_stored: &mut f64,
    belief_updated_at: &mut i64,
    direction: Direction,
    strength: u8,
    p_market_now: f64,
    now: i64,
    params: BeliefParams,
) -> f64 {
    apply_post(
        l_stored,
        belief_updated_at,
        direction,
        strength,
        p_market_now,
        now,
        params.kappa,
        params.halflife_hours,
    )
}

/// Returns `(log-odds, clamped probability)` for an operator `/override`.
pub fn override_probability(p: f64) -> (f64, f64) {
    let p = p.clamp(PROB_CLAMP, 1.0 - PROB_CLAMP);
    (logit(p), p)
}

#[cfg(test)]
mod tests {
    use super::*;

    const EPS: f64 = 1e-12;
    const HOUR: i64 = 3_600;

    #[test]
    fn logit_and_sigmoid_roundtrip() {
        for p in [0.2, 0.5, 0.8] {
            let roundtrip = sigmoid(logit(p));
            assert!((roundtrip - p).abs() < EPS, "p={p} got {roundtrip}");
        }
    }

    #[test]
    fn market_probability_extremes_stay_finite() {
        for p_market in [0.0, 1.0] {
            let l = decayed_log_odds(0.0, 0, p_market, 0, 4.0);
            assert!(l.is_finite(), "log-odds must be finite for {p_market}");
            assert!(l.abs() <= LOG_ODDS_CLAMP);

            let p = current_p_internal(0.0, 0, p_market, 0, 4.0);
            assert!(p.is_finite(), "probability must be finite for {p_market}");
            assert!((PROB_CLAMP..=1.0 - PROB_CLAMP).contains(&p));
        }
    }

    #[test]
    fn zero_elapsed_time_means_no_decay() {
        let l_stored = logit(0.8);
        let decayed = decayed_log_odds(l_stored, 1_000, 0.5, 1_000, 4.0);
        assert!((decayed - l_stored).abs() < EPS);
    }

    #[test]
    fn one_half_life_moves_midway_toward_the_market() {
        let l_stored = logit(0.8);
        let p = current_p_internal(l_stored, 0, 0.5, 4 * HOUR, 4.0);
        assert!((p - 2.0 / 3.0).abs() < 1e-9, "got {p}");
    }

    #[test]
    fn very_large_elapsed_time_converges_to_the_market() {
        let l_stored = logit(0.8);
        let decayed = decayed_log_odds(l_stored, 0, 0.3, 1_000_000_000, 4.0);
        assert!((decayed - logit(0.3)).abs() < 1e-9, "got {decayed}");
    }

    #[test]
    fn decay_factor_guards_degenerate_inputs() {
        assert_eq!(decay_factor(0.0, 4.0), 1.0);
        assert_eq!(decay_factor(-1.0, 4.0), 1.0);
        assert_eq!(decay_factor(4.0, 0.0), 0.0);
        assert!((decay_factor(4.0, 4.0) - 0.5).abs() < EPS);
    }

    #[test]
    fn strong_yes_impulse_uses_full_strength() {
        let mut l = 0.0;
        let mut updated = 0;
        let delta = apply_post(
            &mut l,
            &mut updated,
            Direction::StrongYes,
            3,
            0.5,
            0,
            1.1,
            4.0,
        );
        assert!((delta - 1.1).abs() < EPS);
        assert!((l - 1.1).abs() < EPS);
    }

    #[test]
    fn negative_and_neutral_directions_behave_as_expected() {
        let mut l = 0.0;
        let mut updated = 0;
        let no_delta = apply_post(
            &mut l,
            &mut updated,
            Direction::No,
            3,
            0.5,
            0,
            1.1,
            4.0,
        );
        assert!(no_delta < 0.0);

        let mut strong_no_l = 0.0;
        let mut strong_no_updated = 0;
        let strong_no_delta = apply_post(
            &mut strong_no_l,
            &mut strong_no_updated,
            Direction::StrongNo,
            3,
            0.5,
            0,
            1.1,
            4.0,
        );
        assert!(strong_no_delta < 0.0);

        let mut neutral_l = 0.0;
        let mut neutral_updated = 0;
        let neutral_delta = apply_post(
            &mut neutral_l,
            &mut neutral_updated,
            Direction::Neutral,
            3,
            0.5,
            0,
            1.1,
            4.0,
        );
        assert_eq!(neutral_delta, 0.0);
        assert_eq!(neutral_l, 0.0);

        let zero_strength = apply_post(
            &mut neutral_l,
            &mut neutral_updated,
            Direction::StrongYes,
            0,
            0.5,
            0,
            1.1,
            4.0,
        );
        assert_eq!(zero_strength, 0.0);
    }

    #[test]
    fn strength_above_three_is_clamped() {
        assert!((nominal_impulse(Direction::StrongYes, 9, 1.1) - 1.1).abs() < EPS);
    }

    #[test]
    fn repeated_strong_posts_clamp_probability_and_log_odds() {
        let mut l = 0.0;
        let mut updated = 0;

        for _ in 0..100 {
            apply_post(
                &mut l,
                &mut updated,
                Direction::StrongYes,
                3,
                0.5,
                0,
                1.1,
                4.0,
            );
        }

        assert_eq!(l, LOG_ODDS_CLAMP);
        let p = current_p_internal(l, 0, 0.5, 0, 4.0);
        assert!((p - 0.99).abs() < 1e-5, "got {p}");
    }

    #[test]
    fn returned_impulse_reflects_clamping() {
        let mut l = LOG_ODDS_CLAMP - 0.05;
        let mut updated = 0;
        let delta = apply_post(
            &mut l,
            &mut updated,
            Direction::StrongYes,
            3,
            0.5,
            0,
            1.1,
            4.0,
        );

        assert!((delta - 0.05).abs() < 1e-12, "got {delta}");
        assert_eq!(l, LOG_ODDS_CLAMP);
    }

    #[test]
    fn decay_is_applied_before_the_impulse() {
        // Start at logit(0.8). After one half-life toward 0.5 the belief is
        // logit(2/3) ≈ 0.6931, then a zero-strength post changes nothing.
        let mut l = logit(0.8);
        let mut updated = 0;
        let delta = apply_post(
            &mut l,
            &mut updated,
            Direction::Neutral,
            0,
            0.5,
            4 * HOUR,
            1.1,
            4.0,
        );

        assert_eq!(delta, 0.0);
        assert!((l - std::f64::consts::LN_2).abs() < 1e-9, "got {l}");
        assert_eq!(updated, 4 * HOUR);
    }

    #[test]
    fn override_probability_roundtrips() {
        let (l, p) = override_probability(0.42);
        assert!((p - 0.42).abs() < EPS);
        assert!((sigmoid(l) - 0.42).abs() < EPS);

        let (_, clamped_low) = override_probability(0.0);
        assert_eq!(clamped_low, PROB_CLAMP);
        let (_, clamped_high) = override_probability(1.0);
        assert_eq!(clamped_high, 1.0 - PROB_CLAMP);
    }

    #[test]
    fn apply_post_with_params_delegates() {
        let params = BeliefParams {
            kappa: 1.1,
            halflife_hours: 4.0,
        };
        let mut l = 0.0;
        let mut updated = 0;
        let delta = apply_post_with_params(
            &mut l,
            &mut updated,
            Direction::StrongYes,
            3,
            0.5,
            0,
            params,
        );
        assert!((delta - 1.1).abs() < EPS);
    }
}
