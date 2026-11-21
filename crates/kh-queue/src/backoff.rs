use std::time::Duration;

use rand::Rng;

/// Exponential backoff with optional jitter.
#[derive(Debug, Clone)]
pub struct BackoffPolicy {
    /// Delay after the first failed attempt.
    pub base: Duration,

    /// Upper bound for the computed delay.
    pub max: Duration,

    /// Fraction of random jitter, from `0.0` (none) to `1.0` (±100%).
    pub jitter: f64,
}

impl Default for BackoffPolicy {
    fn default() -> Self {
        Self {
            base: Duration::from_secs(5),
            max: Duration::from_secs(300),
            jitter: 0.2,
        }
    }
}

impl BackoffPolicy {
    /// Delay before the next attempt after `attempt` (1-based) failed.
    ///
    /// The delay doubles per attempt and is capped at [`Self::max`]. Jitter is
    /// applied last and the result never exceeds `max`.
    pub fn delay_for(&self, attempt: i32) -> Duration {
        let exponent = attempt.saturating_sub(1).clamp(0, 20) as u32;
        let factor = 1u64 << exponent;

        let base_ms = self.base.as_millis().min(u128::from(u64::MAX)) as u64;
        let max_ms = self.max.as_millis().min(u128::from(u64::MAX)) as u64;
        let raw_ms = base_ms.saturating_mul(factor).min(max_ms);

        let jitter = self.jitter.clamp(0.0, 1.0);
        let low = (raw_ms as f64 * (1.0 - jitter)).max(0.0);
        let high = (raw_ms as f64 * (1.0 + jitter)).min(max_ms as f64);
        let ms = if high > low {
            rand::rng().random_range(low..=high)
        } else {
            low
        };

        Duration::from_millis(ms.round() as u64)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn policy(base_ms: u64, max_ms: u64, jitter: f64) -> BackoffPolicy {
        BackoffPolicy {
            base: Duration::from_millis(base_ms),
            max: Duration::from_millis(max_ms),
            jitter,
        }
    }

    #[test]
    fn doubles_each_attempt() {
        let policy = policy(100, 10_000, 0.0);

        assert_eq!(policy.delay_for(1), Duration::from_millis(100));
        assert_eq!(policy.delay_for(2), Duration::from_millis(200));
        assert_eq!(policy.delay_for(3), Duration::from_millis(400));
        assert_eq!(policy.delay_for(4), Duration::from_millis(800));
    }

    #[test]
    fn caps_at_max() {
        let policy = policy(100, 500, 0.0);

        assert_eq!(policy.delay_for(10), Duration::from_millis(500));
    }

    #[test]
    fn jitter_stays_within_bounds() {
        let policy = policy(1_000, 10_000, 0.5);

        for _ in 0..100 {
            let delay = policy.delay_for(1);
            assert!(delay >= Duration::from_millis(500));
            assert!(delay <= Duration::from_millis(1_500));
        }
    }
}
