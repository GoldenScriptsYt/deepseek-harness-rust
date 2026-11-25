//! Provider retry classification and bounded backoff. The agent loop owns execution.

use crate::{ErrorCode, LlmError};
use std::time::Duration;

const MAX_TIMER_DELAY: Duration = Duration::from_millis(2_147_483_647);
const MAX_SAFE_RETRIES: u64 = 9_007_199_254_740_991;

/// Failure classes selectable by a normal retry policy, in TypeScript definition order.
pub const RETRY_CODE_CHOICES: &[ErrorCode] = &[
    ErrorCode::EmptyResponse,
    ErrorCode::MalformedResponse,
    ErrorCode::Quota,
    ErrorCode::RateLimit,
    ErrorCode::Server,
    ErrorCode::StreamClosed,
    ErrorCode::Timeout,
    ErrorCode::Transport,
];

/// Finite retry policy with symmetric jitter and provider `Retry-After` support.
#[derive(Debug, Clone, PartialEq)]
pub struct RetryPolicy {
    /// Retries allowed after the initial attempt.
    pub max_retries: usize,
    /// Local exponential-backoff seed.
    pub initial_delay: Duration,
    /// Maximum local or accepted provider delay.
    pub max_delay: Duration,
    /// Symmetric random multiplier range around one; must be in `0.0..=1.0`.
    pub jitter_ratio: f64,
    /// Failure classes eligible for retry.
    pub retryable_codes: Vec<ErrorCode>,
}

impl Default for RetryPolicy {
    fn default() -> Self {
        Self {
            max_retries: 5,
            initial_delay: Duration::from_millis(500),
            max_delay: Duration::from_millis(10_000),
            jitter_ratio: 0.1,
            retryable_codes: vec![
                ErrorCode::EmptyResponse,
                ErrorCode::RateLimit,
                ErrorCode::Server,
                ErrorCode::Timeout,
                ErrorCode::Transport,
            ],
        }
    }
}

impl RetryPolicy {
    /// Validate configuration independently of an error occurrence.
    pub fn validate(&self) -> Result<(), LlmError> {
        if !self.retryable_codes.is_empty()
            && (self.jitter_ratio.is_finite() && (0.0..=1.0).contains(&self.jitter_ratio))
            && !self.initial_delay.is_zero()
            && !self.max_delay.is_zero()
            && self.initial_delay <= self.max_delay
            && self.initial_delay <= MAX_TIMER_DELAY
            && self.max_delay <= MAX_TIMER_DELAY
            && (self.max_retries as u64) <= MAX_SAFE_RETRIES
            && !self
                .retryable_codes
                .iter()
                .enumerate()
                .any(|(index, code)| self.retryable_codes[..index].contains(code))
        {
            Ok(())
        } else {
            Err(LlmError::new(
                ErrorCode::InvalidRequest,
                "retry policy requires positive bounded delays, jitter_ratio in 0..=1, and at least one retryable code",
            ))
        }
    }

    /// Whether this finite policy may repeat the failed attempt.
    pub fn should_retry(&self, error: &LlmError, retry_index: usize) -> bool {
        self.retry_index_in_budget(retry_index) && self.retryable_codes.contains(&error.code)
    }

    fn retry_index_in_budget(&self, retry_index: usize) -> bool {
        retry_index < self.max_retries
    }

    /// Delay before retry number `retry_index` (zero-based), or `None` when not eligible.
    /// A valid provider delay at or below the cap is used exactly. Too-large provider
    /// delays reject this retry rather than silently retrying earlier than requested.
    pub fn retry_delay(&self, error: &LlmError, retry_index: usize) -> Option<Duration> {
        let mut seed = error.status.unwrap_or_default() as u64 ^ retry_index as u64;
        for byte in error.request_id.as_deref().unwrap_or_default().bytes() {
            seed = seed.wrapping_mul(31).wrapping_add(u64::from(byte));
        }
        seed ^= u64::from(error.code.is_retry_choice());
        let unit = ((seed.wrapping_mul(6_364_136_223_846_793_005).wrapping_add(1) >> 11) as f64)
            / ((1u64 << 53) as f64);
        self.retry_delay_with_jitter(error, retry_index, unit)
    }

    /// Testable delay calculation with a caller-provided uniform random sample in `0..=1`.
    pub fn retry_delay_with_jitter(
        &self,
        error: &LlmError,
        retry_index: usize,
        random: f64,
    ) -> Option<Duration> {
        if self.validate().is_err() || !self.should_retry(error, retry_index) {
            return None;
        }
        if let Some(provider_delay) = error.retry_after_ms {
            let provider_delay = Duration::from_millis(provider_delay);
            return (provider_delay <= self.max_delay).then_some(provider_delay);
        }
        let factor = 1u32
            .checked_shl(retry_index.min(31) as u32)
            .unwrap_or(u32::MAX);
        let exponential = self
            .initial_delay
            .saturating_mul(factor)
            .min(self.max_delay);
        let random = if random.is_finite() {
            random.clamp(0.0, 1.0)
        } else {
            0.5
        };
        let lower = (1.0 - self.jitter_ratio).max(0.0);
        let upper = 1.0 + self.jitter_ratio;
        let milliseconds = exponential.as_secs_f64() * 1000.0 * (lower + (upper - lower) * random);
        Some(Duration::from_millis(milliseconds.ceil() as u64).min(self.max_delay))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn error(code: ErrorCode) -> LlmError {
        LlmError::new(code, "fixture")
    }

    #[test]
    fn defaults_are_bounded_and_match_typescript() {
        let policy = RetryPolicy::default();
        assert_eq!(policy.max_retries, 5);
        assert_eq!(policy.initial_delay, Duration::from_millis(500));
        assert_eq!(policy.max_delay, Duration::from_millis(10_000));
        assert!(!policy.should_retry(&error(ErrorCode::StreamClosed), 0));
        assert!(policy.should_retry(&error(ErrorCode::Server), 4));
        assert!(!policy.should_retry(&error(ErrorCode::Server), 5));
        assert!(!policy.should_retry(&error(ErrorCode::Auth), 0));
        assert!(!policy.should_retry(&error(ErrorCode::Http(503)), 0));
    }

    #[test]
    fn backoff_grows_caps_and_jitters_symmetrically() {
        let mut policy = RetryPolicy {
            jitter_ratio: 0.0,
            ..RetryPolicy::default()
        };
        assert_eq!(
            policy.retry_delay(&error(ErrorCode::Transport), 0),
            Some(Duration::from_millis(500))
        );
        assert_eq!(
            policy.retry_delay(&error(ErrorCode::Transport), 4),
            Some(Duration::from_millis(8_000))
        );
        assert_eq!(policy.retry_delay(&error(ErrorCode::Transport), 5), None);
        policy.jitter_ratio = 0.5;
        assert_eq!(
            policy.retry_delay_with_jitter(&error(ErrorCode::Transport), 0, 0.0),
            Some(Duration::from_millis(250))
        );
        assert_eq!(
            policy.retry_delay_with_jitter(&error(ErrorCode::Transport), 0, 1.0),
            Some(Duration::from_millis(750))
        );
    }

    #[test]
    fn provider_delay_is_exact_within_cap_and_rejected_above_it() {
        let policy = RetryPolicy::default();
        let accepted = error(ErrorCode::RateLimit).with_retry_after(2_000);
        assert_eq!(
            policy.retry_delay_with_jitter(&accepted, 0, 0.0),
            Some(Duration::from_millis(2_000))
        );
        let rejected = error(ErrorCode::RateLimit).with_retry_after(10_001);
        assert_eq!(policy.retry_delay(&rejected, 0), None);
        let auth = error(ErrorCode::Auth).with_retry_after(10);
        assert_eq!(policy.retry_delay(&auth, 0), None);
    }

    #[test]
    fn stream_failure_can_be_selected_without_becoming_default_retryable() {
        let mut policy = RetryPolicy::default();
        policy.retryable_codes.push(ErrorCode::StreamClosed);
        assert!(policy.should_retry(&error(ErrorCode::StreamClosed), 0));
    }

    #[test]
    fn invalid_policies_never_schedule_a_retry() {
        let mut policy = RetryPolicy {
            max_delay: Duration::ZERO,
            ..RetryPolicy::default()
        };
        assert!(policy.validate().is_err());
        assert_eq!(policy.retry_delay(&error(ErrorCode::Transport), 0), None);
        policy = RetryPolicy::default();
        policy.retryable_codes.clear();
        assert!(policy.validate().is_err());
        policy = RetryPolicy::default();
        policy.retryable_codes.push(ErrorCode::Transport);
        assert!(policy.validate().is_err());
        policy = RetryPolicy::default();
        policy.max_delay = MAX_TIMER_DELAY + Duration::from_millis(1);
        assert!(policy.validate().is_err());
    }
}
