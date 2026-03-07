//! Error handling utilities for graceful degradation
//!
//! Provides helpers for common error handling patterns with logging and fallbacks.

use tracing::{debug, warn};

/// Execute an operation and log errors without failing
///
/// Returns `Some(T)` on success, `None` on error (after logging).
/// Useful for non-critical operations that should not stop execution.
///
/// # Example
/// ```rust
/// use crate::utils::log_error;
///
/// let result = log_error(|| storage.flush(), "Failed to flush storage");
/// // If flush fails, logs warning and returns None, but execution continues
/// ```
pub fn log_error<F, T, E>(operation: F, context: &str) -> Option<T>
where
    F: FnOnce() -> Result<T, E>,
    E: std::fmt::Display,
{
    match operation() {
        Ok(value) => Some(value),
        Err(e) => {
            warn!("{}: {}", context, e);
            None
        }
    }
}

/// Execute an async operation and log errors without failing
///
/// Returns `Some(T)` on success, `None` on error (after logging).
/// Useful for non-critical async operations that should not stop execution.
///
/// # Example
/// ```rust
/// use crate::utils::log_error_async;
///
/// let result = log_error_async(|| async { storage.flush() }, "Failed to flush storage").await;
/// ```
pub async fn log_error_async<F, Fut, T, E>(operation: F, context: &str) -> Option<T>
where
    F: FnOnce() -> Fut,
    Fut: std::future::Future<Output = Result<T, E>>,
    E: std::fmt::Display,
{
    match operation().await {
        Ok(value) => Some(value),
        Err(e) => {
            warn!("{}: {}", context, e);
            None
        }
    }
}

/// Execute an operation with a fallback on error
///
/// Returns the result of the primary operation on success, or the fallback on error.
/// Logs a warning when fallback is used.
///
/// # Example
/// ```rust
/// use crate::utils::with_fallback;
///
/// let value = with_fallback(
///     || primary_operation(),
///     || fallback_operation(),
///     "Primary operation failed, using fallback"
/// );
/// ```
pub fn with_fallback<F1, F2, T, E>(primary: F1, fallback: F2, context: &str) -> T
where
    F1: FnOnce() -> Result<T, E>,
    F2: FnOnce() -> T,
    E: std::fmt::Display,
{
    match primary() {
        Ok(value) => value,
        Err(e) => {
            warn!("{}: {}", context, e);
            fallback()
        }
    }
}

/// Execute an async operation with a fallback on error
///
/// Returns the result of the primary operation on success, or the fallback on error.
/// Logs a warning when fallback is used.
pub async fn with_fallback_async<F1, Fut1, F2, Fut2, T, E>(
    primary: F1,
    fallback: F2,
    context: &str,
) -> T
where
    F1: FnOnce() -> Fut1,
    Fut1: std::future::Future<Output = Result<T, E>>,
    F2: FnOnce() -> Fut2,
    Fut2: std::future::Future<Output = T>,
    E: std::fmt::Display,
{
    match primary().await {
        Ok(value) => value,
        Err(e) => {
            warn!("{}: {}", context, e);
            fallback().await
        }
    }
}

/// Execute an operation and return a default value on error
///
/// Returns the result on success, or the default on error (after logging at debug level).
/// Useful for operations where failure is expected and a default is acceptable.
///
/// # Example
/// ```rust
/// use crate::utils::with_default;
///
/// let count = with_default(|| get_count(), 0, "Failed to get count");
/// ```
pub fn with_default<F, T, E>(operation: F, default: T, context: &str) -> T
where
    F: FnOnce() -> Result<T, E>,
    E: std::fmt::Display,
{
    match operation() {
        Ok(value) => value,
        Err(e) => {
            debug!("{}: {}, using default", context, e);
            default
        }
    }
}

/// Execute an async operation and return a default value on error
pub async fn with_default_async<F, Fut, T, E>(operation: F, default: T, context: &str) -> T
where
    F: FnOnce() -> Fut,
    Fut: std::future::Future<Output = Result<T, E>>,
    E: std::fmt::Display,
{
    match operation().await {
        Ok(value) => value,
        Err(e) => {
            debug!("{}: {}, using default", context, e);
            default
        }
    }
}

/// Convert an Option to a Result with a context message
///
/// # Example
/// ```rust
/// use crate::utils::err_option_to_result;
///
/// let value = err_option_to_result(opt, || "Value not found")?;
/// ```
pub fn err_option_to_result<T, F>(opt: Option<T>, context: F) -> Result<T, String>
where
    F: FnOnce() -> String,
{
    opt.ok_or_else(context)
}

/// Convert a Result to an Option, logging the error
///
/// Returns `Some(T)` on success, `None` on error (after logging).
pub fn result_to_option<T, E>(result: Result<T, E>, context: &str) -> Option<T>
where
    E: std::fmt::Display,
{
    match result {
        Ok(value) => Some(value),
        Err(e) => {
            warn!("{}: {}", context, e);
            None
        }
    }
}

/// Recover from a poisoned `RwLock` or `Mutex` without panicking.
///
/// When a thread panics while holding a lock, Rust marks it as "poisoned".
/// Subsequent callers get a `PoisonError` rather than the lock guard.
/// This helper logs a warning with a recovery suggestion and returns the
/// last known value inside the lock, which is usually safe to use.
///
/// # Example
/// ```rust,ignore
/// use std::sync::RwLock;
///
/// let lock = RwLock::new(0u32);
/// let guard = recover_lock(lock.read(), "fee cache read");
/// ```
pub fn recover_lock<T>(
    result: std::sync::LockResult<T>,
    context: &str,
) -> T {
    result.unwrap_or_else(|poisoned| {
        warn!(
            "{}: lock was poisoned (a thread panicked while holding it). \
             Recovering with last known value. \
             Suggestion: check logs for earlier panics in this subsystem \
             and consider restarting the node if data inconsistency is suspected.",
            context
        );
        poisoned.into_inner()
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Arc, RwLock};

    #[test]
    fn test_recover_lock_returns_value_on_success() {
        let lock = RwLock::new(42u32);
        let guard = recover_lock(lock.read(), "test lock");
        assert_eq!(*guard, 42);
    }

    #[test]
    fn test_recover_lock_recovers_from_poisoned_read_lock() {
        let lock = Arc::new(RwLock::new(99u32));
        let lock_clone = lock.clone();

        // Poison the lock by panicking while holding a write guard
        let _ = std::panic::catch_unwind(move || {
            let _guard = lock_clone.write().unwrap();
            panic!("intentional poison for test");
        });

        // Must not panic — should return last known value with a warning
        let guard = recover_lock(lock.read(), "test poisoned read lock");
        assert_eq!(*guard, 99);
    }

    #[test]
    fn test_recover_lock_write_recovers_from_poisoned_lock() {
        let lock = Arc::new(RwLock::new(7u32));
        let lock_clone = lock.clone();

        let _ = std::panic::catch_unwind(move || {
            let _guard = lock_clone.write().unwrap();
            panic!("intentional poison for test");
        });

        let mut guard = recover_lock(lock.write(), "test poisoned write lock");
        *guard = 42;
        assert_eq!(*guard, 42);
    }
}
