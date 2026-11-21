use std::collections::HashMap;
use std::sync::Arc;

use kh_core::Handler;

/// Maps job kinds to their handlers.
#[derive(Clone, Default)]
pub struct Registry {
    handlers: HashMap<String, Arc<dyn Handler>>,
}

impl Registry {
    pub fn new() -> Self {
        Self::default()
    }

    /// Register a handler for `kind`.
    ///
    /// # Panics
    ///
    /// Panics if a handler is already registered for `kind`.
    pub fn register(
        &mut self,
        kind: impl Into<String>,
        handler: impl Handler + 'static,
    ) -> &mut Self {
        let kind = kind.into();
        assert!(
            !self.handlers.contains_key(&kind),
            "handler already registered for job kind: {kind}"
        );
        self.handlers.insert(kind, Arc::new(handler));
        self
    }

    /// Builder form of [`Self::register`].
    pub fn with(mut self, kind: impl Into<String>, handler: impl Handler + 'static) -> Self {
        self.register(kind, handler);
        self
    }

    pub fn get(&self, kind: &str) -> Option<Arc<dyn Handler>> {
        self.handlers.get(kind).cloned()
    }

    pub fn len(&self) -> usize {
        self.handlers.len()
    }

    pub fn is_empty(&self) -> bool {
        self.handlers.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use async_trait::async_trait;
    use kh_core::{HandlerError, JobContext};
    use serde_json::Value;

    use super::*;

    struct Noop;

    #[async_trait]
    impl Handler for Noop {
        async fn handle(&self, _ctx: &JobContext, _payload: Value) -> Result<(), HandlerError> {
            Ok(())
        }
    }

    #[test]
    fn registers_and_finds_handlers() {
        let registry = Registry::new().with("noop", Noop);

        assert_eq!(registry.len(), 1);
        assert!(!registry.is_empty());
        assert!(registry.get("noop").is_some());
        assert!(registry.get("missing").is_none());
    }

    #[test]
    #[should_panic(expected = "handler already registered")]
    fn duplicate_kind_panics() {
        Registry::new().with("noop", Noop).with("noop", Noop);
    }
}
