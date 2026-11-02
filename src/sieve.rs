//! Per-market Sieve engine registry.
//!
//! Each tracked market owns one compiled `sieve::CompiledSieve`. The registry
//! keeps those engines behind a single `RwLock` so the ingest loop can match a
//! post against every tracked market while onboarding, `/untrack`, and hot
//! swaps mutate the map.
//!
//! **Text is never pre-processed here.** `sieve` applies its own lexical
//! normalization (case folding, NFKC, zero-width stripping, leetspeak,
//! morphological expansion, implicit word boundaries) inside
//! [`sieve::CompiledSieve::matches`]. Lowercasing, trimming, or otherwise
//! rewriting the text before calling it would double-apply those rules and
//! break case-sensitive `!` literals.
//!
//! # Performance
//!
//! The registry holds one compiled engine per market and the match loop only
//! takes a read lock. Engines are immutable after compilation, so N markets
//! cost N `matches` calls on a shared map with no per-post allocation beyond
//! the returned market list.

use std::collections::HashMap;
use std::sync::{RwLock, RwLockReadGuard, RwLockWriteGuard};

use crate::error::{anyhow, AppResult, Context};
use crate::types::MarketId;

/// A compiled engine that may be shared between threads.
///
/// `sieve::CompiledSieve` keeps a Cranelift `JITModule` alive, and that type is
/// conservatively `!Send + !Sync` because it contains a `RefCell` symbol cache
/// and boxed symbol-lookup closures. None of those fields are reachable after
/// compilation: the only operation this crate performs on an engine is
/// `matches`, which reads immutable prefilter data and calls generated machine
/// code. The JIT module is held solely to keep the executable pages mapped.
/// Wrapping the engine lets the registry live in `Arc` and be shared by the
/// ingest task and per-market triage workers.
struct SendEngine(sieve::CompiledSieve);

// SAFETY: see the type comment. Construction happens on one thread; afterwards
// `matches` is a pure read and generated code is immutable. Dropping the JIT
// module frees mmap-backed memory and is not tied to the creating thread.
unsafe impl Send for SendEngine {}
unsafe impl Sync for SendEngine {}

/// Compiled Sieve engines for every tracked market.
///
/// The map is hot-swappable: registering an ID that already exists replaces the
/// previous engine. All methods are safe to call from multiple threads.
pub struct EngineRegistry {
    engines: RwLock<HashMap<MarketId, SendEngine>>,
}

impl Default for EngineRegistry {
    fn default() -> Self {
        Self::new()
    }
}

impl EngineRegistry {
    /// Creates an empty registry.
    pub fn new() -> Self {
        Self {
            engines: RwLock::new(HashMap::new()),
        }
    }

    /// Acquires a read guard, recovering from lock poisoning.
    ///
    /// A panic while holding the write lock must not take matching down with
    /// it: the map itself is still structurally valid, so the guard is reused.
    fn read(&self) -> RwLockReadGuard<'_, HashMap<MarketId, SendEngine>> {
        self.engines
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    /// Acquires a write guard, recovering from lock poisoning.
    fn write(&self) -> RwLockWriteGuard<'_, HashMap<MarketId, SendEngine>> {
        self.engines
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    /// Inserts or replaces the engine for `id`.
    ///
    /// Replacement is the hot-swap path used when a market is re-onboarded.
    pub fn register(&self, id: MarketId, engine: sieve::CompiledSieve) {
        self.write().insert(id, SendEngine(engine));
    }

    /// Compiles `source` and registers it for `id`.
    ///
    /// The engine is compiled outside the lock so a slow Cranelift compile does
    /// not block matching.
    pub fn register_source(&self, id: MarketId, source: &str) -> AppResult<()> {
        let engine = compile(source)?;
        self.register(id, engine);
        Ok(())
    }

    /// Removes the engine for `id`, returning whether one existed.
    pub fn unregister(&self, id: &str) -> bool {
        self.write().remove(id).is_some()
    }

    /// Returns markets whose Sieve rule matched `text`.
    ///
    /// `text` is passed to Sieve verbatim. The result is sorted so callers get
    /// deterministic ordering regardless of hash-map iteration order.
    pub fn matching_markets(&self, text: &str) -> Vec<MarketId> {
        let engines = self.read();
        let mut matched = Vec::new();

        for (id, engine) in engines.iter() {
            if !engine.0.matches(text).is_empty() {
                matched.push(id.clone());
            }
        }

        matched.sort();
        matched
    }

    /// Returns the registered market IDs in sorted order.
    pub fn market_ids(&self) -> Vec<MarketId> {
        let mut ids: Vec<MarketId> = self.read().keys().cloned().collect();
        ids.sort();
        ids
    }

    /// Number of registered engines.
    pub fn len(&self) -> usize {
        self.read().len()
    }

    /// Whether the registry contains no engines.
    pub fn is_empty(&self) -> bool {
        self.read().is_empty()
    }

    /// Whether an engine is registered for `id`.
    pub fn contains(&self, id: &str) -> bool {
        self.read().contains_key(id)
    }
}

/// Compiles a Sieve source string, adding context to Sieve's error text.
pub fn compile(source: &str) -> AppResult<sieve::CompiledSieve> {
    sieve::CompiledSieve::compile(source)
        .map_err(|message| anyhow!("failed to compile Sieve rule: {message}"))
        .with_context(|| format!("invalid Sieve source: {source}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn engine(source: &str) -> sieve::CompiledSieve {
        compile(source).expect("test Sieve source should compile")
    }

    fn matched(registry: &EngineRegistry, text: &str) -> Vec<MarketId> {
        registry.matching_markets(text)
    }

    #[test]
    fn compiles_registers_and_matches_case_insensitively() {
        let registry = EngineRegistry::new();
        registry.register(
            "Market_test".to_owned(),
            engine(r#"match Market_test { "bitcoin" | "btc" }"#),
        );

        assert_eq!(
            matched(&registry, "Latest BITCOIN news"),
            vec!["Market_test".to_owned()]
        );
        assert_eq!(
            matched(&registry, "btc is up"),
            vec!["Market_test".to_owned()]
        );
        assert!(matched(&registry, "ethereum merge").is_empty());
    }

    #[test]
    fn raw_text_is_not_preprocessed_by_the_registry() {
        // A case-sensitive literal cannot see through dotted obfuscation.
        let case_sensitive = EngineRegistry::new();
        case_sensitive.register("M".to_owned(), engine(r#"match M { "SCAM"! }"#));
        assert!(matched(&case_sensitive, "S.C.A.M").is_empty());

        // The leetspeak modifier asks Sieve to do the expansion itself.
        let leetspeak = EngineRegistry::new();
        leetspeak.register("M".to_owned(), engine(r#"match M { "scam"~ }"#));
        assert_eq!(matched(&leetspeak, "S.C.A.M"), vec!["M".to_owned()]);
    }

    #[test]
    fn register_hot_swaps_the_engine() {
        let registry = EngineRegistry::new();
        registry.register("M".to_owned(), engine(r#"match M { "old" }"#));
        assert_eq!(matched(&registry, "old"), vec!["M".to_owned()]);

        registry.register("M".to_owned(), engine(r#"match M { "new" }"#));
        assert!(matched(&registry, "old").is_empty());
        assert_eq!(matched(&registry, "new"), vec!["M".to_owned()]);
        assert_eq!(registry.len(), 1);
    }

    #[test]
    fn unregister_removes_the_market_and_ids_are_sorted() {
        let registry = EngineRegistry::new();
        for id in ["b", "a", "c"] {
            registry.register(id.to_owned(), engine(r#"match M { "x" }"#));
        }

        assert_eq!(registry.market_ids(), vec!["a", "b", "c"]);
        assert!(registry.contains("b"));
        assert!(registry.unregister("b"));
        assert!(!registry.unregister("b"));
        assert!(!registry.contains("b"));
        assert_eq!(registry.market_ids(), vec!["a", "c"]);
    }

    #[test]
    fn matches_extracted_post_text() {
        let registry = EngineRegistry::new();
        // Sieve measures an unordered window as token-index distance. In this
        // sentence "apple" is token 0 and "product" is token 5, so the window
        // must be 5 (a window of 4 covers four *intervening* tokens and does
        // not match here).
        registry
            .register_source("M".to_owned(), r#"match M { "apple" &..5 "product" }"#)
            .expect("rule should compile");

        let event = serde_json::json!({
            "did": "did:plc:author",
            "time_us": 1_725_911_162_329_308_i64,
            "kind": "commit",
            "commit": {
                "operation": "create",
                "collection": "app.bsky.feed.post",
                "rkey": "3kabc",
                "uri": "at://did:plc:author/app.bsky.feed.post/3kabc",
                "record": {
                    "text": "Apple just announced a new product",
                    "$type": "app.bsky.feed.post",
                }
            }
        });

        let post = crate::extract::extract(&event).expect("event should extract");
        assert_eq!(post.text, "Apple just announced a new product");
        assert_eq!(
            registry.matching_markets(&post.text),
            vec!["M".to_owned()]
        );
    }
}
