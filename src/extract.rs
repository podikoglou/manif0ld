//! Structural extraction of a Bluesky post from a raw Jetstream event.
//!
//! This module only copies fields out of the event JSON. It never changes the
//! character content of the strings it extracts: no case folding, trimming of
//! parts, punctuation stripping, or Unicode normalization happens here. Lexical
//! normalization belongs to the Sieve engine, so the text produced here must be
//! passed to Sieve unchanged by the caller. This module does not depend on the
//! Sieve crate and must not start doing so.
//!
//! The extracted text is the concatenation of the post body, the alt text of
//! every attached image, and the body of a quoted post, joined with single
//! spaces.

use crate::types::ExtractedPost;

/// Extracts a post from a Jetstream event without resolving author handles.
///
/// The returned `handle` is the author DID. Callers with a handle cache should
/// use [`extract_with_handle`].
pub fn extract(event: &serde_json::Value) -> Option<ExtractedPost> {
    extract_with_handle(event, |_| None)
}

/// Extracts a post from a Jetstream event, optionally resolving the author DID
/// to a handle.
///
/// `resolve_handle` is called once with the author DID. A `None` or blank
/// result falls back to the DID so downstream logs always identify an author.
///
/// Returns `None` for events that are not post creations, for events missing a
/// URI or rkey, author DID, or `time_us`, and for posts whose extracted text is
/// empty or whitespace only.
///
/// Real Jetstream commit events omit `commit.uri` and carry only `commit.rkey`.
/// The canonical AT URI is therefore constructed as
/// `at://{did}/{collection}/{rkey}`; an explicit `commit.uri`, when present, is
/// preferred verbatim.
pub fn extract_with_handle<F>(event: &serde_json::Value, resolve_handle: F) -> Option<ExtractedPost>
where
    F: Fn(&str) -> Option<String>,
{
    if event["kind"].as_str() != Some("commit") {
        return None;
    }
    if event["commit"]["operation"].as_str() != Some("create") {
        return None;
    }
    let collection = event["commit"]["collection"].as_str()?;
    if collection != "app.bsky.feed.post" {
        return None;
    }

    let did = event["did"].as_str()?.to_owned();
    let uri = match event["commit"]["uri"].as_str() {
        Some(uri) => uri.to_owned(),
        None => {
            let rkey = event["commit"]["rkey"].as_str()?;
            format!("at://{did}/{collection}/{rkey}")
        }
    };
    let time_us = event["time_us"].as_i64()?;

    let text = extract_text(event);
    // `trim` is used only to decide whether there is anything to index; the
    // stored text keeps the original whitespace.
    if text.trim().is_empty() {
        return None;
    }

    let handle = resolve_handle(&did)
        .filter(|handle| !handle.trim().is_empty())
        .unwrap_or_else(|| did.clone());

    Some(ExtractedPost {
        uri,
        did,
        handle,
        text,
        // Jetstream timestamps are microseconds; the domain uses seconds.
        created_at: time_us / 1_000_000,
    })
}

/// Joins the searchable parts of the record with single spaces.
///
/// Missing fields contribute no part. An image without alt text, or with an
/// empty alt string, contributes no part either. Parts are copied verbatim,
/// including empty ones that are present in the event, because removing or
/// rewriting content is not this module's job.
fn extract_text(event: &serde_json::Value) -> String {
    let record = &event["commit"]["record"];
    let mut parts: Vec<&str> = Vec::new();

    if let Some(body) = record["text"].as_str() {
        parts.push(body);
    }

    if let Some(images) = record["embed"]["images"].as_array() {
        for image in images {
            if let Some(alt) = image["alt"].as_str() {
                if !alt.is_empty() {
                    parts.push(alt);
                }
            }
        }
    }

    if let Some(quoted) = record["embed"]["record"]["value"]["text"].as_str() {
        parts.push(quoted);
    }

    parts.join(" ")
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::{json, Value};

    const DID: &str = "did:plc:author";

    /// Builds a minimal valid `app.bsky.feed.post` creation event.
    ///
    /// Real Jetstream events carry `rkey` but no `commit.uri`; the extractor
    /// must construct the canonical AT URI from the DID, collection, and rkey.
    fn post_event(record: Value) -> Value {
        json!({
            "did": DID,
            "time_us": 1_725_911_162_329_308_i64,
            "kind": "commit",
            "commit": {
                "operation": "create",
                "collection": "app.bsky.feed.post",
                "rkey": "3kabc",
                "record": record
            }
        })
    }

    fn body(text: &str) -> Value {
        json!({ "$type": "app.bsky.feed.post", "text": text })
    }

    #[test]
    fn rejects_non_commit_events() {
        let mut event = post_event(body("hello"));
        event["kind"] = json!("identity");
        assert!(extract(&event).is_none());

        let mut event = post_event(body("hello"));
        event.as_object_mut().expect("object").remove("kind");
        assert!(extract(&event).is_none());
    }

    #[test]
    fn rejects_non_create_operations() {
        for operation in ["delete", "update"] {
            let mut event = post_event(body("hello"));
            event["commit"]["operation"] = json!(operation);
            assert!(extract(&event).is_none(), "operation {operation}");
        }
    }

    #[test]
    fn rejects_other_collections() {
        for collection in ["app.bsky.feed.like", "app.bsky.graph.follow"] {
            let mut event = post_event(body("hello"));
            event["commit"]["collection"] = json!(collection);
            assert!(extract(&event).is_none(), "collection {collection}");
        }
    }

    #[test]
    fn rejects_events_missing_required_fields() {
        let mut event = post_event(body("hello"));
        event["commit"]
            .as_object_mut()
            .expect("commit")
            .remove("rkey");
        assert!(extract(&event).is_none(), "missing rkey");

        let mut event = post_event(body("hello"));
        event.as_object_mut().expect("event").remove("did");
        assert!(extract(&event).is_none(), "missing did");

        let mut event = post_event(body("hello"));
        event.as_object_mut().expect("event").remove("time_us");
        assert!(extract(&event).is_none(), "missing time_us");

        let mut event = post_event(body("hello"));
        event["time_us"] = json!("1725911162329308");
        assert!(extract(&event).is_none(), "time_us must be an integer");
    }

    #[test]
    fn extracts_identity_fields_and_converts_timestamp() {
        let event = post_event(body("hello"));
        let post = extract(&event).expect("valid post");

        assert_eq!(post.uri, "at://did:plc:author/app.bsky.feed.post/3kabc");
        assert_eq!(post.did, DID);
        assert_eq!(post.handle, DID, "no resolver means DID fallback");
        assert_eq!(post.created_at, 1_725_911_162);
    }

    #[test]
    fn joins_body_image_alts_and_quoted_text_in_order() {
        let event = post_event(json!({
            "$type": "app.bsky.feed.post",
            "text": "Body text",
            "embed": {
                "images": [
                    { "alt": "First image" },
                    { "alt": "Second image" }
                ],
                "record": {
                    "value": { "text": "Quoted text" }
                }
            }
        }));

        assert_eq!(
            extract(&event).expect("valid post").text,
            "Body text First image Second image Quoted text"
        );
    }

    #[test]
    fn skips_empty_and_absent_alt_text() {
        let event = post_event(json!({
            "$type": "app.bsky.feed.post",
            "text": "Body",
            "embed": {
                "images": [
                    {},
                    { "alt": "" },
                    { "alt": null },
                    { "alt": "Shown alt" }
                ]
            }
        }));

        assert_eq!(extract(&event).expect("valid post").text, "Body Shown alt");
    }

    #[test]
    fn absent_optional_parts_are_not_added() {
        let event = post_event(body("Only the body"));
        assert_eq!(extract(&event).expect("valid post").text, "Only the body");
    }

    #[test]
    fn raw_text_is_preserved_byte_for_byte() {
        let raw = "S.C.A.M 🚨 4irDr0p\u{200d}";
        let event = post_event(body(raw));

        assert_eq!(extract(&event).expect("valid post").text, raw);
    }

    #[test]
    fn extraction_does_not_normalize_case_or_punctuation() {
        let event = post_event(body("S.C.A.M 🚨 4irDr0p"));

        let text = extract(&event).expect("valid post").text;
        assert!(text.contains("S.C.A.M"), "{text}");
        assert!(text.contains("4irDr0p"), "{text}");
        assert!(
            !text.contains("scam"),
            "text must not be lowercased: {text}"
        );
        assert!(
            !text.contains("airdrop"),
            "leet must not be expanded: {text}"
        );
    }

    #[test]
    fn alt_and_quote_text_are_not_normalized() {
        let event = post_event(json!({
            "$type": "app.bsky.feed.post",
            "text": "MiXeD CaSe!",
            "embed": {
                "images": [{ "alt": "WHALE ALERT 🐋" }],
                "record": { "value": { "text": "S.C.A.M 4irDr0p" } }
            }
        }));

        assert_eq!(
            extract(&event).expect("valid post").text,
            "MiXeD CaSe! WHALE ALERT 🐋 S.C.A.M 4irDr0p"
        );
    }

    #[test]
    fn handle_resolver_is_used() {
        let event = post_event(body("hello"));

        let post = extract_with_handle(&event, |did| {
            assert_eq!(did, DID);
            Some("alice.test".to_owned())
        })
        .expect("valid post");

        assert_eq!(post.handle, "alice.test");
    }

    #[test]
    fn handle_falls_back_to_did() {
        let event = post_event(body("hello"));

        assert_eq!(
            extract_with_handle(&event, |_| None)
                .expect("valid post")
                .handle,
            DID
        );
        assert_eq!(
            extract_with_handle(&event, |_| Some(String::new()))
                .expect("valid post")
                .handle,
            DID
        );
        assert_eq!(
            extract_with_handle(&event, |_| Some("   ".to_owned()))
                .expect("valid post")
                .handle,
            DID
        );
    }

    #[test]
    fn empty_or_whitespace_text_is_rejected() {
        assert!(extract(&post_event(body(""))).is_none());
        assert!(extract(&post_event(body("   \n\t "))).is_none());

        // Whitespace-only alt text is still non-empty and does not make an
        // otherwise empty post valid on its own; the joined result is trimmed
        // only for the emptiness test.
        let event = post_event(json!({
            "$type": "app.bsky.feed.post",
            "text": " ",
            "embed": { "images": [{ "alt": "  " }] }
        }));
        assert!(extract(&event).is_none());
    }

    #[test]
    fn whitespace_in_retained_text_is_not_trimmed() {
        let event = post_event(body("  padded body  "));
        assert_eq!(extract(&event).expect("valid post").text, "  padded body  ");
    }

    #[test]
    fn quoted_text_alone_is_enough() {
        let event = post_event(json!({
            "$type": "app.bsky.feed.post",
            "text": "",
            "embed": { "record": { "value": { "text": "Quoted" } } }
        }));

        assert_eq!(extract(&event).expect("valid post").text, " Quoted");
    }
}
