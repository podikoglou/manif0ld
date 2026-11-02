-- Adds a canonical Manifold page URL to every tracked market.
--
-- The URL is captured from the `GET /v0/slug/{slug}` response at onboarding
-- time. The bare slug cannot reconstruct it (the creator segment is lost), so
-- the canonical `url` is persisted alongside it. Existing rows default to the
-- empty string and fall back to a slug-only URL when rendered.
ALTER TABLE tracked_markets ADD COLUMN market_url TEXT NOT NULL DEFAULT '';