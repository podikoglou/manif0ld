-- 006: the Jetstream cursor is obsolete.
--
-- Ingest always subscribes at the live tail, so the stored `time_us` value is
-- dead data. An old row would also suggest a replay path that no longer
-- exists. `DELETE` of a missing key is a no-op, so this needs no guard and
-- runs on every open.
DELETE FROM kv WHERE key = 'jetstream_cursor_us';
