-- Two-way sync bookkeeping for a user's media tracker connection.
--
-- last_pull_at is the watermark the next delta pull starts from. NULL means
-- nothing has been pulled yet, so the next sync is a full import.
ALTER TABLE user_media_trackers ADD COLUMN last_pull_at DATETIME;
-- The remote account the credentials belong to, for "Connected as …".
ALTER TABLE user_media_trackers ADD COLUMN account_name TEXT;

-- Pull remote changes into every connected tracker once an hour.
INSERT OR IGNORE INTO task_triggers (id, task_id, kind, time_limit_hours, cron) VALUES
    ('default-mediatrackersync-hourly', 'MediaTrackerSync', 'IntervalTrigger', NULL, '0 0 */1 * * *');
