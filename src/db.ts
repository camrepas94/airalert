import fs from "node:fs";
import path from "node:path";
import Database from "better-sqlite3";

const dataDir = path.join(process.cwd(), "data");
if (!fs.existsSync(dataDir)) {
  fs.mkdirSync(dataDir, { recursive: true });
}

const dbPath = path.join(dataDir, "airalert.db");
export const db: InstanceType<typeof Database> = new Database(dbPath);

db.pragma("journal_mode = WAL");
db.pragma("foreign_keys = ON");

db.exec(`
  CREATE TABLE IF NOT EXISTS users (
    id TEXT PRIMARY KEY,
    timezone TEXT NOT NULL DEFAULT 'America/Los_Angeles',
    reminder_hour_local INTEGER NOT NULL DEFAULT 8 CHECK (reminder_hour_local >= 0 AND reminder_hour_local <= 23),
    calendar_token TEXT NOT NULL UNIQUE,
    created_at TEXT NOT NULL DEFAULT (datetime('now'))
  );

  CREATE TABLE IF NOT EXISTS show_subscriptions (
    id TEXT PRIMARY KEY,
    user_id TEXT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    tvmaze_show_id INTEGER NOT NULL,
    show_name TEXT NOT NULL,
    platform_note TEXT,
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    UNIQUE (user_id, tvmaze_show_id)
  );

  CREATE TABLE IF NOT EXISTS episodes_cache (
    tvmaze_show_id INTEGER NOT NULL,
    tvmaze_episode_id INTEGER NOT NULL,
    name TEXT NOT NULL,
    season INTEGER,
    number INTEGER,
    airdate TEXT,
    airtime TEXT,
    runtime INTEGER,
    network TEXT,
    updated_at TEXT NOT NULL DEFAULT (datetime('now')),
    PRIMARY KEY (tvmaze_show_id, tvmaze_episode_id)
  );

  CREATE TABLE IF NOT EXISTS notification_log (
    id TEXT PRIMARY KEY,
    user_id TEXT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    tvmaze_episode_id INTEGER NOT NULL,
    show_name TEXT NOT NULL,
    episode_label TEXT NOT NULL,
    airdate TEXT NOT NULL,
    channel TEXT NOT NULL DEFAULT 'dry_run',
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    UNIQUE (user_id, tvmaze_episode_id, channel)
  );

  CREATE TABLE IF NOT EXISTS devices (
    id TEXT PRIMARY KEY,
    user_id TEXT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    platform TEXT NOT NULL,
    push_token TEXT NOT NULL,
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    UNIQUE (user_id, platform, push_token)
  );

  CREATE TABLE IF NOT EXISTS web_push_subscriptions (
    id TEXT PRIMARY KEY,
    user_id TEXT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    endpoint TEXT NOT NULL UNIQUE,
    p256dh TEXT NOT NULL,
    auth TEXT NOT NULL,
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    updated_at TEXT NOT NULL DEFAULT (datetime('now'))
  );

  CREATE INDEX IF NOT EXISTS idx_subscriptions_user ON show_subscriptions(user_id);
  CREATE INDEX IF NOT EXISTS idx_notification_user ON notification_log(user_id);
  CREATE INDEX IF NOT EXISTS idx_web_push_user ON web_push_subscriptions(user_id);
`);
