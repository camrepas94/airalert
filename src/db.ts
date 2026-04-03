import fs from "node:fs";
import path from "node:path";
import Database from "better-sqlite3";

/**
 * Subscriptions and users live in SQLite under this directory.
 * Priority: AIRALERT_DATA_DIR → Railway's RAILWAY_VOLUME_MOUNT_PATH → ./data (under cwd, usually /app/data on Railway).
 *
 * Railway: mount the volume at the SAME path you use here. Common choice: mount volume to /app/data and either
 * leave env unset (uses ./data = /app/data) OR set AIRALERT_DATA_DIR=/app/data
 */
type PersistenceSource = "AIRALERT_DATA_DIR" | "RAILWAY_VOLUME_MOUNT_PATH" | "cwd";

const explicitDataDir = process.env.AIRALERT_DATA_DIR?.trim();
const railwayMount = process.env.RAILWAY_VOLUME_MOUNT_PATH?.trim();

let persistenceSource: PersistenceSource;
const dataDir = (() => {
  if (explicitDataDir) {
    persistenceSource = "AIRALERT_DATA_DIR";
    return path.resolve(explicitDataDir);
  }
  if (railwayMount) {
    persistenceSource = "RAILWAY_VOLUME_MOUNT_PATH";
    return path.resolve(railwayMount);
  }
  persistenceSource = "cwd";
  return path.join(process.cwd(), "data");
})();

if (!fs.existsSync(dataDir)) {
  fs.mkdirSync(dataDir, { recursive: true });
}

/** Verify we can write — SQLite will fail silently later if this is wrong on some hosts */
try {
  const probe = path.join(dataDir, ".airalert-write-test");
  fs.writeFileSync(probe, "ok", "utf8");
  fs.unlinkSync(probe);
} catch (e) {
  console.error("[airalert] FATAL: cannot write to data directory:", dataDir, e);
}

const dbPath = path.join(dataDir, "airalert.db");
export const db: InstanceType<typeof Database> = new Database(dbPath);

const onRailway = Boolean(process.env.RAILWAY_ENVIRONMENT || process.env.RAILWAY_PROJECT_ID);
if (onRailway && persistenceSource === "cwd") {
  console.error(
    "[airalert] WARNING: No persistent DB path. Attach a volume to THIS service. Mount it at /app/data (recommended) " +
      "so it matches the app default, or set AIRALERT_DATA_DIR to your volume mount path. " +
      "Otherwise subscriptions are lost on every deploy.",
  );
}

console.log(
  `[airalert] SQLite: ${dbPath} (source=${persistenceSource}, cwd=${process.cwd()})` +
    (railwayMount ? ` RAILWAY_VOLUME_MOUNT_PATH=${railwayMount}` : ""),
);

export function getSqlitePersistenceInfo(): {
  dbPath: string;
  dataDir: string;
  persistenceSource: PersistenceSource;
  cwd: string;
  railwayVolumeMount: string | null;
  airalertDataDir: string | null;
  dbFileExists: boolean;
  dbFileBytes: number;
  onRailway: boolean;
  looksEphemeral: boolean;
} {
  let dbFileBytes = 0;
  let dbFileExists = false;
  try {
    const st = fs.statSync(dbPath);
    dbFileExists = true;
    dbFileBytes = st.size;
  } catch {
    /* empty db may not exist until first write in some edge cases */
  }
  return {
    dbPath,
    dataDir,
    persistenceSource,
    cwd: process.cwd(),
    railwayVolumeMount: railwayMount || null,
    airalertDataDir: explicitDataDir || null,
    dbFileExists,
    dbFileBytes,
    onRailway,
    looksEphemeral: onRailway && persistenceSource === "cwd",
  };
}

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
