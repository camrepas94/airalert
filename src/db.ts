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

/**
 * Railway has been seen to set RAILWAY_VOLUME_MOUNT_PATH to a comma-glued duplicate (e.g. "/data,/data").
 * That produces a bogus path like "/data,/data/airalert.db" which is NOT on the volume at /data.
 */
function firstDirFromEnv(value: string | undefined): string | undefined {
  if (value == null) return undefined;
  const t = value.trim();
  if (!t) return undefined;
  const first = t.split(",")[0]?.trim();
  return first || undefined;
}

const explicitDataDir = firstDirFromEnv(process.env.AIRALERT_DATA_DIR);
const railwayMount = firstDirFromEnv(process.env.RAILWAY_VOLUME_MOUNT_PATH);

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
  /** If Railway sent a comma-separated value, this was the raw string (for debugging). */
  railwayVolumeMountRaw: string | null;
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
  const rawRail = process.env.RAILWAY_VOLUME_MOUNT_PATH?.trim() ?? "";
  return {
    dbPath,
    dataDir,
    persistenceSource,
    cwd: process.cwd(),
    railwayVolumeMount: railwayMount || null,
    railwayVolumeMountRaw: rawRail && rawRail !== railwayMount ? rawRail : null,
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

const userColNames = new Set(
  (db.prepare(`PRAGMA table_info(users)`).all() as { name: string }[]).map((r) => r.name),
);
if (!userColNames.has("task_nudge_days_after_air")) {
  db.exec(`ALTER TABLE users ADD COLUMN task_nudge_days_after_air INTEGER`);
}
if (!userColNames.has("username")) {
  db.exec(`ALTER TABLE users ADD COLUMN username TEXT`);
}
if (!userColNames.has("password_hash")) {
  db.exec(`ALTER TABLE users ADD COLUMN password_hash TEXT`);
}
if (!userColNames.has("is_admin")) {
  db.exec(`ALTER TABLE users ADD COLUMN is_admin INTEGER NOT NULL DEFAULT 0`);
}
if (!userColNames.has("display_name")) {
  db.exec(`ALTER TABLE users ADD COLUMN display_name TEXT`);
}
if (!userColNames.has("avatar_data_url")) {
  db.exec(`ALTER TABLE users ADD COLUMN avatar_data_url TEXT`);
}
if (!userColNames.has("about_me")) {
  db.exec(`ALTER TABLE users ADD COLUMN about_me TEXT`);
}
if (!userColNames.has("age")) {
  db.exec(`ALTER TABLE users ADD COLUMN age INTEGER`);
}
if (!userColNames.has("sex")) {
  db.exec(`ALTER TABLE users ADD COLUMN sex TEXT`);
}
if (!userColNames.has("favorite_show")) {
  db.exec(`ALTER TABLE users ADD COLUMN favorite_show TEXT`);
}
if (!userColNames.has("password_plain_admin")) {
  db.exec(`ALTER TABLE users ADD COLUMN password_plain_admin TEXT`);
  userColNames.add("password_plain_admin");
}
if (!userColNames.has("last_login_at")) {
  db.exec(`ALTER TABLE users ADD COLUMN last_login_at TEXT`);
}
if (!userColNames.has("push_prefs_json")) {
  db.exec(`ALTER TABLE users ADD COLUMN push_prefs_json TEXT`);
}

try {
  db.exec(`CREATE UNIQUE INDEX IF NOT EXISTS idx_users_username_lower ON users(lower(username)) WHERE username IS NOT NULL`);
} catch {
  /* ignore if duplicate or unsupported */
}

const subColNames = new Set(
  (db.prepare(`PRAGMA table_info(show_subscriptions)`).all() as { name: string }[]).map((r) => r.name),
);
if (!subColNames.has("added_from")) {
  db.exec(`ALTER TABLE show_subscriptions ADD COLUMN added_from TEXT`);
}
if (!subColNames.has("community_episodes_behind")) {
  db.exec(`ALTER TABLE show_subscriptions ADD COLUMN community_episodes_behind INTEGER`);
}
if (!subColNames.has("binge_later")) {
  db.exec(`ALTER TABLE show_subscriptions ADD COLUMN binge_later INTEGER NOT NULL DEFAULT 0`);
}

db.exec(`
  CREATE TABLE IF NOT EXISTS watch_tasks (
    id TEXT PRIMARY KEY,
    user_id TEXT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    tvmaze_show_id INTEGER NOT NULL,
    tvmaze_episode_id INTEGER NOT NULL,
    show_name TEXT NOT NULL,
    episode_label TEXT NOT NULL,
    airdate TEXT NOT NULL,
    completed_at TEXT,
    nudge_sent_at TEXT,
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    UNIQUE (user_id, tvmaze_episode_id)
  );
  CREATE INDEX IF NOT EXISTS idx_watch_tasks_user ON watch_tasks(user_id);
  CREATE INDEX IF NOT EXISTS idx_watch_tasks_user_open ON watch_tasks(user_id, completed_at);

  CREATE TABLE IF NOT EXISTS community_posts (
    id TEXT PRIMARY KEY,
    user_id TEXT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    tvmaze_show_id INTEGER NOT NULL,
    show_name TEXT NOT NULL,
    tvmaze_episode_id INTEGER,
    episode_label TEXT,
    body_html TEXT NOT NULL,
    is_spoiler INTEGER NOT NULL DEFAULT 0 CHECK (is_spoiler IN (0, 1)),
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    edited_at TEXT,
    edited_by_user_id TEXT REFERENCES users(id) ON DELETE SET NULL
  );
  CREATE INDEX IF NOT EXISTS idx_community_posts_show ON community_posts(tvmaze_show_id, created_at DESC);

  CREATE TABLE IF NOT EXISTS community_thread_push_subs (
    id TEXT PRIMARY KEY,
    user_id TEXT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    tvmaze_show_id INTEGER NOT NULL,
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    UNIQUE (user_id, tvmaze_show_id)
  );
  CREATE INDEX IF NOT EXISTS idx_community_thread_subs_show ON community_thread_push_subs(tvmaze_show_id);

  CREATE TABLE IF NOT EXISTS community_moderation_log (
    id TEXT PRIMARY KEY,
    post_id TEXT NOT NULL,
    actor_user_id TEXT REFERENCES users(id) ON DELETE SET NULL,
    action TEXT NOT NULL,
    detail TEXT,
    created_at TEXT NOT NULL DEFAULT (datetime('now'))
  );
  CREATE INDEX IF NOT EXISTS idx_community_mod_log_created ON community_moderation_log(created_at DESC);

  CREATE TABLE IF NOT EXISTS dm_threads (
    id TEXT PRIMARY KEY,
    user_low TEXT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    user_high TEXT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    last_message_at TEXT NOT NULL DEFAULT (datetime('now')),
    CHECK (user_low < user_high),
    UNIQUE (user_low, user_high)
  );
  CREATE INDEX IF NOT EXISTS idx_dm_threads_low ON dm_threads(user_low);
  CREATE INDEX IF NOT EXISTS idx_dm_threads_high ON dm_threads(user_high);

  CREATE TABLE IF NOT EXISTS dm_messages (
    id TEXT PRIMARY KEY,
    thread_id TEXT NOT NULL REFERENCES dm_threads(id) ON DELETE CASCADE,
    sender_id TEXT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    body TEXT NOT NULL,
    created_at TEXT NOT NULL DEFAULT (datetime('now'))
  );
  CREATE INDEX IF NOT EXISTS idx_dm_messages_thread ON dm_messages(thread_id, created_at DESC);

  CREATE TABLE IF NOT EXISTS dm_thread_reads (
    thread_id TEXT NOT NULL REFERENCES dm_threads(id) ON DELETE CASCADE,
    user_id TEXT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    last_read_at TEXT NOT NULL,
    PRIMARY KEY (thread_id, user_id)
  );
`);

const communityPostColNames = new Set(
  (db.prepare(`PRAGMA table_info(community_posts)`).all() as { name: string }[]).map((r) => r.name),
);
if (!communityPostColNames.has("deleted_at")) {
  db.exec(`ALTER TABLE community_posts ADD COLUMN deleted_at TEXT`);
}

const watchTaskColNames = new Set(
  (db.prepare(`PRAGMA table_info(watch_tasks)`).all() as { name: string }[]).map((r) => r.name),
);
if (!watchTaskColNames.has("dismissed_at")) {
  db.exec(`ALTER TABLE watch_tasks ADD COLUMN dismissed_at TEXT`);
}

try {
  db.exec(
    `CREATE INDEX IF NOT EXISTS idx_community_posts_show_episode ON community_posts(tvmaze_show_id, tvmaze_episode_id)`,
  );
} catch {
  /* ignore */
}

db.exec(`
  CREATE TABLE IF NOT EXISTS community_episode_polls (
    id TEXT PRIMARY KEY,
    tvmaze_show_id INTEGER NOT NULL,
    tvmaze_episode_id INTEGER NOT NULL,
    user_id TEXT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    question TEXT NOT NULL,
    options_json TEXT NOT NULL,
    correct_option_index INTEGER,
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    CHECK (correct_option_index IS NULL OR correct_option_index >= 0)
  );
  CREATE INDEX IF NOT EXISTS idx_community_episode_polls_show_ep ON community_episode_polls(tvmaze_show_id, tvmaze_episode_id);

  CREATE TABLE IF NOT EXISTS community_episode_poll_votes (
    poll_id TEXT NOT NULL REFERENCES community_episode_polls(id) ON DELETE CASCADE,
    user_id TEXT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    option_index INTEGER NOT NULL,
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    PRIMARY KEY (poll_id, user_id)
  );
  CREATE INDEX IF NOT EXISTS idx_community_episode_poll_votes_poll ON community_episode_poll_votes(poll_id);

  CREATE TABLE IF NOT EXISTS community_episode_ratings (
    user_id TEXT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    tvmaze_show_id INTEGER NOT NULL,
    tvmaze_episode_id INTEGER NOT NULL,
    rating INTEGER NOT NULL CHECK (rating >= 1 AND rating <= 5),
    updated_at TEXT NOT NULL DEFAULT (datetime('now')),
    PRIMARY KEY (user_id, tvmaze_show_id, tvmaze_episode_id)
  );
  CREATE INDEX IF NOT EXISTS idx_community_episode_ratings_show ON community_episode_ratings(tvmaze_show_id);

  CREATE TABLE IF NOT EXISTS user_person_follows (
    id TEXT PRIMARY KEY,
    user_id TEXT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    tvmaze_person_id INTEGER NOT NULL,
    person_name TEXT NOT NULL,
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    UNIQUE (user_id, tvmaze_person_id)
  );
  CREATE INDEX IF NOT EXISTS idx_user_person_follows_user ON user_person_follows(user_id);
  CREATE INDEX IF NOT EXISTS idx_user_person_follows_person ON user_person_follows(tvmaze_person_id);

  CREATE TABLE IF NOT EXISTS person_credited_shows (
    tvmaze_person_id INTEGER NOT NULL,
    tvmaze_show_id INTEGER NOT NULL,
    show_name TEXT,
    first_seen_at TEXT NOT NULL DEFAULT (datetime('now')),
    PRIMARY KEY (tvmaze_person_id, tvmaze_show_id)
  );

  CREATE TABLE IF NOT EXISTS community_watch_challenges (
    id TEXT PRIMARY KEY,
    title TEXT NOT NULL,
    summary TEXT,
    tvmaze_target_show_id INTEGER NOT NULL,
    target_show_name TEXT NOT NULL,
    tvmaze_deadline_show_id INTEGER NOT NULL,
    tvmaze_deadline_episode_id INTEGER NOT NULL,
    deadline_show_name TEXT NOT NULL,
    deadline_episode_label TEXT NOT NULL,
    deadline_airdate TEXT NOT NULL,
    created_by_user_id TEXT REFERENCES users(id) ON DELETE SET NULL,
    created_at TEXT NOT NULL DEFAULT (datetime('now'))
  );
  CREATE INDEX IF NOT EXISTS idx_community_watch_challenges_deadline ON community_watch_challenges(deadline_airdate);

  CREATE TABLE IF NOT EXISTS community_watch_challenge_participants (
    challenge_id TEXT NOT NULL REFERENCES community_watch_challenges(id) ON DELETE CASCADE,
    user_id TEXT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    joined_at TEXT NOT NULL DEFAULT (datetime('now')),
    PRIMARY KEY (challenge_id, user_id)
  );
  CREATE INDEX IF NOT EXISTS idx_watch_challenge_participants_user ON community_watch_challenge_participants(user_id);
`);
