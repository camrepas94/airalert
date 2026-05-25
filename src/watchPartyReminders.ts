import { v4 as uuidv4 } from "uuid";
import { db } from "./db.js";
import { insertActivityNotification } from "./activityNotifications.js";
import { getPushPrefsForUser, sendWebPushToUser } from "./push.js";

export type WatchPartyReminderRow = {
  id: string;
  user_id: string;
  party_key: string;
  party_title: string;
  remind_at: string;
  sent_at: string | null;
  created_at: string;
};

export function ensureWatchPartyRemindersTable(): void {
  db.exec(`
    CREATE TABLE IF NOT EXISTS watch_party_reminders (
      id TEXT PRIMARY KEY,
      user_id TEXT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
      party_key TEXT NOT NULL,
      party_title TEXT NOT NULL,
      remind_at TEXT NOT NULL,
      sent_at TEXT,
      created_at TEXT NOT NULL DEFAULT (datetime('now')),
      UNIQUE (user_id, party_key)
    );
    CREATE INDEX IF NOT EXISTS idx_watch_party_reminders_due
      ON watch_party_reminders(remind_at) WHERE sent_at IS NULL;
  `);
}

export function upsertWatchPartyReminder(opts: {
  userId: string;
  partyKey: string;
  partyTitle: string;
  remindAt: string;
}): { id: string; remindAt: string } {
  ensureWatchPartyRemindersTable();
  const partyKey = String(opts.partyKey).trim().slice(0, 120);
  const partyTitle = String(opts.partyTitle).trim().slice(0, 200);
  const remindAt = String(opts.remindAt).trim();
  if (!partyKey || !partyTitle || !remindAt) {
    throw new Error("partyKey, partyTitle, and remindAt are required");
  }
  const remindMs = new Date(remindAt).getTime();
  if (!Number.isFinite(remindMs)) throw new Error("Invalid remindAt");
  if (remindMs <= Date.now()) throw new Error("Reminder time must be in the future");

  const existing = db
    .prepare(`SELECT id FROM watch_party_reminders WHERE user_id = ? AND party_key = ?`)
    .get(opts.userId, partyKey) as { id: string } | undefined;

  const id = existing?.id ?? uuidv4();
  if (existing) {
    db.prepare(
      `UPDATE watch_party_reminders
       SET party_title = ?, remind_at = ?, sent_at = NULL
       WHERE id = ? AND user_id = ?`,
    ).run(partyTitle, remindAt, id, opts.userId);
  } else {
    db.prepare(
      `INSERT INTO watch_party_reminders (id, user_id, party_key, party_title, remind_at)
       VALUES (?, ?, ?, ?, ?)`,
    ).run(id, opts.userId, partyKey, partyTitle, remindAt);
  }
  return { id, remindAt };
}

export function listWatchPartyReminders(userId: string): WatchPartyReminderRow[] {
  ensureWatchPartyRemindersTable();
  return db
    .prepare(
      `SELECT id, user_id, party_key, party_title, remind_at, sent_at, created_at
       FROM watch_party_reminders
       WHERE user_id = ?
       ORDER BY datetime(remind_at) ASC`,
    )
    .all(userId) as WatchPartyReminderRow[];
}

function watchPartyReminderUrl(reminderId: string): string {
  return `/?tab=community&socialSection=watch&wpReminder=${encodeURIComponent(reminderId)}`;
}

/** Fire due reminders: Activity inbox row + web push (acknowledged via Activity UI). */
export async function runWatchPartyReminderNotifications(): Promise<number> {
  ensureWatchPartyRemindersTable();
  const now = new Date().toISOString();
  const rows = db
    .prepare(
      `SELECT id, user_id, party_key, party_title, remind_at
       FROM watch_party_reminders
       WHERE sent_at IS NULL AND remind_at <= ?`,
    )
    .all(now) as {
    id: string;
    user_id: string;
    party_key: string;
    party_title: string;
    remind_at: string;
  }[];

  let sent = 0;
  for (const row of rows) {
    const prefs = getPushPrefsForUser(row.user_id);
    if (!prefs.watchPartyReminder) {
      db.prepare(`UPDATE watch_party_reminders SET sent_at = datetime('now') WHERE id = ?`).run(row.id);
      continue;
    }
    const url = watchPartyReminderUrl(row.id);
    const title = "Watch party starting soon";
    const summary = row.party_title;
    insertActivityNotification({
      recipientUserId: row.user_id,
      kind: "watch_party_reminder",
      title,
      summary,
      url,
      actorUserId: null,
    });
    await sendWebPushToUser(
      row.user_id,
      {
        title: "Watch party reminder",
        body: summary,
        url,
      },
      { kind: "watchPartyReminder", activityKind: "watch_party_reminder" },
    );
    db.prepare(`UPDATE watch_party_reminders SET sent_at = datetime('now') WHERE id = ?`).run(row.id);
    sent += 1;
  }
  return sent;
}
