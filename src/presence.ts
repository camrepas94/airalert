import { db } from "./db.js";

/** Activity at or within this many seconds → `online` (green). */
export const PRESENCE_ONLINE_SEC = 60;

/** After online window, still within this many seconds since last activity → `idle` (yellow). */
export const PRESENCE_IDLE_SEC = 300;

/** Beyond {@link PRESENCE_IDLE_SEC} → `offline` (no bubble). */
export type PresenceStatus = "online" | "idle" | "offline";

export function touchUserPresence(userId: string): void {
  db.prepare(
    `INSERT INTO user_presence (user_id, last_activity_at) VALUES (?, datetime('now'))
     ON CONFLICT(user_id) DO UPDATE SET last_activity_at = excluded.last_activity_at`,
  ).run(userId);
}

export function presenceStatusFromLastActivity(lastActivityAt: string | null | undefined, nowMs: number): PresenceStatus {
  if (!lastActivityAt) return "offline";
  const iso = String(lastActivityAt).includes("T") ? String(lastActivityAt) : String(lastActivityAt).replace(" ", "T");
  const t = Date.parse(iso);
  if (!Number.isFinite(t)) return "offline";
  const ageSec = (nowMs - t) / 1000;
  if (ageSec <= PRESENCE_ONLINE_SEC) return "online";
  if (ageSec <= PRESENCE_IDLE_SEC) return "idle";
  return "offline";
}

export function getPresenceMapForUserIds(ids: string[], nowMs: number): Record<string, PresenceStatus> {
  const out: Record<string, PresenceStatus> = {};
  if (!ids.length) return out;
  const unique = [...new Set(ids.map((x) => String(x).trim()).filter(Boolean))].slice(0, 80);
  if (!unique.length) return out;
  const placeholders = unique.map(() => "?").join(", ");
  const rows = db
    .prepare(`SELECT user_id AS userId, last_activity_at AS lastActivityAt FROM user_presence WHERE user_id IN (${placeholders})`)
    .all(...unique) as { userId: string; lastActivityAt: string }[];
  const byId = new Map(rows.map((r) => [r.userId, r.lastActivityAt]));
  for (const id of unique) {
    out[id] = presenceStatusFromLastActivity(byId.get(id) ?? null, nowMs);
  }
  return out;
}
