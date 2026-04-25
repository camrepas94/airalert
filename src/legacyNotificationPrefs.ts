import { db } from "./db.js";
import { mergePushPrefsFromJson, parsePushPrefsJson, type PushPrefs } from "./push.js";

/**
 * The `notification_preferences` SQLite table is **deprecated** — it duplicated toggles that now live
 * in `users.push_prefs_json` (and task nudge cadence in `users.task_nudge_days_after_air`).
 * This module maps between the legacy API shape and the current storage so GET/PATCH stay backward
 * compatible without two sources of truth in application logic.
 *
 * Column mapping to PushPrefs (conceptual, not 1:1 with old names):
 * - mention_in_thread → communityMention
 * - reply_to_post → communityReply
 * - thread_reply → communityThreadNewPost
 */

export type LegacyNotificationPrefsRow = {
  user_id: string;
  episode_airs: number;
  dm_message: number;
  mention_in_thread: number;
  reply_to_post: number;
  thread_reply: number;
  still_watching_days: number;
};

function to01(b: boolean): number {
  return b ? 1 : 0;
}

function nudgeToStillWatchingDays(nudge: number | null | undefined): number {
  if (nudge === 1 || nudge === 3 || nudge === 7) return nudge;
  return 0;
}

export function getLegacyNotificationPrefsForUser(userId: string): LegacyNotificationPrefsRow {
  const row = db
    .prepare(`SELECT push_prefs_json, task_nudge_days_after_air FROM users WHERE id = ?`)
    .get(userId) as { push_prefs_json: string | null; task_nudge_days_after_air: number | null } | undefined;
  if (!row) {
    const p0: PushPrefs = parsePushPrefsJson(null);
    return {
      user_id: userId,
      episode_airs: to01(p0.episodeAirsToday),
      dm_message: to01(p0.dmMessage),
      mention_in_thread: to01(p0.communityMention),
      reply_to_post: to01(p0.communityReply),
      thread_reply: to01(p0.communityThreadNewPost),
      still_watching_days: 0,
    };
  }
  const p: PushPrefs = parsePushPrefsJson(row.push_prefs_json);
  return {
    user_id: userId,
    episode_airs: to01(p.episodeAirsToday),
    dm_message: to01(p.dmMessage),
    mention_in_thread: to01(p.communityMention),
    reply_to_post: to01(p.communityReply),
    thread_reply: to01(p.communityThreadNewPost),
    still_watching_days: nudgeToStillWatchingDays(row?.task_nudge_days_after_air ?? null),
  };
}

export function applyLegacyNotificationPrefsPatch(
  userId: string,
  body: Record<string, unknown>,
): { ok: true; pushPrefs: PushPrefs; stillWatchingDays: number | "unchanged" } | { ok: false; reason: "user_not_found" } {
  const row = db
    .prepare(`SELECT push_prefs_json, task_nudge_days_after_air FROM users WHERE id = ?`)
    .get(userId) as { push_prefs_json: string | null; task_nudge_days_after_air: number | null } | undefined;
  if (!row) {
    return { ok: false, reason: "user_not_found" };
  }

  let next = parsePushPrefsJson(row.push_prefs_json);
  const patch: Partial<Record<keyof PushPrefs, boolean>> = {};
  if ("episode_airs" in body) patch.episodeAirsToday = body.episode_airs === true || body.episode_airs === 1;
  if ("dm_message" in body) patch.dmMessage = body.dm_message === true || body.dm_message === 1;
  if ("mention_in_thread" in body)
    patch.communityMention = body.mention_in_thread === true || body.mention_in_thread === 1;
  if ("reply_to_post" in body) patch.communityReply = body.reply_to_post === true || body.reply_to_post === 1;
  if ("thread_reply" in body) patch.communityThreadNewPost = body.thread_reply === true || body.thread_reply === 1;
  if (Object.keys(patch).length) {
    next = mergePushPrefsFromJson(row.push_prefs_json, patch);
  }

  let nudge: number | "unchanged" = "unchanged";
  if ("still_watching_days" in body) {
    const n = Number(body.still_watching_days);
    nudge = n === 1 || n === 3 || n === 7 ? n : 0;
  }

  if (Object.keys(patch).length) {
    db.prepare(`UPDATE users SET push_prefs_json = ? WHERE id = ?`).run(JSON.stringify(next), userId);
  }
  if (nudge !== "unchanged") {
    if (nudge === 0) {
      db.prepare(`UPDATE users SET task_nudge_days_after_air = NULL WHERE id = ?`).run(userId);
    } else {
      db.prepare(`UPDATE users SET task_nudge_days_after_air = ? WHERE id = ?`).run(nudge, userId);
    }
  }

  return { ok: true, pushPrefs: next, stillWatchingDays: nudge };
}
