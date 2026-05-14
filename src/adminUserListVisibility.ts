import type { Database } from "better-sqlite3";

/**
 * True when a `users` row should appear in Admin > User Accounts and in `/api/admin/overview` aggregates.
 *
 * - Registered / linked identities always qualify (`auth_provider` ≠ `guest`, or email/username/password/Google).
 * - Staff and QA rows always qualify.
 * - Pure guest rows qualify only if they have My List, watch/community/social/push activity, or saved profile prefs.
 *
 * Keep alias alphanumeric (caller passes `u`).
 */
export function adminUserListVisibilitySql(userAlias: string): string {
  const u = userAlias;
  return `(
    (COALESCE(${u}.is_admin, 0) != 0)
    OR (COALESCE(${u}.is_test_account, 0) != 0)
    OR lower(trim(coalesce(${u}.auth_provider, ''))) != 'guest'
    OR length(trim(coalesce(${u}.email, ''))) > 0
    OR length(trim(coalesce(${u}.username, ''))) > 0
    OR (${u}.password_hash IS NOT NULL AND trim(${u}.password_hash) != '')
    OR length(trim(coalesce(${u}.google_sub, ''))) > 0
    OR (SELECT COUNT(*) FROM show_subscriptions s WHERE s.user_id = ${u}.id) > 0
    OR EXISTS (SELECT 1 FROM watch_tasks w WHERE w.user_id = ${u}.id LIMIT 1)
    OR EXISTS (
      SELECT 1 FROM community_posts p
      WHERE p.user_id = ${u}.id AND (p.deleted_at IS NULL)
      LIMIT 1
    )
    OR EXISTS (SELECT 1 FROM community_thread_push_subs t WHERE t.user_id = ${u}.id LIMIT 1)
    OR EXISTS (SELECT 1 FROM community_episode_ratings r WHERE r.user_id = ${u}.id LIMIT 1)
    OR EXISTS (SELECT 1 FROM community_episode_poll_votes v WHERE v.user_id = ${u}.id LIMIT 1)
    OR EXISTS (SELECT 1 FROM community_episode_polls pol WHERE pol.user_id = ${u}.id LIMIT 1)
    OR EXISTS (SELECT 1 FROM community_watch_challenge_participants cwp WHERE cwp.user_id = ${u}.id LIMIT 1)
    OR EXISTS (SELECT 1 FROM user_person_follows pf WHERE pf.user_id = ${u}.id LIMIT 1)
    OR EXISTS (
      SELECT 1 FROM user_follows uf
      WHERE uf.follower_id = ${u}.id OR uf.followed_id = ${u}.id
      LIMIT 1
    )
    OR EXISTS (SELECT 1 FROM user_blocks b WHERE b.blocker_user_id = ${u}.id OR b.blocked_user_id = ${u}.id LIMIT 1)
    OR EXISTS (SELECT 1 FROM moderation_reports mr WHERE mr.reporter_user_id = ${u}.id LIMIT 1)
    OR EXISTS (SELECT 1 FROM dm_messages dm WHERE dm.sender_id = ${u}.id LIMIT 1)
    OR EXISTS (SELECT 1 FROM dm_group_messages gm WHERE gm.sender_id = ${u}.id LIMIT 1)
    OR EXISTS (SELECT 1 FROM dm_group_members gmem WHERE gmem.user_id = ${u}.id LIMIT 1)
    OR EXISTS (SELECT 1 FROM web_push_subscriptions wps WHERE wps.user_id = ${u}.id LIMIT 1)
    OR EXISTS (SELECT 1 FROM devices d WHERE d.user_id = ${u}.id LIMIT 1)
    OR EXISTS (SELECT 1 FROM beta_feedback bf WHERE bf.user_id = ${u}.id LIMIT 1)
    OR length(trim(coalesce(${u}.display_name, ''))) > 0
    OR length(trim(coalesce(${u}.about_me, ''))) > 0
    OR (${u}.avatar_data_url IS NOT NULL AND length(trim(${u}.avatar_data_url)) > 0)
    OR length(trim(coalesce(${u}.onboarding_prefs_json, ''))) > 4
    OR length(trim(coalesce(${u}.favorite_show, ''))) > 0
    OR length(trim(coalesce(${u}.favorite_show_2, ''))) > 0
    OR length(trim(coalesce(${u}.favorite_show_3, ''))) > 0
    OR length(trim(coalesce(${u}.viewer_role_override, ''))) > 0
  )`;
}

/** Guest rows that are hidden from the admin list (no identity, no My List, no engagement). */
export function countAdminExcludedDormantGuests(db: Database): number {
  const vis = adminUserListVisibilitySql("u");
  const row = db
    .prepare(
      `SELECT COUNT(*) AS c FROM users u
       WHERE lower(trim(coalesce(u.auth_provider, ''))) = 'guest'
         AND NOT (${vis})`,
    )
    .get() as { c: number };
  return Number(row?.c) || 0;
}

export type PurgeDormantGuestsResult = { deleted: number };

/**
 * Permanently removes **guest** accounts that are dormant (same definition as admin exclusion), older than
 * `minAgeDays`, never staff/QA. Never touches registered (email/username/password/google) users.
 */
export function purgeDormantGuestAccountsOlderThan(
  db: Database,
  opts: { minAgeDays: number; maxPerRun?: number },
): PurgeDormantGuestsResult {
  if (process.env.DISABLE_DORMANT_GUEST_PURGE === "1") {
    return { deleted: 0 };
  }
  const minAgeDays = Math.max(1, Math.floor(opts.minAgeDays || 7));
  const cap = Math.min(5000, Math.max(1, Math.floor(opts.maxPerRun ?? 2000)));
  const vis = adminUserListVisibilitySql("u");
  const ageMod = `-${minAgeDays} days`;
  const del = db.prepare(
    `DELETE FROM users WHERE id IN (
       SELECT u.id FROM users u
       WHERE lower(trim(coalesce(u.auth_provider, ''))) = 'guest'
         AND NOT (${vis})
         AND (COALESCE(u.is_admin, 0) = 0)
         AND (COALESCE(u.is_test_account, 0) = 0)
         AND datetime(u.created_at) < datetime('now', ?)
       LIMIT ?
     )`,
  );
  const info = del.run(ageMod, cap);
  return { deleted: Number(info.changes) || 0 };
}
