import { db } from "./db.js";

/** Mirrors client / API: derived from subscribed show count (not stored in DB). */
export type ViewerRole = "newb" | "tv_watcher" | "tv_binger";

/** Inclusive upper bound for Newb tier (0–2 shows). */
export const NEWB_MAX_SUBSCRIBED_SHOWS = 2;
/** Inclusive upper bound for TV Watcher tier (3–9 shows). */
export const TV_WATCHER_MAX_SUBSCRIBED_SHOWS = 9;

/** Subscribed show count required to exit “Getting Started” / unlock social (Community writes, Inbox, DMs). */
export const ACTIVATION_MIN_SUBSCRIBED_SHOWS = NEWB_MAX_SUBSCRIBED_SHOWS + 1;

/** True when the user is still in the early activation band (0–2 subscribed shows). */
export function isLowShowUserBySubscribedCount(subscribedShowCount: number): boolean {
  const n = Math.max(0, Math.floor(Number(subscribedShowCount) || 0));
  return n < ACTIVATION_MIN_SUBSCRIBED_SHOWS;
}

/** True when show count alone would grant TV Watcher+ (ignores admin / viewer_role_override). */
export function isActivatedBySubscribedCountAlone(subscribedShowCount: number): boolean {
  return !isLowShowUserBySubscribedCount(subscribedShowCount);
}

export const UNLOCK_SOCIAL_FEATURES_MESSAGE = "Add 3 shows to unlock this feature.";

export function getSubscribedShowCountForUser(userId: string): number {
  const row = db
    .prepare(`SELECT COUNT(*) AS c FROM show_subscriptions WHERE user_id = ?`)
    .get(userId) as { c: number } | undefined;
  return Number(row?.c ?? 0) || 0;
}

export function deriveViewerRole(subscribedShowCount: number): ViewerRole {
  const n = Math.max(0, Math.floor(Number(subscribedShowCount) || 0));
  if (n <= NEWB_MAX_SUBSCRIBED_SHOWS) return "newb";
  if (n <= TV_WATCHER_MAX_SUBSCRIBED_SHOWS) return "tv_watcher";
  return "tv_binger";
}

function isUserDbAdmin(userId: string): boolean {
  const row = db.prepare(`SELECT is_admin FROM users WHERE id = ?`).get(userId) as { is_admin: number } | undefined;
  return Boolean(row && Number(row.is_admin));
}

/** Stored admin override; `null` = use subscription-based derivation. */
export function getViewerRoleOverrideForUser(userId: string): ViewerRole | null {
  const row = db
    .prepare(`SELECT viewer_role_override FROM users WHERE id = ?`)
    .get(userId) as { viewer_role_override: string | null } | undefined;
  const raw = row?.viewer_role_override;
  if (raw == null || !String(raw).trim()) return null;
  const t = String(raw).trim();
  if (t === "newb" || t === "tv_watcher" || t === "tv_binger") return t;
  return null;
}

/** Effective role: manual override when set, otherwise from subscribed show count. */
export function effectiveViewerRoleForUser(userId: string): ViewerRole {
  const o = getViewerRoleOverrideForUser(userId);
  if (o) return o;
  return deriveViewerRole(getSubscribedShowCountForUser(userId));
}

/** Inbox, DMs, community writes, and similar social features (server source of truth). */
export function hasFullSocialAccess(userId: string): boolean {
  if (isUserDbAdmin(userId)) return true;
  return effectiveViewerRoleForUser(userId) !== "newb";
}

export function viewerRolePayloadForUser(userId: string): {
  subscribedShowCount: number;
  viewerRole: ViewerRole;
  viewerRoleDerived: ViewerRole;
  viewerRoleOverride: ViewerRole | null;
} {
  const subscribedShowCount = getSubscribedShowCountForUser(userId);
  const viewerRoleDerived = deriveViewerRole(subscribedShowCount);
  const viewerRoleOverride = getViewerRoleOverrideForUser(userId);
  const viewerRole = viewerRoleOverride ?? viewerRoleDerived;
  return { subscribedShowCount, viewerRole, viewerRoleDerived, viewerRoleOverride };
}
