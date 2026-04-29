import webpush from "web-push";
import { db } from "./db.js";
import {
  pushNotificationCategory,
  pushNotificationSurface,
  type PushNotificationKind,
} from "./notificationTypes.js";

/** Strips quotes/BOM/whitespace often introduced when pasting into Railway or .env files. */
function normalizeVapidKey(raw: string | undefined | null): string | null {
  if (raw == null) return null;
  let s = String(raw).trim();
  if (s.charCodeAt(0) === 0xfeff) s = s.slice(1).trim();
  if ((s.startsWith('"') && s.endsWith('"')) || (s.startsWith("'") && s.endsWith("'"))) {
    s = s.slice(1, -1).trim();
  }
  s = s.replace(/\s/g, "");
  return s.length > 0 ? s : null;
}

export function getVapidPublicKey(): string | null {
  return normalizeVapidKey(process.env.VAPID_PUBLIC_KEY);
}

/** Returns true if VAPID keys are set and web-push is configured. */
export function configureWebPush(): boolean {
  const pub = normalizeVapidKey(process.env.VAPID_PUBLIC_KEY);
  const priv = normalizeVapidKey(process.env.VAPID_PRIVATE_KEY);
  const subject = process.env.VAPID_SUBJECT?.trim() || "mailto:airalert@example.com";
  if (!pub || !priv) return false;
  webpush.setVapidDetails(subject, pub, priv);
  return true;
}

export function isWebPushConfigured(): boolean {
  return !!(normalizeVapidKey(process.env.VAPID_PUBLIC_KEY) && normalizeVapidKey(process.env.VAPID_PRIVATE_KEY));
}

type SubRow = { endpoint: string; p256dh: string; auth: string };

/** Granular push toggles (stored in users.push_prefs_json). All default true when unset. */
export type PushPrefs = {
  episodeAirsToday: boolean;
  taskStillWatching: boolean;
  dmMessage: boolean;
  communityMention: boolean;
  communityReply: boolean;
  communityThreadNewPost: boolean;
  /** People you follow — new show on their cast/crew credits */
  personNewProject: boolean;
};

export const DEFAULT_PUSH_PREFS: PushPrefs = {
  episodeAirsToday: true,
  taskStillWatching: true,
  dmMessage: true,
  communityMention: true,
  communityReply: true,
  communityThreadNewPost: true,
  personNewProject: true,
};

export function parsePushPrefsJson(raw: string | null | undefined): PushPrefs {
  const base: PushPrefs = { ...DEFAULT_PUSH_PREFS };
  if (!raw || !String(raw).trim()) return base;
  try {
    const o = JSON.parse(raw) as Record<string, unknown>;
    if (typeof o !== "object" || o === null) return base;
    for (const k of Object.keys(DEFAULT_PUSH_PREFS) as (keyof PushPrefs)[]) {
      if (k in o) base[k] = o[k] !== false;
    }
    return base;
  } catch {
    return base;
  }
}

export function getPushPrefsForUser(userId: string): PushPrefs {
  const row = db
    .prepare(`SELECT push_prefs_json FROM users WHERE id = ?`)
    .get(userId) as { push_prefs_json: string | null } | undefined;
  return parsePushPrefsJson(row?.push_prefs_json);
}

/** Merge partial client updates into stored JSON; unknown keys ignored. */
export function mergePushPrefsFromJson(
  existing: string | null | undefined,
  patch: Partial<Record<keyof PushPrefs, boolean>> | null | undefined,
): PushPrefs {
  const base = parsePushPrefsJson(existing);
  if (!patch || typeof patch !== "object") return base;
  for (const k of Object.keys(DEFAULT_PUSH_PREFS) as (keyof PushPrefs)[]) {
    if (Object.prototype.hasOwnProperty.call(patch, k) && typeof patch[k] === "boolean") {
      base[k] = patch[k]!;
    }
  }
  return base;
}

type SendOpts = { kind?: PushNotificationKind };

export async function sendWebPushToUser(
  userId: string,
  payload: { title: string; body: string; url?: string },
  options?: SendOpts,
): Promise<void> {
  if (!isWebPushConfigured()) return;

  if (options?.kind) {
    const prefs = getPushPrefsForUser(userId);
    if (!prefs[options.kind]) return;
  }

  const rows = db
    .prepare(`SELECT endpoint, p256dh, auth FROM web_push_subscriptions WHERE user_id = ?`)
    .all(userId) as SubRow[];

  if (!rows.length) return;

  const body = JSON.stringify({
    title: payload.title,
    body: payload.body,
    url: payload.url ?? "/",
    kind: options?.kind ?? null,
    category: options?.kind ? pushNotificationCategory(options.kind) : null,
    surface: options?.kind ? pushNotificationSurface(options.kind) : null,
  });

  for (const row of rows) {
    const subscription = {
      endpoint: row.endpoint,
      keys: { p256dh: row.p256dh, auth: row.auth },
    };
    try {
      await webpush.sendNotification(subscription, body, {
        TTL: 60 * 60 * 12,
        urgency: "normal",
      });
    } catch (err: unknown) {
      const status = typeof err === "object" && err !== null && "statusCode" in err ? (err as { statusCode?: number }).statusCode : undefined;
      if (status === 404 || status === 410) {
        db.prepare(`DELETE FROM web_push_subscriptions WHERE endpoint = ?`).run(row.endpoint);
      } else {
        const msg = err instanceof Error ? err.message : String(err);
        console.warn("[web-push] send failed", { userId, status, endpointPrefix: row.endpoint.slice(0, 48), message: msg });
      }
    }
  }
}
