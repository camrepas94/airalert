import webpush from "web-push";
import { db } from "./db.js";

export function getVapidPublicKey(): string | null {
  const k = process.env.VAPID_PUBLIC_KEY?.trim();
  return k && k.length > 0 ? k : null;
}

/** Returns true if VAPID keys are set and web-push is configured. */
export function configureWebPush(): boolean {
  const pub = process.env.VAPID_PUBLIC_KEY?.trim();
  const priv = process.env.VAPID_PRIVATE_KEY?.trim();
  const subject = process.env.VAPID_SUBJECT?.trim() || "mailto:airalert@example.com";
  if (!pub || !priv) return false;
  webpush.setVapidDetails(subject, pub, priv);
  return true;
}

export function isWebPushConfigured(): boolean {
  return !!(process.env.VAPID_PUBLIC_KEY?.trim() && process.env.VAPID_PRIVATE_KEY?.trim());
}

type SubRow = { endpoint: string; p256dh: string; auth: string };

export async function sendWebPushToUser(
  userId: string,
  payload: { title: string; body: string; url?: string },
): Promise<void> {
  if (!isWebPushConfigured()) return;

  const rows = db
    .prepare(`SELECT endpoint, p256dh, auth FROM web_push_subscriptions WHERE user_id = ?`)
    .all(userId) as SubRow[];

  if (!rows.length) return;

  const body = JSON.stringify({
    title: payload.title,
    body: payload.body,
    url: payload.url ?? "/",
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
      }
    }
  }
}
