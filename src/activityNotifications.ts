import { v4 as uuidv4 } from "uuid";
import { db } from "./db.js";

/** Human-driven events surfaced in the header activity inbox (not episode/system feed). */
export type ActivityNotificationKind = "community_mention" | "community_reply" | "group_chat_invite";

export function insertActivityNotification(opts: {
  recipientUserId: string;
  kind: ActivityNotificationKind;
  title: string;
  summary: string | null;
  url: string | null;
  actorUserId: string | null;
  sourcePostId?: string | null;
}): void {
  const id = uuidv4();
  db.prepare(
    `INSERT INTO activity_notifications (id, user_id, kind, title, summary, url, actor_user_id, source_post_id)
     VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
  ).run(
    id,
    opts.recipientUserId,
    opts.kind,
    opts.title,
    opts.summary ?? null,
    opts.url ?? null,
    opts.actorUserId,
    opts.sourcePostId ?? null,
  );
}
