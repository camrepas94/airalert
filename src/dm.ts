import { v4 as uuidv4 } from "uuid";
import type { WebSocket } from "ws";
import { db } from "./db.js";
import { sendWebPushToUser } from "./push.js";

const MAX_DM_BODY_LEN = 4000;
const MAX_GROUP_NAME_LEN = 80;
const MAX_GROUP_MEMBERS = 50;

function normalizeGroupName(raw: string): string {
  const t = raw.replace(/\r\n/g, "\n").trim();
  if (!t) return "";
  return t.length > MAX_GROUP_NAME_LEN ? t.slice(0, MAX_GROUP_NAME_LEN) : t;
}

const socketsByUser = new Map<string, Set<WebSocket>>();

function authorLabelForUser(userId: string): string {
  const row = db
    .prepare(`SELECT display_name AS displayName, username FROM users WHERE id = ?`)
    .get(userId) as { displayName: string | null; username: string | null } | undefined;
  if (!row) return "Someone";
  if (row.displayName && String(row.displayName).trim()) return String(row.displayName).trim();
  if (row.username && String(row.username).trim()) return "@" + String(row.username).trim();
  return "Member";
}

export function registerDmSocket(userId: string, socket: WebSocket): void {
  let set = socketsByUser.get(userId);
  if (!set) {
    set = new Set();
    socketsByUser.set(userId, set);
  }
  set.add(socket);
}

export function unregisterDmSocket(userId: string, socket: WebSocket): void {
  const set = socketsByUser.get(userId);
  if (!set) return;
  set.delete(socket);
  if (set.size === 0) socketsByUser.delete(userId);
}

function userHasActiveDmSocket(userId: string): boolean {
  const set = socketsByUser.get(userId);
  return Boolean(set && set.size > 0);
}

function broadcastToUser(userId: string, payload: Record<string, unknown>): void {
  const set = socketsByUser.get(userId);
  if (!set) return;
  const raw = JSON.stringify(payload);
  for (const ws of set) {
    if (ws.readyState === 1) {
      try {
        ws.send(raw);
      } catch {
        /* ignore */
      }
    }
  }
}

export function broadcastDmUnreadTotal(userId: string): void {
  const total = getDmUnreadTotal(userId);
  broadcastToUser(userId, { type: "dm_unread", total });
}

function orderedPair(a: string, b: string): [string, string] {
  return a < b ? [a, b] : [b, a];
}

export function getOrCreateDmThread(userIdA: string, userIdB: string): string {
  if (userIdA === userIdB) throw new Error("Cannot message yourself");
  const [low, high] = orderedPair(userIdA, userIdB);
  const existing = db
    .prepare(`SELECT id FROM dm_threads WHERE user_low = ? AND user_high = ?`)
    .get(low, high) as { id: string } | undefined;
  if (existing) return existing.id;
  const id = uuidv4();
  db.prepare(
    `INSERT INTO dm_threads (id, user_low, user_high, last_message_at) VALUES (?, ?, ?, datetime('now'))`,
  ).run(id, low, high);
  return id;
}

function assertThreadMember(threadId: string, userId: string): { low: string; high: string } | null {
  const row = db
    .prepare(`SELECT user_low AS low, user_high AS high FROM dm_threads WHERE id = ?`)
    .get(threadId) as { low: string; high: string } | undefined;
  if (!row) return null;
  if (row.low !== userId && row.high !== userId) return null;
  return row;
}

export function otherParticipant(threadLow: string, threadHigh: string, me: string): string {
  return me === threadLow ? threadHigh : threadLow;
}

export function normalizeDmBody(raw: string): string {
  const t = raw.replace(/\r\n/g, "\n").trim();
  if (!t) return "";
  if (t.length > MAX_DM_BODY_LEN) return t.slice(0, MAX_DM_BODY_LEN);
  return t;
}

export type DmMessageRow = {
  id: string;
  threadId: string;
  senderId: string;
  body: string;
  createdAt: string;
};

export type DmMessageApiRow = DmMessageRow & { readByRecipient?: boolean };

function wsPayloadToString(data: unknown): string | null {
  if (typeof data === "string") return data;
  if (Buffer.isBuffer(data)) return data.toString("utf8");
  if (data instanceof ArrayBuffer) return Buffer.from(data).toString("utf8");
  if (Array.isArray(data) && data.every((x) => Buffer.isBuffer(x))) return Buffer.concat(data as Buffer[]).toString("utf8");
  return null;
}

/** Client → server WebSocket JSON (typing, etc.). */
export function handleDmClientSocketMessage(userId: string, raw: unknown): void {
  const text = wsPayloadToString(raw);
  if (!text || text.length > 2048) return;
  let parsed: unknown;
  try {
    parsed = JSON.parse(text);
  } catch {
    return;
  }
  if (!parsed || typeof parsed !== "object") return;
  const d = parsed as Record<string, unknown>;
  if (d.type === "dm_typing") {
    if (typeof d.threadId === "string" && typeof d.typing === "boolean") {
      relayDmTyping(userId, d.threadId, d.typing);
    }
  }
}

export function relayDmTyping(fromUserId: string, threadId: string, typing: boolean): void {
  const members = assertThreadMember(threadId, fromUserId);
  if (!members) return;
  const recipientId = otherParticipant(members.low, members.high, fromUserId);
  broadcastToUser(recipientId, {
    type: "dm_typing" as const,
    threadId,
    userId: fromUserId,
    typing,
  });
}

export function getOtherParticipantLastReadAt(threadId: string, meUserId: string): string | null {
  const members = assertThreadMember(threadId, meUserId);
  if (!members) return null;
  const other = otherParticipant(members.low, members.high, meUserId);
  const r = db
    .prepare(`SELECT last_read_at AS lastReadAt FROM dm_thread_reads WHERE thread_id = ? AND user_id = ?`)
    .get(threadId, other) as { lastReadAt: string } | undefined;
  return r?.lastReadAt ?? null;
}

/** Parse server/SQLite datetime strings for ordering (handles space vs "T" separators). */
function dmInstantMs(s: string): number {
  const t = String(s).trim().replace(" ", "T");
  const ms = Date.parse(t);
  return Number.isFinite(ms) ? ms : 0;
}

/** True if the other participant's read cursor covers this outgoing message. */
export function messageReadByRecipient(messageCreatedAt: string, otherLastReadAt: string | null): boolean {
  if (!otherLastReadAt) return false;
  return dmInstantMs(otherLastReadAt) >= dmInstantMs(messageCreatedAt);
}

export function enrichMessagesWithReadState(
  messagesChronological: DmMessageRow[],
  viewerId: string,
  otherLastReadAt: string | null,
): DmMessageApiRow[] {
  return messagesChronological.map((m) => ({
    ...m,
    readByRecipient:
      m.senderId === viewerId ? messageReadByRecipient(m.createdAt, otherLastReadAt) : undefined,
  }));
}

export function sendDmMessage(senderId: string, threadId: string, body: string): DmMessageRow | null {
  const members = assertThreadMember(threadId, senderId);
  if (!members) return null;
  const text = normalizeDmBody(body);
  if (!text) return null;

  const msgId = uuidv4();
  db.prepare(
    `INSERT INTO dm_messages (id, thread_id, sender_id, body, created_at) VALUES (?, ?, ?, ?, datetime('now'))`,
  ).run(msgId, threadId, senderId, text);
  db.prepare(`UPDATE dm_threads SET last_message_at = datetime('now') WHERE id = ?`).run(threadId);

  const row = db
    .prepare(
      `SELECT id, thread_id AS threadId, sender_id AS senderId, body, created_at AS createdAt FROM dm_messages WHERE id = ?`,
    )
    .get(msgId) as DmMessageRow;

  const recipientId = otherParticipant(members.low, members.high, senderId);

  const payload = {
    type: "dm_message" as const,
    threadId,
    message: {
      id: row.id,
      senderId: row.senderId,
      body: row.body,
      createdAt: row.createdAt,
    },
  };

  broadcastToUser(recipientId, payload);
  broadcastToUser(senderId, payload);

  broadcastDmUnreadTotal(recipientId);

  if (!userHasActiveDmSocket(recipientId)) {
    const label = authorLabelForUser(senderId);
    const dmUrl = `/?dmThread=${encodeURIComponent(threadId)}`;
    void sendWebPushToUser(
      recipientId,
      {
        title: `Message from ${label}`,
        body: text.length > 140 ? text.slice(0, 137) + "…" : text,
        url: dmUrl,
      },
      { kind: "dmMessage" },
    );
  }

  return row;
}

export function getDmUnreadTotal(userId: string): number {
  const threads = db
    .prepare(`SELECT id, user_low AS low, user_high AS high FROM dm_threads WHERE user_low = ? OR user_high = ?`)
    .all(userId, userId) as { id: string; low: string; high: string }[];

  let total = 0;
  const readStmt = db.prepare(
    `SELECT last_read_at AS lastReadAt FROM dm_thread_reads WHERE thread_id = ? AND user_id = ?`,
  );
  const countStmt = db.prepare(
    `SELECT COUNT(*) AS c FROM dm_messages
     WHERE thread_id = ? AND sender_id != ? AND (datetime(created_at) > datetime(?))`,
  );

  for (const t of threads) {
    const readRow = readStmt.get(t.id, userId) as { lastReadAt: string | null } | undefined;
    const cutoff = readRow?.lastReadAt ?? "1970-01-01 00:00:00";
    const c = countStmt.get(t.id, userId, cutoff) as { c: number };
    total += Number(c.c) || 0;
  }

  const groupIds = db
    .prepare(`SELECT group_id AS id FROM dm_group_members WHERE user_id = ?`)
    .all(userId) as { id: string }[];
  for (const g of groupIds) {
    total += groupUnreadCountForUser(userId, g.id);
  }
  return total;
}

export function markDmThreadRead(threadId: string, userId: string): void {
  const members = assertThreadMember(threadId, userId);
  if (!members) return;
  const maxRow = db
    .prepare(`SELECT MAX(created_at) AS m FROM dm_messages WHERE thread_id = ?`)
    .get(threadId) as { m: string | null } | undefined;
  const at = maxRow?.m ?? new Date().toISOString().replace("T", " ").slice(0, 19);
  db.prepare(
    `INSERT INTO dm_thread_reads (thread_id, user_id, last_read_at) VALUES (?, ?, ?)
     ON CONFLICT(thread_id, user_id) DO UPDATE SET last_read_at = excluded.last_read_at`,
  ).run(threadId, userId, at);
  broadcastDmUnreadTotal(userId);

  const notifyUserId = otherParticipant(members.low, members.high, userId);
  broadcastToUser(notifyUserId, {
    type: "dm_thread_read" as const,
    threadId,
    readerUserId: userId,
    readAt: at,
  });
}

/** Clear read cursor so messages from the other person count as unread again. */
export function markDmThreadUnread(threadId: string, userId: string): void {
  if (!assertThreadMember(threadId, userId)) return;
  db.prepare(`DELETE FROM dm_thread_reads WHERE thread_id = ? AND user_id = ?`).run(threadId, userId);
  broadcastDmUnreadTotal(userId);
}

/** Deletes the thread for both participants (messages and read state cascade). */
export function deleteDmThreadAsMember(threadId: string, userId: string): boolean {
  const members = assertThreadMember(threadId, userId);
  if (!members) return false;
  const other = otherParticipant(members.low, members.high, userId);
  db.prepare(`DELETE FROM dm_threads WHERE id = ?`).run(threadId);
  broadcastDmUnreadTotal(userId);
  broadcastDmUnreadTotal(other);
  broadcastToUser(userId, { type: "dm_thread_deleted" as const, threadId });
  broadcastToUser(other, { type: "dm_thread_deleted" as const, threadId });
  return true;
}

export function listDmThreadsForUser(userId: string): {
  threadId: string;
  otherUserId: string;
  otherDisplayName: string | null;
  otherUsername: string | null;
  otherAvatarDataUrl: string | null;
  lastMessageAt: string;
  lastBody: string | null;
  lastSenderId: string | null;
  unreadCount: number;
}[] {
  const rows = db
    .prepare(
      `SELECT t.id AS threadId, t.user_low AS low, t.user_high AS high, t.last_message_at AS lastMessageAt
       FROM dm_threads t
       WHERE t.user_low = ? OR t.user_high = ?
       ORDER BY datetime(t.last_message_at) DESC`,
    )
    .all(userId, userId) as { threadId: string; low: string; high: string; lastMessageAt: string }[];

  const lastMsgStmt = db.prepare(
    `SELECT body, sender_id AS senderId FROM dm_messages WHERE thread_id = ? ORDER BY datetime(created_at) DESC, id DESC LIMIT 1`,
  );
  const readStmt = db.prepare(
    `SELECT last_read_at AS lastReadAt FROM dm_thread_reads WHERE thread_id = ? AND user_id = ?`,
  );
  const countStmt = db.prepare(
    `SELECT COUNT(*) AS c FROM dm_messages
     WHERE thread_id = ? AND sender_id != ? AND (datetime(created_at) > datetime(?))`,
  );
  const userStmt = db.prepare(
    `SELECT display_name AS displayName, username, avatar_data_url AS avatarDataUrl FROM users WHERE id = ?`,
  );

  return rows.map((t) => {
    const other = otherParticipant(t.low, t.high, userId);
    const u = userStmt.get(other) as {
      displayName: string | null;
      username: string | null;
      avatarDataUrl: string | null;
    } | null;
    const lm = lastMsgStmt.get(t.threadId) as { body: string; senderId: string } | undefined;
    const readRow = readStmt.get(t.threadId, userId) as { lastReadAt: string | null } | undefined;
    const cutoff = readRow?.lastReadAt ?? "1970-01-01 00:00:00";
    const c = countStmt.get(t.threadId, userId, cutoff) as { c: number };
    return {
      threadId: t.threadId,
      otherUserId: other,
      otherDisplayName: u?.displayName ?? null,
      otherUsername: u?.username ?? null,
      otherAvatarDataUrl: u?.avatarDataUrl ?? null,
      lastMessageAt: t.lastMessageAt,
      lastBody: lm?.body ?? null,
      lastSenderId: lm?.senderId ?? null,
      unreadCount: Number(c.c) || 0,
    };
  });
}

export function listDmMessages(threadId: string, userId: string, limit: number, beforeId: string | null): DmMessageRow[] {
  if (!assertThreadMember(threadId, userId)) return [];
  const lim = Math.min(100, Math.max(1, limit));
  if (beforeId) {
    const pivot = db
      .prepare(`SELECT datetime(created_at) AS ts, id FROM dm_messages WHERE id = ? AND thread_id = ?`)
      .get(beforeId, threadId) as { ts: string; id: string } | undefined;
    if (!pivot) {
      return db
        .prepare(
          `SELECT id, thread_id AS threadId, sender_id AS senderId, body, created_at AS createdAt
           FROM dm_messages WHERE thread_id = ? ORDER BY datetime(created_at) DESC, id DESC LIMIT ?`,
        )
        .all(threadId, lim) as DmMessageRow[];
    }
    return db
      .prepare(
        `SELECT id, thread_id AS threadId, sender_id AS senderId, body, created_at AS createdAt
         FROM dm_messages
         WHERE thread_id = ? AND (datetime(created_at) < datetime(?) OR (datetime(created_at) = datetime(?) AND id < ?))
         ORDER BY datetime(created_at) DESC, id DESC LIMIT ?`,
      )
      .all(threadId, pivot.ts, pivot.ts, pivot.id, lim) as DmMessageRow[];
  }
  return db
    .prepare(
      `SELECT id, thread_id AS threadId, sender_id AS senderId, body, created_at AS createdAt
       FROM dm_messages WHERE thread_id = ? ORDER BY datetime(created_at) DESC, id DESC LIMIT ?`,
    )
    .all(threadId, lim) as DmMessageRow[];
}

/* ── Group DMs ───────────────────────────────────────────── */

export type DmGroupListRow = {
  groupId: string;
  name: string;
  lastMessageAt: string;
  lastBody: string | null;
  lastSenderId: string | null;
  memberCount: number;
  unreadCount: number;
};

export type DmGroupMessageRow = {
  id: string;
  groupId: string;
  senderId: string;
  body: string;
  createdAt: string;
};

function assertGroupMember(groupId: string, userId: string): boolean {
  const r = db
    .prepare(`SELECT 1 FROM dm_group_members WHERE group_id = ? AND user_id = ?`)
    .get(groupId, userId);
  return Boolean(r);
}

export function createDmGroup(creatorId: string, rawName: string, memberUserIds: string[]): string {
  const name = normalizeGroupName(rawName) || "Group chat";
  const ids = [...new Set((memberUserIds || []).map((x) => String(x).trim()).filter(Boolean))].filter((id) => id !== creatorId);
  if (ids.length === 0) throw new Error("Add at least one other member");
  if (ids.length > MAX_GROUP_MEMBERS - 1) throw new Error(`At most ${MAX_GROUP_MEMBERS - 1} other members`);

  for (const uid of ids) {
    const ex = db.prepare(`SELECT 1 FROM users WHERE id = ?`).get(uid);
    if (!ex) throw new Error("One or more members were not found");
  }

  const groupId = uuidv4();
  db
    .prepare(
      `INSERT INTO dm_group_threads (id, name, created_by, last_message_at) VALUES (?, ?, ?, datetime('now'))`,
    )
    .run(groupId, name, creatorId);
  const ins = db.prepare(`INSERT INTO dm_group_members (group_id, user_id) VALUES (?, ?)`);
  ins.run(groupId, creatorId);
  for (const uid of ids) {
    ins.run(groupId, uid);
  }
  return groupId;
}

export function listDmGroupsForUser(userId: string): DmGroupListRow[] {
  const rows = db
    .prepare(
      `SELECT g.id AS groupId, g.name, g.last_message_at AS lastMessageAt
       FROM dm_group_threads g
       INNER JOIN dm_group_members m ON m.group_id = g.id AND m.user_id = ?
       ORDER BY datetime(g.last_message_at) DESC`,
    )
    .all(userId) as { groupId: string; name: string; lastMessageAt: string }[];

  const lastMsgStmt = db.prepare(
    `SELECT body, sender_id AS senderId FROM dm_group_messages WHERE group_id = ? ORDER BY datetime(created_at) DESC, id DESC LIMIT 1`,
  );
  const readStmt = db.prepare(
    `SELECT last_read_at AS lastReadAt FROM dm_group_reads WHERE group_id = ? AND user_id = ?`,
  );
  const countStmt = db.prepare(
    `SELECT COUNT(*) AS c FROM dm_group_messages
     WHERE group_id = ? AND sender_id != ? AND (datetime(created_at) > datetime(?))`,
  );
  const memCountStmt = db.prepare(`SELECT COUNT(*) AS c FROM dm_group_members WHERE group_id = ?`);

  return rows.map((t) => {
    const lm = lastMsgStmt.get(t.groupId) as { body: string; senderId: string } | undefined;
    const readRow = readStmt.get(t.groupId, userId) as { lastReadAt: string | null } | undefined;
    const cutoff = readRow?.lastReadAt ?? "1970-01-01 00:00:00";
    const c = countStmt.get(t.groupId, userId, cutoff) as { c: number };
    const mc = memCountStmt.get(t.groupId) as { c: number };
    return {
      groupId: t.groupId,
      name: t.name,
      lastMessageAt: t.lastMessageAt,
      lastBody: lm?.body ?? null,
      lastSenderId: lm?.senderId ?? null,
      memberCount: Number(mc.c) || 0,
      unreadCount: Number(c.c) || 0,
    };
  });
}

export function listDmGroupMessages(groupId: string, userId: string, limit: number): DmGroupMessageRow[] {
  if (!assertGroupMember(groupId, userId)) return [];
  const lim = Math.min(100, Math.max(1, limit));
  const raw = db
    .prepare(
      `SELECT id, group_id AS groupId, sender_id AS senderId, body, created_at AS createdAt
       FROM dm_group_messages WHERE group_id = ? ORDER BY datetime(created_at) DESC, id DESC LIMIT ?`,
    )
    .all(groupId, lim) as DmGroupMessageRow[];
  return raw.slice().reverse();
}

export function sendDmGroupMessage(senderId: string, groupId: string, body: string): DmGroupMessageRow | null {
  if (!assertGroupMember(groupId, senderId)) return null;
  const text = normalizeDmBody(body);
  if (!text) return null;

  const msgId = uuidv4();
  db
    .prepare(
      `INSERT INTO dm_group_messages (id, group_id, sender_id, body, created_at) VALUES (?, ?, ?, ?, datetime('now'))`,
    )
    .run(msgId, groupId, senderId, text);
  db.prepare(`UPDATE dm_group_threads SET last_message_at = datetime('now') WHERE id = ?`).run(groupId);

  const row = db
    .prepare(
      `SELECT id, group_id AS groupId, sender_id AS senderId, body, created_at AS createdAt FROM dm_group_messages WHERE id = ?`,
    )
    .get(msgId) as DmGroupMessageRow;

  const members = db
    .prepare(`SELECT user_id AS userId FROM dm_group_members WHERE group_id = ?`)
    .all(groupId) as { userId: string }[];

  const payload = {
    type: "dm_group_message" as const,
    groupId,
    message: {
      id: row.id,
      senderId: row.senderId,
      body: row.body,
      createdAt: row.createdAt,
    },
  };

  for (const m of members) {
    broadcastToUser(m.userId, payload);
    if (m.userId !== senderId) {
      broadcastDmUnreadTotal(m.userId);
    }
  }

  return row;
}

export function markDmGroupRead(groupId: string, userId: string): void {
  if (!assertGroupMember(groupId, userId)) return;
  const maxRow = db
    .prepare(`SELECT MAX(created_at) AS m FROM dm_group_messages WHERE group_id = ?`)
    .get(groupId) as { m: string | null } | undefined;
  const at = maxRow?.m ?? new Date().toISOString().replace("T", " ").slice(0, 19);
  db.prepare(
    `INSERT INTO dm_group_reads (group_id, user_id, last_read_at) VALUES (?, ?, ?)
     ON CONFLICT(group_id, user_id) DO UPDATE SET last_read_at = excluded.last_read_at`,
  ).run(groupId, userId, at);
  broadcastDmUnreadTotal(userId);
}

export function markDmGroupUnread(groupId: string, userId: string): void {
  if (!assertGroupMember(groupId, userId)) return;
  db.prepare(`DELETE FROM dm_group_reads WHERE group_id = ? AND user_id = ?`).run(groupId, userId);
  broadcastDmUnreadTotal(userId);
}

/** Creator deleting the group removes it for everyone; others only leave. */
export function leaveOrDeleteDmGroup(groupId: string, userId: string): boolean {
  const g = db
    .prepare(`SELECT created_by AS createdBy FROM dm_group_threads WHERE id = ?`)
    .get(groupId) as { createdBy: string } | undefined;
  if (!g) return false;
  const isMember = assertGroupMember(groupId, userId);
  if (!isMember) return false;

  if (g.createdBy === userId) {
    const memberIds = db
      .prepare(`SELECT user_id AS userId FROM dm_group_members WHERE group_id = ?`)
      .all(groupId) as { userId: string }[];
    db.prepare(`DELETE FROM dm_group_threads WHERE id = ?`).run(groupId);
    for (const m of memberIds) {
      broadcastToUser(m.userId, { type: "dm_group_deleted" as const, groupId });
      broadcastDmUnreadTotal(m.userId);
    }
    return true;
  }

  db.prepare(`DELETE FROM dm_group_members WHERE group_id = ? AND user_id = ?`).run(groupId, userId);
  broadcastDmUnreadTotal(userId);
  return true;
}

function groupUnreadCountForUser(userId: string, groupId: string): number {
  const readRow = db
    .prepare(`SELECT last_read_at AS lastReadAt FROM dm_group_reads WHERE group_id = ? AND user_id = ?`)
    .get(groupId, userId) as { lastReadAt: string | null } | undefined;
  const cutoff = readRow?.lastReadAt ?? "1970-01-01 00:00:00";
  const c = db
    .prepare(
      `SELECT COUNT(*) AS c FROM dm_group_messages
       WHERE group_id = ? AND sender_id != ? AND (datetime(created_at) > datetime(?))`,
    )
    .get(groupId, userId, cutoff) as { c: number };
  return Number(c.c) || 0;
}
