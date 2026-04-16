import { v4 as uuidv4 } from "uuid";
import type { WebSocket } from "ws";
import { db } from "./db.js";
import { sendWebPushToUser } from "./push.js";
import { insertActivityNotification } from "./activityNotifications.js";
import { touchUserPresence } from "./presence.js";

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
  touchUserPresence(userId);
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
  if (d.type === "dm_group_typing") {
    if (typeof d.groupId === "string" && typeof d.typing === "boolean") {
      relayDmGroupTyping(userId, d.groupId, d.typing);
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

/** Relay typing in a group to all other members (WebSocket). */
export function relayDmGroupTyping(fromUserId: string, groupId: string, typing: boolean): void {
  if (!assertGroupMember(groupId, fromUserId)) return;
  const members = db
    .prepare(`SELECT user_id AS userId FROM dm_group_members WHERE group_id = ?`)
    .all(groupId) as { userId: string }[];
  const label = authorLabelForUser(fromUserId);
  for (const m of members) {
    if (m.userId === fromUserId) continue;
    broadcastToUser(m.userId, {
      type: "dm_group_typing" as const,
      groupId,
      userId: fromUserId,
      typing,
      label,
    });
  }
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

  touchUserPresence(senderId);

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

const MAX_GROUP_AVATAR_DATA_URL_LEN = 450_000;

export type DmGroupListRow = {
  groupId: string;
  name: string;
  /** Custom group photo; when null, clients may show initials or member stack. */
  avatarDataUrl: string | null;
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

type UserPublicProfile = {
  displayName: string | null;
  username: string | null;
  avatarDataUrl: string | null;
};

function getUserPublicProfile(userId: string): UserPublicProfile {
  const row = db
    .prepare(`SELECT display_name AS displayName, username, avatar_data_url AS avatarDataUrl FROM users WHERE id = ?`)
    .get(userId) as UserPublicProfile | undefined;
  return row ?? { displayName: null, username: null, avatarDataUrl: null };
}

export type DmGroupMessageApiRow = DmGroupMessageRow & {
  senderDisplayName: string | null;
  senderUsername: string | null;
  senderAvatarDataUrl: string | null;
  /** Populated for messages you sent: other members whose read cursor covers this message. */
  seenBy?: { userId: string; label: string }[];
};

function enrichDmGroupMessagesForViewer(
  groupId: string,
  messages: DmGroupMessageRow[],
  viewerId: string,
): DmGroupMessageApiRow[] {
  const senderIds = [...new Set(messages.map((m) => m.senderId))];
  const profiles = new Map<string, UserPublicProfile>();
  for (const id of senderIds) {
    profiles.set(id, getUserPublicProfile(id));
  }
  const memberRows = db
    .prepare(`SELECT user_id AS userId FROM dm_group_members WHERE group_id = ?`)
    .all(groupId) as { userId: string }[];
  const reads = new Map<string, string>();
  for (const m of memberRows) {
    const r = db
      .prepare(`SELECT last_read_at AS lastReadAt FROM dm_group_reads WHERE group_id = ? AND user_id = ?`)
      .get(groupId, m.userId) as { lastReadAt: string } | undefined;
    if (r?.lastReadAt) reads.set(m.userId, r.lastReadAt);
  }
  return messages.map((m) => {
    const p = profiles.get(m.senderId)!;
    const base: DmGroupMessageApiRow = {
      ...m,
      senderDisplayName: p.displayName,
      senderUsername: p.username,
      senderAvatarDataUrl: p.avatarDataUrl,
    };
    if (m.senderId === viewerId) {
      const seen: { userId: string; label: string }[] = [];
      for (const mem of memberRows) {
        if (mem.userId === m.senderId) continue;
        const ra = reads.get(mem.userId);
        if (ra && dmInstantMs(ra) >= dmInstantMs(m.createdAt)) {
          seen.push({ userId: mem.userId, label: authorLabelForUser(mem.userId) });
        }
      }
      base.seenBy = seen;
    }
    return base;
  });
}

export function enrichSingleDmGroupMessage(groupId: string, row: DmGroupMessageRow, viewerId: string): DmGroupMessageApiRow {
  return enrichDmGroupMessagesForViewer(groupId, [row], viewerId)[0];
}

export type DmGroupDetailMember = {
  userId: string;
  displayName: string | null;
  username: string | null;
  avatarDataUrl: string | null;
  isOwner: boolean;
};

export function getDmGroupDetail(
  groupId: string,
  requesterId: string,
): {
  groupId: string;
  name: string;
  createdBy: string;
  avatarDataUrl: string | null;
  members: DmGroupDetailMember[];
} | null {
  if (!assertGroupMember(groupId, requesterId)) return null;
  const g = db
    .prepare(
      `SELECT id, name, created_by AS createdBy, avatar_data_url AS avatarDataUrl FROM dm_group_threads WHERE id = ?`,
    )
    .get(groupId) as { id: string; name: string; createdBy: string; avatarDataUrl: string | null } | undefined;
  if (!g) return null;
  const mems = db.prepare(`SELECT user_id AS userId FROM dm_group_members WHERE group_id = ?`).all(groupId) as { userId: string }[];
  const members: DmGroupDetailMember[] = mems.map((m) => {
    const p = getUserPublicProfile(m.userId);
    return {
      userId: m.userId,
      displayName: p.displayName,
      username: p.username,
      avatarDataUrl: p.avatarDataUrl,
      isOwner: g.createdBy === m.userId,
    };
  });
  return {
    groupId: g.id,
    name: g.name,
    createdBy: g.createdBy,
    avatarDataUrl: g.avatarDataUrl ?? null,
    members,
  };
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
  const creatorLabel = authorLabelForUser(creatorId);
  for (const uid of ids) {
    ins.run(groupId, uid);
    insertActivityNotification({
      recipientUserId: uid,
      kind: "group_chat_invite",
      title: "Group chat",
      summary: `${creatorLabel} started "${name}" with you`,
      url: "/?openInbox=1",
      actorUserId: creatorId,
      sourcePostId: null,
    });
  }
  return groupId;
}

export function listDmGroupsForUser(userId: string): DmGroupListRow[] {
  const rows = db
    .prepare(
      `SELECT g.id AS groupId, g.name, g.avatar_data_url AS avatarDataUrl, g.last_message_at AS lastMessageAt
       FROM dm_group_threads g
       INNER JOIN dm_group_members m ON m.group_id = g.id AND m.user_id = ?
       ORDER BY datetime(g.last_message_at) DESC`,
    )
    .all(userId) as { groupId: string; name: string; avatarDataUrl: string | null; lastMessageAt: string }[];

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
      avatarDataUrl: t.avatarDataUrl ?? null,
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

/** Group messages with sender profile + per-viewer read receipts (for your own messages). */
export function listDmGroupMessagesForApi(groupId: string, userId: string, limit: number): DmGroupMessageApiRow[] {
  const raw = listDmGroupMessages(groupId, userId, limit);
  return enrichDmGroupMessagesForViewer(groupId, raw, userId);
}

export function sendDmGroupMessage(senderId: string, groupId: string, body: string): DmGroupMessageApiRow | null {
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
  touchUserPresence(senderId);

  const row = db
    .prepare(
      `SELECT id, group_id AS groupId, sender_id AS senderId, body, created_at AS createdAt FROM dm_group_messages WHERE id = ?`,
    )
    .get(msgId) as DmGroupMessageRow;

  const members = db
    .prepare(`SELECT user_id AS userId FROM dm_group_members WHERE group_id = ?`)
    .all(groupId) as { userId: string }[];

  for (const m of members) {
    const message = enrichSingleDmGroupMessage(groupId, row, m.userId);
    broadcastToUser(m.userId, {
      type: "dm_group_message" as const,
      groupId,
      message,
    });
    if (m.userId !== senderId) {
      broadcastDmUnreadTotal(m.userId);
    }
  }

  return enrichSingleDmGroupMessage(groupId, row, senderId);
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

export function patchDmGroup(
  groupId: string,
  requesterId: string,
  patch: { rawName?: string; avatarDataUrl?: string | null },
): { ok: true } | { error: string } {
  const g = db
    .prepare(`SELECT created_by AS createdBy FROM dm_group_threads WHERE id = ?`)
    .get(groupId) as { createdBy: string } | undefined;
  if (!g) return { error: "Group not found" };
  if (g.createdBy !== requesterId) return { error: "Only the group owner can update the group" };
  if (!assertGroupMember(groupId, requesterId)) return { error: "Not a member" };

  const hasName = patch.rawName !== undefined;
  const hasAvatar = patch.avatarDataUrl !== undefined;
  if (!hasName && !hasAvatar) return { error: "Nothing to update" };

  if (hasName) {
    const name = normalizeGroupName(patch.rawName!) || "Group chat";
    db.prepare(`UPDATE dm_group_threads SET name = ? WHERE id = ?`).run(name, groupId);
  }
  if (hasAvatar) {
    const av = patch.avatarDataUrl;
    if (av === null || av === "") {
      db.prepare(`UPDATE dm_group_threads SET avatar_data_url = NULL WHERE id = ?`).run(groupId);
    } else if (typeof av === "string") {
      const trimmed = av.trim();
      const ok =
        /^\s*data:image\/(jpeg|jpg|png|webp);base64,/i.test(trimmed) &&
        trimmed.length > 0 &&
        trimmed.length <= MAX_GROUP_AVATAR_DATA_URL_LEN;
      if (!ok) {
        return { error: "Avatar must be a JPEG, PNG, or WebP data URL under the size limit" };
      }
      db.prepare(`UPDATE dm_group_threads SET avatar_data_url = ? WHERE id = ?`).run(trimmed, groupId);
    } else {
      return { error: "Invalid avatar" };
    }
  }

  const row = db
    .prepare(`SELECT name, avatar_data_url AS avatarDataUrl FROM dm_group_threads WHERE id = ?`)
    .get(groupId) as { name: string; avatarDataUrl: string | null };
  const memberIds = db
    .prepare(`SELECT user_id AS userId FROM dm_group_members WHERE group_id = ?`)
    .all(groupId) as { userId: string }[];
  for (const m of memberIds) {
    broadcastToUser(m.userId, {
      type: "dm_group_meta" as const,
      groupId,
      name: row.name,
      avatarDataUrl: row.avatarDataUrl ?? null,
    });
  }
  return { ok: true };
}

export function addDmGroupMembers(
  groupId: string,
  requesterId: string,
  rawIds: string[],
): { ok: true } | { error: string } {
  const g = db
    .prepare(`SELECT created_by AS createdBy FROM dm_group_threads WHERE id = ?`)
    .get(groupId) as { createdBy: string } | undefined;
  if (!g) return { error: "Group not found" };
  if (g.createdBy !== requesterId) return { error: "Only the group owner can add members" };
  if (!assertGroupMember(groupId, requesterId)) return { error: "Not a member" };
  const ids = [...new Set(rawIds.map((x) => String(x).trim()).filter(Boolean))];
  if (ids.length === 0) return { error: "No users to add" };
  const gNameRow = db.prepare(`SELECT name FROM dm_group_threads WHERE id = ?`).get(groupId) as { name: string } | undefined;
  const groupName = gNameRow?.name ?? "Group chat";
  const inviterLabel = authorLabelForUser(requesterId);
  const mcRow = db.prepare(`SELECT COUNT(*) AS c FROM dm_group_members WHERE group_id = ?`).get(groupId) as { c: number };
  let current = Number(mcRow.c) || 0;
  const ins = db.prepare(`INSERT INTO dm_group_members (group_id, user_id) VALUES (?, ?)`);
  let added = 0;
  for (const uid of ids) {
    if (current >= MAX_GROUP_MEMBERS) {
      return { error: `Group is full (${MAX_GROUP_MEMBERS} members)` };
    }
    const exists = db.prepare(`SELECT 1 FROM dm_group_members WHERE group_id = ? AND user_id = ?`).get(groupId, uid);
    if (exists) continue;
    const u = db.prepare(`SELECT 1 FROM users WHERE id = ?`).get(uid);
    if (!u) continue;
    ins.run(groupId, uid);
    current++;
    added++;
    broadcastDmUnreadTotal(uid);
    insertActivityNotification({
      recipientUserId: uid,
      kind: "group_chat_invite",
      title: "Added to group",
      summary: `${inviterLabel} added you to "${groupName}"`,
      url: "/?openInbox=1",
      actorUserId: requesterId,
      sourcePostId: null,
    });
  }
  if (added === 0) return { error: "No new members added (already in group or invalid users)" };
  const memberIds = db
    .prepare(`SELECT user_id AS userId FROM dm_group_members WHERE group_id = ?`)
    .all(groupId) as { userId: string }[];
  for (const m of memberIds) {
    broadcastToUser(m.userId, { type: "dm_group_members_updated" as const, groupId });
  }
  return { ok: true };
}

export function removeDmGroupMember(
  groupId: string,
  requesterId: string,
  targetUserId: string,
): { ok: true } | { error: string } {
  const g = db
    .prepare(`SELECT created_by AS createdBy FROM dm_group_threads WHERE id = ?`)
    .get(groupId) as { createdBy: string } | undefined;
  if (!g) return { error: "Group not found" };
  if (g.createdBy !== requesterId) return { error: "Only the group owner can remove members" };
  if (requesterId === targetUserId) {
    return { error: "To leave the group, use Leave from the inbox swipe menu" };
  }
  if (!assertGroupMember(groupId, targetUserId)) return { error: "User is not in this group" };
  db.prepare(`DELETE FROM dm_group_members WHERE group_id = ? AND user_id = ?`).run(groupId, targetUserId);
  broadcastDmUnreadTotal(targetUserId);
  broadcastToUser(targetUserId, { type: "dm_group_removed" as const, groupId });
  const memberIds = db
    .prepare(`SELECT user_id AS userId FROM dm_group_members WHERE group_id = ?`)
    .all(groupId) as { userId: string }[];
  for (const m of memberIds) {
    broadcastToUser(m.userId, { type: "dm_group_members_updated" as const, groupId });
  }
  return { ok: true };
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
