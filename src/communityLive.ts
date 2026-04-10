import { v4 as uuidv4 } from "uuid";
import type { WebSocket } from "ws";
import { db } from "./db.js";
import { normalizeEpisodeAirdate } from "./time.js";

const MAX_LIVE_CHAT_LEN = 280;
const CHAT_COOLDOWN_MS = 1200;

type ClientRecord = {
  socket: WebSocket;
  userId: string;
  watching: boolean;
};

const rooms = new Map<string, Set<ClientRecord>>();
const socketMeta = new Map<WebSocket, { roomKey: string; rec: ClientRecord }>();
const lastChatAt = new Map<string, number>();

function wsPayloadToString(data: unknown): string | null {
  if (typeof data === "string") return data;
  if (Buffer.isBuffer(data)) return data.toString("utf8");
  if (data instanceof ArrayBuffer) return Buffer.from(data).toString("utf8");
  if (Array.isArray(data) && data.every((x) => Buffer.isBuffer(x))) return Buffer.concat(data as Buffer[]).toString("utf8");
  return null;
}

function authorLabelForUser(userId: string): string {
  const row = db
    .prepare(`SELECT display_name AS displayName, username FROM users WHERE id = ?`)
    .get(userId) as { displayName: string | null; username: string | null } | undefined;
  if (!row) return "Someone";
  if (row.displayName && String(row.displayName).trim()) return String(row.displayName).trim();
  if (row.username && String(row.username).trim()) return "@" + String(row.username).trim();
  return "Member";
}

export function parseThreadLiveRoomQuery(q: { showId?: string; episode?: string }):
  | { ok: true; roomKey: string; showId: number; tvmazeEpisodeId: number | null }
  | { ok: false; error: string } {
  const showId = Number(q.showId);
  if (!Number.isInteger(showId) || showId < 1) return { ok: false, error: "Invalid show" };
  const ep = q.episode;
  if (ep == null || ep === "" || ep === "general") {
    return { ok: true, roomKey: `${showId}:g`, showId, tvmazeEpisodeId: null };
  }
  const n = Number(ep);
  if (!Number.isInteger(n) || n < 1) return { ok: false, error: "Invalid episode" };
  return { ok: true, roomKey: `${showId}:e:${n}`, showId, tvmazeEpisodeId: n };
}

/** Parse `roomKey` from {@link parseThreadLiveRoomQuery} (`showId:g` or `showId:e:id`). */
function parseRoomKeyForEpisode(roomKey: string): { showId: number; tvmazeEpisodeId: number | null } | null {
  const parts = roomKey.split(":");
  if (parts.length < 2) return null;
  const showId = Number(parts[0]);
  if (!Number.isInteger(showId) || showId < 1) return null;
  if (parts[1] === "g") return { showId, tvmazeEpisodeId: null };
  if (parts[1] === "e" && parts.length >= 3) {
    const ep = Number(parts[2]);
    if (!Number.isInteger(ep) || ep < 1) return null;
    return { showId, tvmazeEpisodeId: ep };
  }
  return null;
}

const AIR_NIGHT_MS = 24 * 60 * 60 * 1000;

/**
 * True while `now` is within 24 hours starting at UTC midnight on the episode's calendar air date.
 * General threads / missing airdate → false.
 */
export function isEpisodeLiveAirNightWindow(showId: number, tvmazeEpisodeId: number | null): boolean {
  if (tvmazeEpisodeId == null) return false;
  const row = db
    .prepare(`SELECT airdate FROM episodes_cache WHERE tvmaze_show_id = ? AND tvmaze_episode_id = ?`)
    .get(showId, tvmazeEpisodeId) as { airdate: string | null } | undefined;
  const ymd = normalizeEpisodeAirdate(row?.airdate ?? null);
  if (!ymd) return false;
  const m = /^(\d{4})-(\d{2})-(\d{2})$/.exec(ymd);
  if (!m) return false;
  const y = Number(m[1]);
  const mo = Number(m[2]);
  const d = Number(m[3]);
  const start = Date.UTC(y, mo - 1, d, 0, 0, 0, 0);
  const now = Date.now();
  return now >= start && now < start + AIR_NIGHT_MS;
}

function countDistinctWatching(set: Set<ClientRecord>): number {
  const u = new Set<string>();
  for (const c of set) {
    if (c.watching) u.add(c.userId);
  }
  return u.size;
}

function countDistinctViewers(set: Set<ClientRecord>): number {
  const u = new Set<string>();
  for (const c of set) {
    u.add(c.userId);
  }
  return u.size;
}

function broadcastPresence(roomKey: string): void {
  const set = rooms.get(roomKey);
  if (!set) return;

  const watchingUsers = new Map<string, string>();
  for (const c of set) {
    if (c.watching && !watchingUsers.has(c.userId)) {
      watchingUsers.set(c.userId, authorLabelForUser(c.userId));
    }
  }
  const watchingCount = watchingUsers.size;
  const viewerCount = countDistinctViewers(set);
  const watchers = [...watchingUsers.entries()].slice(0, 24).map(([userId, handle]) => ({ userId, handle }));
  const parsed = parseRoomKeyForEpisode(roomKey);
  const liveAirNight =
    parsed != null &&
    parsed.tvmazeEpisodeId != null &&
    isEpisodeLiveAirNightWindow(parsed.showId, parsed.tvmazeEpisodeId);
  const railOpen = watchingCount >= 2 || !!liveAirNight;

  const payload = JSON.stringify({
    type: "thread_live_presence" as const,
    watchingCount,
    viewerCount,
    watchers,
    railOpen,
    liveAirNight,
  });

  for (const c of set) {
    if (c.socket.readyState === 1) {
      try {
        c.socket.send(payload);
      } catch {
        /* ignore */
      }
    }
  }
}

export function registerCommunityThreadLiveSocket(userId: string, socket: WebSocket, roomKey: string): void {
  let set = rooms.get(roomKey);
  if (!set) {
    set = new Set();
    rooms.set(roomKey, set);
  }
  const rec: ClientRecord = { socket, userId, watching: false };
  set.add(rec);
  socketMeta.set(socket, { roomKey, rec });
  broadcastPresence(roomKey);
}

export function unregisterCommunityThreadLiveSocket(socket: WebSocket): void {
  const meta = socketMeta.get(socket);
  if (!meta) return;
  socketMeta.delete(socket);
  const set = rooms.get(meta.roomKey);
  if (set) {
    set.delete(meta.rec);
    if (set.size === 0) rooms.delete(meta.roomKey);
  }
  broadcastPresence(meta.roomKey);
}

function normalizeLiveChatBody(raw: unknown): string {
  if (typeof raw !== "string") return "";
  const t = raw.replace(/\r\n/g, "\n").trim();
  if (!t) return "";
  return t.length > MAX_LIVE_CHAT_LEN ? t.slice(0, MAX_LIVE_CHAT_LEN) : t;
}

export function handleCommunityThreadLiveMessage(userId: string, socket: WebSocket, raw: unknown): void {
  const meta = socketMeta.get(socket);
  if (!meta || meta.rec.userId !== userId) return;

  const text = wsPayloadToString(raw);
  if (!text || text.length > 4096) return;
  let parsed: unknown;
  try {
    parsed = JSON.parse(text);
  } catch {
    return;
  }
  if (!parsed || typeof parsed !== "object") return;
  const d = parsed as Record<string, unknown>;

  if (d.type === "watching") {
    meta.rec.watching = Boolean(d.active);
    broadcastPresence(meta.roomKey);
    return;
  }

  if (d.type === "ping") {
    broadcastPresence(meta.roomKey);
    return;
  }

  if (d.type === "chat") {
    const set = rooms.get(meta.roomKey);
    if (!set) return;
    const parsed = parseRoomKeyForEpisode(meta.roomKey);
    const airNight =
      parsed != null &&
      parsed.tvmazeEpisodeId != null &&
      isEpisodeLiveAirNightWindow(parsed.showId, parsed.tvmazeEpisodeId);
    if (!airNight) {
      if (countDistinctWatching(set) < 2) return;
      if (!meta.rec.watching) return;
    }
    const body = normalizeLiveChatBody(d.body);
    if (!body) return;

    const key = `${meta.roomKey}:${userId}`;
    const now = Date.now();
    if (now - (lastChatAt.get(key) ?? 0) < CHAT_COOLDOWN_MS) return;
    lastChatAt.set(key, now);

    const msg = {
      type: "thread_live_chat" as const,
      id: uuidv4(),
      userId,
      handle: authorLabelForUser(userId),
      body,
      createdAt: new Date().toISOString(),
    };
    const out = JSON.stringify(msg);
    for (const c of set) {
      if (c.socket.readyState === 1) {
        try {
          c.socket.send(out);
        } catch {
          /* ignore */
        }
      }
    }
  }
}
