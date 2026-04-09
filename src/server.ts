import fs from "node:fs";
import path from "node:path";
import crypto from "node:crypto";
import Fastify, { type FastifyReply, type FastifyRequest } from "fastify";
import cors from "@fastify/cors";
import cron from "node-cron";
import { v4 as uuidv4 } from "uuid";
import "./db.js";
import { db, getSqlitePersistenceInfo } from "./db.js";
import {
  searchShowsWithCatalog,
  fetchShow,
  fetchShowEpisodes,
  rankSearchResults,
  fetchPreviousEpisodeAirdates,
} from "./tvmaze.js";
import { normalizeEpisodeAirdate, safeTodayInTimeZone } from "./time.js";
import { buildIcsCalendar, episodeUid } from "./ics.js";
import { refreshAllSubscribedShows, runDailyNotifications, runTaskNudgeNotifications } from "./jobs.js";
import { configureWebPush, getVapidPublicKey, isWebPushConfigured, sendWebPushToUser } from "./push.js";
import { computeRecommendedShows, computeTrendingShows } from "./recommend.js";
import { hashPassword, verifyPassword } from "./password.js";

const PORT = Number(process.env.PORT) || 3000;
const publicDir = path.join(process.cwd(), "public");

/** Client sends a resized data URL; cap size to keep SQLite and responses reasonable. */
const MAX_AVATAR_DATA_URL_LEN = 450_000;

const webPushReady = configureWebPush();

const app = Fastify({
  logger: true,
  routerOptions: {
    ignoreTrailingSlash: true,
  },
});

await app.register(cors, {
  origin: true,
  credentials: true,
});

if (webPushReady) {
  app.log.info("Web Push: VAPID keys loaded");
} else {
  app.log.info(
    "Web Push: off until VAPID_PUBLIC_KEY + VAPID_PRIVATE_KEY (+ optional VAPID_SUBJECT) are set — server is fine without them",
  );
}

function randomToken(): string {
  return crypto.randomBytes(24).toString("hex");
}

/** HttpOnly session — per browser profile / device (not synced like localStorage can be across linked devices). */
const SESSION_COOKIE = "airalert_uid";

function parseCookies(cookieHeader: string | undefined): Record<string, string> {
  const out: Record<string, string> = {};
  if (!cookieHeader) return out;
  for (const part of cookieHeader.split(";")) {
    const idx = part.indexOf("=");
    if (idx === -1) continue;
    const k = part.slice(0, idx).trim();
    const v = part.slice(idx + 1).trim();
    if (k) {
      try {
        out[k] = decodeURIComponent(v);
      } catch {
        out[k] = v;
      }
    }
  }
  return out;
}

function sessionCookieSecureSuffix(request: FastifyRequest): string {
  const fwd = String(request.headers["x-forwarded-proto"] ?? "")
    .split(",")[0]
    .trim();
  const proto = fwd || (request as { protocol?: string }).protocol || "http";
  return proto === "https" ? "; Secure" : "";
}

function setSessionCookie(reply: FastifyReply, request: FastifyRequest, userId: string) {
  const sec = sessionCookieSecureSuffix(request);
  reply.header(
    "Set-Cookie",
    `${SESSION_COOKIE}=${encodeURIComponent(userId)}; Path=/; HttpOnly; SameSite=Lax; Max-Age=31536000${sec}`,
  );
}

function clearSessionCookie(reply: FastifyReply, request: FastifyRequest) {
  const sec = sessionCookieSecureSuffix(request);
  reply.header("Set-Cookie", `${SESSION_COOKIE}=; Path=/; HttpOnly; SameSite=Lax; Max-Age=0${sec}`);
}

function sessionUserIdFromRequest(request: FastifyRequest): string | undefined {
  const raw = parseCookies(request.headers.cookie)[SESSION_COOKIE];
  if (typeof raw !== "string" || !raw.trim()) return undefined;
  return raw.trim();
}

/** Admin UI + APIs; set `AIRALERT_ADMIN_PASSWORD` to enable. Session cookie is HMAC-signed, 7-day expiry. */
const ADMIN_COOKIE = "airalert_admin_sess";

function adminPasswordConfigured(): string | null {
  const p = process.env.AIRALERT_ADMIN_PASSWORD?.trim();
  return p && p.length > 0 ? p : null;
}

function adminSigningKey(password: string): Buffer {
  return crypto.createHash("sha256").update(`airalert-admin-v1\x00${password}`, "utf8").digest();
}

function signAdminSessionToken(password: string): string {
  const exp = Math.floor(Date.now() / 1000) + 7 * 86400;
  const payload = Buffer.from(JSON.stringify({ exp }), "utf8").toString("base64url");
  const sig = crypto.createHmac("sha256", adminSigningKey(password)).update(payload).digest("base64url");
  return `${payload}.${sig}`;
}

function verifyAdminSessionToken(raw: string | undefined, password: string): boolean {
  if (!raw || typeof raw !== "string" || !raw.includes(".")) return false;
  const dot = raw.indexOf(".");
  const payloadB64 = raw.slice(0, dot);
  const sig = raw.slice(dot + 1);
  const expectedSig = crypto.createHmac("sha256", adminSigningKey(password)).update(payloadB64).digest("base64url");
  const a = Buffer.from(sig, "utf8");
  const b = Buffer.from(expectedSig, "utf8");
  if (a.length !== b.length) return false;
  try {
    if (!crypto.timingSafeEqual(a, b)) return false;
  } catch {
    return false;
  }
  try {
    const payload = JSON.parse(Buffer.from(payloadB64, "base64url").toString("utf8")) as { exp?: number };
    if (typeof payload.exp !== "number" || payload.exp < Math.floor(Date.now() / 1000)) return false;
    return true;
  } catch {
    return false;
  }
}

function adminSessionFromRequest(request: FastifyRequest): boolean {
  const pw = adminPasswordConfigured();
  if (!pw) return false;
  const raw = parseCookies(request.headers.cookie)[ADMIN_COOKIE];
  return verifyAdminSessionToken(raw, pw);
}

/** Env-password admin cookie or signed-in user with `is_admin`. */
function isRequestAdmin(request: FastifyRequest): boolean {
  if (adminSessionFromRequest(request)) return true;
  const sid = sessionUserIdFromRequest(request);
  if (!sid) return false;
  const row = db.prepare(`SELECT is_admin FROM users WHERE id = ?`).get(sid) as { is_admin: number } | undefined;
  return Boolean(row?.is_admin);
}

function assertSelfOrAdmin(request: FastifyRequest, reply: FastifyReply, userId: string): boolean {
  if (isRequestAdmin(request)) return true;
  const sid = sessionUserIdFromRequest(request);
  if (!sid) {
    reply.code(401).send({ error: "Sign in required" });
    return false;
  }
  if (sid === userId) return true;
  reply.code(403).send({ error: "Forbidden" });
  return false;
}

function setAdminSessionCookie(reply: FastifyReply, request: FastifyRequest, token: string) {
  const sec = sessionCookieSecureSuffix(request);
  reply.header(
    "Set-Cookie",
    `${ADMIN_COOKIE}=${encodeURIComponent(token)}; Path=/; HttpOnly; SameSite=Lax; Max-Age=604800${sec}`,
  );
}

function clearAdminSessionCookie(reply: FastifyReply, request: FastifyRequest) {
  const sec = sessionCookieSecureSuffix(request);
  reply.header("Set-Cookie", `${ADMIN_COOKIE}=; Path=/; HttpOnly; SameSite=Lax; Max-Age=0${sec}`);
}

function stripHtml(html: string): string {
  return html.replace(/<[^>]+>/g, " ").replace(/\s+/g, " ").trim();
}

type UserCreateInput = { timezone?: string; reminderHourLocal?: number };

function normalizeUserCreateInput(body: UserCreateInput): { timezone: string; reminderHourLocal: number } {
  const timezone = typeof body.timezone === "string" && body.timezone.trim() ? body.timezone.trim() : "America/Los_Angeles";
  let reminderHourLocal = 8;
  if (typeof body.reminderHourLocal === "number" && Number.isInteger(body.reminderHourLocal)) {
    reminderHourLocal = Math.min(23, Math.max(0, body.reminderHourLocal));
  }
  return { timezone, reminderHourLocal };
}

function createUserRecord(timezone: string, reminderHourLocal: number): {
  id: string;
  timezone: string;
  reminderHourLocal: number;
  calendarToken: string;
} {
  const id = uuidv4();
  const calendarToken = randomToken();
  db.prepare(
    `INSERT INTO users (id, timezone, reminder_hour_local, calendar_token) VALUES (?, ?, ?, ?)`,
  ).run(id, timezone, reminderHourLocal, calendarToken);
  return { id, timezone, reminderHourLocal, calendarToken };
}

function createRegisteredUser(
  username: string,
  password: string,
  timezone: string,
  reminderHourLocal: number,
  isAdmin: boolean,
): { id: string; timezone: string; reminderHourLocal: number; calendarToken: string } {
  const id = uuidv4();
  const calendarToken = randomToken();
  db.prepare(
    `INSERT INTO users (id, timezone, reminder_hour_local, calendar_token, username, password_hash, is_admin)
     VALUES (?, ?, ?, ?, ?, ?, ?)`,
  ).run(id, timezone, reminderHourLocal, calendarToken, username, hashPassword(password), isAdmin ? 1 : 0);
  return { id, timezone, reminderHourLocal, calendarToken };
}

function ensureInitialAdminFromEnv(): void {
  const u = process.env.AIRALERT_INITIAL_ADMIN_USERNAME?.trim();
  const p = process.env.AIRALERT_INITIAL_ADMIN_PASSWORD?.trim();
  if (!u || !p || p.length < 8) return;
  const n = db.prepare(`SELECT COUNT(*) AS c FROM users WHERE is_admin = 1`).get() as { c: number };
  if (Number(n.c) > 0) return;
  try {
    createRegisteredUser(u, p, "America/Los_Angeles", 8, true);
    console.log("[airalert] Created first admin from AIRALERT_INITIAL_ADMIN_USERNAME / AIRALERT_INITIAL_ADMIN_PASSWORD");
  } catch (e) {
    const msg = e instanceof Error ? e.message : String(e);
    console.error("[airalert] Could not create initial admin user:", msg);
  }
}

ensureInitialAdminFromEnv();

app.get("/api/health", async () => ({
  ok: true,
  /** Open in a browser after deploy to confirm DB is on a volume (looksEphemeral should be false). */
  sqlite: getSqlitePersistenceInfo(),
}));

app.get("/api/admin/status", async (request) => ({
  authenticated: isRequestAdmin(request),
  envPasswordLoginAvailable: Boolean(adminPasswordConfigured()),
}));

app.post("/api/admin/login", async (request, reply) => {
  const pw = adminPasswordConfigured();
  if (!pw) {
    reply.code(404);
    return { error: "Admin not configured (set AIRALERT_ADMIN_PASSWORD)" };
  }
  const body = (request.body ?? {}) as { password?: string };
  const attempt = typeof body.password === "string" ? body.password : "";
  const ab = Buffer.from(attempt, "utf8");
  const pb = Buffer.from(pw, "utf8");
  if (ab.length !== pb.length || !crypto.timingSafeEqual(ab, pb)) {
    reply.code(401);
    return { error: "Invalid password" };
  }
  setAdminSessionCookie(reply, request, signAdminSessionToken(pw));
  return { ok: true };
});

app.post("/api/admin/logout", async (request, reply) => {
  clearAdminSessionCookie(reply, request);
  return { ok: true };
});

app.get("/api/admin/overview", async (request, reply) => {
  if (!isRequestAdmin(request)) {
    reply.code(401);
    return { error: "Unauthorized" };
  }
  const rows = db
    .prepare(
      `SELECT
         u.id AS id,
         u.username AS username,
         u.is_admin AS isAdmin,
         u.created_at AS createdAt,
         u.timezone AS timezone,
         (SELECT COUNT(*) FROM show_subscriptions s WHERE s.user_id = u.id) AS subscriptionCount,
         (SELECT COUNT(*) FROM show_subscriptions s WHERE s.user_id = u.id AND s.added_from = 'recommended') AS fromRecommendedCount,
         (SELECT COUNT(*) FROM show_subscriptions s WHERE s.user_id = u.id AND s.added_from = 'search') AS fromSearchCount,
         (SELECT COUNT(*) FROM show_subscriptions s WHERE s.user_id = u.id AND (s.added_from IS NULL OR TRIM(s.added_from) = '')) AS fromUnknownCount,
         (SELECT COUNT(*) FROM watch_tasks w WHERE w.user_id = u.id) AS tasksTotal,
         (SELECT COUNT(*) FROM watch_tasks w WHERE w.user_id = u.id AND w.completed_at IS NOT NULL) AS tasksCompleted
       FROM users u
       ORDER BY datetime(u.created_at) DESC`,
    )
    .all() as {
    id: string;
    username: string | null;
    isAdmin: number;
    createdAt: string;
    timezone: string;
    subscriptionCount: number;
    fromRecommendedCount: number;
    fromSearchCount: number;
    fromUnknownCount: number;
    tasksTotal: number;
    tasksCompleted: number;
  }[];
  const totals = rows.reduce(
    (acc, r) => {
      acc.users += 1;
      acc.subscriptions += r.subscriptionCount;
      acc.fromRecommended += r.fromRecommendedCount;
      acc.fromSearch += r.fromSearchCount;
      acc.fromUnknown += r.fromUnknownCount;
      acc.tasksTotal += r.tasksTotal;
      acc.tasksCompleted += r.tasksCompleted;
      return acc;
    },
    { users: 0, subscriptions: 0, fromRecommended: 0, fromSearch: 0, fromUnknown: 0, tasksTotal: 0, tasksCompleted: 0 },
  );
  const userIds = rows.map((r) => r.id);
  const genreMap = await topGenresForAdminUsers(userIds);
  const usersOut = rows.map((r) => ({
    ...r,
    topGenres: genreMap.get(r.id) ?? [],
  }));
  return { users: usersOut, totals };
});

/** Primary genres listed first on TVMaze — weight those higher than trailing tags. */
function adminGenrePositionWeight(index: number): number {
  if (index === 0) return 3;
  if (index === 1) return 2;
  if (index === 2) return 1;
  return 0.4;
}

/**
 * TVMaze `type` (Reality, Scripted, …) is more reliable than genre tags alone.
 * Scripted is too common to add — it would dominate every user.
 */
function adminShowFormatWeight(showType: string | null | undefined): { key: string | null; points: number } {
  if (!showType || typeof showType !== "string") return { key: null, points: 0 };
  const s = showType.trim().toLowerCase();
  if (!s || s === "scripted") return { key: null, points: 0 };
  const high = new Set([
    "reality",
    "documentary",
    "animation",
    "talk show",
    "game show",
    "news",
    "sports",
    "variety",
    "panel show",
    "award show",
  ]);
  const points = high.has(s) ? 3.5 : 1.1;
  return { key: s, points };
}

function adminIsRealityFormat(showType: string | null | undefined, genres: string[]): boolean {
  const t = showType?.trim().toLowerCase();
  if (t === "reality") return true;
  return genres.some((g) => g.trim().toLowerCase() === "reality");
}

/** Broad tags TVMaze often attaches to reality/unscripted shows; downweight when format is Reality. */
const ADMIN_GENRE_DOWNWEIGHT_ON_REALITY = new Set(["comedy", "drama", "family"]);

/** Top 5 format/genre labels per user: uses TVMaze `type`, position-weighted genres, and reality-aware damping. */
async function topGenresForAdminUsers(userIds: string[]): Promise<Map<string, string[]>> {
  const out = new Map<string, string[]>();
  for (const id of userIds) out.set(id, []);
  if (userIds.length === 0) return out;

  const placeholders = userIds.map(() => "?").join(",");
  const subs = db
    .prepare(`SELECT user_id, tvmaze_show_id FROM show_subscriptions WHERE user_id IN (${placeholders})`)
    .all(...userIds) as { user_id: string; tvmaze_show_id: number }[];

  const byUser = new Map<string, number[]>();
  for (const s of subs) {
    if (!byUser.has(s.user_id)) byUser.set(s.user_id, []);
    byUser.get(s.user_id)!.push(s.tvmaze_show_id);
  }

  const uniqueShowIds = [...new Set(subs.map((s) => s.tvmaze_show_id))];
  const showMeta = new Map<number, { genres: string[]; type: string | null }>();
  for (let i = 0; i < uniqueShowIds.length; i += 10) {
    const chunk = uniqueShowIds.slice(i, i + 10);
    const settled = await Promise.allSettled(chunk.map((id) => fetchShow(id)));
    for (let j = 0; j < chunk.length; j++) {
      const r = settled[j];
      const id = chunk[j];
      if (r.status === "fulfilled") {
        const v = r.value;
        showMeta.set(id, { genres: v.genres ?? [], type: v.type ?? null });
      } else {
        showMeta.set(id, { genres: [], type: null });
      }
    }
  }

  for (const uid of userIds) {
    const counts = new Map<string, number>();
    for (const sid of byUser.get(uid) ?? []) {
      const meta = showMeta.get(sid);
      if (!meta) continue;
      const { genres, type } = meta;
      const { key: formatKey, points: formatPts } = adminShowFormatWeight(type);
      if (formatKey && formatPts > 0) {
        counts.set(formatKey, (counts.get(formatKey) ?? 0) + formatPts);
      }
      const realityFmt = adminIsRealityFormat(type, genres);
      genres.forEach((g, i) => {
        const k = g.trim().toLowerCase();
        if (k.length < 2) return;
        let w = adminGenrePositionWeight(i);
        if (realityFmt && ADMIN_GENRE_DOWNWEIGHT_ON_REALITY.has(k)) {
          w *= 0.12;
        }
        counts.set(k, (counts.get(k) ?? 0) + w);
      });
    }
    const top = [...counts.entries()]
      .sort((a, b) => b[1] - a[1] || a[0].localeCompare(b[0]))
      .slice(0, 5)
      .map(([g]) => g);
    out.set(uid, top);
  }
  return out;
}

app.delete("/api/admin/users/:userId", async (request, reply) => {
  if (!isRequestAdmin(request)) {
    reply.code(401);
    return { error: "Unauthorized" };
  }
  const { userId } = request.params as { userId: string };
  const row = db
    .prepare(`SELECT username, is_admin FROM users WHERE id = ?`)
    .get(userId) as { username: string | null; is_admin: number } | undefined;
  if (!row) {
    reply.code(404);
    return { error: "User not found" };
  }
  const un = row.username?.trim() ?? "";
  if (un.length > 0) {
    reply.code(400);
    return { error: "Only guest accounts (no username) can be deleted here" };
  }
  const sid = sessionUserIdFromRequest(request);
  if (sid === userId) {
    reply.code(400);
    return { error: "Cannot delete the account you are signed in as" };
  }
  db.prepare(`DELETE FROM users WHERE id = ?`).run(userId);
  return { ok: true };
});

app.get("/api/admin/users/:userId", async (request, reply) => {
  if (!isRequestAdmin(request)) {
    reply.code(401);
    return { error: "Unauthorized" };
  }
  const { userId } = request.params as { userId: string };
  const user = db
    .prepare(
      `SELECT id, username, is_admin AS isAdmin, timezone, reminder_hour_local AS reminderHourLocal,
              task_nudge_days_after_air AS taskNudgeDaysAfterAir, created_at AS createdAt
       FROM users WHERE id = ?`,
    )
    .get(userId) as Record<string, unknown> | undefined;
  if (!user) {
    reply.code(404);
    return { error: "User not found" };
  }
  const subscriptions = db
    .prepare(
      `SELECT id, tvmaze_show_id AS tvmazeShowId, show_name AS showName,
              added_from AS addedFrom, created_at AS createdAt
       FROM show_subscriptions WHERE user_id = ? ORDER BY datetime(created_at) DESC`,
    )
    .all(userId);
  const taskRow = db
    .prepare(
      `SELECT
         COUNT(*) AS total,
         SUM(CASE WHEN completed_at IS NOT NULL THEN 1 ELSE 0 END) AS completed
       FROM watch_tasks WHERE user_id = ?`,
    )
    .get(userId) as { total: number; completed: number | null };
  const tasksTotal = Number(taskRow?.total ?? 0);
  const tasksCompleted = Number(taskRow?.completed ?? 0);
  return {
    user,
    subscriptions,
    tasks: { total: tasksTotal, completed: tasksCompleted, open: tasksTotal - tasksCompleted },
  };
});

app.patch("/api/admin/users/:userId", async (request, reply) => {
  if (!isRequestAdmin(request)) {
    reply.code(401);
    return { error: "Unauthorized" };
  }
  const { userId } = request.params as { userId: string };
  const body = (request.body ?? {}) as { isAdmin?: boolean };
  const existing = db.prepare(`SELECT id FROM users WHERE id = ?`).get(userId);
  if (!existing) {
    reply.code(404);
    return { error: "User not found" };
  }
  if (typeof body.isAdmin === "boolean") {
    db.prepare(`UPDATE users SET is_admin = ? WHERE id = ?`).run(body.isAdmin ? 1 : 0, userId);
  } else {
    reply.code(400);
    return { error: "Set isAdmin: true or false" };
  }
  const row = db
    .prepare(
      `SELECT id, username, is_admin AS isAdmin, timezone, reminder_hour_local AS reminderHourLocal,
              task_nudge_days_after_air AS taskNudgeDaysAfterAir, created_at AS createdAt
       FROM users WHERE id = ?`,
    )
    .get(userId);
  return row;
});

app.post("/api/admin/users/:userId/test-push", async (request, reply) => {
  if (!isRequestAdmin(request)) {
    reply.code(401);
    return { error: "Unauthorized" };
  }
  const { userId } = request.params as { userId: string };
  const u = db.prepare(`SELECT id FROM users WHERE id = ?`).get(userId);
  if (!u) {
    reply.code(404);
    return { error: "User not found" };
  }
  if (!isWebPushConfigured()) {
    reply.code(503);
    return { error: "Push not configured on server (missing VAPID keys)" };
  }
  const body = (request.body ?? {}) as { title?: string; body?: string };
  const titleRaw = typeof body.title === "string" ? body.title.trim() : "";
  const bodyRaw = typeof body.body === "string" ? body.body.trim() : "";
  const title = titleRaw.slice(0, 200) || "Airalert test";
  const text = bodyRaw.slice(0, 500) || "Test push from admin panel.";
  const n = db.prepare(`SELECT COUNT(*) AS c FROM web_push_subscriptions WHERE user_id = ?`).get(userId) as { c: number };
  if (n.c === 0) {
    return { ok: true, sent: false, subscriptions: 0, message: "No registered push devices for this user." };
  }
  await sendWebPushToUser(userId, { title, body: text, url: "/" });
  return { ok: true, sent: true, subscriptions: n.c };
});

app.post("/api/admin/users/:userId/subscriptions", async (request, reply) => {
  if (!isRequestAdmin(request)) {
    reply.code(401);
    return { error: "Unauthorized" };
  }
  const { userId } = request.params as { userId: string };
  const body = (request.body ?? {}) as { tvmazeShowId?: number };
  const u = db.prepare(`SELECT id FROM users WHERE id = ?`).get(userId);
  if (!u) {
    reply.code(404);
    return { error: "User not found" };
  }
  if (typeof body.tvmazeShowId !== "number" || !Number.isInteger(body.tvmazeShowId)) {
    reply.code(400);
    return { error: "tvmazeShowId required" };
  }
  const show = await fetchShow(body.tvmazeShowId);
  const id = uuidv4();
  try {
    db.prepare(
      `INSERT INTO show_subscriptions (id, user_id, tvmaze_show_id, show_name, platform_note, added_from)
       VALUES (?, ?, ?, ?, ?, 'admin')`,
    ).run(id, userId, show.id, show.name, null);
  } catch (e) {
    const msg = e instanceof Error ? e.message : "";
    if (msg.includes("UNIQUE")) {
      reply.code(409);
      return { error: "User already subscribed to this show" };
    }
    throw e;
  }
  reply.code(201);
  return { id, tvmazeShowId: show.id, showName: show.name, addedFrom: "admin" };
});

app.delete("/api/admin/subscriptions/:subscriptionId", async (request, reply) => {
  if (!isRequestAdmin(request)) {
    reply.code(401);
    return { error: "Unauthorized" };
  }
  const { subscriptionId } = request.params as { subscriptionId: string };
  const r = db.prepare(`DELETE FROM show_subscriptions WHERE id = ?`).run(subscriptionId);
  if (r.changes === 0) {
    reply.code(404);
    return { error: "Not found" };
  }
  return { ok: true };
});

app.post("/api/users", async (request, reply) => {
  const body = (request.body ?? {}) as UserCreateInput;
  const { timezone, reminderHourLocal } = normalizeUserCreateInput(body);
  const created = createUserRecord(timezone, reminderHourLocal);
  reply.code(201);
  return created;
});

app.post("/api/users/bootstrap", async (request, reply) => {
  const body = (request.body ?? {}) as UserCreateInput;
  const { timezone, reminderHourLocal } = normalizeUserCreateInput(body);
  /** Always a new row + new session cookie (no resumeUserId — synced localStorage caused multi-phone same account). */
  const created = createUserRecord(timezone, reminderHourLocal);
  setSessionCookie(reply, request, created.id);
  reply.code(201);
  return { ...created, reused: false };
});

app.post("/api/auth/register", async (request, reply) => {
  const body = (request.body ?? {}) as UserCreateInput & { username?: string; password?: string };
  const usernameRaw = typeof body.username === "string" ? body.username.trim() : "";
  const password = typeof body.password === "string" ? body.password : "";
  if (!/^[a-zA-Z0-9._-]{3,32}$/.test(usernameRaw)) {
    reply.code(400);
    return { error: "Username must be 3–32 characters (letters, numbers, . _ -)" };
  }
  if (password.length < 8) {
    reply.code(400);
    return { error: "Password must be at least 8 characters" };
  }
  const taken = db
    .prepare(`SELECT id FROM users WHERE username IS NOT NULL AND lower(trim(username)) = lower(?)`)
    .get(usernameRaw) as { id: string } | undefined;
  if (taken) {
    reply.code(409);
    return { error: "Username already taken" };
  }
  const { timezone, reminderHourLocal } = normalizeUserCreateInput(body);
  const sid = sessionUserIdFromRequest(request);

  if (sid) {
    const me = db
      .prepare(`SELECT id, password_hash FROM users WHERE id = ?`)
      .get(sid) as { id: string; password_hash: string | null } | undefined;
    if (me?.password_hash) {
      reply.code(409);
      return { error: "Already signed in with an account. Sign out first to create another." };
    }
    if (me) {
      db.prepare(
        `UPDATE users SET username = ?, password_hash = ?, timezone = ?, reminder_hour_local = ? WHERE id = ?`,
      ).run(usernameRaw, hashPassword(password), timezone, reminderHourLocal, sid);
      setSessionCookie(reply, request, sid);
      reply.code(201);
      return { id: sid, username: usernameRaw, timezone, reminderHourLocal };
    }
  }

  const created = createRegisteredUser(usernameRaw, password, timezone, reminderHourLocal, false);
  setSessionCookie(reply, request, created.id);
  reply.code(201);
  return {
    id: created.id,
    username: usernameRaw,
    timezone: created.timezone,
    reminderHourLocal: created.reminderHourLocal,
  };
});

app.post("/api/auth/login", async (request, reply) => {
  const body = (request.body ?? {}) as { username?: string; password?: string };
  const usernameRaw = typeof body.username === "string" ? body.username.trim() : "";
  const password = typeof body.password === "string" ? body.password : "";
  const row = db
    .prepare(`SELECT id, password_hash FROM users WHERE username IS NOT NULL AND lower(trim(username)) = lower(?)`)
    .get(usernameRaw) as { id: string; password_hash: string | null } | undefined;
  if (!row?.password_hash || !verifyPassword(password, row.password_hash)) {
    reply.code(401);
    return { error: "Invalid username or password" };
  }
  setSessionCookie(reply, request, row.id);
  return { ok: true, id: row.id };
});

app.post("/api/auth/logout", async (request, reply) => {
  clearSessionCookie(reply, request);
  return { ok: true };
});

/** Current browser session (cookie). Register before `/api/users/:id` so `me` is not parsed as an id. */
app.get("/api/users/me", async (request, reply) => {
  const sid = sessionUserIdFromRequest(request);
  if (!sid) {
    reply.code(401);
    return { error: "No session" };
  }
  const row = db
    .prepare(
      `SELECT id, timezone, reminder_hour_local AS reminderHourLocal, calendar_token AS calendarToken,
              task_nudge_days_after_air AS taskNudgeDaysAfterAir, created_at AS createdAt,
              username, display_name AS displayName, avatar_data_url AS avatarDataUrl,
              (password_hash IS NOT NULL AND trim(password_hash) != '') AS hasPassword,
              is_admin AS isAdmin
       FROM users WHERE id = ?`,
    )
    .get(sid) as Record<string, unknown> | undefined;
  if (!row) {
    clearSessionCookie(reply, request);
    reply.code(401);
    return { error: "Session invalid" };
  }
  return row;
});

/** Clears the HttpOnly session cookie so the next bootstrap creates a fresh user on this device. */
app.post("/api/users/session/clear", async (request, reply) => {
  clearSessionCookie(reply, request);
  return { ok: true };
});

app.get("/api/users/:id", async (request, reply) => {
  const { id } = request.params as { id: string };
  if (!assertSelfOrAdmin(request, reply, id)) return;
  const row = db
    .prepare(
      `SELECT id, timezone, reminder_hour_local AS reminderHourLocal, calendar_token AS calendarToken,
              task_nudge_days_after_air AS taskNudgeDaysAfterAir, created_at AS createdAt,
              username, display_name AS displayName, avatar_data_url AS avatarDataUrl,
              is_admin AS isAdmin,
              (password_hash IS NOT NULL AND trim(password_hash) != '') AS hasPassword
       FROM users WHERE id = ?`,
    )
    .get(id) as Record<string, unknown> | undefined;
  if (!row) {
    reply.code(404);
    return { error: "User not found" };
  }
  return row;
});

app.patch("/api/users/:id", async (request, reply) => {
  const { id } = request.params as { id: string };
  if (!assertSelfOrAdmin(request, reply, id)) return;
  const body = (request.body ?? {}) as {
    timezone?: string;
    reminderHourLocal?: number;
    taskNudgeDaysAfterAir?: number | null;
    displayName?: string | null;
    avatarDataUrl?: string | null;
  };
  const existing = db.prepare(`SELECT id FROM users WHERE id = ?`).get(id);
  if (!existing) {
    reply.code(404);
    return { error: "User not found" };
  }
  if ("displayName" in body) {
    if (body.displayName === null || body.displayName === "") {
      db.prepare(`UPDATE users SET display_name = NULL WHERE id = ?`).run(id);
    } else if (typeof body.displayName === "string") {
      const dn = body.displayName.trim().slice(0, 120);
      db.prepare(`UPDATE users SET display_name = ? WHERE id = ?`).run(dn || null, id);
    }
  }
  if ("avatarDataUrl" in body) {
    const a = body.avatarDataUrl;
    if (a === null || a === "") {
      db.prepare(`UPDATE users SET avatar_data_url = NULL WHERE id = ?`).run(id);
    } else if (typeof a === "string") {
      const ok =
        /^\s*data:image\/(jpeg|jpg|png|webp);base64,/i.test(a) &&
        a.length > 0 &&
        a.length <= MAX_AVATAR_DATA_URL_LEN;
      if (!ok) {
        reply.code(400);
        return { error: "Avatar must be a JPEG, PNG, or WebP data URL under the size limit" };
      }
      db.prepare(`UPDATE users SET avatar_data_url = ? WHERE id = ?`).run(a.trim(), id);
    }
  }
  if (typeof body.timezone === "string" && body.timezone.trim()) {
    db.prepare(`UPDATE users SET timezone = ? WHERE id = ?`).run(body.timezone.trim(), id);
  }
  if (typeof body.reminderHourLocal === "number" && Number.isInteger(body.reminderHourLocal)) {
    const h = Math.min(23, Math.max(0, body.reminderHourLocal));
    db.prepare(`UPDATE users SET reminder_hour_local = ? WHERE id = ?`).run(h, id);
  }
  if ("taskNudgeDaysAfterAir" in body) {
    const v = body.taskNudgeDaysAfterAir;
    if (v === null || v === 0) {
      db.prepare(`UPDATE users SET task_nudge_days_after_air = NULL WHERE id = ?`).run(id);
    } else if (v === 1 || v === 3 || v === 7) {
      db.prepare(`UPDATE users SET task_nudge_days_after_air = ? WHERE id = ?`).run(v, id);
    }
  }
  const row = db
    .prepare(
      `SELECT id, timezone, reminder_hour_local AS reminderHourLocal, calendar_token AS calendarToken,
              task_nudge_days_after_air AS taskNudgeDaysAfterAir, created_at AS createdAt,
              username, display_name AS displayName, avatar_data_url AS avatarDataUrl,
              (password_hash IS NOT NULL AND trim(password_hash) != '') AS hasPassword, is_admin AS isAdmin
       FROM users WHERE id = ?`,
    )
    .get(id);
  return row;
});

app.get("/api/shows/search", async (request, reply) => {
  const q = (request.query as { q?: string }).q?.trim() ?? "";
  const title = (request.query as { title?: string }).title?.trim() ?? "";
  const excludeUserId = (request.query as { excludeUserId?: string }).excludeUserId?.trim() ?? "";
  const limitRaw = Number((request.query as { limit?: string }).limit);
  const limit = Number.isFinite(limitRaw) ? Math.min(100, Math.max(1, Math.floor(limitRaw))) : 100;

  /** TVMaze `/search/shows` only matches show names — use title, else legacy `q`. */
  const tvmazeSearch = title || q;
  if (!tvmazeSearch) {
    reply.code(400);
    return { error: "Enter a show title (or pass q=… for legacy clients)" };
  }

  const fast =
    (request.query as { fast?: string }).fast === "1" || (request.query as { fast?: string }).fast === "true";
  const catalogPagesRaw = Number((request.query as { catalogPages?: string }).catalogPages);
  const catalogMaxPages = Number.isFinite(catalogPagesRaw)
    ? Math.min(400, Math.max(40, Math.floor(catalogPagesRaw)))
    : undefined;

  const raw = await searchShowsWithCatalog(tvmazeSearch, {
    skipCatalog: fast,
    catalogMaxPages,
  });
  const rankQuery = title || tvmazeSearch;
  const ranked = rankSearchResults(raw, rankQuery);

  let excluded = new Set<number>();
  if (excludeUserId) {
    const u = db.prepare(`SELECT id FROM users WHERE id = ?`).get(excludeUserId);
    if (u) {
      const subs = db
        .prepare(`SELECT tvmaze_show_id FROM show_subscriptions WHERE user_id = ?`)
        .all(excludeUserId) as { tvmaze_show_id: number }[];
      excluded = new Set(subs.map((s) => Number(s.tvmaze_show_id)));
    }
  }

  const candidates = ranked.filter((r) => !excluded.has(Number(r.show.id)));
  const trimmedCandidates = candidates.slice(0, Math.min(200, limit * 2));
  const ids = trimmedCandidates.map((r) => r.show.id);
  const lastAiredById = await fetchPreviousEpisodeAirdates(ids);

  type ShowHit = {
    id: number;
    name: string;
    network: string | null;
    premiered: string | null;
    image: string | null;
    lastAiredDate: string | null;
  };

  const shows: ShowHit[] = trimmedCandidates.map((r) => ({
    id: r.show.id,
    name: r.show.name,
    network: r.show.network?.name ?? r.show.webChannel?.name ?? null,
    premiered: r.show.premiered ?? null,
    image: r.show.image?.medium ?? null,
    lastAiredDate: lastAiredById.get(r.show.id) ?? null,
  }));

  shows.sort((a, b) => {
    if (a.lastAiredDate && b.lastAiredDate) return b.lastAiredDate.localeCompare(a.lastAiredDate);
    if (a.lastAiredDate && !b.lastAiredDate) return -1;
    if (!a.lastAiredDate && b.lastAiredDate) return 1;
    return a.name.localeCompare(b.name);
  });

  return { shows: shows.slice(0, limit) };
});

type EpisodeRow = {
  id: number;
  name: string;
  season: number;
  number: number;
  airdate: string | null;
  airtime: string;
  runtime: number | null;
  network: string | null;
};

async function buildShowDetailsJson(showId: number, tzQuery: string | undefined) {
  const todayForNext = tzQuery ? safeTodayInTimeZone(tzQuery) : new Date().toISOString().slice(0, 10);

  let show: Awaited<ReturnType<typeof fetchShow>>;
  try {
    show = await fetchShow(showId);
  } catch {
    const subRow = db
      .prepare(`SELECT show_name AS showName FROM show_subscriptions WHERE tvmaze_show_id = ? LIMIT 1`)
      .get(showId) as { showName: string } | undefined;
    const cachedRows = db
      .prepare(`SELECT COUNT(*) AS c FROM episodes_cache WHERE tvmaze_show_id = ?`)
      .get(showId) as { c: number };
    if (!subRow && cachedRows.c === 0) {
      throw new Error("Show not found (TVMaze unavailable or show removed, and nothing in cache).");
    }
    show = {
      id: showId,
      name: subRow?.showName ?? "Show",
      premiered: null,
      status: null,
      summary: null,
      network: null,
      webChannel: null,
      image: null,
    };
  }

  const cached = db
    .prepare(
      `SELECT tvmaze_episode_id AS id, name, season, number, airdate, airtime, runtime, network
       FROM episodes_cache WHERE tvmaze_show_id = ? ORDER BY season ASC, number ASC`,
    )
    .all(showId) as EpisodeRow[];

  let episodes: EpisodeRow[];
  if (cached.length > 0) {
    episodes = cached.map((e) => ({
      ...e,
      airdate: normalizeEpisodeAirdate(e.airdate),
    }));
  } else {
    const raw = await fetchShowEpisodes(showId);
    episodes = raw.map((e) => ({
      id: e.id,
      name: e.name || "TBA",
      season: e.season,
      number: e.number,
      airdate: normalizeEpisodeAirdate(e.airdate),
      airtime: e.airtime || "",
      runtime: e.runtime,
      network: null,
    }));
  }

  const next =
    episodes.find((e) => {
      const d = normalizeEpisodeAirdate(e.airdate);
      return d != null && d >= todayForNext;
    }) ?? null;

  const seasonNums = new Set(episodes.map((e) => e.season));
  const bySeason = new Map<number, EpisodeRow[]>();
  for (const ep of episodes) {
    const list = bySeason.get(ep.season) ?? [];
    list.push(ep);
    bySeason.set(ep.season, list);
  }
  const seasons = [...bySeason.entries()]
    .sort((a, b) => a[0] - b[0])
    .map(([season, eps]) => ({
      season,
      episodes: eps.sort((a, b) => a.number - b.number),
    }));

  return {
    show: {
      id: show.id,
      name: show.name,
      network: show.network?.name ?? show.webChannel?.name ?? null,
      premiered: show.premiered ?? null,
      status: show.status ?? null,
      summary: show.summary ? stripHtml(show.summary) : null,
      image: show.image?.medium ?? show.image?.original ?? null,
    },
    seasonCount: seasonNums.size,
    episodeCount: episodes.length,
    nextEpisode: next
      ? {
          season: next.season,
          number: next.number,
          name: next.name,
          airdate: normalizeEpisodeAirdate(next.airdate),
          airtime: next.airtime || null,
        }
      : null,
    seasons,
    source: cached.length > 0 ? "cache" : "tvmaze",
  };
}

/** Query-string route avoids any proxy/path confusion with `/api/shows/:id/details`. */
app.get("/api/show-details", async (request, reply) => {
  const rawId = (request.query as { showId?: string }).showId;
  const showId = Number(rawId);
  if (rawId === undefined || String(rawId).trim() === "" || !Number.isInteger(showId) || showId < 1) {
    reply.code(400);
    return { error: "Missing or invalid showId query parameter" };
  }
  const tzQuery = (request.query as { timezone?: string }).timezone?.trim();
  try {
    return await buildShowDetailsJson(showId, tzQuery);
  } catch (err) {
    const msg = err instanceof Error ? err.message : String(err);
    app.log.error(err, "show-details failed");
    reply.code(502);
    return { error: msg };
  }
});

app.get("/api/shows/:showId/details", async (request, reply) => {
  const showId = Number((request.params as { showId: string }).showId);
  if (!Number.isInteger(showId) || showId < 1) {
    reply.code(400);
    return { error: "Invalid show id" };
  }
  const tzQuery = (request.query as { timezone?: string }).timezone?.trim();
  try {
    return await buildShowDetailsJson(showId, tzQuery);
  } catch (err) {
    const msg = err instanceof Error ? err.message : String(err);
    app.log.error(err, "show-details failed");
    reply.code(502);
    return { error: msg };
  }
});

app.get("/api/users/:userId/subscriptions", async (request, reply) => {
  const { userId } = request.params as { userId: string };
  const u = db.prepare(`SELECT id FROM users WHERE id = ?`).get(userId);
  if (!u) {
    reply.code(404);
    return { error: "User not found" };
  }
  if (!assertSelfOrAdmin(request, reply, userId)) return;
  const rows = db
    .prepare(
      `SELECT id, tvmaze_show_id AS tvmazeShowId, show_name AS showName,
              added_from AS addedFrom, created_at AS createdAt
       FROM show_subscriptions WHERE user_id = ? ORDER BY created_at DESC`,
    )
    .all(userId) as {
    id: string;
    tvmazeShowId: number;
    showName: string;
    addedFrom: string | null;
    createdAt: string;
  }[];

  const userRow = db.prepare(`SELECT timezone FROM users WHERE id = ?`).get(userId) as { timezone: string } | undefined;
  const todayStr = safeTodayInTimeZone(userRow?.timezone);

  /** SQLite date() accepts YYYY-MM-DD and ISO timestamps; avoids brittle substr/length checks. */
  const nextStmt = db.prepare(`
    SELECT date(airdate) AS airdate, airtime, name, season, number
    FROM episodes_cache
    WHERE tvmaze_show_id = ?
      AND airdate IS NOT NULL
      AND TRIM(airdate) != ''
      AND date(airdate) IS NOT NULL
      AND date(airdate) >= date(?)
    ORDER BY date(airdate) ASC, season ASC, number ASC
    LIMIT 1
  `);

  const lastAiredStmt = db.prepare(`
    SELECT date(airdate) AS airdate, airtime, name, season, number
    FROM episodes_cache
    WHERE tvmaze_show_id = ?
      AND airdate IS NOT NULL
      AND TRIM(airdate) != ''
      AND date(airdate) IS NOT NULL
    ORDER BY date(airdate) DESC, season DESC, number DESC
    LIMIT 1
  `);

  const tbaNextStmt = db.prepare(`
    SELECT name, season, number
    FROM episodes_cache
    WHERE tvmaze_show_id = ?
      AND (airdate IS NULL OR TRIM(airdate) = '')
    ORDER BY season DESC, number DESC
    LIMIT 1
  `);

  const subscriptions = rows.map((row) => {
    const next = nextStmt.get(row.tvmazeShowId, todayStr) as
      | { airdate: string; airtime: string; name: string; season: number; number: number }
      | undefined;
    const last = !next
      ? (lastAiredStmt.get(row.tvmazeShowId) as
          | { airdate: string; airtime: string; name: string; season: number; number: number }
          | undefined)
      : undefined;
    const tba =
      !next && !last
        ? (tbaNextStmt.get(row.tvmazeShowId) as { name: string; season: number; number: number } | undefined)
        : undefined;
    return {
      ...row,
      nextEpisode: next
        ? {
            airdate: next.airdate,
            airtime: next.airtime || null,
            name: next.name,
            season: next.season,
            number: next.number,
            label: `S${next.season}E${next.number} — ${next.name}`,
            dateTba: false,
          }
        : tba
          ? {
              airdate: null,
              airtime: null,
              name: tba.name,
              season: tba.season,
              number: tba.number,
              label: `S${tba.season}E${tba.number} — ${tba.name}`,
              dateTba: true,
            }
          : null,
      lastAiredEpisode: last
        ? {
            airdate: last.airdate,
            airtime: last.airtime || null,
            name: last.name,
            season: last.season,
            number: last.number,
            label: `S${last.season}E${last.number} — ${last.name}`,
          }
        : null,
    };
  });

  return { subscriptions };
});

app.get("/api/users/:userId/recommended-shows", async (request, reply) => {
  const { userId } = request.params as { userId: string };
  const u = db.prepare(`SELECT id FROM users WHERE id = ?`).get(userId);
  if (!u) {
    reply.code(404);
    return { error: "User not found" };
  }
  if (!assertSelfOrAdmin(request, reply, userId)) return;
  const rows = db
    .prepare(`SELECT tvmaze_show_id AS id FROM show_subscriptions WHERE user_id = ?`)
    .all(userId) as { id: number }[];
  const ids = rows.map((r) => Number(r.id)).filter((n) => Number.isInteger(n) && n > 0);
  try {
    const { shows, queriesUsed } = await computeRecommendedShows(ids);
    return { shows, queriesUsed };
  } catch (err) {
    const msg = err instanceof Error ? err.message : String(err);
    app.log.error(err, "recommended-shows failed");
    reply.code(502);
    return { error: msg };
  }
});

app.get("/api/users/:userId/trending-shows", async (request, reply) => {
  const { userId } = request.params as { userId: string };
  const u = db.prepare(`SELECT id FROM users WHERE id = ?`).get(userId);
  if (!u) {
    reply.code(404);
    return { error: "User not found" };
  }
  if (!assertSelfOrAdmin(request, reply, userId)) return;
  const rows = db
    .prepare(`SELECT tvmaze_show_id AS id FROM show_subscriptions WHERE user_id = ?`)
    .all(userId) as { id: number }[];
  const ids = rows.map((r) => Number(r.id)).filter((n) => Number.isInteger(n) && n > 0);
  try {
    const shows = await computeTrendingShows(ids);
    return { shows };
  } catch (err) {
    const msg = err instanceof Error ? err.message : String(err);
    app.log.error(err, "trending-shows failed");
    reply.code(502);
    return { error: msg };
  }
});

app.post("/api/users/:userId/subscriptions", async (request, reply) => {
  const { userId } = request.params as { userId: string };
  const body = (request.body ?? {}) as { tvmazeShowId?: number; addedFrom?: string };
  const u = db.prepare(`SELECT id FROM users WHERE id = ?`).get(userId);
  if (!u) {
    reply.code(404);
    return { error: "User not found" };
  }
  if (!assertSelfOrAdmin(request, reply, userId)) return;
  if (typeof body.tvmazeShowId !== "number" || !Number.isInteger(body.tvmazeShowId)) {
    reply.code(400);
    return { error: "tvmazeShowId required" };
  }
  let addedFrom: string | null = null;
  if (body.addedFrom === "search" || body.addedFrom === "recommended" || body.addedFrom === "trending") {
    addedFrom = body.addedFrom;
  }
  const show = await fetchShow(body.tvmazeShowId);
  const id = uuidv4();
  try {
    db.prepare(
      `INSERT INTO show_subscriptions (id, user_id, tvmaze_show_id, show_name, platform_note, added_from)
       VALUES (?, ?, ?, ?, ?, ?)`,
    ).run(id, userId, show.id, show.name, null, addedFrom);
  } catch (e) {
    const msg = e instanceof Error ? e.message : "";
    if (msg.includes("UNIQUE")) {
      reply.code(409);
      return { error: "Already subscribed" };
    }
    throw e;
  }
  reply.code(201);
  return { id, tvmazeShowId: show.id, showName: show.name, addedFrom };
});

app.delete("/api/subscriptions/:subscriptionId", async (request, reply) => {
  const { subscriptionId } = request.params as { subscriptionId: string };
  const sub = db
    .prepare(`SELECT user_id FROM show_subscriptions WHERE id = ?`)
    .get(subscriptionId) as { user_id: string } | undefined;
  if (!sub) {
    reply.code(404);
    return { error: "Not found" };
  }
  if (!assertSelfOrAdmin(request, reply, sub.user_id)) return;
  db.prepare(`DELETE FROM show_subscriptions WHERE id = ?`).run(subscriptionId);
  return { ok: true };
});

app.post("/api/users/:userId/devices", async (request, reply) => {
  const { userId } = request.params as { userId: string };
  const body = (request.body ?? {}) as { platform?: string; pushToken?: string };
  const u = db.prepare(`SELECT id FROM users WHERE id = ?`).get(userId);
  if (!u) {
    reply.code(404);
    return { error: "User not found" };
  }
  if (!assertSelfOrAdmin(request, reply, userId)) return;
  if (typeof body.platform !== "string" || !body.platform.trim()) {
    reply.code(400);
    return { error: "platform required" };
  }
  if (typeof body.pushToken !== "string" || !body.pushToken.trim()) {
    reply.code(400);
    return { error: "pushToken required" };
  }
  const id = uuidv4();
  const platform = body.platform.trim().slice(0, 32);
  const pushToken = body.pushToken.trim().slice(0, 2048);
  const result = db
    .prepare(`INSERT OR IGNORE INTO devices (id, user_id, platform, push_token) VALUES (?, ?, ?, ?)`)
    .run(id, userId, platform, pushToken);
  if (result.changes === 0) {
    const existing = db
      .prepare(`SELECT id FROM devices WHERE user_id = ? AND platform = ? AND push_token = ?`)
      .get(userId, platform, pushToken) as { id: string } | undefined;
    return { ok: true, id: existing?.id ?? id, duplicate: true };
  }
  reply.code(201);
  return { ok: true, id };
});

app.get("/api/push/vapid-public-key", async () => ({
  publicKey: getVapidPublicKey(),
  enabled: !!getVapidPublicKey(),
}));

app.post("/api/users/:userId/push-subscription", async (request, reply) => {
  const { userId } = request.params as { userId: string };
  const body = (request.body ?? {}) as {
    endpoint?: string;
    keys?: { p256dh?: string; auth?: string };
  };
  const u = db.prepare(`SELECT id FROM users WHERE id = ?`).get(userId);
  if (!u) {
    reply.code(404);
    return { error: "User not found" };
  }
  if (!assertSelfOrAdmin(request, reply, userId)) return;
  if (!getVapidPublicKey()) {
    reply.code(503);
    return { error: "Push not configured on server (missing VAPID keys)" };
  }
  const endpoint = typeof body.endpoint === "string" ? body.endpoint.trim() : "";
  const p256dh = typeof body.keys?.p256dh === "string" ? body.keys.p256dh.trim() : "";
  const auth = typeof body.keys?.auth === "string" ? body.keys.auth.trim() : "";
  if (!endpoint || !p256dh || !auth) {
    reply.code(400);
    return { error: "Invalid subscription (need endpoint, keys.p256dh, keys.auth)" };
  }
  const id = uuidv4();
  db.prepare(
    `INSERT INTO web_push_subscriptions (id, user_id, endpoint, p256dh, auth, updated_at)
     VALUES (?, ?, ?, ?, ?, datetime('now'))
     ON CONFLICT(endpoint) DO UPDATE SET
       user_id = excluded.user_id,
       p256dh = excluded.p256dh,
       auth = excluded.auth,
       updated_at = datetime('now')`,
  ).run(id, userId, endpoint, p256dh, auth);
  reply.code(201);
  return { ok: true };
});

/** Same as admin test push — caller must be the user or an admin. */
app.post("/api/users/:userId/test-push", async (request, reply) => {
  const { userId } = request.params as { userId: string };
  const u = db.prepare(`SELECT id FROM users WHERE id = ?`).get(userId);
  if (!u) {
    reply.code(404);
    return { error: "User not found" };
  }
  if (!assertSelfOrAdmin(request, reply, userId)) return;
  if (!isWebPushConfigured()) {
    reply.code(503);
    return { error: "Push not configured on server (missing VAPID keys)" };
  }
  const body = (request.body ?? {}) as { title?: string; body?: string };
  const titleRaw = typeof body.title === "string" ? body.title.trim() : "";
  const bodyRaw = typeof body.body === "string" ? body.body.trim() : "";
  const title = titleRaw.slice(0, 200) || "Airalert test";
  const text = bodyRaw.slice(0, 500) || "Test notification from your profile.";
  const n = db.prepare(`SELECT COUNT(*) AS c FROM web_push_subscriptions WHERE user_id = ?`).get(userId) as { c: number };
  if (n.c === 0) {
    return { ok: true, sent: false, subscriptions: 0, message: "No registered push devices for this user." };
  }
  await sendWebPushToUser(userId, { title, body: text, url: "/" });
  return { ok: true, sent: true, subscriptions: n.c };
});

/** Admin / dev: refresh episode cache + run notification pass. */
app.post("/api/jobs/run", async () => {
  const refreshed = await refreshAllSubscribedShows();
  const notifications = await runDailyNotifications();
  const taskNudgesSent = await runTaskNudgeNotifications();
  return { refreshed, notificationsCreated: notifications.length, notifications, taskNudgesSent };
});

app.get("/api/users/:userId/watch-tasks", async (request, reply) => {
  const { userId } = request.params as { userId: string };
  const u = db.prepare(`SELECT id FROM users WHERE id = ?`).get(userId);
  if (!u) {
    reply.code(404);
    return { error: "User not found" };
  }
  if (!assertSelfOrAdmin(request, reply, userId)) return;
  const rows = db
    .prepare(
      `SELECT id, tvmaze_show_id AS tvmazeShowId, tvmaze_episode_id AS tvmazeEpisodeId,
              show_name AS showName, episode_label AS episodeLabel, airdate,
              completed_at AS completedAt, nudge_sent_at AS nudgeSentAt, created_at AS createdAt
       FROM watch_tasks WHERE user_id = ?
       ORDER BY (completed_at IS NULL) DESC, airdate DESC, created_at DESC
       LIMIT 120`,
    )
    .all(userId);
  return { tasks: rows };
});

app.patch("/api/users/:userId/watch-tasks/:taskId", async (request, reply) => {
  const { userId, taskId } = request.params as { userId: string; taskId: string };
  const body = (request.body ?? {}) as { completed?: boolean };
  const u = db.prepare(`SELECT id FROM users WHERE id = ?`).get(userId);
  if (!u) {
    reply.code(404);
    return { error: "User not found" };
  }
  if (!assertSelfOrAdmin(request, reply, userId)) return;
  const task = db
    .prepare(`SELECT id FROM watch_tasks WHERE id = ? AND user_id = ?`)
    .get(taskId, userId);
  if (!task) {
    reply.code(404);
    return { error: "Task not found" };
  }
  if (body.completed === true) {
    db.prepare(`UPDATE watch_tasks SET completed_at = datetime('now') WHERE id = ? AND user_id = ?`).run(taskId, userId);
  } else if (body.completed === false) {
    db.prepare(`UPDATE watch_tasks SET completed_at = NULL WHERE id = ? AND user_id = ?`).run(taskId, userId);
  } else {
    reply.code(400);
    return { error: "Set completed: true or false" };
  }
  const row = db
    .prepare(
      `SELECT id, tvmaze_show_id AS tvmazeShowId, tvmaze_episode_id AS tvmazeEpisodeId,
              show_name AS showName, episode_label AS episodeLabel, airdate,
              completed_at AS completedAt, nudge_sent_at AS nudgeSentAt
       FROM watch_tasks WHERE id = ? AND user_id = ?`,
    )
    .get(taskId, userId);
  return row;
});

app.get("/api/users/:userId/notifications", async (request, reply) => {
  const { userId } = request.params as { userId: string };
  const u = db.prepare(`SELECT id FROM users WHERE id = ?`).get(userId);
  if (!u) {
    reply.code(404);
    return { error: "User not found" };
  }
  if (!assertSelfOrAdmin(request, reply, userId)) return;
  const rows = db
    .prepare(
      `SELECT id, show_name AS showName, episode_label AS episodeLabel, airdate, channel, created_at AS createdAt
       FROM notification_log WHERE user_id = ? ORDER BY created_at DESC LIMIT 100`,
    )
    .all(userId);
  return { notifications: rows };
});

app.get("/api/users/:userId/upcoming", async (request, reply) => {
  const { userId } = request.params as { userId: string };
  const u = db.prepare(`SELECT id FROM users WHERE id = ?`).get(userId);
  if (!u) {
    reply.code(404);
    return { error: "User not found" };
  }
  if (!assertSelfOrAdmin(request, reply, userId)) return;
  const subs = db
    .prepare(`SELECT tvmaze_show_id AS tvmazeShowId, show_name AS showName FROM show_subscriptions WHERE user_id = ?`)
    .all(userId) as { tvmazeShowId: number; showName: string }[];

  const userRow = db.prepare(`SELECT timezone FROM users WHERE id = ?`).get(userId) as { timezone: string } | undefined;
  const todayStr = safeTodayInTimeZone(userRow?.timezone);

  const epStmt = db.prepare(`
    SELECT tvmaze_episode_id AS id, name, season, number, date(airdate) AS airdate, airtime, runtime
    FROM episodes_cache
    WHERE tvmaze_show_id = ?
      AND airdate IS NOT NULL
      AND TRIM(airdate) != ''
      AND date(airdate) IS NOT NULL
      AND date(airdate) >= date(?)
    ORDER BY date(airdate) ASC, season ASC, number ASC
    LIMIT 20
  `);

  const upcoming: {
    showName: string;
    tvmazeShowId: number;
    episodes: { id: number; name: string; season: number; number: number; airdate: string; airtime: string; runtime: number | null }[];
  }[] = [];

  for (const s of subs) {
    const episodes = epStmt.all(s.tvmazeShowId, todayStr) as {
      id: number;
      name: string;
      season: number;
      number: number;
      airdate: string;
      airtime: string;
      runtime: number | null;
    }[];
    if (episodes.length) {
      upcoming.push({ showName: s.showName, tvmazeShowId: s.tvmazeShowId, episodes });
    }
  }
  return { upcoming };
});

app.get("/calendar/:filename", async (request, reply) => {
  const raw = (request.params as { filename: string }).filename;
  const token = raw.replace(/\.ics$/i, "");
  const user = db
    .prepare(`SELECT id, timezone FROM users WHERE calendar_token = ?`)
    .get(token) as { id: string; timezone: string } | undefined;
  if (!user) {
    reply.code(404);
    return "Not found";
  }

  const subs = db
    .prepare(`SELECT tvmaze_show_id AS tvmazeShowId, show_name AS showName FROM show_subscriptions WHERE user_id = ?`)
    .all(user.id) as { tvmazeShowId: number; showName: string }[];

  const epStmt = db.prepare(`
    SELECT tvmaze_show_id AS showId, tvmaze_episode_id AS episodeId, name, season, number,
      date(airdate) AS airdate, network
    FROM episodes_cache
    WHERE tvmaze_show_id = ?
      AND airdate IS NOT NULL
      AND TRIM(airdate) != ''
      AND date(airdate) IS NOT NULL
      AND date(airdate) >= date('now', '-7 days')
    ORDER BY date(airdate) ASC, season ASC, number ASC
    LIMIT 200
  `);

  const events: { uid: string; summary: string; description?: string; airdate: string }[] = [];

  for (const s of subs) {
    const eps = epStmt.all(s.tvmazeShowId) as {
      showId: number;
      episodeId: number;
      name: string;
      season: number;
      number: number;
      airdate: string;
      network: string | null;
    }[];
    for (const ep of eps) {
      const summary = `${s.showName} — S${ep.season}E${ep.number}`;
      const bits = [ep.name, ep.network ? ep.network : null].filter(Boolean);
      events.push({
        uid: episodeUid(ep.showId, ep.episodeId),
        summary,
        description: bits.join("\n"),
        airdate: ep.airdate,
      });
    }
  }

  const ics = buildIcsCalendar("Airalert — my shows", events);
  reply.header("Content-Type", "text/calendar; charset=utf-8");
  reply.header("Content-Disposition", 'attachment; filename="airalert.ics"');
  return ics;
});

function readPublicHtml(name: string): string {
  return fs.readFileSync(path.join(publicDir, name), "utf8");
}
app.get("/", async (_req, reply) => {
  reply.header("Cache-Control", "no-store, max-age=0");
  reply.type("text/html; charset=utf-8").send(readPublicHtml("index.html"));
});
app.get("/index.html", async (_req, reply) => {
  reply.header("Cache-Control", "no-store, max-age=0");
  reply.type("text/html; charset=utf-8").send(readPublicHtml("index.html"));
});
app.get("/search-results.html", async (_req, reply) => {
  reply.header("Cache-Control", "no-store, max-age=0");
  reply.type("text/html; charset=utf-8").send(readPublicHtml("search-results.html"));
});
app.get("/admin.html", async (_req, reply) => {
  reply.header("Cache-Control", "no-store, max-age=0");
  reply.type("text/html; charset=utf-8").send(readPublicHtml("admin.html"));
});
app.get("/admin", async (_req, reply) => {
  reply.redirect("/admin.html", 302);
});

app.get("/sw.js", async (_req, reply) => {
  const p = path.join(publicDir, "sw.js");
  if (!fs.existsSync(p)) {
    reply.code(404);
    return "Not found";
  }
  reply.header("Cache-Control", "no-store, max-age=0");
  return reply.type("application/javascript; charset=utf-8").send(fs.readFileSync(p, "utf8"));
});

app.get("/manifest.json", async (_req, reply) => {
  const p = path.join(publicDir, "manifest.json");
  if (!fs.existsSync(p)) {
    reply.code(404);
    return "Not found";
  }
  reply.header("Cache-Control", "no-store, max-age=0");
  return reply.type("application/manifest+json; charset=utf-8").send(fs.readFileSync(p, "utf8"));
});

app.get("/logo.svg", async (_req, reply) => {
  const p = path.join(publicDir, "logo.svg");
  if (!fs.existsSync(p)) {
    reply.code(404);
    return "Not found";
  }
  reply.header("Cache-Control", "public, max-age=86400");
  return reply.type("image/svg+xml; charset=utf-8").send(fs.readFileSync(p, "utf8"));
});

cron.schedule("5 * * * *", async () => {
  try {
    await refreshAllSubscribedShows();
    const n = await runDailyNotifications();
    if (n.length) {
      app.log.info({ count: n.length }, "notifications recorded");
    }
    const nudges = await runTaskNudgeNotifications();
    if (nudges > 0) {
      app.log.info({ count: nudges }, "task nudge pushes sent");
    }
  } catch (err) {
    app.log.error(err, "scheduled job failed");
  }
});

await app.listen({ port: PORT, host: "0.0.0.0" });
app.log.info(`Airalert V1 http://localhost:${PORT}`);
