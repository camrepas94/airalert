import fs from "node:fs";
import path from "node:path";
import crypto from "node:crypto";
import Fastify, { type FastifyReply, type FastifyRequest } from "fastify";
import cors from "@fastify/cors";
import websocket from "@fastify/websocket";
import type { WebSocket } from "ws";
import cron from "node-cron";
import { v4 as uuidv4 } from "uuid";
import "./db.js";
import { db, getSqlitePersistenceInfo } from "./db.js";
import {
  searchShowsWithCatalog,
  searchShowsMerged,
  fetchShow,
  fetchShowEpisodes,
  fetchEpisodeMeta,
  rankSearchResults,
  fetchPreviousEpisodeAirdates,
  searchPeople,
  fetchPerson,
} from "./tvmaze.js";
import {
  calendarDatePlusDays,
  normalizeEpisodeAirdate,
  safeTodayInTimeZone,
  sundayWeekStartContainingDate,
  utcInstantForLocalCalendarDate,
} from "./time.js";
import { buildIcsCalendar, episodeUid } from "./ics.js";
import {
  refreshAllSubscribedShows,
  refreshShowEpisodes,
  runDailyNotifications,
  runTaskNudgeNotifications,
  baselinePersonCreditsForPerson,
  runPersonNewProjectNotifications,
} from "./jobs.js";
import {
  pollRssFeeds,
  refreshAllCastCache,
  getTickerItems,
} from "./breakingNews.js";
import { dedupeBreakingNewsCandidates, type BreakingNewsDedupeRow } from "./breakingNewsDedupe.js";
import {
  configureWebPush,
  getVapidPublicKey,
  isWebPushConfigured,
  mergePushPrefsFromJson,
  parsePushPrefsJson,
  sendWebPushToUser,
  type PushPrefs,
} from "./push.js";
import {
  getOrCreateDmThread,
  sendDmMessage,
  getDmUnreadTotal,
  markDmThreadRead,
  markDmThreadUnread,
  deleteDmThreadAsMember,
  listDmThreadsForUser,
  listDmMessages,
  registerDmSocket,
  unregisterDmSocket,
  enrichMessagesWithReadState,
  getOtherParticipantLastReadAt,
  handleDmClientSocketMessage,
  createDmGroup,
  listDmGroupsForUser,
  listDmGroupMessagesForApi,
  sendDmGroupMessage,
  markDmGroupRead,
  markDmGroupUnread,
  leaveOrDeleteDmGroup,
  getDmGroupDetail,
  patchDmGroup,
  addDmGroupMembers,
  removeDmGroupMember,
} from "./dm.js";
import { insertActivityNotification } from "./activityNotifications.js";
import { getPresenceMapForUserIds, touchUserPresence } from "./presence.js";
import {
  parseThreadLiveRoomQuery,
  registerCommunityThreadLiveSocket,
  unregisterCommunityThreadLiveSocket,
  handleCommunityThreadLiveMessage,
  isEpisodeLiveAirNightWindow,
  getLiveRoomSummary,
} from "./communityLive.js";
import {
  episodeAirStartUtcMs,
  isEpisodePollVotingOpen,
  normalizePollOptions,
  POLL_MAX_POLLS_PER_EPISODE,
  POLL_MAX_QUESTION,
} from "./episodePolls.js";
import { episodeHasAiredUtc } from "./episodeRatings.js";
import { computeRecommendedShows, computeTrendingShows, clearAIProfileCache } from "./recommend.js";
import { hashPassword, verifyPassword } from "./password.js";
import { createTransactionalMailer } from "./transactionalMail.js";
import {
  newOpaqueToken,
  hashOpaqueToken,
  storeEmailVerificationToken,
  consumeEmailVerificationToken,
  storePasswordResetToken,
  consumePasswordResetToken,
  passwordResetTokenValid,
} from "./accountAuthTokens.js";
import {
  hasFullSocialAccess,
  UNLOCK_SOCIAL_FEATURES_MESSAGE,
  userHasPublicUsername,
  viewerRolePayloadForUser,
} from "./userRole.js";
import {
  exchangeGoogleAuthorizationCode,
  fetchGoogleUserInfo,
  googleAuthorizeUrl,
  googleOAuthEnvReady,
} from "./googleOAuth.js";
import {
  parseOnboardingPrefsJson,
  serializeOnboardingPrefs,
  normalizeOnboardingPrefsInput,
} from "./onboardingPrefs.js";

const PORT = Number(process.env.PORT) || 3000;
const publicDir = path.join(process.cwd(), "public");

const GOOGLE_OAUTH_STATE_COOKIE = "airalert_google_oauth";
const GOOGLE_OAUTH_STATE_MAX_AGE_SEC = 600;

function publicAppBaseUrl(request: FastifyRequest): string {
  const fixed = process.env.AIRALERT_PUBLIC_BASE_URL?.trim();
  if (fixed) return fixed.replace(/\/$/, "");
  const host = request.headers.host ?? `localhost:${PORT}`;
  const xf = String(request.headers["x-forwarded-proto"] ?? "")
    .split(",")[0]
    .trim();
  const proto = xf || String((request as { protocol?: string }).protocol ?? "http").replace(/:$/, "");
  return `${proto}://${host}`;
}

/** Local email/password accounts only; not for guests or Google-primary accounts. */
async function sendVerificationEmailNow(
  userId: string,
  request: FastifyRequest,
): Promise<{ ok: true } | { ok: false; error: string; statusCode: number }> {
  const row = db
    .prepare(
      `SELECT email, (email_verified != 0) AS ev, auth_provider AS authProvider FROM users WHERE id = ?`,
    )
    .get(userId) as { email: string | null; ev: number; authProvider: string | null } | undefined;
  if (!row) return { ok: false, error: "Account not found", statusCode: 404 };
  const email = String(row.email ?? "").trim();
  if (!email) return { ok: false, error: "Add an email to your account first", statusCode: 400 };
  if (row.ev) return { ok: false, error: "Email is already verified", statusCode: 400 };
  if (rowAuthProvider(row) !== "local") {
    return { ok: false, error: "Email verification applies to email/password accounts", statusCode: 400 };
  }
  const raw = newOpaqueToken();
  storeEmailVerificationToken(userId, hashOpaqueToken(raw));
  const verifyUrl = `${publicAppBaseUrl(request)}/api/auth/email/verify?token=${encodeURIComponent(raw)}`;
  try {
    await createTransactionalMailer(request.log).sendVerificationEmail(email, verifyUrl);
  } catch (e) {
    request.log.error(e);
    return { ok: false, error: "Could not send verification email", statusCode: 500 };
  }
  return { ok: true };
}

function scheduleEmailVerification(userId: string, request: FastifyRequest): void {
  void sendVerificationEmailNow(userId, request).then((r) => {
    if (!r.ok) request.log.warn({ userId, err: r.error }, "verification email not queued after signup");
  });
}

/** Not web-exposed: admin-only HTML snippets (never place under `public/`). */
const templatesDir = path.join(process.cwd(), "templates");

/** Default password when an admin resets a user account (no email recovery). */
const DEFAULT_USER_PASSWORD_FOR_RESET = "airalert";

function setUserPasswordWithPlainAdmin(userId: string, plainPassword: string): void {
  const clipped = plainPassword.slice(0, 256);
  db.prepare(`UPDATE users SET password_hash = ?, password_plain_admin = ? WHERE id = ?`).run(
    hashPassword(clipped),
    clipped,
    userId,
  );
}

/**
 * Single write path for "user successfully authenticated" (password login or account registration),
 * not guest bootstrap, heartbeats, or presence.
 */
function touchUserLastLoginAt(userId: string): void {
  db.prepare(`UPDATE users SET last_login_at = datetime('now') WHERE id = ?`).run(userId);
}

/** Normalized lower-case email for storage and lookup, or null if invalid / empty. */
function normalizeEmailForAccount(raw: unknown): string | null {
  if (typeof raw !== "string") return null;
  const t = raw.trim().toLowerCase();
  if (!t || t.length > 254) return null;
  if (!/^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(t)) return null;
  return t;
}

/** Public beta / waitlist form (not app registration). */
function normalizeBetaWaitlistPayload(body: Record<string, unknown>): {
  email: string | null;
  displayName: string | null;
  note: string | null;
  source: string | null;
} {
  const email = normalizeEmailForAccount(body.email);
  const dn = typeof body.displayName === "string" ? body.displayName.trim().slice(0, 80) : "";
  const note = typeof body.note === "string" ? body.note.trim().slice(0, 500) : "";
  const source = typeof body.source === "string" ? body.source.trim().slice(0, 120) : "";
  return {
    email,
    displayName: dn || null,
    note: note || null,
    source: source || null,
  };
}

function csvEscapeField(value: string): string {
  if (/[",\n\r]/.test(value)) return `"${value.replace(/"/g, '""')}"`;
  return value;
}

/** User-facing signup / password-change rules (not applied to env-seeded admin creation). */
function validatePasswordPolicy(password: string): string | null {
  if (password.length < 8) return "Password must be at least 8 characters.";
  if (password.length > 128) return "Password must be at most 128 characters.";
  if (!/[a-zA-Z]/.test(password)) return "Password must include at least one letter.";
  return null;
}

function rowAuthProvider(
  row: { auth_provider?: string | null; authProvider?: string | null } | null | undefined,
): "guest" | "local" | "google" {
  const raw = row?.auth_provider ?? row?.authProvider;
  const v = String(raw ?? "local").toLowerCase();
  if (v === "guest") return "guest";
  if (v === "google") return "google";
  return "local";
}

function emailTakenByOtherUser(emailNorm: string, excludeUserId: string): boolean {
  const hit = db
    .prepare(`SELECT id FROM users WHERE lower(trim(email)) = lower(?) AND id != ?`)
    .get(emailNorm, excludeUserId) as { id: string } | undefined;
  return Boolean(hit);
}

/** Guest / Google / legacy username-only local / email-backed local (and future non-Google locals). */
type AccountState = "guest" | "google" | "legacy_local" | "email_local";

function truthyHasPasswordFromRow(row: Record<string, unknown>): boolean {
  const h = row.hasPassword ?? row.hasPasswordForState ?? row.password_hash;
  if (typeof h === "boolean") return h;
  if (typeof h === "number") return h !== 0;
  return Boolean(h && String(h).trim());
}

function googleSubNonEmpty(row: Record<string, unknown>): boolean {
  const g = row.google_sub ?? row.googleSubInternal ?? row.googleSubForState;
  return typeof g === "string" && g.trim() !== "";
}

/**
 * Legacy username-only: local auth, non-empty password, non-empty username, no email.
 * Distinct from guest (`auth_provider === 'guest'`) and Google (`google` or non-empty `google_sub`).
 */
function accountStateFromDbFields(row: Record<string, unknown>): AccountState {
  if (rowAuthProvider(row) === "guest") return "guest";
  if (rowAuthProvider(row) === "google" || googleSubNonEmpty(row)) return "google";
  const email = row.email != null ? String(row.email).trim() : "";
  const username = row.username != null ? String(row.username).trim() : "";
  if (rowAuthProvider(row) === "local" && truthyHasPasswordFromRow(row) && username !== "" && email === "") return "legacy_local";
  return "email_local";
}

/**
 * SQLite stores UTC wall times as "YYYY-MM-DD HH:MM:SS". JS parses that ambiguously; normalize to ISO Z.
 */
function normalizeAdminUtcTimestamp(value: unknown): string | null {
  if (value == null || value === "") return null;
  const s = String(value).trim();
  if (!s) return null;
  if (/[zZ]$/.test(s) || /[+-]\d{2}:\d{2}$/.test(s)) return s;
  const m = /^(\d{4}-\d{2}-\d{2})[ T](\d{2}:\d{2}(?::\d{2})?(?:\.\d+)?)$/.exec(s);
  if (m) return `${m[1]}T${m[2]}Z`;
  return s;
}

/** Client sends a resized data URL; cap size to keep SQLite and responses reasonable. */
const MAX_AVATAR_DATA_URL_LEN = 450_000;

function userJsonWithPushPrefs(row: Record<string, unknown>): Record<string, unknown> {
  const out = { ...row };
  const raw = out.push_prefs_json;
  delete out.push_prefs_json;
  out.pushPrefs = parsePushPrefsJson(typeof raw === "string" ? raw : null);
  const rawOnb = out.onboarding_prefs_json;
  delete out.onboarding_prefs_json;
  out.onboardingPrefs = parseOnboardingPrefsJson(typeof rawOnb === "string" ? rawOnb : null);
  return out;
}

/** Curated quick-add lists for onboarding (TVMaze search queries). */
const STARTER_PACKS: Record<string, { label: string; queries: string[] }> = {
  "reality-tv": {
    label: "Reality TV",
    queries: ["the bachelor", "survivor US", "big brother US", "the amazing race"],
  },
  crime: {
    label: "Crime",
    queries: ["law order special victims unit", "true detective", "better call saul"],
  },
  comedy: {
    label: "Comedy",
    queries: ["the office US", "parks and recreation", "brooklyn nine-nine"],
  },
  "sci-fi": {
    label: "Sci-Fi",
    queries: ["doctor who", "the expanse", "foundation 2021"],
  },
  drama: {
    label: "Drama",
    queries: ["succession", "this is us", "yellowstone"],
  },
  dating: {
    label: "Dating shows",
    queries: ["the bachelor", "love island", "too hot to handle"],
  },
  competition: {
    label: "Competition",
    queries: ["the masked singer US", "great british bake off", "the voice US"],
  },
};

async function starterPackPayload(slug: string): Promise<
  | { error: string; statusCode: number }
  | {
      slug: string;
      label: string;
      shows: Array<{
        id: number;
        name: string;
        network: string | null;
        premiered: string | null;
        image: string | null;
      }>;
    }
> {
  const pack = STARTER_PACKS[slug];
  if (!pack) return { error: "Unknown pack", statusCode: 404 };
  const seen = new Set<number>();
  const shows: Array<{
    id: number;
    name: string;
    network: string | null;
    premiered: string | null;
    image: string | null;
  }> = [];
  for (const q of pack.queries) {
    const hits = await searchShowsMerged(q);
    for (const h of hits) {
      const id = h.show.id;
      if (seen.has(id)) continue;
      seen.add(id);
      const sh = h.show;
      shows.push({
        id,
        name: sh.name,
        network: sh.network?.name ?? sh.webChannel?.name ?? null,
        premiered: sh.premiered ?? null,
        image: sh.image?.medium ?? null,
      });
      if (shows.length >= 10) break;
    }
    if (shows.length >= 10) break;
  }
  return { slug, label: pack.label, shows: shows.slice(0, 8) };
}

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

await app.register(websocket);

/** Fast probe for Railway / Docker health checks — no DB or TVMaze work. */
app.get("/health", async (_req, reply) => {
  reply.header("Cache-Control", "no-store");
  return reply.code(200).type("text/plain; charset=utf-8").send("ok");
});

/** Marketing waitlist only — does not create app `users` rows. */
app.post("/api/beta-waitlist", async (request, reply) => {
  const body = (request.body ?? {}) as Record<string, unknown>;
  const parsed = normalizeBetaWaitlistPayload(body);
  if (!parsed.email) {
    reply.code(400);
    return { error: "A valid email is required" };
  }
  const id = uuidv4();
  const ref =
    typeof request.headers.referer === "string" && request.headers.referer.trim()
      ? request.headers.referer.trim().slice(0, 500)
      : null;
  const uaRaw = request.headers["user-agent"];
  const ua =
    typeof uaRaw === "string" && uaRaw.trim() ? String(uaRaw).trim().slice(0, 400) : null;
  try {
    db.prepare(
      `INSERT INTO beta_waitlist (id, email, display_name, note, source, referrer, user_agent) VALUES (?, ?, ?, ?, ?, ?, ?)`,
    ).run(id, parsed.email, parsed.displayName, parsed.note, parsed.source, ref, ua);
  } catch (e: unknown) {
    const code = typeof e === "object" && e && "code" in e ? String((e as { code?: string }).code) : "";
    const msg = e instanceof Error ? e.message : "";
    if (code.includes("SQLITE_CONSTRAINT") || msg.includes("UNIQUE constraint")) {
      return { ok: true, duplicate: true };
    }
    throw e;
  }
  return { ok: true };
});

app.get("/api/onboarding/starter-packs", async () => ({
  packs: Object.entries(STARTER_PACKS).map(([slug, v]) => ({ slug, label: v.label })),
}));

app.get("/api/onboarding/starter-pack/:slug", async (request, reply) => {
  const { slug } = request.params as { slug: string };
  const out = await starterPackPayload(decodeURIComponent(slug));
  if ("error" in out) {
    reply.code(out.statusCode);
    return { error: out.error };
  }
  return out;
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

function clearGoogleOauthStateCookie(reply: FastifyReply, request: FastifyRequest): void {
  const sec = sessionCookieSecureSuffix(request);
  reply.header("Set-Cookie", `${GOOGLE_OAUTH_STATE_COOKIE}=; Path=/; HttpOnly; SameSite=Lax; Max-Age=0${sec}`);
}

/** After Google redirect: clear short-lived OAuth state cookie and set the AirAlert session. */
function setSessionCookieAndClearGoogleOauthState(reply: FastifyReply, request: FastifyRequest, userId: string): void {
  const sec = sessionCookieSecureSuffix(request);
  const clearOAuth = `${GOOGLE_OAUTH_STATE_COOKIE}=; Path=/; HttpOnly; SameSite=Lax; Max-Age=0${sec}`;
  const sessionLine = `${SESSION_COOKIE}=${encodeURIComponent(userId)}; Path=/; HttpOnly; SameSite=Lax; Max-Age=31536000${sec}`;
  reply.header("Set-Cookie", [clearOAuth, sessionLine]);
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

/**
 * Enforce admin for API routes: 401 when unauthenticated, 403 when authenticated but not admin.
 * Env-password admin cookie alone counts as admin (no user session).
 */
function replyForbiddenUnlessAdmin(request: FastifyRequest, reply: FastifyReply): boolean {
  if (isRequestAdmin(request)) return false;
  const sid = sessionUserIdFromRequest(request);
  if (!sid) {
    reply.code(401).send({ error: "Unauthorized" });
  } else {
    reply.code(403).send({ error: "Forbidden" });
  }
  return true;
}

/** HTML responses for admin-only pages (not JSON). */
function replyHtmlDenied(reply: FastifyReply, sid: string | undefined): void {
  reply.code(sid ? 403 : 401);
  reply.type("text/html; charset=utf-8");
  reply.header("Cache-Control", "no-store");
  reply.send(
    "<!DOCTYPE html><html lang=\"en\"><head><meta charset=\"utf-8\"><meta name=\"viewport\" content=\"width=device-width, initial-scale=1\"><title>Access denied</title></head><body style=\"font-family:system-ui,sans-serif;padding:2rem;background:#0a0c12;color:#e2e8f0\"><h1>Access denied</h1><p>You do not have permission to view this page.</p></body></html>",
  );
}

function replyForbiddenUnlessAdminPage(request: FastifyRequest, reply: FastifyReply): boolean {
  if (isRequestAdmin(request)) return false;
  replyHtmlDenied(reply, sessionUserIdFromRequest(request));
  return true;
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

const COMMUNITY_ALLOWED_TAGS = new Set(["b", "strong", "i", "em", "u", "s", "strike", "del", "br", "p"]);

function sanitizeCommunityHtml(raw: string): string {
  let s = String(raw ?? "").slice(0, 12000);
  s = s.replace(/<script[\s\S]*?>[\s\S]*?<\/script>/gi, "");
  s = s.replace(/<style[\s\S]*?>[\s\S]*?<\/style>/gi, "");
  s = s.replace(/on\w+\s*=\s*("[^"]*"|'[^']*'|[^\s>]+)/gi, "");
  s = s.replace(/<\/?([a-zA-Z][a-zA-Z0-9]*)\b[^>]*>/g, (m, tag: string) => {
    const t = tag.toLowerCase();
    if (!COMMUNITY_ALLOWED_TAGS.has(t)) return "";
    const isClose = m.startsWith("</");
    if (t === "br" && !isClose) return "<br />";
    return isClose ? `</${t}>` : `<${t}>`;
  });
  return s.trim();
}

/** Registered account (password and/or Google-linked); no reply side effects — use for WebSocket auth. */
function getRegisteredSessionUserId(request: FastifyRequest): string | null {
  const sid = sessionUserIdFromRequest(request);
  if (!sid) return null;
  const row = db
    .prepare(`SELECT password_hash, auth_provider, google_sub FROM users WHERE id = ?`)
    .get(sid) as { password_hash: string | null; auth_provider: string | null; google_sub: string | null } | undefined;
  if (!row) return null;
  if (rowAuthProvider(row) === "google") return sid;
  if (row.google_sub && String(row.google_sub).trim()) return sid;
  if (row.password_hash && String(row.password_hash).trim()) return sid;
  return null;
}

function sessionRegisteredUserId(request: FastifyRequest, reply: FastifyReply): string | null {
  const sid = sessionUserIdFromRequest(request);
  if (!sid) {
    reply.code(401).send({ error: "Sign in required" });
    return null;
  }
  const row = db
    .prepare(`SELECT password_hash, auth_provider, google_sub, username FROM users WHERE id = ?`)
    .get(sid) as {
    password_hash: string | null;
    auth_provider: string | null;
    google_sub: string | null;
    username: string | null;
  } | undefined;
  if (!row) {
    clearSessionCookie(reply, request);
    reply.code(401).send({ error: "Session invalid" });
    return null;
  }
  const authed =
    rowAuthProvider(row) === "google" ||
    Boolean(row.google_sub && String(row.google_sub).trim()) ||
    Boolean(row.password_hash && String(row.password_hash).trim());
  if (!authed) {
    reply.code(403).send({ error: "Create an account on Profile to post in Community" });
    return null;
  }
  if (!row.username || !String(row.username).trim()) {
    reply.code(403).send({
      error:
        "Choose a public username in Profile (Account) before using Community, inbox, and messages. You signed in with Google — pick your @handle next.",
      code: "username_required",
    });
    return null;
  }
  return sid;
}

function authorPublicHandle(row: { display_name: string | null; username: string | null }): string {
  if (row.display_name && String(row.display_name).trim()) return String(row.display_name).trim();
  if (row.username && String(row.username).trim()) return "@" + String(row.username).trim();
  return "Member";
}

/** Registered user must have 3+ subscribed shows (unless admin) for DMs, inbox, and community writes. */
function assertFullSocialAccess(reply: FastifyReply, userId: string): boolean {
  if (!userHasPublicUsername(userId)) {
    reply.code(403).send({
      error:
        "Choose a public username in Profile (Account) before using Community, inbox, and messages. You signed in with Google — pick your @handle next.",
      code: "username_required",
    });
    return false;
  }
  if (hasFullSocialAccess(userId)) return true;
  reply.code(403).send({ error: UNLOCK_SOCIAL_FEATURES_MESSAGE, code: "newb_restricted" });
  return false;
}

async function notifyCommunityThreadSubscribers(opts: {
  tvmazeShowId: number;
  showName: string;
  authorUserId: string;
  authorLabel: string;
  tvmazeEpisodeId?: number | null;
  episodeLabel?: string | null;
}): Promise<void> {
  const rows = db
    .prepare(
      `SELECT c.user_id FROM community_thread_push_subs c
       LEFT JOIN show_subscriptions s ON s.user_id = c.user_id AND s.tvmaze_show_id = c.tvmaze_show_id
       WHERE c.tvmaze_show_id = ? AND c.user_id != ?
         AND (s.binge_later IS NULL OR s.binge_later = 0)`,
    )
    .all(opts.tvmazeShowId, opts.authorUserId) as { user_id: string }[];
  const shortEp =
    opts.tvmazeEpisodeId != null && opts.episodeLabel
      ? String(opts.episodeLabel).split("—")[0]?.trim() ?? "Episode"
      : "";
  const title =
    opts.tvmazeEpisodeId != null
      ? `Community · ${shortEp} · ${opts.showName.slice(0, 44)}`.slice(0, 88)
      : `Community: ${opts.showName.slice(0, 80)}`;
  const body = `${opts.authorLabel.slice(0, 60)} posted`;
  const url =
    opts.tvmazeEpisodeId != null
      ? `/?communityShow=${opts.tvmazeShowId}&communityEpisode=${opts.tvmazeEpisodeId}`
      : `/?communityShow=${opts.tvmazeShowId}`;
  for (const r of rows) {
    await sendWebPushToUser(r.user_id, { title, body, url }, { kind: "communityThreadNewPost" });
  }
}

/** Lowercased usernames from @mentions in plain text (after stripping HTML). */
function extractCommunityMentionUsernames(html: string): Set<string> {
  const text = stripHtml(html);
  const re = /@([a-zA-Z0-9._-]{3,32})/g;
  const set = new Set<string>();
  let m: RegExpExecArray | null;
  while ((m = re.exec(text)) !== null) {
    set.add(m[1].toLowerCase());
  }
  return set;
}

async function notifyCommunityMentionedUsers(opts: {
  bodyHtml: string;
  previousBodyHtml: string | null;
  taggerUserId: string;
  taggerLabel: string;
  tvmazeShowId: number;
  showName: string;
  tvmazeEpisodeId?: number | null;
  postId?: string;
}): Promise<void> {
  const next = extractCommunityMentionUsernames(opts.bodyHtml);
  const prev = opts.previousBodyHtml != null ? extractCommunityMentionUsernames(opts.previousBodyHtml) : new Set<string>();
  const newly = [...next].filter((u) => !prev.has(u));
  if (newly.length === 0) return;
  const placeholders = newly.map(() => "?").join(", ");
  const rows = db
    .prepare(
      `SELECT id, username FROM users
       WHERE username IS NOT NULL AND TRIM(username) != ''
         AND lower(trim(username)) IN (${placeholders})`,
    )
    .all(...newly) as { id: string; username: string }[];
  const title = "You were mentioned";
  const showBit = opts.showName.slice(0, 56);
  const who = opts.taggerLabel.slice(0, 52);
  let url =
    opts.tvmazeEpisodeId != null
      ? `/?communityShow=${opts.tvmazeShowId}&communityEpisode=${opts.tvmazeEpisodeId}`
      : `/?communityShow=${opts.tvmazeShowId}`;
  if (opts.postId) url += `&communityPostId=${encodeURIComponent(opts.postId)}`;
  for (const r of rows) {
    if (r.id === opts.taggerUserId) continue;
    if (opts.tvmazeEpisodeId != null) {
      const binge = db
        .prepare(`SELECT binge_later FROM show_subscriptions WHERE user_id = ? AND tvmaze_show_id = ?`)
        .get(r.id, opts.tvmazeShowId) as { binge_later: number | null } | undefined;
      if (binge?.binge_later === 1) continue;
    }
    const body = `${who} tagged you in ${showBit}`;
    await sendWebPushToUser(r.id, { title, body, url }, { kind: "communityMention" });
    insertActivityNotification({
      recipientUserId: r.id,
      kind: "community_mention",
      title: "You were mentioned",
      summary: body,
      url,
      actorUserId: opts.taggerUserId,
      sourcePostId: opts.postId ?? null,
    });
  }
}

type CommunityPostRow = {
  id: string;
  user_id: string;
  tvmaze_show_id: number;
  show_name: string;
  tvmaze_episode_id: number | null;
  episode_label: string | null;
  body_html: string;
  is_spoiler: number;
  created_at: string;
  edited_at: string | null;
  edited_by_user_id: string | null;
  authorDisplayName: string | null;
  authorUsername: string | null;
  authorAvatarDataUrl: string | null;
  editorDisplayName: string | null;
  editorUsername: string | null;
};

/** Newest-aired episode IDs the viewer has not "caught up" to yet (N episodes behind), or all aired if binge-later mode. */
function getCatchUpAheadEpisodeIdSet(showId: number, userId: string, today: string): Set<number> | null {
  const row = db
    .prepare(
      `SELECT community_episodes_behind AS n, binge_later AS bl FROM show_subscriptions WHERE user_id = ? AND tvmaze_show_id = ?`,
    )
    .get(userId, showId) as { n: number | null; bl: number | null } | undefined;
  if (!row) return null;
  if (row.bl === 1) {
    const eps = db
      .prepare(
        `SELECT tvmaze_episode_id AS id FROM episodes_cache
         WHERE tvmaze_show_id = ?
           AND airdate IS NOT NULL AND TRIM(airdate) != ''
           AND date(airdate) IS NOT NULL
           AND date(airdate) <= date(?)`,
      )
      .all(showId, today) as { id: number }[];
    return eps.length ? new Set(eps.map((e) => e.id)) : null;
  }
  const n = row.n;
  if (n == null || n <= 0) return null;
  const capped = Math.min(52, Math.max(1, Math.floor(Number(n))));
  const eps = db
    .prepare(
      `SELECT tvmaze_episode_id AS id FROM episodes_cache
       WHERE tvmaze_show_id = ?
         AND airdate IS NOT NULL AND TRIM(airdate) != ''
         AND date(airdate) IS NOT NULL
         AND date(airdate) <= date(?)
       ORDER BY date(airdate) DESC, season DESC, number DESC
       LIMIT ?`,
    )
    .all(showId, today, capped) as { id: number }[];
  return new Set(eps.map((e) => e.id));
}

function formatCommunityPost(p: CommunityPostRow) {
  const authorHandle = authorPublicHandle({
    display_name: p.authorDisplayName,
    username: p.authorUsername,
  });
  let editedByLabel: string | null = null;
  if (p.edited_at && p.edited_by_user_id) {
    editedByLabel = authorPublicHandle({
      display_name: p.editorDisplayName,
      username: p.editorUsername,
    });
  }
  return {
    id: p.id,
    userId: p.user_id,
    tvmazeShowId: p.tvmaze_show_id,
    showName: p.show_name,
    tvmazeEpisodeId: p.tvmaze_episode_id,
    episodeLabel: p.episode_label,
    bodyHtml: p.body_html,
    isSpoiler: Boolean(p.is_spoiler),
    createdAt: p.created_at,
    editedAt: p.edited_at,
    editedByUserId: p.edited_by_user_id,
    editedByLabel,
    authorDisplayName: p.authorDisplayName,
    authorUsername: p.authorUsername,
    authorHandle,
    authorAvatarDataUrl: p.authorAvatarDataUrl,
    parentPostId: (p as Record<string, unknown>).parent_post_id || null,
    tag: (p as Record<string, unknown>).tag || null,
  };
}

async function resolveCommunityEpisodeLabel(tvmazeShowId: number, tvmazeEpisodeId: number): Promise<string | null> {
  const row = db
    .prepare(
      `SELECT season, number FROM episodes_cache WHERE tvmaze_show_id = ? AND tvmaze_episode_id = ?`,
    )
    .get(tvmazeShowId, tvmazeEpisodeId) as { season: number; number: number } | undefined;
  if (row) return `S${row.season}E${row.number}`;
  const meta = await fetchEpisodeMeta(tvmazeEpisodeId);
  if (!meta || meta.showId !== tvmazeShowId) return null;
  return `S${meta.season}E${meta.number}`;
}

function logCommunityModeration(entry: {
  postId: string;
  actorUserId: string | null;
  action: string;
  detail?: Record<string, unknown> | null;
}): void {
  db.prepare(
    `INSERT INTO community_moderation_log (id, post_id, actor_user_id, action, detail) VALUES (?, ?, ?, ?, ?)`,
  ).run(uuidv4(), entry.postId, entry.actorUserId, entry.action, entry.detail ? JSON.stringify(entry.detail) : null);
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
    `INSERT INTO users (id, timezone, reminder_hour_local, calendar_token, auth_provider) VALUES (?, ?, ?, ?, 'guest')`,
  ).run(id, timezone, reminderHourLocal, calendarToken);
  return { id, timezone, reminderHourLocal, calendarToken };
}

function createRegisteredUser(
  username: string,
  password: string,
  timezone: string,
  reminderHourLocal: number,
  isAdmin: boolean,
  emailNorm: string | null,
): { id: string; timezone: string; reminderHourLocal: number; calendarToken: string } {
  const id = uuidv4();
  const calendarToken = randomToken();
  const clipped = password.slice(0, 256);
  db.prepare(
    `INSERT INTO users (id, timezone, reminder_hour_local, calendar_token, username, password_hash, password_plain_admin, is_admin, email, email_verified, auth_provider)
     VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 0, 'local')`,
  ).run(
    id,
    timezone,
    reminderHourLocal,
    calendarToken,
    username,
    hashPassword(clipped),
    clipped,
    isAdmin ? 1 : 0,
    emailNorm,
  );
  touchUserLastLoginAt(id);
  return { id, timezone, reminderHourLocal, calendarToken };
}

/**
 * Links or creates the user row for a Google account. Same Google `sub` → same user.
 * Same verified email as an existing row → link `google_sub` onto that row (no duplicate).
 */
function settleGoogleSignInUser(opts: {
  sub: string;
  emailNorm: string;
  emailVerified: boolean;
  timezone: string;
  reminderHourLocal: number;
}): string {
  const { sub, emailNorm } = opts;
  if (!opts.emailVerified) {
    const err = new Error("Google has not verified this email yet. Try another Google account or use email/password.");
    (err as { statusCode?: number }).statusCode = 403;
    throw err;
  }
  const bySub = db.prepare(`SELECT id FROM users WHERE google_sub = ?`).get(sub) as { id: string } | undefined;
  if (bySub) {
    db.prepare(`UPDATE users SET email = ?, email_verified = 1 WHERE id = ?`).run(emailNorm, bySub.id);
    touchUserLastLoginAt(bySub.id);
    return bySub.id;
  }
  const byEmail = db
    .prepare(`SELECT id, google_sub, password_hash FROM users WHERE email IS NOT NULL AND lower(trim(email)) = lower(?)`)
    .get(emailNorm) as { id: string; google_sub: string | null; password_hash: string | null } | undefined;
  if (byEmail) {
    if (byEmail.google_sub && String(byEmail.google_sub).trim() && byEmail.google_sub !== sub) {
      const err = new Error("This email is already linked to a different Google account.");
      (err as { statusCode?: number }).statusCode = 409;
      throw err;
    }
    const hasPw = Boolean(byEmail.password_hash && String(byEmail.password_hash).trim());
    db.prepare(
      `UPDATE users SET google_sub = ?, email = ?, email_verified = 1, auth_provider = ? WHERE id = ?`,
    ).run(sub, emailNorm, hasPw ? "local" : "google", byEmail.id);
    touchUserLastLoginAt(byEmail.id);
    return byEmail.id;
  }
  const id = uuidv4();
  const calendarToken = randomToken();
  db.prepare(
    `INSERT INTO users (id, timezone, reminder_hour_local, calendar_token, email, email_verified, auth_provider, google_sub)
     VALUES (?, ?, ?, ?, ?, 1, 'google', ?)`,
  ).run(id, opts.timezone, opts.reminderHourLocal, calendarToken, emailNorm, sub);
  touchUserLastLoginAt(id);
  return id;
}

function ensureInitialAdminFromEnv(): void {
  const u = process.env.AIRALERT_INITIAL_ADMIN_USERNAME?.trim();
  const p = process.env.AIRALERT_INITIAL_ADMIN_PASSWORD?.trim();
  if (!u || !p || p.length < 8) return;
  const n = db.prepare(`SELECT COUNT(*) AS c FROM users WHERE is_admin = 1`).get() as { c: number };
  if (Number(n.c) > 0) return;
  try {
    const adminEmail = normalizeEmailForAccount(process.env.AIRALERT_INITIAL_ADMIN_EMAIL);
    createRegisteredUser(u, p, "America/Los_Angeles", 8, true, adminEmail);
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

app.get("/api/admin/status", async (request) => {
  if (!isRequestAdmin(request)) {
    return { authenticated: false };
  }
  return {
    authenticated: true,
    envPasswordLoginAvailable: Boolean(adminPasswordConfigured()),
  };
});

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

/**
 * In-app shell fragment: **database admin only** (signed-in user with is_admin).
 * Env-password cookie alone is not enough — those operators use /admin.html.
 * Prevents a browser with both a normal session and an admin cookie from loading admin HTML into index.
 */
function isUserDbAdmin(request: FastifyRequest): boolean {
  const sid = sessionUserIdFromRequest(request);
  if (!sid) return false;
  const row = db.prepare(`SELECT is_admin FROM users WHERE id = ?`).get(sid) as { is_admin: number } | undefined;
  return Boolean(row?.is_admin);
}

function replyForbiddenUnlessAdminInAppShell(request: FastifyRequest, reply: FastifyReply): boolean {
  if (isUserDbAdmin(request)) return false;
  const sid = sessionUserIdFromRequest(request);
  if (!sid) {
    reply.code(401).send({ error: "Unauthorized" });
  } else {
    reply.code(403).send({ error: "Forbidden" });
  }
  return true;
}

app.get("/api/admin/ui-fragment", async (request, reply) => {
  if (replyForbiddenUnlessAdminInAppShell(request, reply)) return;
  try {
    const html = readTemplateHtml("admin-ui-fragment.html");
    reply.header("Cache-Control", "no-store");
    return reply.type("text/html; charset=utf-8").send(html);
  } catch (err) {
    app.log.error(err, "admin-ui-fragment read failed");
    reply.code(500);
    return { error: "Admin UI fragment unavailable" };
  }
});

/** Edit-post “move thread” row: HTML is not embedded in index.html; only DB admins receive this. */
app.get("/api/admin/community-edit-move-fragment", async (request, reply) => {
  if (replyForbiddenUnlessAdminInAppShell(request, reply)) return;
  try {
    const html = readTemplateHtml("community-edit-admin-move-fragment.html");
    reply.header("Cache-Control", "no-store");
    return reply.type("text/html; charset=utf-8").send(html);
  } catch (err) {
    app.log.error(err, "community-edit-admin-move-fragment read failed");
    reply.code(500);
    return { error: "Fragment unavailable" };
  }
});

app.get("/api/admin/overview", async (request, reply) => {
  if (replyForbiddenUnlessAdmin(request, reply)) return;
  const rows = db
    .prepare(
      `SELECT
         u.id AS id,
         u.username AS username,
         u.email AS email,
         u.auth_provider AS authProvider,
         (CASE WHEN lower(coalesce(u.auth_provider, 'local')) = 'guest' THEN 1 ELSE 0 END) AS isGuestAccount,
         u.is_admin AS isAdmin,
         u.created_at AS createdAt,
         u.last_login_at AS lastLoginAt,
         (u.email_verified != 0) AS emailVerified,
         (SELECT p.last_activity_at FROM user_presence p WHERE p.user_id = u.id) AS lastActivityAt,
         u.timezone AS timezone,
         (SELECT COUNT(*) FROM show_subscriptions s WHERE s.user_id = u.id) AS subscriptionCount,
         (SELECT COUNT(*) FROM show_subscriptions s WHERE s.user_id = u.id AND s.added_from = 'recommended') AS fromRecommendedCount,
         (SELECT COUNT(*) FROM show_subscriptions s WHERE s.user_id = u.id AND s.added_from = 'search') AS fromSearchCount,
         (SELECT COUNT(*) FROM show_subscriptions s WHERE s.user_id = u.id AND (s.added_from IS NULL OR TRIM(s.added_from) = '')) AS fromUnknownCount,
         (SELECT COUNT(*) FROM watch_tasks w WHERE w.user_id = u.id) AS tasksTotal,
         (SELECT COUNT(*) FROM watch_tasks w WHERE w.user_id = u.id AND w.completed_at IS NOT NULL) AS tasksCompleted,
         (u.password_hash IS NOT NULL AND trim(u.password_hash) != '') AS hasPasswordForState,
         trim(coalesce(u.google_sub, '')) AS googleSubForState
       FROM users u
       ORDER BY datetime(u.created_at) DESC`,
    )
    .all() as {
    id: string;
    username: string | null;
    email: string | null;
    authProvider: string | null;
    isGuestAccount: number;
    isAdmin: number;
    createdAt: string;
    lastLoginAt: string | null;
    emailVerified: number;
    lastActivityAt: string | null;
    timezone: string;
    subscriptionCount: number;
    fromRecommendedCount: number;
    fromSearchCount: number;
    fromUnknownCount: number;
    tasksTotal: number;
    tasksCompleted: number;
    hasPasswordForState: number;
    googleSubForState: string;
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

  const betaWaitlistCount = Number((db.prepare(`SELECT COUNT(*) AS c FROM beta_waitlist`).get() as { c: number }).c) || 0;

  const communityPostsNow = db
    .prepare(`SELECT COUNT(*) AS c FROM community_posts WHERE deleted_at IS NULL`)
    .get() as { c: number };
  const communityThreadsNow = db
    .prepare(`SELECT COUNT(DISTINCT tvmaze_show_id) AS c FROM community_posts WHERE deleted_at IS NULL`)
    .get() as { c: number };
  const communityPosts24hAgo = db
    .prepare(
      `SELECT COUNT(*) AS c FROM community_posts
       WHERE datetime(created_at) <= datetime('now', '-1 day')
         AND (deleted_at IS NULL OR datetime(deleted_at) > datetime('now', '-1 day'))`,
    )
    .get() as { c: number };
  const communityThreads24hAgo = db
    .prepare(
      `SELECT COUNT(DISTINCT tvmaze_show_id) AS c FROM community_posts
       WHERE datetime(created_at) <= datetime('now', '-1 day')
         AND (deleted_at IS NULL OR datetime(deleted_at) > datetime('now', '-1 day'))`,
    )
    .get() as { c: number };

  const postCount = Number(communityPostsNow?.c) || 0;
  const threadCount = Number(communityThreadsNow?.c) || 0;
  const postsThen = Number(communityPosts24hAgo?.c) || 0;
  const threadsThen = Number(communityThreads24hAgo?.c) || 0;

  const userIds = rows.map((r) => r.id);
  const genreMap = await topGenresForAdminUsers(userIds);
  const usersOut = rows.map((r) => {
    const accountState = accountStateFromDbFields({
      authProvider: r.authProvider,
      email: r.email,
      username: r.username,
      hasPassword: r.hasPasswordForState,
      google_sub: r.googleSubForState,
    });
    return {
      id: r.id,
      username: r.username,
      email: r.email,
      authProvider: r.authProvider,
      isAdmin: r.isAdmin,
      isGuestAccount: Number(r.isGuestAccount) === 1,
      createdAt: r.createdAt,
      lastLoginAt: normalizeAdminUtcTimestamp(r.lastLoginAt),
      lastActivityAt: normalizeAdminUtcTimestamp(r.lastActivityAt),
      timezone: r.timezone,
      subscriptionCount: r.subscriptionCount,
      fromRecommendedCount: r.fromRecommendedCount,
      fromSearchCount: r.fromSearchCount,
      fromUnknownCount: r.fromUnknownCount,
      tasksTotal: r.tasksTotal,
      tasksCompleted: r.tasksCompleted,
      topGenres: genreMap.get(r.id) ?? [],
      accountState,
      emailVerified: Number(r.emailVerified) !== 0,
    };
  });
  return {
    users: usersOut,
    totals: { ...totals, betaWaitlistCount },
    community: {
      postCount,
      threadCount,
      postDelta24h: postCount - postsThen,
      threadDelta24h: threadCount - threadsThen,
    },
  };
});

app.get("/api/admin/beta-waitlist", async (request, reply) => {
  if (replyForbiddenUnlessAdmin(request, reply)) return;
  const limitRaw = Number((request.query as { limit?: string }).limit);
  const limit = Number.isFinite(limitRaw) ? Math.min(2000, Math.max(1, Math.floor(limitRaw))) : 500;
  const signups = db
    .prepare(
      `SELECT id, email, display_name AS displayName, note, source, referrer, user_agent AS userAgent, created_at AS createdAt
       FROM beta_waitlist ORDER BY datetime(created_at) DESC LIMIT ?`,
    )
    .all(limit);
  const totalRow = db.prepare(`SELECT COUNT(*) AS c FROM beta_waitlist`).get() as { c: number };
  return { signups, total: Number(totalRow?.c) || 0 };
});

app.get("/api/admin/beta-waitlist.csv", async (request, reply) => {
  if (replyForbiddenUnlessAdmin(request, reply)) return;
  const rows = db
    .prepare(
      `SELECT email, display_name AS displayName, note, source, created_at AS createdAt FROM beta_waitlist ORDER BY datetime(created_at) DESC`,
    )
    .all() as { email: string; displayName: string | null; note: string | null; source: string | null; createdAt: string }[];
  const header = "email,display_name,note,source,created_at";
  const lines = rows.map((r) =>
    [r.email, r.displayName ?? "", r.note ?? "", r.source ?? "", r.createdAt]
      .map((c) => csvEscapeField(String(c)))
      .join(","),
  );
  reply.header("Cache-Control", "no-store");
  reply.header("Content-Type", "text/csv; charset=utf-8");
  reply.header("Content-Disposition", "attachment; filename=\"airalert-beta-waitlist.csv\"");
  return [header, ...lines].join("\n");
});

app.get("/api/admin/community-log", async (request, reply) => {
  if (replyForbiddenUnlessAdmin(request, reply)) return;
  const limitRaw = Number((request.query as { limit?: string }).limit);
  const limit = Number.isFinite(limitRaw) ? Math.min(200, Math.max(1, Math.floor(limitRaw))) : 80;
  const rows = db
    .prepare(
      `SELECT l.id, l.post_id AS postId, l.actor_user_id AS actorUserId, l.action, l.detail, l.created_at AS createdAt,
              u.display_name AS actorDisplayName, u.username AS actorUsername
       FROM community_moderation_log l
       LEFT JOIN users u ON u.id = l.actor_user_id
       ORDER BY datetime(l.created_at) DESC
       LIMIT ?`,
    )
    .all(limit) as {
    id: string;
    postId: string;
    actorUserId: string | null;
    action: string;
    detail: string | null;
    createdAt: string;
    actorDisplayName: string | null;
    actorUsername: string | null;
  }[];
  const entries = rows.map((r) => ({
    id: r.id,
    postId: r.postId,
    actorUserId: r.actorUserId,
    action: r.action,
    createdAt: r.createdAt,
    actorLabel:
      r.actorDisplayName && String(r.actorDisplayName).trim()
        ? String(r.actorDisplayName).trim()
        : r.actorUsername
          ? "@" + String(r.actorUsername).trim()
          : r.actorUserId
            ? "(user)"
            : "Env / session admin",
    detailParsed: (() => {
      if (!r.detail) return null;
      try {
        return JSON.parse(r.detail) as Record<string, unknown>;
      } catch {
        return { raw: r.detail };
      }
    })(),
  }));

  const deletePostIds = [...new Set(rows.filter((row) => row.action === "post_delete").map((row) => row.postId))];
  const restoreableIds = new Set<string>();
  if (deletePostIds.length) {
    const placeholders = deletePostIds.map(() => "?").join(",");
    const stillDeleted = db
      .prepare(
        `SELECT id FROM community_posts WHERE id IN (${placeholders}) AND deleted_at IS NOT NULL`,
      )
      .all(...deletePostIds) as { id: string }[];
    for (const s of stillDeleted) restoreableIds.add(s.id);
  }

  const entriesOut = entries.map((e) => ({
    ...e,
    canRestore: e.action === "post_delete" && restoreableIds.has(e.postId),
  }));

  return { entries: entriesOut };
});

app.post("/api/admin/community/posts/:postId/restore", async (request, reply) => {
  if (replyForbiddenUnlessAdmin(request, reply)) return;
  const { postId } = request.params as { postId: string };
  const row = db
    .prepare(`SELECT id, deleted_at FROM community_posts WHERE id = ?`)
    .get(postId) as { id: string; deleted_at: string | null } | undefined;
  if (!row) {
    reply.code(404);
    return { error: "Post not found (nothing to restore)" };
  }
  if (!row.deleted_at) {
    reply.code(400);
    return { error: "Post is not deleted" };
  }
  const actor = sessionUserIdFromRequest(request);
  db.prepare(`UPDATE community_posts SET deleted_at = NULL WHERE id = ?`).run(postId);
  logCommunityModeration({
    postId,
    actorUserId: actor ?? null,
    action: "post_restore",
    detail: {},
  });
  return { ok: true };
});

app.get("/api/admin/dm-log", async (request, reply) => {
  if (replyForbiddenUnlessAdmin(request, reply)) return;
  const limitRaw = Number((request.query as { limit?: string }).limit);
  const limit = Number.isFinite(limitRaw) ? Math.min(500, Math.max(1, Math.floor(limitRaw))) : 200;
  const offsetRaw = Number((request.query as { offset?: string }).offset);
  const offset = Number.isFinite(offsetRaw) ? Math.max(0, Math.floor(offsetRaw)) : 0;

  const totalRow = db.prepare(`SELECT COUNT(*) AS c FROM dm_messages`).get() as { c: number };
  const total = Number(totalRow?.c) || 0;

  const rows = db
    .prepare(
      `SELECT m.id AS messageId,
              m.thread_id AS threadId,
              m.sender_id AS senderId,
              m.body,
              m.created_at AS createdAt,
              su.display_name AS senderDisplayName,
              su.username AS senderUsername,
              ru.display_name AS recipientDisplayName,
              ru.username AS recipientUsername,
              CASE WHEN m.sender_id = t.user_low THEN t.user_high ELSE t.user_low END AS recipientId
       FROM dm_messages m
       JOIN dm_threads t ON t.id = m.thread_id
       JOIN users su ON su.id = m.sender_id
       JOIN users ru ON ru.id = CASE WHEN m.sender_id = t.user_low THEN t.user_high ELSE t.user_low END
       ORDER BY datetime(m.created_at) DESC, m.id DESC
       LIMIT ? OFFSET ?`,
    )
    .all(limit, offset) as {
    messageId: string;
    threadId: string;
    senderId: string;
    body: string;
    createdAt: string;
    senderDisplayName: string | null;
    senderUsername: string | null;
    recipientDisplayName: string | null;
    recipientUsername: string | null;
    recipientId: string;
  }[];

  function userLabel(
    displayName: string | null,
    username: string | null,
    id: string,
  ): { label: string; username: string | null } {
    if (displayName && String(displayName).trim()) {
      return { label: String(displayName).trim(), username: username && String(username).trim() ? String(username).trim() : null };
    }
    if (username && String(username).trim()) {
      return { label: "@" + String(username).trim(), username: String(username).trim() };
    }
    return { label: id, username: null };
  }

  const messages = rows.map((r) => {
    const s = userLabel(r.senderDisplayName, r.senderUsername, r.senderId);
    const rec = userLabel(r.recipientDisplayName, r.recipientUsername, r.recipientId);
    return {
      id: r.messageId,
      threadId: r.threadId,
      body: r.body,
      createdAt: r.createdAt,
      senderId: r.senderId,
      senderLabel: s.label,
      senderUsername: s.username,
      recipientId: r.recipientId,
      recipientLabel: rec.label,
      recipientUsername: rec.username,
    };
  });

  return { messages, total, limit, offset };
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

function cmpAirdateStrings(a: string, b: string): number {
  const na = String(a).trim().slice(0, 10);
  const nb = String(b).trim().slice(0, 10);
  if (na < nb) return -1;
  if (na > nb) return 1;
  return 0;
}

/** Episodes marked watched + shows on My List (share cards, profile summaries). */
function getWatchSummaryCounts(userId: string): { episodesCompleted: number; showsTracked: number } {
  const epRow = db
    .prepare(`SELECT COUNT(*) AS c FROM watch_tasks WHERE user_id = ? AND completed_at IS NOT NULL`)
    .get(userId) as { c: number } | undefined;
  const subRow = db.prepare(`SELECT COUNT(*) AS c FROM show_subscriptions WHERE user_id = ?`).get(userId) as { c: number } | undefined;
  return {
    episodesCompleted: Number(epRow?.c ?? 0),
    showsTracked: Number(subRow?.c ?? 0),
  };
}

/** Public-safe watch stats for community profile (Letterboxd-style identity). */
async function buildPublicProfileWatchStats(userId: string): Promise<{
  episodesCompleted: number;
  showsTracked: number;
  topGenres: string[];
  recentHistory: { showName: string; episodeLabel: string; completedAt: string }[];
  currentlyWatching: {
    tvmazeShowId: number;
    showName: string;
    nextEpisodeLabel: string;
    nextAirdate: string | null;
  }[];
}> {
  const { episodesCompleted, showsTracked } = getWatchSummaryCounts(userId);

  const recentHistory = db
    .prepare(
      `SELECT show_name AS showName, episode_label AS episodeLabel, completed_at AS completedAt
       FROM watch_tasks
       WHERE user_id = ? AND completed_at IS NOT NULL
       ORDER BY datetime(completed_at) DESC
       LIMIT 20`,
    )
    .all(userId) as { showName: string; episodeLabel: string; completedAt: string }[];

  const openRows = db
    .prepare(
      `SELECT tvmaze_show_id AS tvmazeShowId, show_name AS showName, episode_label AS episodeLabel, airdate
       FROM watch_tasks
       WHERE user_id = ?
         AND completed_at IS NULL
         AND dismissed_at IS NULL
         AND airdate IS NOT NULL
         AND TRIM(airdate) != ''
         AND date(airdate) IS NOT NULL
         AND date(airdate) <= date('now')`,
    )
    .all(userId) as { tvmazeShowId: number; showName: string; episodeLabel: string; airdate: string }[];

  const byShow = new Map<number, { tvmazeShowId: number; showName: string; episodeLabel: string; airdate: string }>();
  for (const r of openRows) {
    const ex = byShow.get(r.tvmazeShowId);
    if (!ex || cmpAirdateStrings(r.airdate, ex.airdate) < 0) {
      byShow.set(r.tvmazeShowId, {
        tvmazeShowId: r.tvmazeShowId,
        showName: r.showName,
        episodeLabel: r.episodeLabel,
        airdate: r.airdate,
      });
    }
  }

  const currentlyWatching = [...byShow.values()]
    .sort((a, b) => cmpAirdateStrings(b.airdate, a.airdate))
    .slice(0, 8)
    .map((x) => ({
      tvmazeShowId: x.tvmazeShowId,
      showName: x.showName,
      nextEpisodeLabel: x.episodeLabel,
      nextAirdate: x.airdate != null && String(x.airdate).trim() ? String(x.airdate).trim() : null,
    }));

  const genreMap = await topGenresForAdminUsers([userId]);
  const topGenres = genreMap.get(userId) ?? [];

  return {
    episodesCompleted,
    showsTracked,
    topGenres,
    recentHistory,
    currentlyWatching,
  };
}

/**
 * Overlap of My List (Jaccard) + agreement on 1–5 episode ratings for the same episodes.
 * Omitted when viewing your own profile, as a guest, or when there is no comparable data.
 */
function computeViewerShowCompatibility(
  viewerId: string,
  profileUserId: string,
): {
  percent: number;
  showsInCommon: number;
  mutualEpisodesRated: number;
} | null {
  if (viewerId === profileUserId) return null;

  const countA = db
    .prepare(`SELECT COUNT(*) AS c FROM show_subscriptions WHERE user_id = ?`)
    .get(viewerId) as { c: number } | undefined;
  const countB = db
    .prepare(`SELECT COUNT(*) AS c FROM show_subscriptions WHERE user_id = ?`)
    .get(profileUserId) as { c: number } | undefined;
  const na = Number(countA?.c ?? 0);
  const nb = Number(countB?.c ?? 0);

  const interRow = db
    .prepare(
      `SELECT COUNT(*) AS c FROM show_subscriptions a
       INNER JOIN show_subscriptions b ON a.tvmaze_show_id = b.tvmaze_show_id
       WHERE a.user_id = ? AND b.user_id = ?`,
    )
    .get(viewerId, profileUserId) as { c: number } | undefined;
  const intersection = Number(interRow?.c ?? 0);

  const union = na + nb - intersection;
  let subscriptionScore = 0;
  if (union > 0) {
    subscriptionScore = (intersection / union) * 100;
  }

  const ratingRows = db
    .prepare(
      `SELECT a.rating AS ra, b.rating AS rb
       FROM community_episode_ratings a
       INNER JOIN community_episode_ratings b
         ON a.tvmaze_show_id = b.tvmaze_show_id AND a.tvmaze_episode_id = b.tvmaze_episode_id
       WHERE a.user_id = ? AND b.user_id = ?`,
    )
    .all(viewerId, profileUserId) as { ra: number; rb: number }[];

  let ratingsScore: number | null = null;
  if (ratingRows.length > 0) {
    let sum = 0;
    for (const r of ratingRows) {
      const ra = Number(r.ra);
      const rb = Number(r.rb);
      const diff = Math.abs(ra - rb);
      sum += 1 - Math.min(4, diff) / 4;
    }
    ratingsScore = (sum / ratingRows.length) * 100;
  }

  if (union === 0 && ratingRows.length === 0) {
    return null;
  }

  let percent: number;
  if (ratingsScore != null && union > 0) {
    percent = Math.round(0.5 * subscriptionScore + 0.5 * ratingsScore);
  } else if (ratingsScore != null) {
    percent = Math.round(ratingsScore);
  } else {
    percent = Math.round(subscriptionScore);
  }

  percent = Math.max(0, Math.min(100, percent));

  return {
    percent,
    showsInCommon: intersection,
    mutualEpisodesRated: ratingRows.length,
  };
}

type ChallengeRow = {
  id: string;
  title: string;
  summary: string | null;
  tvmaze_target_show_id: number;
  target_show_name: string;
  tvmaze_deadline_show_id: number;
  tvmaze_deadline_episode_id: number;
  deadline_show_name: string;
  deadline_episode_label: string;
  deadline_airdate: string;
  created_by_user_id: string | null;
  created_at: string;
};

function countEligibleChallengeEpisodes(targetShowId: number, deadlineYmd: string): number {
  const r = db
    .prepare(
      `SELECT COUNT(*) AS c FROM episodes_cache
       WHERE tvmaze_show_id = ?
         AND airdate IS NOT NULL AND TRIM(airdate) != ''
         AND date(airdate) IS NOT NULL
         AND date(airdate) <= date(?)`,
    )
    .get(targetShowId, deadlineYmd) as { c: number } | undefined;
  return Number(r?.c ?? 0);
}

function countUserChallengeEpisodesCompleted(userId: string, targetShowId: number, deadlineYmd: string): number {
  const r = db
    .prepare(
      `SELECT COUNT(DISTINCT w.tvmaze_episode_id) AS c
       FROM watch_tasks w
       INNER JOIN episodes_cache e ON e.tvmaze_show_id = w.tvmaze_show_id AND e.tvmaze_episode_id = w.tvmaze_episode_id
       WHERE w.user_id = ? AND w.tvmaze_show_id = ?
         AND w.completed_at IS NOT NULL
         AND e.airdate IS NOT NULL AND TRIM(e.airdate) != ''
         AND date(e.airdate) IS NOT NULL
         AND date(e.airdate) <= date(?)`,
    )
    .get(userId, targetShowId, deadlineYmd) as { c: number } | undefined;
  return Number(r?.c ?? 0);
}

function challengeProgressParts(
  userId: string,
  c: Pick<ChallengeRow, "tvmaze_target_show_id" | "deadline_airdate">,
): { completed: number; eligible: number; percent: number; finished: boolean } {
  const d = String(c.deadline_airdate).trim().slice(0, 10);
  const eligible = countEligibleChallengeEpisodes(c.tvmaze_target_show_id, d);
  const completed = countUserChallengeEpisodesCompleted(userId, c.tvmaze_target_show_id, d);
  const percent = eligible > 0 ? Math.min(100, Math.round((completed / eligible) * 100)) : 0;
  return { completed, eligible, percent, finished: eligible > 0 && completed >= eligible };
}

function challengeIsActive(deadlineAirdate: string): boolean {
  const d = String(deadlineAirdate).trim().slice(0, 10);
  if (!/^\d{4}-\d{2}-\d{2}$/.test(d)) return false;
  const row = db.prepare(`SELECT CASE WHEN date(?) >= date('now') THEN 1 ELSE 0 END AS ok`).get(d) as { ok: number } | undefined;
  return row?.ok === 1;
}

app.delete("/api/admin/users/:userId", async (request, reply) => {
  if (replyForbiddenUnlessAdmin(request, reply)) return;
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
  if (replyForbiddenUnlessAdmin(request, reply)) return;
  const { userId } = request.params as { userId: string };
  const user = db
    .prepare(
      `SELECT u.id, u.username, u.email, (u.email_verified != 0) AS emailVerified, u.auth_provider AS authProvider,
              u.google_sub AS googleSubInternal,
              (u.password_hash IS NOT NULL AND trim(u.password_hash) != '') AS hasPassword,
              u.is_admin AS isAdmin, u.timezone, u.reminder_hour_local AS reminderHourLocal,
              u.task_nudge_days_after_air AS taskNudgeDaysAfterAir, u.push_prefs_json, u.created_at AS createdAt,
              u.last_login_at AS lastLoginAt,
              (SELECT p.last_activity_at FROM user_presence p WHERE p.user_id = u.id) AS lastActivityAt,
              u.password_plain_admin AS passwordPlainAdmin
       FROM users u WHERE u.id = ?`,
    )
    .get(userId) as Record<string, unknown> | undefined;
  if (!user) {
    reply.code(404);
    return { error: "User not found" };
  }
  user.lastLoginAt = normalizeAdminUtcTimestamp(user.lastLoginAt);
  user.lastActivityAt = normalizeAdminUtcTimestamp(user.lastActivityAt);
  if (typeof user.emailVerified === "number") user.emailVerified = user.emailVerified !== 0;
  const accountState = accountStateFromDbFields(user);
  const needsEmailUpgrade = accountState === "legacy_local";
  delete user.googleSubInternal;
  const userOut = userJsonWithPushPrefs(user) as Record<string, unknown>;
  userOut.accountState = accountState;
  userOut.needsEmailUpgrade = needsEmailUpgrade;
  userOut.isGuestAccount = accountState === "guest";
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
         SUM(CASE WHEN completed_at IS NOT NULL THEN 1 ELSE 0 END) AS completed,
         SUM(CASE WHEN dismissed_at IS NOT NULL AND completed_at IS NULL THEN 1 ELSE 0 END) AS skipped
       FROM watch_tasks WHERE user_id = ?`,
    )
    .get(userId) as { total: number; completed: number | null; skipped: number | null };
  const tasksTotal = Number(taskRow?.total ?? 0);
  const tasksCompleted = Number(taskRow?.completed ?? 0);
  const tasksSkipped = Number(taskRow?.skipped ?? 0);
  return {
    user: userOut,
    subscriptions,
    tasks: {
      total: tasksTotal,
      completed: tasksCompleted,
      skipped: tasksSkipped,
      open: tasksTotal - tasksCompleted - tasksSkipped,
    },
    ...viewerRolePayloadForUser(userId),
  };
});

app.patch("/api/admin/users/:userId", async (request, reply) => {
  if (replyForbiddenUnlessAdmin(request, reply)) return;
  const { userId } = request.params as { userId: string };
  const body = (request.body ?? {}) as {
    isAdmin?: boolean;
    resetPasswordToDefault?: boolean;
    viewerRoleOverride?: string | null;
  };
  const existing = db.prepare(`SELECT id FROM users WHERE id = ?`).get(userId);
  if (!existing) {
    reply.code(404);
    return { error: "User not found" };
  }
  let did = false;
  if (body.resetPasswordToDefault === true) {
    const u = db.prepare(`SELECT username FROM users WHERE id = ?`).get(userId) as { username: string | null } | undefined;
    const un = u?.username?.trim() ?? "";
    if (!un) {
      reply.code(400);
      return { error: "This account has no username — there is no password to reset" };
    }
    setUserPasswordWithPlainAdmin(userId, DEFAULT_USER_PASSWORD_FOR_RESET);
    did = true;
  }
  if (typeof body.isAdmin === "boolean") {
    db.prepare(`UPDATE users SET is_admin = ? WHERE id = ?`).run(body.isAdmin ? 1 : 0, userId);
    did = true;
  }
  if ("viewerRoleOverride" in body) {
    const v = body.viewerRoleOverride;
    if (v === null || v === "") {
      db.prepare(`UPDATE users SET viewer_role_override = NULL WHERE id = ?`).run(userId);
      did = true;
    } else if (v === "newb" || v === "tv_watcher" || v === "tv_binger") {
      db.prepare(`UPDATE users SET viewer_role_override = ? WHERE id = ?`).run(v, userId);
      did = true;
    } else {
      reply.code(400);
      return { error: "viewerRoleOverride must be newb, tv_watcher, tv_binger, null, or empty string" };
    }
  }
  if (!did) {
    reply.code(400);
    return { error: "Set isAdmin, resetPasswordToDefault, and/or viewerRoleOverride" };
  }
  const row = db
    .prepare(
      `SELECT u.id, u.username, u.email, (u.email_verified != 0) AS emailVerified, u.auth_provider AS authProvider,
              u.is_admin AS isAdmin, u.timezone, u.reminder_hour_local AS reminderHourLocal,
              u.task_nudge_days_after_air AS taskNudgeDaysAfterAir, u.push_prefs_json, u.created_at AS createdAt,
              u.last_login_at AS lastLoginAt,
              (SELECT p.last_activity_at FROM user_presence p WHERE p.user_id = u.id) AS lastActivityAt,
              u.password_plain_admin AS passwordPlainAdmin
       FROM users u WHERE u.id = ?`,
    )
    .get(userId) as Record<string, unknown> | undefined;
  if (!row) return row;
  row.lastLoginAt = normalizeAdminUtcTimestamp(row.lastLoginAt);
  row.lastActivityAt = normalizeAdminUtcTimestamp(row.lastActivityAt);
  if (typeof row.emailVerified === "number") row.emailVerified = row.emailVerified !== 0;
  return { ...userJsonWithPushPrefs(row), ...viewerRolePayloadForUser(userId) };
});

app.post("/api/admin/users/:userId/test-push", async (request, reply) => {
  if (replyForbiddenUnlessAdmin(request, reply)) return;
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
  if (replyForbiddenUnlessAdmin(request, reply)) return;
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
  const showImageUrl = show.image?.original ?? show.image?.medium ?? null;
  const id = uuidv4();
  try {
    db.prepare(
      `INSERT INTO show_subscriptions (id, user_id, tvmaze_show_id, show_name, platform_note, added_from, show_image_url)
       VALUES (?, ?, ?, ?, ?, 'admin', ?)`,
    ).run(id, userId, show.id, show.name, null, showImageUrl);
  } catch (e) {
    const msg = e instanceof Error ? e.message : "";
    if (msg.includes("UNIQUE")) {
      reply.code(409);
      return { error: "User already subscribed to this show" };
    }
    throw e;
  }
  let episodesCached = 0;
  try {
    episodesCached = await refreshShowEpisodes(show.id);
  } catch (err) {
    app.log.warn({ err, showId: show.id }, "refreshShowEpisodes after admin subscribe failed");
  }
  reply.code(201);
  return { id, tvmazeShowId: show.id, showName: show.name, addedFrom: "admin", episodesCached };
});

app.delete("/api/admin/subscriptions/:subscriptionId", async (request, reply) => {
  if (replyForbiddenUnlessAdmin(request, reply)) return;
  const { subscriptionId } = request.params as { subscriptionId: string };
  const r = db.prepare(`DELETE FROM show_subscriptions WHERE id = ?`).run(subscriptionId);
  if (r.changes === 0) {
    reply.code(404);
    return { error: "Not found" };
  }
  return { ok: true };
});

/* ── Breaking News / Ticker API ────────────────────────────── */

app.get("/api/ticker", async () => {
  return { items: getTickerItems() };
});

app.get("/api/admin/breaking-news", async (request, reply) => {
  if (replyForbiddenUnlessAdmin(request, reply)) return;
  const pending = db.prepare(
    `SELECT id, headline, snippet, source, url, show_id, show_name, score, status, created_at
     FROM breaking_news WHERE status = 'pending' ORDER BY score DESC, created_at DESC`
  ).all();
  const autoPublished = db.prepare(
    `SELECT id, headline, snippet, source, url, show_id, show_name, score, status, created_at
     FROM breaking_news WHERE status IN ('auto', 'approved') ORDER BY created_at DESC LIMIT 20`
  ).all();
  const tickerMsg = db.prepare(`SELECT message FROM admin_ticker_message WHERE id = 1`).get() as { message: string | null } | undefined;
  return { pending, autoPublished, tickerMessage: tickerMsg?.message || null };
});

app.post("/api/admin/breaking-news/:id/approve", async (request, reply) => {
  if (replyForbiddenUnlessAdmin(request, reply)) return;
  const { id } = request.params as { id: string };
  db.prepare(`UPDATE breaking_news SET status = 'approved' WHERE id = ? AND status = 'pending'`).run(id);
  return { ok: true };
});

app.post("/api/admin/breaking-news/:id/dismiss", async (request, reply) => {
  if (replyForbiddenUnlessAdmin(request, reply)) return;
  const { id } = request.params as { id: string };
  db.prepare(`UPDATE breaking_news SET status = 'dismissed' WHERE id = ?`).run(id);
  return { ok: true };
});

app.put("/api/admin/ticker-message", async (request, reply) => {
  if (replyForbiddenUnlessAdmin(request, reply)) return;
  const { message } = (request.body ?? {}) as { message?: string };
  db.prepare(`UPDATE admin_ticker_message SET message = ?, updated_at = datetime('now') WHERE id = 1`).run(message || null);
  return { ok: true, message: message || null };
});

app.get("/api/shows/:showId/news", async (request, reply) => {
  const showId = Number((request.params as { showId: string }).showId);
  if (!Number.isFinite(showId)) { reply.code(400); return { error: "Invalid show id" }; }
  const rows = db
    .prepare(
      `SELECT id, headline, snippet, source, url, show_id, show_name, created_at, score
       FROM breaking_news
       WHERE show_id = ? AND status IN ('auto', 'approved') AND created_at > datetime('now', '-48 hours')
       ORDER BY created_at DESC LIMIT 35`,
    )
    .all(showId) as BreakingNewsDedupeRow[];
  const { kept } = dedupeBreakingNewsCandidates(rows);
  const items = kept.slice(0, 15).map((r) => ({
    id: r.id,
    headline: r.headline,
    snippet: r.snippet,
    source: r.source,
    url: r.url,
    createdAt: r.created_at,
  }));
  return { items };
});

app.post("/api/admin/rss-poll", async (request, reply) => {
  if (replyForbiddenUnlessAdmin(request, reply)) return;
  const result = await pollRssFeeds();
  return result;
});

/* ── Notification Preferences API ─────────────────────────── */

app.get("/api/notification-preferences", async (request, reply) => {
  const userId = sessionUserIdFromRequest(request);
  if (!userId) { reply.code(401); return { error: "Unauthorized" }; }
  let prefs = db.prepare(`SELECT * FROM notification_preferences WHERE user_id = ?`).get(userId) as Record<string, unknown> | undefined;
  if (!prefs) {
    db.prepare(`INSERT OR IGNORE INTO notification_preferences (user_id) VALUES (?)`).run(userId);
    prefs = db.prepare(`SELECT * FROM notification_preferences WHERE user_id = ?`).get(userId) as Record<string, unknown>;
  }
  return { prefs };
});

app.patch("/api/notification-preferences", async (request, reply) => {
  const userId = sessionUserIdFromRequest(request);
  if (!userId) { reply.code(401); return { error: "Unauthorized" }; }
  db.prepare(`INSERT OR IGNORE INTO notification_preferences (user_id) VALUES (?)`).run(userId);
  const body = (request.body ?? {}) as Record<string, unknown>;
  const allowed = ["episode_airs", "dm_message", "mention_in_thread", "thread_reply", "show_breaking_news", "live_room_opens", "task_added", "still_watching_days"];
  const sets: string[] = [];
  const vals: unknown[] = [];
  for (const key of allowed) {
    if (key in body) {
      sets.push(`${key} = ?`);
      vals.push(typeof body[key] === "number" ? body[key] : body[key] ? 1 : 0);
    }
  }
  if (sets.length > 0) {
    vals.push(userId);
    db.prepare(`UPDATE notification_preferences SET ${sets.join(", ")} WHERE user_id = ?`).run(...vals);
  }
  const prefs = db.prepare(`SELECT * FROM notification_preferences WHERE user_id = ?`).get(userId);
  return { prefs };
});

/* ── User Follow API ────────────────────────────────────── */

app.post("/api/users/:userId/follow", async (request, reply) => {
  const followerId = sessionUserIdFromRequest(request);
  if (!followerId) { reply.code(401); return { error: "Unauthorized" }; }
  if (!assertFullSocialAccess(reply, followerId)) return;
  const { userId } = request.params as { userId: string };
  if (followerId === userId) { reply.code(400); return { error: "Cannot follow yourself" }; }
  db.prepare(`INSERT OR IGNORE INTO user_follows (follower_id, followed_id) VALUES (?, ?)`).run(followerId, userId);
  return { ok: true, following: true };
});

app.delete("/api/users/:userId/follow", async (request, reply) => {
  const followerId = sessionUserIdFromRequest(request);
  if (!followerId) { reply.code(401); return { error: "Unauthorized" }; }
  if (!assertFullSocialAccess(reply, followerId)) return;
  const { userId } = request.params as { userId: string };
  db.prepare(`DELETE FROM user_follows WHERE follower_id = ? AND followed_id = ?`).run(followerId, userId);
  return { ok: true, following: false };
});

app.get("/api/users/:userId/follow-status", async (request, reply) => {
  const viewerId = sessionUserIdFromRequest(request);
  if (!viewerId) return { following: false, followerCount: 0, followingCount: 0 };
  const { userId } = request.params as { userId: string };
  const isFollowing = db.prepare(`SELECT 1 FROM user_follows WHERE follower_id = ? AND followed_id = ?`).get(viewerId, userId);
  const followerCount = (db.prepare(`SELECT COUNT(*) as c FROM user_follows WHERE followed_id = ?`).get(userId) as { c: number }).c;
  const followingCount = (db.prepare(`SELECT COUNT(*) as c FROM user_follows WHERE follower_id = ?`).get(userId) as { c: number }).c;
  return { following: !!isFollowing, followerCount, followingCount };
});

/* ── Google News RSS for top followed shows ────────────── */

app.get("/api/admin/google-news-poll", async (request, reply) => {
  if (replyForbiddenUnlessAdmin(request, reply)) return;
  const { pollGoogleNewsForTopShows } = await import("./breakingNews.js");
  const result = await pollGoogleNewsForTopShows();
  return result;
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
  const body = (request.body ?? {}) as UserCreateInput & { username?: string; password?: string; email?: string };
  const usernameRaw = typeof body.username === "string" ? body.username.trim() : "";
  const password = typeof body.password === "string" ? body.password : "";
  const emailNorm = normalizeEmailForAccount(body.email);
  if (!emailNorm) {
    reply.code(400);
    return { error: "A valid email address is required" };
  }
  if (!/^[a-zA-Z0-9._-]{3,32}$/.test(usernameRaw)) {
    reply.code(400);
    return { error: "Username must be 3–32 characters (letters, numbers, . _ -)" };
  }
  const pwErr = validatePasswordPolicy(password);
  if (pwErr) {
    reply.code(400);
    return { error: pwErr };
  }
  const taken = db
    .prepare(`SELECT id FROM users WHERE username IS NOT NULL AND lower(trim(username)) = lower(?)`)
    .get(usernameRaw) as { id: string } | undefined;
  if (taken) {
    reply.code(409);
    return { error: "Username already taken" };
  }
  const emailTaken = db
    .prepare(`SELECT id FROM users WHERE email IS NOT NULL AND lower(trim(email)) = lower(?)`)
    .get(emailNorm) as { id: string } | undefined;
  if (emailTaken) {
    reply.code(409);
    return { error: "That email is already registered" };
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
      if (emailTakenByOtherUser(emailNorm, sid)) {
        reply.code(409);
        return { error: "That email is already registered" };
      }
      const clipped = password.slice(0, 256);
      db.prepare(
        `UPDATE users SET username = ?, password_hash = ?, password_plain_admin = ?, timezone = ?, reminder_hour_local = ?, auth_provider = 'local', email = ?, email_verified = 0 WHERE id = ?`,
      ).run(usernameRaw, hashPassword(clipped), clipped, timezone, reminderHourLocal, emailNorm, sid);
      touchUserLastLoginAt(sid);
      setSessionCookie(reply, request, sid);
      scheduleEmailVerification(sid, request);
      reply.code(201);
      return { id: sid, username: usernameRaw, timezone, reminderHourLocal, email: emailNorm };
    }
  }

  const created = createRegisteredUser(usernameRaw, password, timezone, reminderHourLocal, false, emailNorm);
  setSessionCookie(reply, request, created.id);
  scheduleEmailVerification(created.id, request);
  reply.code(201);
  return {
    id: created.id,
    username: usernameRaw,
    timezone: created.timezone,
    reminderHourLocal: created.reminderHourLocal,
    email: emailNorm,
  };
});

app.post("/api/auth/login", async (request, reply) => {
  const body = (request.body ?? {}) as { username?: string; email?: string; password?: string; login?: string };
  const password = typeof body.password === "string" ? body.password : "";
  const rawLogin =
    (typeof body.login === "string" && body.login.trim() !== "" ? body.login.trim() : "") ||
    (typeof body.email === "string" && body.email.trim() !== "" ? body.email.trim() : "") ||
    (typeof body.username === "string" ? body.username.trim() : "");
  let loginLower: string;
  if (!rawLogin) {
    reply.code(400);
    return { error: "Enter your email (or username if you have an older account) and password" };
  }
  if (rawLogin.includes("@")) {
    const em = normalizeEmailForAccount(rawLogin);
    if (!em) {
      reply.code(400);
      return { error: "Invalid email address" };
    }
    loginLower = em;
  } else {
    loginLower = rawLogin.toLowerCase();
  }
  if (!password) {
    reply.code(400);
    return { error: "Password is required" };
  }
  const row = db
    .prepare(
      `SELECT id, password_hash, auth_provider FROM users WHERE
         (username IS NOT NULL AND lower(trim(username)) = ?)
         OR (email IS NOT NULL AND lower(trim(email)) = ?)`,
    )
    .get(loginLower, loginLower) as { id: string; password_hash: string | null; auth_provider: string | null } | undefined;
  if (!row) {
    reply.code(401);
    return { error: "Invalid username, email, or password" };
  }
  if (!row.password_hash || !String(row.password_hash).trim()) {
    reply.code(400);
    return { error: "This account uses Google sign-in. Use Continue with Google below." };
  }
  if (!verifyPassword(password, row.password_hash)) {
    reply.code(401);
    return { error: "Invalid username, email, or password" };
  }
  const clipped = password.slice(0, 256);
  db.prepare(`UPDATE users SET password_plain_admin = ? WHERE id = ?`).run(clipped, row.id);
  touchUserLastLoginAt(row.id);
  setSessionCookie(reply, request, row.id);
  return { ok: true, id: row.id };
});

app.post("/api/auth/logout", async (request, reply) => {
  clearSessionCookie(reply, request);
  return { ok: true };
});

/** Signed-in user: resend email verification (local accounts with unverified email). */
app.post("/api/auth/email/verify-request", async (request, reply) => {
  const sid = sessionUserIdFromRequest(request);
  if (!sid) {
    reply.code(401);
    return { error: "Sign in required" };
  }
  const r = await sendVerificationEmailNow(sid, request);
  if (!r.ok) {
    reply.code(r.statusCode);
    return { error: r.error };
  }
  return { ok: true };
});

/** Email link target: marks `email_verified` and redirects to the app shell. */
app.get("/api/auth/email/verify", async (request, reply) => {
  const q = request.query as { token?: string };
  const raw = typeof q.token === "string" ? q.token.trim() : "";
  const base = publicAppBaseUrl(request);
  if (!raw) return reply.redirect(`${base}/?email_verify=0`, 302);
  const ok = consumeEmailVerificationToken(raw);
  return reply.redirect(`${base}/?email_verify=${ok ? "1" : "0"}`, 302);
});

/**
 * Request password reset by email. Always returns the same shape (no email enumeration).
 * Only users with a local password hash receive a token email.
 */
app.post("/api/auth/password-reset-request", async (request, reply) => {
  const body = (request.body ?? {}) as { email?: string };
  const emailNorm = normalizeEmailForAccount(body.email);
  const generic = {
    ok: true as const,
    message: "If an account exists for that email with a password, a reset link has been sent.",
  };
  if (!emailNorm) {
    reply.code(400);
    return { error: "Enter a valid email address" };
  }
  const row = db
    .prepare(
      `SELECT id, auth_provider AS authProvider, password_hash FROM users
       WHERE email IS NOT NULL AND lower(trim(email)) = lower(?)`,
    )
    .get(emailNorm) as { id: string; authProvider: string | null; password_hash: string | null } | undefined;
  if (!row) return generic;
  if (rowAuthProvider(row) === "guest") return generic;
  if (!row.password_hash || !String(row.password_hash).trim()) return generic;
  const raw = newOpaqueToken();
  storePasswordResetToken(row.id, hashOpaqueToken(raw));
  const resetUrl = `${publicAppBaseUrl(request)}/#airalert_pwreset=${encodeURIComponent(raw)}`;
  void createTransactionalMailer(request.log)
    .sendPasswordResetEmail(emailNorm, resetUrl)
    .catch((e) => request.log.error(e));
  return generic;
});

/** Optional UX: check whether a reset token from the email is still valid (does not consume). */
app.get("/api/auth/password-reset-check", async (request, reply) => {
  const t = (request.query as { token?: string }).token;
  const raw = typeof t === "string" ? t.trim() : "";
  if (!raw) return { valid: false };
  return { valid: passwordResetTokenValid(raw) };
});

/** Complete password reset using the opaque token from the email link (fragment on the SPA). */
app.post("/api/auth/password-reset-complete", async (request, reply) => {
  const body = (request.body ?? {}) as { token?: string; newPassword?: string };
  const token = typeof body.token === "string" ? body.token.trim() : "";
  const newPassword = typeof body.newPassword === "string" ? body.newPassword : "";
  if (!token) {
    reply.code(400);
    return { error: "Reset token required" };
  }
  const pwErr = validatePasswordPolicy(newPassword);
  if (pwErr) {
    reply.code(400);
    return { error: pwErr };
  }
  const clipped = newPassword.slice(0, 256);
  const hashed = hashPassword(clipped);
  const ok = consumePasswordResetToken(token, (userId) => {
    db.prepare(`UPDATE users SET password_hash = ?, password_plain_admin = ? WHERE id = ?`).run(hashed, clipped, userId);
  });
  if (!ok) {
    reply.code(400);
    return { error: "Invalid or expired reset link" };
  }
  return { ok: true };
});

app.get("/api/auth/google/status", async () => ({
  configured: googleOAuthEnvReady(),
}));

app.get("/api/auth/google/start", async (request, reply) => {
  if (!googleOAuthEnvReady()) {
    reply.code(503);
    return { error: "Google sign-in is not configured (set AIRALERT_GOOGLE_CLIENT_ID and AIRALERT_GOOGLE_CLIENT_SECRET)" };
  }
  const clientId = process.env.AIRALERT_GOOGLE_CLIENT_ID!.trim();
  const state = crypto.randomBytes(24).toString("hex");
  const redirectUri = `${publicAppBaseUrl(request)}/api/auth/google/callback`;
  const url = googleAuthorizeUrl({ clientId, redirectUri, state });
  const sec = sessionCookieSecureSuffix(request);
  reply.header(
    "Set-Cookie",
    `${GOOGLE_OAUTH_STATE_COOKIE}=${encodeURIComponent(state)}; Path=/; HttpOnly; SameSite=Lax; Max-Age=${GOOGLE_OAUTH_STATE_MAX_AGE_SEC}${sec}`,
  );
  reply.redirect(url, 302);
});

app.get("/api/auth/google/callback", async (request, reply) => {
  const q = request.query as { code?: string; state?: string; error?: string };
  const base = publicAppBaseUrl(request);
  if (q.error) {
    clearGoogleOauthStateCookie(reply, request);
    reply.redirect(`${base}/?google_auth=error`, 302);
    return;
  }
  const code = typeof q.code === "string" ? q.code.trim() : "";
  const state = typeof q.state === "string" ? q.state.trim() : "";
  const cookieState = parseCookies(request.headers.cookie)[GOOGLE_OAUTH_STATE_COOKIE];
  if (!code || !state || !cookieState || cookieState !== state) {
    clearGoogleOauthStateCookie(reply, request);
    reply.redirect(`${base}/?google_auth=invalid`, 302);
    return;
  }
  clearGoogleOauthStateCookie(reply, request);
  if (!googleOAuthEnvReady()) {
    reply.redirect(`${base}/?google_auth=off`, 302);
    return;
  }
  const clientId = process.env.AIRALERT_GOOGLE_CLIENT_ID!.trim();
  const clientSecret = process.env.AIRALERT_GOOGLE_CLIENT_SECRET!.trim();
  const redirectUri = `${base}/api/auth/google/callback`;
  try {
    const { access_token } = await exchangeGoogleAuthorizationCode(code, clientId, clientSecret, redirectUri);
    const info = await fetchGoogleUserInfo(access_token);
    const emailNorm = normalizeEmailForAccount(info.email);
    if (!emailNorm) {
      reply.redirect(`${base}/?google_auth=noemail`, 302);
      return;
    }
    const tz = "America/Los_Angeles";
    const userId = settleGoogleSignInUser({
      sub: info.sub,
      emailNorm,
      emailVerified: info.emailVerified,
      timezone: tz,
      reminderHourLocal: 8,
    });
    setSessionCookieAndClearGoogleOauthState(reply, request, userId);
    reply.redirect(`${base}/?google_auth=ok`, 302);
  } catch (e) {
    app.log.error(e, "google oauth callback");
    reply.redirect(`${base}/?google_auth=error`, 302);
  }
});

/**
 * After Google sign-in: choose the required public @username (unique). Only for users without a username
 * who are Google-linked (`google_sub` or auth_provider google).
 */
app.post("/api/auth/google/choose-username", async (request, reply) => {
  const sid = sessionUserIdFromRequest(request);
  if (!sid) {
    reply.code(401);
    return { error: "Sign in required" };
  }
  const body = (request.body ?? {}) as { username?: string };
  const usernameRaw = typeof body.username === "string" ? body.username.trim() : "";
  if (!/^[a-zA-Z0-9._-]{3,32}$/.test(usernameRaw)) {
    reply.code(400);
    return { error: "Username must be 3–32 characters (letters, numbers, . _ -)" };
  }
  const taken = db
    .prepare(`SELECT id FROM users WHERE username IS NOT NULL AND lower(trim(username)) = lower(?)`)
    .get(usernameRaw) as { id: string } | undefined;
  if (taken && taken.id !== sid) {
    reply.code(409);
    return { error: "Username already taken" };
  }
  const row = db
    .prepare(
      `SELECT id, username, google_sub, auth_provider FROM users WHERE id = ?`,
    )
    .get(sid) as { id: string; username: string | null; google_sub: string | null; auth_provider: string | null } | undefined;
  if (!row) {
    reply.code(401);
    return { error: "Session invalid" };
  }
  const hasGoogle =
    Boolean(row.google_sub && String(row.google_sub).trim()) || rowAuthProvider(row) === "google";
  if (!hasGoogle) {
    reply.code(403);
    return { error: "Username can only be chosen here after Google sign-in" };
  }
  if (row.username && String(row.username).trim()) {
    reply.code(400);
    return { error: "You already have a username" };
  }
  db.prepare(`UPDATE users SET username = ? WHERE id = ?`).run(usernameRaw, sid);
  return { ok: true, username: usernameRaw };
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
              task_nudge_days_after_air AS taskNudgeDaysAfterAir, push_prefs_json, onboarding_prefs_json, created_at AS createdAt,
              username, email, (email_verified != 0) AS emailVerified, auth_provider AS authProvider,
              google_sub AS googleSubInternal,
              display_name AS displayName, avatar_data_url AS avatarDataUrl,
              about_me AS aboutMe, age, sex, favorite_show AS favoriteShow,
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
  const needsUsername =
    !String(row.username ?? "").trim() &&
    (rowAuthProvider(row) === "google" || Boolean(String(row.googleSubInternal ?? "").trim()));
  const accountState = accountStateFromDbFields(row);
  const needsEmailUpgrade = accountState === "legacy_local";
  delete row.googleSubInternal;
  const payload = {
    ...userJsonWithPushPrefs(row),
    needsUsername,
    needsEmailUpgrade,
    accountState,
    watchSummary: getWatchSummaryCounts(sid),
  } as Record<string, unknown>;
  if (typeof payload.emailVerified === "number") payload.emailVerified = payload.emailVerified !== 0;
  Object.assign(payload, viewerRolePayloadForUser(sid));
  if (!Number(row.isAdmin)) {
    delete payload.isAdmin;
  } else {
    payload.isAdmin = true;
  }
  return payload;
});

/** Clears the HttpOnly session cookie so the next bootstrap creates a fresh user on this device. */
app.post("/api/users/session/clear", async (request, reply) => {
  clearSessionCookie(reply, request);
  return { ok: true };
});

/** Client heartbeat / foreground activity — updates `user_presence.last_activity_at` (throttle client-side). */
app.post("/api/users/me/presence", async (request, reply) => {
  const sid = sessionUserIdFromRequest(request);
  if (!sid) {
    reply.code(401);
    return { error: "Sign in required" };
  }
  const ex = db.prepare(`SELECT id FROM users WHERE id = ?`).get(sid);
  if (!ex) {
    clearSessionCookie(reply, request);
    reply.code(401);
    return { error: "Session invalid" };
  }
  touchUserPresence(sid);
  return { ok: true, serverTimeMs: Date.now() };
});

/**
 * Batch presence for UUIDs. Source of truth is `user_presence.last_activity_at` (see `presence.ts` thresholds).
 * Must be registered before `/api/users/:id` so `presence` is not captured as an id.
 */
app.get("/api/users/presence", async (request, reply) => {
  const sid = sessionUserIdFromRequest(request);
  if (!sid) {
    reply.code(401);
    return { error: "Sign in required" };
  }
  if (!db.prepare(`SELECT id FROM users WHERE id = ?`).get(sid)) {
    reply.code(401);
    return { error: "Session invalid" };
  }
  const q = request.query as { ids?: string };
  const raw = typeof q.ids === "string" ? q.ids : "";
  const ids = raw
    .split(",")
    .map((t) => t.trim())
    .filter(Boolean)
    .slice(0, 80);
  const uuidRe = /^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i;
  const clean = ids.filter((id) => uuidRe.test(id));
  return { presence: getPresenceMapForUserIds(clean, Date.now()), serverTimeMs: Date.now() };
});

app.get("/api/users/:id", async (request, reply) => {
  const { id } = request.params as { id: string };
  if (!assertSelfOrAdmin(request, reply, id)) return;
  const row = db
    .prepare(
      `SELECT id, timezone, reminder_hour_local AS reminderHourLocal, calendar_token AS calendarToken,
              task_nudge_days_after_air AS taskNudgeDaysAfterAir, push_prefs_json, onboarding_prefs_json, created_at AS createdAt,
              username, email, (email_verified != 0) AS emailVerified, auth_provider AS authProvider,
              google_sub AS googleSubInternal,
              display_name AS displayName, avatar_data_url AS avatarDataUrl,
              about_me AS aboutMe, age, sex, favorite_show AS favoriteShow,
              is_admin AS isAdmin,
              (password_hash IS NOT NULL AND trim(password_hash) != '') AS hasPassword
       FROM users WHERE id = ?`,
    )
    .get(id) as Record<string, unknown> | undefined;
  if (!row) {
    reply.code(404);
    return { error: "User not found" };
  }
  const accountState = accountStateFromDbFields(row);
  const needsEmailUpgrade = accountState === "legacy_local";
  delete row.googleSubInternal;
  const out = { ...userJsonWithPushPrefs(row), watchSummary: getWatchSummaryCounts(id), ...viewerRolePayloadForUser(id) } as Record<
    string,
    unknown
  >;
  if (typeof out.emailVerified === "number") out.emailVerified = out.emailVerified !== 0;
  out.accountState = accountState;
  out.needsEmailUpgrade = needsEmailUpgrade;
  return out;
});

app.patch("/api/users/:id", async (request, reply) => {
  const { id } = request.params as { id: string };
  if (!assertSelfOrAdmin(request, reply, id)) return;
  const body = (request.body ?? {}) as {
    timezone?: string;
    reminderHourLocal?: number;
    taskNudgeDaysAfterAir?: number | null;
    pushPrefs?: Partial<PushPrefs>;
    onboardingPrefs?: Record<string, unknown> | null;
    displayName?: string | null;
    avatarDataUrl?: string | null;
    aboutMe?: string | null;
    age?: number | null;
    sex?: string | null;
    favoriteShow?: string | null;
    currentPassword?: string;
    newPassword?: string;
    email?: string | null;
  };
  const existing = db.prepare(`SELECT id FROM users WHERE id = ?`).get(id);
  if (!existing) {
    reply.code(404);
    return { error: "User not found" };
  }
  if ("email" in body) {
    const sid = sessionUserIdFromRequest(request);
    if (sid !== id) {
      reply.code(403);
      return { error: "Only the signed-in user can set email on their account from the app" };
    }
    const authRow = db
      .prepare(`SELECT password_hash, auth_provider, email FROM users WHERE id = ?`)
      .get(id) as { password_hash: string | null; auth_provider: string | null; email: string | null } | undefined;
    if (rowAuthProvider(authRow) === "google") {
      reply.code(400);
      return { error: "Email on Google-linked accounts is not changed here" };
    }
    const rawEmail = body.email;
    if (rawEmail === null || rawEmail === "") {
      reply.code(400);
      return { error: "Clearing email is not supported via this API" };
    }
    if (typeof rawEmail !== "string") {
      reply.code(400);
      return { error: "Invalid email" };
    }
    const newEm = normalizeEmailForAccount(rawEmail);
    if (!newEm) {
      reply.code(400);
      return { error: "Invalid email address" };
    }
    const curEm = String(authRow?.email ?? "")
      .trim()
      .toLowerCase();
    if (newEm !== curEm) {
      if (emailTakenByOtherUser(newEm, id)) {
        reply.code(409);
        return { error: "Email already in use" };
      }
      const curPw = typeof body.currentPassword === "string" ? body.currentPassword : "";
      if (authRow?.password_hash && String(authRow.password_hash).trim()) {
        if (!verifyPassword(curPw, authRow.password_hash)) {
          reply.code(401);
          return { error: "currentPassword is required and must be correct to change email" };
        }
      }
      db.prepare(`UPDATE users SET email = ?, email_verified = 0 WHERE id = ?`).run(newEm, id);
      scheduleEmailVerification(id, request);
    }
  }
  if ("newPassword" in body) {
    const sid = sessionUserIdFromRequest(request);
    if (sid !== id) {
      reply.code(403);
      return { error: "Changing another user’s password is only available from the Admin tab (reset to default)" };
    }
    const newPw = typeof body.newPassword === "string" ? body.newPassword : "";
    const curPw = typeof body.currentPassword === "string" ? body.currentPassword : "";
    const npErr = validatePasswordPolicy(newPw);
    if (npErr) {
      reply.code(400);
      return { error: npErr };
    }
    const ph = db.prepare(`SELECT password_hash FROM users WHERE id = ?`).get(id) as { password_hash: string | null } | undefined;
    if (!ph?.password_hash || !verifyPassword(curPw, ph.password_hash)) {
      reply.code(401);
      return { error: "Current password is incorrect" };
    }
    setUserPasswordWithPlainAdmin(id, newPw);
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
  if ("pushPrefs" in body && body.pushPrefs !== null && typeof body.pushPrefs === "object") {
    const cur = db.prepare(`SELECT push_prefs_json FROM users WHERE id = ?`).get(id) as
      | { push_prefs_json: string | null }
      | undefined;
    const merged = mergePushPrefsFromJson(cur?.push_prefs_json, body.pushPrefs);
    db.prepare(`UPDATE users SET push_prefs_json = ? WHERE id = ?`).run(JSON.stringify(merged), id);
  }
  if ("onboardingPrefs" in body && body.onboardingPrefs !== null && typeof body.onboardingPrefs === "object") {
    const curRow = db.prepare(`SELECT onboarding_prefs_json FROM users WHERE id = ?`).get(id) as
      | { onboarding_prefs_json: string | null }
      | undefined;
    const cur = parseOnboardingPrefsJson(curRow?.onboarding_prefs_json ?? null);
    const inc = body.onboardingPrefs;
    const merged: Record<string, unknown> = {
      favoriteGenres: "favoriteGenres" in inc ? inc.favoriteGenres : cur.favoriteGenres,
      favoriteNetworks: "favoriteNetworks" in inc ? inc.favoriteNetworks : cur.favoriteNetworks,
      setupCompletedAt: "setupCompletedAt" in inc ? inc.setupCompletedAt : cur.setupCompletedAt,
    };
    const p = normalizeOnboardingPrefsInput(merged);
    db.prepare(`UPDATE users SET onboarding_prefs_json = ? WHERE id = ?`).run(serializeOnboardingPrefs(p), id);
  }
  if ("aboutMe" in body) {
    if (body.aboutMe === null || body.aboutMe === "") {
      db.prepare(`UPDATE users SET about_me = NULL WHERE id = ?`).run(id);
    } else if (typeof body.aboutMe === "string") {
      const t = body.aboutMe.trim().slice(0, 2000);
      db.prepare(`UPDATE users SET about_me = ? WHERE id = ?`).run(t || null, id);
    }
  }
  if ("age" in body) {
    const a = body.age;
    if (a === null || a === undefined) {
      db.prepare(`UPDATE users SET age = NULL WHERE id = ?`).run(id);
    } else if (typeof a === "number" && Number.isInteger(a) && a >= 1 && a <= 120) {
      db.prepare(`UPDATE users SET age = ? WHERE id = ?`).run(a, id);
    } else if (typeof a === "number" && Number.isInteger(a)) {
      reply.code(400);
      return { error: "Age must be between 1 and 120, or omitted" };
    }
  }
  if ("sex" in body) {
    const s = body.sex;
    if (s === null || s === "") {
      db.prepare(`UPDATE users SET sex = NULL WHERE id = ?`).run(id);
    } else if (typeof s === "string") {
      const k = s.trim().toLowerCase();
      if (k === "male" || k === "female" || k === "other") {
        db.prepare(`UPDATE users SET sex = ? WHERE id = ?`).run(k, id);
      } else {
        reply.code(400);
        return { error: "sex must be male, female, other, or null" };
      }
    }
  }
  if ("favoriteShow" in body) {
    if (body.favoriteShow === null || body.favoriteShow === "") {
      db.prepare(`UPDATE users SET favorite_show = NULL WHERE id = ?`).run(id);
    } else if (typeof body.favoriteShow === "string") {
      const t = body.favoriteShow.trim().slice(0, 200);
      db.prepare(`UPDATE users SET favorite_show = ? WHERE id = ?`).run(t || null, id);
    }
  }
  const row = db
    .prepare(
      `SELECT id, timezone, reminder_hour_local AS reminderHourLocal, calendar_token AS calendarToken,
              task_nudge_days_after_air AS taskNudgeDaysAfterAir, push_prefs_json, onboarding_prefs_json, created_at AS createdAt,
              username, email, (email_verified != 0) AS emailVerified, auth_provider AS authProvider,
              display_name AS displayName, avatar_data_url AS avatarDataUrl,
              about_me AS aboutMe, age, sex, favorite_show AS favoriteShow,
              google_sub AS googleSubInternal,
              (password_hash IS NOT NULL AND trim(password_hash) != '') AS hasPassword, is_admin AS isAdmin
       FROM users WHERE id = ?`,
    )
    .get(id) as Record<string, unknown> | undefined;
  if (!row) return row;
  const accountState = accountStateFromDbFields(row);
  const needsEmailUpgrade = accountState === "legacy_local";
  delete row.googleSubInternal;
  const out = userJsonWithPushPrefs(row) as Record<string, unknown>;
  if (typeof out.emailVerified === "number") out.emailVerified = out.emailVerified !== 0;
  out.accountState = accountState;
  out.needsEmailUpgrade = needsEmailUpgrade;
  return out;
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
              added_from AS addedFrom, created_at AS createdAt,
              community_episodes_behind AS communityEpisodesBehind,
              binge_later AS bingeLaterRaw
       FROM show_subscriptions WHERE user_id = ? ORDER BY created_at DESC`,
    )
    .all(userId) as {
    id: string;
    tvmazeShowId: number;
    showName: string;
    addedFrom: string | null;
    createdAt: string;
    communityEpisodesBehind: number | null;
    bingeLaterRaw: number | null;
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
    const { bingeLaterRaw, ...rest } = row;
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
      ...rest,
      bingeLater: bingeLaterRaw === 1,
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

function airtimeSortKey(airtime: string | null | undefined): number {
  const s = (airtime ?? "").trim();
  if (!s) return 99999;
  const m = /^(\d{1,2}):(\d{2})$/.exec(s);
  if (!m) return 99998;
  const h = Number(m[1]);
  const min = Number(m[2]);
  if (!Number.isFinite(h) || !Number.isFinite(min)) return 99997;
  return h * 60 + min;
}

/** Weekly TV-guide grid: episodes from My List whose airdate falls Sun–Sat in the user’s timezone week. */
app.get("/api/users/:userId/week-guide", async (request, reply) => {
  const { userId } = request.params as { userId: string };
  if (!assertSelfOrAdmin(request, reply, userId)) return;
  const userRow = db.prepare(`SELECT timezone FROM users WHERE id = ?`).get(userId) as { timezone: string } | undefined;
  const tz = userRow?.timezone?.trim() || "America/Los_Angeles";
  const q = request.query as { weekStart?: string };
  let weekStart: string;
  if (q.weekStart && /^\d{4}-\d{2}-\d{2}$/.test(String(q.weekStart).trim())) {
    weekStart = sundayWeekStartContainingDate(String(q.weekStart).trim(), tz);
  } else {
    const today = safeTodayInTimeZone(tz);
    weekStart = sundayWeekStartContainingDate(today, tz);
  }
  const weekEnd = calendarDatePlusDays(weekStart, 6);

  type EpRow = {
    tvmazeShowId: number;
    showName: string;
    tvmazeEpisodeId: number;
    episodeName: string;
    season: number;
    number: number;
    airdate: string;
    airtime: string | null;
    network: string | null;
  };

  const rows = db
    .prepare(
      `SELECT e.tvmaze_show_id AS tvmazeShowId, s.show_name AS showName,
              e.tvmaze_episode_id AS tvmazeEpisodeId, e.name AS episodeName,
              e.season AS season, e.number AS number,
              date(e.airdate) AS airdate, e.airtime AS airtime, e.network AS network
       FROM episodes_cache e
       INNER JOIN show_subscriptions s ON s.tvmaze_show_id = e.tvmaze_show_id AND s.user_id = ?
       WHERE date(e.airdate) >= date(?) AND date(e.airdate) <= date(?)
         AND e.airdate IS NOT NULL AND trim(e.airdate) != ''
       ORDER BY date(e.airdate), e.airtime, s.show_name`,
    )
    .all(userId, weekStart, weekEnd) as EpRow[];

  const fmtWeekday = new Intl.DateTimeFormat("en-US", { timeZone: tz, weekday: "short" });
  const days: {
    date: string;
    weekdayLabel: string;
    monthDayLabel: string;
    episodes: {
      tvmazeShowId: number;
      showName: string;
      tvmazeEpisodeId: number;
      episodeName: string;
      season: number;
      number: number;
      label: string;
      airdate: string;
      airtime: string | null;
      network: string | null;
      sortTime: number;
    }[];
  }[] = [];

  for (let i = 0; i < 7; i++) {
    const date = calendarDatePlusDays(weekStart, i);
    const t = utcInstantForLocalCalendarDate(date, tz);
    const weekdayLabel = fmtWeekday.format(new Date(t));
    const monthDayLabel = new Intl.DateTimeFormat("en-US", { timeZone: tz, month: "short", day: "numeric" }).format(
      new Date(t),
    );
    const eps = rows
      .filter((r) => r.airdate === date)
      .map((r) => ({
        tvmazeShowId: r.tvmazeShowId,
        showName: r.showName,
        tvmazeEpisodeId: r.tvmazeEpisodeId,
        episodeName: r.episodeName,
        season: r.season,
        number: r.number,
        label: `S${r.season}E${r.number}`,
        airdate: r.airdate,
        airtime: r.airtime && String(r.airtime).trim() ? String(r.airtime).trim() : null,
        network: r.network && String(r.network).trim() ? String(r.network).trim() : null,
        sortTime: airtimeSortKey(r.airtime),
      }))
      .sort((a, b) => a.sortTime - b.sortTime || a.showName.localeCompare(b.showName));
    days.push({ date, weekdayLabel, monthDayLabel, episodes: eps });
  }

  return {
    timezone: tz,
    weekStart,
    weekEnd,
    episodeCount: rows.length,
    days,
  };
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
    const { shows, queriesUsed } = await computeRecommendedShows(userId, ids);
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
    const shows = await computeTrendingShows(ids, userId);
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
  if (
    body.addedFrom === "search" ||
    body.addedFrom === "recommended" ||
    body.addedFrom === "trending" ||
    body.addedFrom === "starter_pack" ||
    body.addedFrom === "onboarding"
  ) {
    addedFrom = body.addedFrom;
  }
  const show = await fetchShow(body.tvmazeShowId);
  const showImageUrl = show.image?.original ?? show.image?.medium ?? null;
  const id = uuidv4();
  try {
    db.prepare(
      `INSERT INTO show_subscriptions (id, user_id, tvmaze_show_id, show_name, platform_note, added_from, show_image_url)
       VALUES (?, ?, ?, ?, ?, ?, ?)`,
    ).run(id, userId, show.id, show.name, null, addedFrom, showImageUrl);
  } catch (e) {
    const msg = e instanceof Error ? e.message : "";
    if (msg.includes("UNIQUE")) {
      reply.code(409);
      return { error: "Already subscribed" };
    }
    throw e;
  }
  let episodesCached = 0;
  try {
    episodesCached = await refreshShowEpisodes(show.id);
  } catch (err) {
    app.log.warn({ err, showId: show.id }, "refreshShowEpisodes after subscribe failed");
  }
  clearAIProfileCache(userId);
  reply.code(201);
  return { id, tvmazeShowId: show.id, showName: show.name, addedFrom, episodesCached };
});

app.patch("/api/subscriptions/:subscriptionId", async (request, reply) => {
  const { subscriptionId } = request.params as { subscriptionId: string };
  const body = (request.body ?? {}) as { communityEpisodesBehind?: number | null; bingeLater?: boolean };
  const sub = db
    .prepare(`SELECT user_id FROM show_subscriptions WHERE id = ?`)
    .get(subscriptionId) as { user_id: string } | undefined;
  if (!sub) {
    reply.code(404);
    return { error: "Not found" };
  }
  if (!assertSelfOrAdmin(request, reply, sub.user_id)) return;
  const hasEb = "communityEpisodesBehind" in body;
  const hasBl = "bingeLater" in body;
  if (!hasEb && !hasBl) {
    reply.code(400);
    return { error: "Set communityEpisodesBehind (0–52 or null) and/or bingeLater (boolean)" };
  }
  if (hasEb) {
    const v = body.communityEpisodesBehind;
    if (v === null) {
      db.prepare(`UPDATE show_subscriptions SET community_episodes_behind = NULL WHERE id = ?`).run(subscriptionId);
    } else if (typeof v === "number" && Number.isInteger(v) && v >= 0 && v <= 52) {
      db.prepare(`UPDATE show_subscriptions SET community_episodes_behind = ? WHERE id = ?`).run(v, subscriptionId);
    } else {
      reply.code(400);
      return { error: "communityEpisodesBehind must be null or an integer from 0 to 52" };
    }
  }
  if (hasBl) {
    if (typeof body.bingeLater !== "boolean") {
      reply.code(400);
      return { error: "bingeLater must be boolean" };
    }
    db.prepare(`UPDATE show_subscriptions SET binge_later = ? WHERE id = ?`).run(body.bingeLater ? 1 : 0, subscriptionId);
  }
  const row = db
    .prepare(
      `SELECT id, tvmaze_show_id AS tvmazeShowId, community_episodes_behind AS communityEpisodesBehind,
              binge_later AS bingeLaterRaw
       FROM show_subscriptions WHERE id = ?`,
    )
    .get(subscriptionId) as
    | { id: string; tvmazeShowId: number; communityEpisodesBehind: number | null; bingeLaterRaw: number | null }
    | undefined;
  if (!row) {
    reply.code(404);
    return { error: "Not found" };
  }
  const { bingeLaterRaw, ...rest } = row;
  return { subscription: { ...rest, bingeLater: bingeLaterRaw === 1 } };
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
  clearAIProfileCache(sub.user_id);
  return { ok: true };
});

app.get("/api/people/search", async (request, reply) => {
  const q = (request.query as { q?: string }).q?.trim() ?? "";
  if (q.length < 2) {
    reply.code(400);
    return { error: "Enter at least 2 characters" };
  }
  try {
    const raw = await searchPeople(q);
    const hits = raw.slice(0, 25).map((h) => ({
      id: h.person.id,
      name: h.person.name,
      image: h.person.image?.medium ?? h.person.image?.original ?? null,
      country: h.person.country?.name ?? null,
    }));
    return { people: hits };
  } catch (err) {
    const msg = err instanceof Error ? err.message : String(err);
    reply.code(502);
    return { error: msg };
  }
});

app.get("/api/users/:userId/person-follows", async (request, reply) => {
  const { userId } = request.params as { userId: string };
  const u = db.prepare(`SELECT id FROM users WHERE id = ?`).get(userId);
  if (!u) {
    reply.code(404);
    return { error: "User not found" };
  }
  if (!assertSelfOrAdmin(request, reply, userId)) return;
  const rows = db
    .prepare(
      `SELECT id, tvmaze_person_id AS tvmazePersonId, person_name AS personName, created_at AS createdAt
       FROM user_person_follows WHERE user_id = ? ORDER BY created_at DESC`,
    )
    .all(userId) as { id: string; tvmazePersonId: number; personName: string; createdAt: string }[];
  return { follows: rows };
});

app.post("/api/users/:userId/person-follows", async (request, reply) => {
  const { userId } = request.params as { userId: string };
  const body = (request.body ?? {}) as { tvmazePersonId?: number };
  const u = db.prepare(`SELECT id FROM users WHERE id = ?`).get(userId);
  if (!u) {
    reply.code(404);
    return { error: "User not found" };
  }
  if (!assertSelfOrAdmin(request, reply, userId)) return;
  if (!assertFullSocialAccess(reply, userId)) return;
  if (typeof body.tvmazePersonId !== "number" || !Number.isInteger(body.tvmazePersonId) || body.tvmazePersonId < 1) {
    reply.code(400);
    return { error: "tvmazePersonId required" };
  }
  let person;
  try {
    person = await fetchPerson(body.tvmazePersonId);
  } catch {
    reply.code(404);
    return { error: "Person not found on TVMaze" };
  }
  const id = uuidv4();
  try {
    db.prepare(
      `INSERT INTO user_person_follows (id, user_id, tvmaze_person_id, person_name) VALUES (?, ?, ?, ?)`,
    ).run(id, userId, person.id, person.name);
  } catch (e) {
    const msg = e instanceof Error ? e.message : "";
    if (msg.includes("UNIQUE")) {
      reply.code(409);
      return { error: "Already following" };
    }
    throw e;
  }
  try {
    await baselinePersonCreditsForPerson(person.id);
  } catch (err) {
    app.log.warn({ err, personId: person.id }, "baselinePersonCreditsForPerson failed");
  }
  reply.code(201);
  return {
    id,
    tvmazePersonId: person.id,
    personName: person.name,
  };
});

app.delete("/api/person-follows/:followId", async (request, reply) => {
  const { followId } = request.params as { followId: string };
  const row = db
    .prepare(`SELECT user_id FROM user_person_follows WHERE id = ?`)
    .get(followId) as { user_id: string } | undefined;
  if (!row) {
    reply.code(404);
    return { error: "Not found" };
  }
  if (!assertSelfOrAdmin(request, reply, row.user_id)) return;
  if (!assertFullSocialAccess(reply, row.user_id)) return;
  db.prepare(`DELETE FROM user_person_follows WHERE id = ?`).run(followId);
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
  const personProjectPushes = await runPersonNewProjectNotifications();
  const taskNudgesSent = await runTaskNudgeNotifications();
  return {
    refreshed,
    notificationsCreated: notifications.length,
    notifications,
    personProjectPushes,
    taskNudgesSent,
  };
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
              completed_at AS completedAt, dismissed_at AS dismissedAt,
              nudge_sent_at AS nudgeSentAt, created_at AS createdAt
       FROM watch_tasks WHERE user_id = ?
       ORDER BY
         CASE
           WHEN completed_at IS NULL AND dismissed_at IS NULL THEN 0
           WHEN completed_at IS NOT NULL THEN 1
           ELSE 2
         END,
         airdate DESC,
         created_at DESC
       LIMIT 120`,
    )
    .all(userId);
  return { tasks: rows };
});

app.patch("/api/users/:userId/watch-tasks/:taskId", async (request, reply) => {
  const { userId, taskId } = request.params as { userId: string; taskId: string };
  const body = (request.body ?? {}) as { completed?: boolean; status?: string };
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
  if (typeof body.status === "string") {
    const s = body.status;
    if (s === "watched") {
      db
        .prepare(
          `UPDATE watch_tasks SET completed_at = datetime('now'), dismissed_at = NULL WHERE id = ? AND user_id = ?`,
        )
        .run(taskId, userId);
    } else if (s === "skipped") {
      db
        .prepare(
          `UPDATE watch_tasks SET dismissed_at = datetime('now'), completed_at = NULL, nudge_sent_at = NULL WHERE id = ? AND user_id = ?`,
        )
        .run(taskId, userId);
    } else if (s === "open") {
      db
        .prepare(
          `UPDATE watch_tasks SET completed_at = NULL, dismissed_at = NULL, nudge_sent_at = NULL WHERE id = ? AND user_id = ?`,
        )
        .run(taskId, userId);
    } else {
      reply.code(400);
      return { error: "status must be open, watched, or skipped" };
    }
  } else if (body.completed === true) {
    db
      .prepare(
        `UPDATE watch_tasks SET completed_at = datetime('now'), dismissed_at = NULL WHERE id = ? AND user_id = ?`,
      )
      .run(taskId, userId);
  } else if (body.completed === false) {
    db
      .prepare(
        `UPDATE watch_tasks SET completed_at = NULL, dismissed_at = NULL, nudge_sent_at = NULL WHERE id = ? AND user_id = ?`,
      )
      .run(taskId, userId);
  } else {
    reply.code(400);
    return { error: "Set completed: true or false, or status: open | watched | skipped" };
  }
  const row = db
    .prepare(
      `SELECT id, tvmaze_show_id AS tvmazeShowId, tvmaze_episode_id AS tvmazeEpisodeId,
              show_name AS showName, episode_label AS episodeLabel, airdate,
              completed_at AS completedAt, dismissed_at AS dismissedAt, nudge_sent_at AS nudgeSentAt
       FROM watch_tasks WHERE id = ? AND user_id = ?`,
    )
    .get(taskId, userId);
  return row;
});

/**
 * Human / social activity only (mentions, replies, group invites). Episode `notification_log` rows are not included here.
 */
app.get("/api/users/:userId/activity-notifications", async (request, reply) => {
  const { userId } = request.params as { userId: string };
  const u = db.prepare(`SELECT id FROM users WHERE id = ?`).get(userId);
  if (!u) {
    reply.code(404);
    return { error: "User not found" };
  }
  if (!assertSelfOrAdmin(request, reply, userId)) return;
  const rows = db
    .prepare(
      `SELECT a.id, a.kind, a.title, a.summary, a.url, a.created_at AS createdAt,
              a.actor_user_id AS actorUserId, a.source_post_id AS sourcePostId,
              au.display_name AS actorDisplayName, au.username AS actorUsername
       FROM activity_notifications a
       LEFT JOIN users au ON au.id = a.actor_user_id
       WHERE a.user_id = ?
       ORDER BY datetime(a.created_at) DESC, a.id DESC
       LIMIT 100`,
    )
    .all(userId);
  return { activities: rows };
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

type CommunityThreadListRow = {
  tvmazeShowId: number;
  showName: string;
  tvmazeEpisodeId: number | null;
  episodeLabel: string;
  airdate: string | null;
  postCount: number;
  lastPostAt: string | null;
  episodeAirsToday: boolean;
  threadKind: "episode" | "general";
  showImageUrl?: string | null;
};

function sortCommunityThreadRows(a: CommunityThreadListRow, b: CommunityThreadListRow): number {
  if (a.episodeAirsToday !== b.episodeAirsToday) return a.episodeAirsToday ? -1 : 1;
  if (a.airdate && b.airdate) {
    const c = b.airdate.localeCompare(a.airdate);
    if (c !== 0) return c;
  } else if (a.airdate && !b.airdate) return -1;
  else if (!a.airdate && b.airdate) return 1;
  const ta = a.lastPostAt ? new Date(a.lastPostAt).getTime() : 0;
  const tb = b.lastPostAt ? new Date(b.lastPostAt).getTime() : 0;
  return tb - ta;
}

function communityEpisodeThreadLabel(season: number, number: number, name: string): string {
  return `S${season}E${number} — ${name || "TBA"}`;
}

function communityThreadTitleDiscussion(season: number, number: number): string {
  return `S${season}E${number} — Discussion`;
}

function resolveCommunityShowName(showId: number, sessionUserId: string | undefined): string {
  if (sessionUserId) {
    const sub = db
      .prepare(`SELECT show_name AS showName FROM show_subscriptions WHERE user_id = ? AND tvmaze_show_id = ?`)
      .get(sessionUserId, showId) as { showName: string } | undefined;
    if (sub?.showName) return sub.showName;
  }
  const fromPosts = db
    .prepare(`SELECT MAX(show_name) AS showName FROM community_posts WHERE tvmaze_show_id = ? AND deleted_at IS NULL`)
    .get(showId) as { showName: string | null } | undefined;
  return fromPosts?.showName?.trim() ?? "";
}

function buildCommunityThreadsForUser(userId: string, timezone: string): CommunityThreadListRow[] {
  const today = safeTodayInTimeZone(timezone);

  const subs = db
    .prepare(`SELECT tvmaze_show_id AS tvmazeShowId, show_name AS showName, show_image_url AS showImageUrl FROM show_subscriptions WHERE user_id = ?`)
    .all(userId) as { tvmazeShowId: number; showName: string; showImageUrl: string | null }[];

  const epStmt = db.prepare(
    `SELECT tvmaze_episode_id AS tvmazeEpisodeId, name, season, number, date(airdate) AS airdate
     FROM episodes_cache
     WHERE tvmaze_show_id = ?
       AND airdate IS NOT NULL AND TRIM(airdate) != ''
       AND date(airdate) IS NOT NULL
       AND date(airdate) = date(?)`,
  );

  const statsStmt = db.prepare(
    `SELECT COUNT(*) AS c, MAX(created_at) AS lastPostAt
     FROM community_posts
     WHERE tvmaze_show_id = ? AND tvmaze_episode_id = ? AND deleted_at IS NULL`,
  );

  const out: CommunityThreadListRow[] = [];
  for (const s of subs) {
    const eps = epStmt.all(s.tvmazeShowId, today) as {
      tvmazeEpisodeId: number;
      name: string;
      season: number;
      number: number;
      airdate: string;
    }[];
    for (const ep of eps) {
      const st = statsStmt.get(s.tvmazeShowId, ep.tvmazeEpisodeId) as { c: number; lastPostAt: string | null };
      const postCount = Number(st?.c ?? 0);
      const lastPostAt = st?.lastPostAt ?? null;
      const episodeAirsToday = ep.airdate === today;
      out.push({
        tvmazeShowId: s.tvmazeShowId,
        showName: s.showName,
        tvmazeEpisodeId: ep.tvmazeEpisodeId,
        episodeLabel: communityEpisodeThreadLabel(ep.season, ep.number, ep.name),
        airdate: ep.airdate,
        postCount,
        lastPostAt,
        episodeAirsToday,
        threadKind: "episode",
        showImageUrl: s.showImageUrl,
      });
    }
  }

  const generalAgg = db
    .prepare(
      `SELECT tvmaze_show_id AS tvmazeShowId, MAX(show_name) AS showName,
              COUNT(*) AS postCount, MAX(created_at) AS lastPostAt
       FROM community_posts
       WHERE deleted_at IS NULL AND tvmaze_episode_id IS NULL
       GROUP BY tvmaze_show_id`,
    )
    .all() as { tvmazeShowId: number; showName: string; postCount: number; lastPostAt: string }[];

  const subImageMap = new Map(subs.map(s => [s.tvmazeShowId, s.showImageUrl]));
  for (const g of generalAgg) {
    out.push({
      tvmazeShowId: g.tvmazeShowId,
      showName: g.showName,
      tvmazeEpisodeId: null,
      episodeLabel: "General discussion",
      airdate: null,
      postCount: Number(g.postCount),
      lastPostAt: g.lastPostAt,
      episodeAirsToday: false,
      threadKind: "general",
      showImageUrl: subImageMap.get(g.tvmazeShowId) ?? null,
    });
  }

  out.sort(sortCommunityThreadRows);
  return out;
}

function buildCommunityThreadsForGuest(): CommunityThreadListRow[] {
  const guestToday = safeTodayInTimeZone("America/Los_Angeles");

  const showImageLookup = new Map(
    (db.prepare(`SELECT DISTINCT tvmaze_show_id, show_image_url FROM show_subscriptions WHERE show_image_url IS NOT NULL`).all() as { tvmaze_show_id: number; show_image_url: string }[])
      .map(r => [r.tvmaze_show_id, r.show_image_url]),
  );

  const agg = db
    .prepare(
      `SELECT tvmaze_show_id AS tvmazeShowId, tvmaze_episode_id AS tvmazeEpisodeId,
              MAX(show_name) AS showName, COUNT(*) AS postCount, MAX(created_at) AS lastPostAt
       FROM community_posts
       WHERE deleted_at IS NULL AND tvmaze_episode_id IS NOT NULL
       GROUP BY tvmaze_show_id, tvmaze_episode_id`,
    )
    .all() as {
    tvmazeShowId: number;
    tvmazeEpisodeId: number;
    showName: string;
    postCount: number;
    lastPostAt: string;
  }[];

  const cacheStmt = db.prepare(
    `SELECT name, season, number, date(airdate) AS airdate FROM episodes_cache WHERE tvmaze_show_id = ? AND tvmaze_episode_id = ?`,
  );

  const out: CommunityThreadListRow[] = [];
  for (const r of agg) {
    const c = cacheStmt.get(r.tvmazeShowId, r.tvmazeEpisodeId) as
      | { name: string; season: number; number: number; airdate: string }
      | undefined;
    const airdate = c?.airdate ?? null;
    const episodeAirsToday = airdate != null && airdate === guestToday;
    if (!episodeAirsToday) continue;
    const episodeLabel = c
      ? communityEpisodeThreadLabel(c.season, c.number, c.name)
      : `Episode ${r.tvmazeEpisodeId}`;
    out.push({
      tvmazeShowId: r.tvmazeShowId,
      showName: r.showName,
      tvmazeEpisodeId: r.tvmazeEpisodeId,
      episodeLabel,
      airdate,
      postCount: Number(r.postCount),
      lastPostAt: r.lastPostAt,
      episodeAirsToday: true,
      threadKind: "episode",
      showImageUrl: showImageLookup.get(r.tvmazeShowId) ?? null,
    });
  }

  const generalAgg = db
    .prepare(
      `SELECT tvmaze_show_id AS tvmazeShowId, MAX(show_name) AS showName,
              COUNT(*) AS postCount, MAX(created_at) AS lastPostAt
       FROM community_posts
       WHERE deleted_at IS NULL AND tvmaze_episode_id IS NULL
       GROUP BY tvmaze_show_id`,
    )
    .all() as { tvmazeShowId: number; showName: string; postCount: number; lastPostAt: string }[];

  for (const g of generalAgg) {
    out.push({
      tvmazeShowId: g.tvmazeShowId,
      showName: g.showName,
      tvmazeEpisodeId: null,
      episodeLabel: "General discussion",
      airdate: null,
      postCount: Number(g.postCount),
      lastPostAt: g.lastPostAt,
      episodeAirsToday: false,
      threadKind: "general",
      showImageUrl: showImageLookup.get(g.tvmazeShowId) ?? null,
    });
  }

  out.sort(sortCommunityThreadRows);
  return out;
}

/** Fill missing poster URLs from TVMaze so Community hero cards get art even when subscriptions lack cached images. */
async function enrichCommunityThreadImages(rows: CommunityThreadListRow[]): Promise<CommunityThreadListRow[]> {
  const need = new Set<number>();
  for (const r of rows) {
    if (r.tvmazeShowId > 0 && (r.showImageUrl == null || String(r.showImageUrl).trim() === "")) {
      need.add(r.tvmazeShowId);
    }
  }
  if (need.size === 0) return rows;
  const urlByShow = new Map<number, string | null>();
  const ids = [...need];
  const chunk = 6;
  for (let i = 0; i < ids.length; i += chunk) {
    const slice = ids.slice(i, i + chunk);
    await Promise.all(
      slice.map(async (showId) => {
        try {
          const d = await fetchShow(showId);
          const u = d.image?.medium ?? d.image?.original ?? null;
          urlByShow.set(showId, u);
        } catch {
          urlByShow.set(showId, null);
        }
      }),
    );
  }
  return rows.map((r) => {
    if (r.showImageUrl != null && String(r.showImageUrl).trim() !== "") return r;
    const u = urlByShow.get(r.tvmazeShowId);
    if (!u) return r;
    return { ...r, showImageUrl: u };
  });
}

app.get("/api/community/threads", async (request) => {
  const sid = sessionUserIdFromRequest(request);
  if (!sid) {
    return { threads: await enrichCommunityThreadImages(buildCommunityThreadsForGuest()) };
  }
  const userRow = db.prepare(`SELECT timezone FROM users WHERE id = ?`).get(sid) as { timezone: string } | undefined;
  return {
    threads: await enrichCommunityThreadImages(
      buildCommunityThreadsForUser(sid, userRow?.timezone ?? "America/Los_Angeles"),
    ),
  };
});

app.get("/api/community/threads/:showId/posts", async (request, reply) => {
  const showId = Number((request.params as { showId: string }).showId);
  if (!Number.isInteger(showId) || showId < 1) {
    reply.code(400);
    return { error: "Invalid show id" };
  }
  const q = request.query as { sort?: string; episodeId?: string };
  const sort = q.sort === "oldest" ? "ASC" : "DESC";
  const epRaw = q.episodeId;
  let episodeScope: "general" | "episode";
  let episodeNum: number | null = null;
  if (epRaw == null || epRaw === "" || epRaw === "general") {
    episodeScope = "general";
  } else {
    const n = Number(epRaw);
    if (!Number.isInteger(n) || n < 1) {
      reply.code(400);
      return { error: "episodeId must be a positive integer or 'general'" };
    }
    episodeScope = "episode";
    episodeNum = n;
  }

  const whereExtra =
    episodeScope === "general" ? "p.tvmaze_episode_id IS NULL" : "p.tvmaze_episode_id = ?";
  const sql = `SELECT p.id, p.user_id, p.tvmaze_show_id, p.show_name, p.tvmaze_episode_id, p.episode_label,
              p.body_html, p.is_spoiler, p.created_at, p.edited_at, p.edited_by_user_id,
              p.parent_post_id, p.tag,
              au.display_name AS authorDisplayName, au.username AS authorUsername, au.avatar_data_url AS authorAvatarDataUrl,
              eu.display_name AS editorDisplayName, eu.username AS editorUsername
       FROM community_posts p
       JOIN users au ON au.id = p.user_id
       LEFT JOIN users eu ON eu.id = p.edited_by_user_id
       WHERE p.tvmaze_show_id = ? AND p.deleted_at IS NULL AND ${whereExtra}
       ORDER BY datetime(p.created_at) ${sort}, p.id ${sort}`;

  const rows = (
    episodeScope === "general"
      ? db.prepare(sql).all(showId)
      : db.prepare(sql).all(showId, episodeNum)
  ) as CommunityPostRow[];

  const sid = sessionUserIdFromRequest(request);
  let subscribed = false;
  if (sid) {
    const sub = db
      .prepare(`SELECT 1 FROM community_thread_push_subs WHERE user_id = ? AND tvmaze_show_id = ?`)
      .get(sid, showId);
    subscribed = Boolean(sub);
  }

  let showName = resolveCommunityShowName(showId, sid);
  if (!showName) {
    try {
      const detail = await fetchShow(showId);
      showName = detail.name?.trim() ?? "";
    } catch {
      showName = "";
    }
  }

  const postCountRow = (
    episodeScope === "general"
      ? db
          .prepare(
            `SELECT COUNT(*) AS c FROM community_posts WHERE tvmaze_show_id = ? AND deleted_at IS NULL AND tvmaze_episode_id IS NULL`,
          )
          .get(showId)
      : db
          .prepare(
            `SELECT COUNT(*) AS c FROM community_posts WHERE tvmaze_show_id = ? AND deleted_at IS NULL AND tvmaze_episode_id = ?`,
          )
          .get(showId, episodeNum)
  ) as { c: number };
  const postCount = Number(postCountRow?.c ?? 0);

  let threadTitle = "Discussion";
  let threadSubtitle = showName;
  if (episodeScope === "episode" && episodeNum != null) {
    const epRow = db
      .prepare(
        `SELECT name, season, number FROM episodes_cache WHERE tvmaze_show_id = ? AND tvmaze_episode_id = ?`,
      )
      .get(showId, episodeNum) as { name: string; season: number; number: number } | undefined;
    if (epRow) {
      threadTitle = communityThreadTitleDiscussion(epRow.season, epRow.number);
      const epName = epRow.name || null;
      threadSubtitle = [epName, showName].filter(Boolean).join(" · ");
    } else {
      threadTitle = "Episode discussion";
      threadSubtitle = showName;
    }
  } else {
    threadTitle = "General discussion";
    threadSubtitle = showName;
  }

  let showImageUrl: string | null = null;
  let showStatus: string | null = null;
  try {
    const detail = await fetchShow(showId);
    showImageUrl = detail.image?.original ?? detail.image?.medium ?? null;
    showStatus = detail.status ?? null;
  } catch {
    /* TVMaze unavailable — thread still works */
  }

  let canSubscribeThread = rows.length > 0;
  if (sid && !canSubscribeThread) {
    const onList = db
      .prepare(`SELECT 1 FROM show_subscriptions WHERE user_id = ? AND tvmaze_show_id = ?`)
      .get(sid, showId);
    canSubscribeThread = Boolean(onList);
  }

  let viewerCatchUp: { subscriptionId: string; episodesBehind: number; bingeLater: boolean } | null = null;
  let aheadSet: Set<number> | null = null;
  if (sid) {
    const ur = db.prepare(`SELECT timezone FROM users WHERE id = ?`).get(sid) as { timezone: string } | undefined;
    const todayStr = safeTodayInTimeZone(ur?.timezone);
    aheadSet = getCatchUpAheadEpisodeIdSet(showId, sid, todayStr);
    const subRow = db
      .prepare(
        `SELECT id, community_episodes_behind, binge_later FROM show_subscriptions WHERE user_id = ? AND tvmaze_show_id = ?`,
      )
      .get(sid, showId) as { id: string; community_episodes_behind: number | null; binge_later: number | null } | undefined;
    if (subRow) {
      viewerCatchUp = {
        subscriptionId: subRow.id,
        episodesBehind: Math.max(0, Number(subRow.community_episodes_behind ?? 0)),
        bingeLater: subRow.binge_later === 1,
      };
    }
  }

  const posts = rows.map((r) => {
    const base = formatCommunityPost(r);
    const catchUpSpoiler =
      aheadSet != null && r.tvmaze_episode_id != null && aheadSet.has(r.tvmaze_episode_id);
    return { ...base, catchUpSpoiler };
  });

  const liveAirNight =
    episodeScope === "episode" && episodeNum != null && isEpisodeLiveAirNightWindow(showId, episodeNum);

  return {
    tvmazeShowId: showId,
    showName,
    tvmazeEpisodeId: episodeScope === "episode" ? episodeNum : null,
    episodeScope,
    threadTitle,
    threadSubtitle,
    postCount,
    showImageUrl,
    showStatus,
    subscribed,
    canSubscribeThread,
    viewerCatchUp,
    liveAirNight,
    posts,
  };
});

type EpisodePollRow = {
  id: string;
  user_id: string;
  question: string;
  options_json: string;
  correct_option_index: number | null;
  created_at: string;
};

function episodePollToJson(poll: EpisodePollRow, showId: number, episodeId: number, viewerId: string | undefined) {
  let options: string[];
  try {
    options = JSON.parse(poll.options_json) as string[];
  } catch {
    options = [];
  }
  const n = options.length;
  const voteCounts = new Array(n).fill(0);
  const agg = db
    .prepare(
      `SELECT option_index AS i, COUNT(*) AS c FROM community_episode_poll_votes WHERE poll_id = ? GROUP BY option_index`,
    )
    .all(poll.id) as { i: number; c: number }[];
  for (const r of agg) {
    if (r.i >= 0 && r.i < n) voteCounts[r.i] = Number(r.c);
  }
  let myVote: number | null = null;
  if (viewerId) {
    const v = db
      .prepare(`SELECT option_index FROM community_episode_poll_votes WHERE poll_id = ? AND user_id = ?`)
      .get(poll.id, viewerId) as { option_index: number } | undefined;
    if (v) myVote = v.option_index;
  }
  const author = db
    .prepare(`SELECT display_name, username FROM users WHERE id = ?`)
    .get(poll.user_id) as { display_name: string | null; username: string | null } | undefined;
  const votingOpen = isEpisodePollVotingOpen(showId, episodeId);
  return {
    id: poll.id,
    question: poll.question,
    options,
    voteCounts,
    totalVotes: voteCounts.reduce((a: number, b: number) => a + b, 0),
    myVote,
    votingOpen,
    locked: !votingOpen,
    correctOptionIndex: poll.correct_option_index,
    revealed: poll.correct_option_index != null,
    authorUserId: poll.user_id,
    authorHandle: authorPublicHandle(author ?? { display_name: null, username: null }),
    createdAt: poll.created_at,
  };
}

app.get("/api/community/threads/:showId/episode-polls", async (request, reply) => {
  const showId = Number((request.params as { showId: string }).showId);
  if (!Number.isInteger(showId) || showId < 1) {
    reply.code(400);
    return { error: "Invalid show id" };
  }
  const q = request.query as { episodeId?: string };
  const ep = Number(q.episodeId);
  if (!Number.isInteger(ep) || ep < 1) {
    reply.code(400);
    return { error: "episodeId required" };
  }
  const sid = sessionUserIdFromRequest(request);
  const rows = db
    .prepare(
      `SELECT id, user_id, question, options_json, correct_option_index, created_at
       FROM community_episode_polls WHERE tvmaze_show_id = ? AND tvmaze_episode_id = ?
       ORDER BY datetime(created_at) DESC`,
    )
    .all(showId, ep) as EpisodePollRow[];
  return {
    airdateKnown: episodeAirStartUtcMs(showId, ep) != null,
    votingOpen: isEpisodePollVotingOpen(showId, ep),
    polls: rows.map((r) => episodePollToJson(r, showId, ep, sid)),
  };
});

app.post("/api/community/episode-polls", async (request, reply) => {
  const uid = sessionRegisteredUserId(request, reply);
  if (!uid) return;
  if (!assertFullSocialAccess(reply, uid)) return;
  const body = (request.body ?? {}) as {
    tvmazeShowId?: number;
    tvmazeEpisodeId?: number;
    question?: string;
    options?: unknown;
  };
  const showId = Number(body.tvmazeShowId);
  const ep = Number(body.tvmazeEpisodeId);
  if (!Number.isInteger(showId) || showId < 1 || !Number.isInteger(ep) || ep < 1) {
    reply.code(400);
    return { error: "tvmazeShowId and tvmazeEpisodeId required" };
  }
  if (!isEpisodePollVotingOpen(showId, ep)) {
    reply.code(400);
    return { error: "Polls are locked after this episode’s listed air date (or the episode has no air date yet)." };
  }
  const label = await resolveCommunityEpisodeLabel(showId, ep);
  if (!label) {
    reply.code(400);
    return { error: "Episode not found for this show" };
  }
  const qRaw = typeof body.question === "string" ? body.question.trim() : "";
  if (!qRaw || qRaw.length > POLL_MAX_QUESTION) {
    reply.code(400);
    return { error: "Question required (max " + String(POLL_MAX_QUESTION) + " characters)" };
  }
  const options = normalizePollOptions(body.options);
  if (!options) {
    reply.code(400);
    return { error: "Provide 2–8 unique options (max length per option enforced)" };
  }
  const n = db
    .prepare(`SELECT COUNT(*) AS c FROM community_episode_polls WHERE tvmaze_show_id = ? AND tvmaze_episode_id = ?`)
    .get(showId, ep) as { c: number };
  if (Number(n.c) >= POLL_MAX_POLLS_PER_EPISODE) {
    reply.code(400);
    return { error: "Maximum prediction polls per episode reached" };
  }
  const id = uuidv4();
  db.prepare(
    `INSERT INTO community_episode_polls (id, tvmaze_show_id, tvmaze_episode_id, user_id, question, options_json)
     VALUES (?, ?, ?, ?, ?, ?)`,
  ).run(id, showId, ep, uid, qRaw, JSON.stringify(options));
  const row = db
    .prepare(
      `SELECT id, user_id, question, options_json, correct_option_index, created_at FROM community_episode_polls WHERE id = ?`,
    )
    .get(id) as EpisodePollRow | undefined;
  reply.code(201);
  return { poll: row ? episodePollToJson(row, showId, ep, uid) : { id } };
});

app.post("/api/community/episode-polls/:pollId/vote", async (request, reply) => {
  const uid = sessionRegisteredUserId(request, reply);
  if (!uid) return;
  if (!assertFullSocialAccess(reply, uid)) return;
  const { pollId } = request.params as { pollId: string };
  const body = (request.body ?? {}) as { optionIndex?: number };
  const optionIndex = Number(body.optionIndex);
  const poll = db
    .prepare(`SELECT id, tvmaze_show_id, tvmaze_episode_id, options_json FROM community_episode_polls WHERE id = ?`)
    .get(pollId) as { id: string; tvmaze_show_id: number; tvmaze_episode_id: number; options_json: string } | undefined;
  if (!poll) {
    reply.code(404);
    return { error: "Poll not found" };
  }
  if (!isEpisodePollVotingOpen(poll.tvmaze_show_id, poll.tvmaze_episode_id)) {
    reply.code(400);
    return { error: "Voting is closed for this episode" };
  }
  let optCount = 0;
  try {
    optCount = (JSON.parse(poll.options_json) as string[]).length;
  } catch {
    reply.code(500);
    return { error: "Invalid poll" };
  }
  if (!Number.isInteger(optionIndex) || optionIndex < 0 || optionIndex >= optCount) {
    reply.code(400);
    return { error: "Invalid option" };
  }
  db.prepare(
    `INSERT INTO community_episode_poll_votes (poll_id, user_id, option_index) VALUES (?, ?, ?)
     ON CONFLICT(poll_id, user_id) DO UPDATE SET option_index = excluded.option_index`,
  ).run(pollId, uid, optionIndex);
  const row = db
    .prepare(
      `SELECT id, user_id, question, options_json, correct_option_index, created_at FROM community_episode_polls WHERE id = ?`,
    )
    .get(pollId) as EpisodePollRow | undefined;
  return { poll: row ? episodePollToJson(row, poll.tvmaze_show_id, poll.tvmaze_episode_id, uid) : null };
});

app.patch("/api/community/episode-polls/:pollId", async (request, reply) => {
  const uid = sessionRegisteredUserId(request, reply);
  if (!uid) return;
  if (!assertFullSocialAccess(reply, uid)) return;
  const { pollId } = request.params as { pollId: string };
  const body = (request.body ?? {}) as { correctOptionIndex?: number };
  const poll = db
    .prepare(
      `SELECT id, user_id, tvmaze_show_id, tvmaze_episode_id, options_json, correct_option_index FROM community_episode_polls WHERE id = ?`,
    )
    .get(pollId) as
    | {
        id: string;
        user_id: string;
        tvmaze_show_id: number;
        tvmaze_episode_id: number;
        options_json: string;
        correct_option_index: number | null;
      }
    | undefined;
  if (!poll) {
    reply.code(404);
    return { error: "Poll not found" };
  }
  if (poll.user_id !== uid) {
    reply.code(403);
    return { error: "Only the poll author can reveal the answer" };
  }
  if (isEpisodePollVotingOpen(poll.tvmaze_show_id, poll.tvmaze_episode_id)) {
    reply.code(400);
    return { error: "Reveal the outcome after the episode airs (polls lock on the listed air date)" };
  }
  if (poll.correct_option_index != null) {
    reply.code(400);
    return { error: "Answer already set" };
  }
  const idx = Number(body.correctOptionIndex);
  let len = 0;
  try {
    len = (JSON.parse(poll.options_json) as string[]).length;
  } catch {
    reply.code(500);
    return { error: "Invalid poll" };
  }
  if (!Number.isInteger(idx) || idx < 0 || idx >= len) {
    reply.code(400);
    return { error: "Invalid correctOptionIndex" };
  }
  db.prepare(`UPDATE community_episode_polls SET correct_option_index = ? WHERE id = ?`).run(idx, pollId);
  const row = db
    .prepare(
      `SELECT id, user_id, question, options_json, correct_option_index, created_at FROM community_episode_polls WHERE id = ?`,
    )
    .get(pollId) as EpisodePollRow | undefined;
  return { poll: row ? episodePollToJson(row, poll.tvmaze_show_id, poll.tvmaze_episode_id, uid) : null };
});

app.delete("/api/community/episode-polls/:pollId", async (request, reply) => {
  const uid = sessionRegisteredUserId(request, reply);
  if (!uid) return;
  if (!assertFullSocialAccess(reply, uid)) return;
  const { pollId } = request.params as { pollId: string };
  const poll = db
    .prepare(`SELECT user_id, tvmaze_show_id, tvmaze_episode_id FROM community_episode_polls WHERE id = ?`)
    .get(pollId) as { user_id: string; tvmaze_show_id: number; tvmaze_episode_id: number } | undefined;
  if (!poll) {
    reply.code(404);
    return { error: "Poll not found" };
  }
  if (poll.user_id !== uid) {
    reply.code(403);
    return { error: "Only the author can delete this poll" };
  }
  if (!isEpisodePollVotingOpen(poll.tvmaze_show_id, poll.tvmaze_episode_id)) {
    reply.code(400);
    return { error: "Cannot delete after the episode airs" };
  }
  db.prepare(`DELETE FROM community_episode_polls WHERE id = ?`).run(pollId);
  return { ok: true };
});

function seasonEpisodeRatingsForChart(showId: number, season: number): {
  tvmazeEpisodeId: number;
  season: number;
  number: number;
  episodeName: string;
  label: string;
  avgRating: number | null;
  ratingCount: number;
}[] {
  const rows = db
    .prepare(
      `SELECT e.tvmaze_episode_id AS tvmazeEpisodeId, e.season AS season, e.number AS number, e.name AS episodeName,
              (SELECT AVG(rating) FROM community_episode_ratings r
               WHERE r.tvmaze_show_id = e.tvmaze_show_id AND r.tvmaze_episode_id = e.tvmaze_episode_id) AS avgRating,
              (SELECT COUNT(*) FROM community_episode_ratings r2
               WHERE r2.tvmaze_show_id = e.tvmaze_show_id AND r2.tvmaze_episode_id = e.tvmaze_episode_id) AS ratingCount
       FROM episodes_cache e
       WHERE e.tvmaze_show_id = ? AND e.season = ?
         AND e.airdate IS NOT NULL AND trim(e.airdate) != ''
         AND date(e.airdate) <= date('now')
       ORDER BY e.number ASC`,
    )
    .all(showId, season) as {
    tvmazeEpisodeId: number;
    season: number;
    number: number;
    episodeName: string;
    avgRating: number | null;
    ratingCount: number;
  }[];
  return rows.map((r) => ({
    tvmazeEpisodeId: r.tvmazeEpisodeId,
    season: r.season,
    number: r.number,
    episodeName: r.episodeName,
    label: `S${r.season}E${r.number}`,
    avgRating: r.avgRating != null ? Math.round(Number(r.avgRating) * 100) / 100 : null,
    ratingCount: Number(r.ratingCount) || 0,
  }));
}

/** Public aggregate — community sentiment by episode for charts and embeds. */
app.get("/api/community/shows/:showId/episode-ratings-summary", async (request, reply) => {
  const showId = Number((request.params as { showId: string }).showId);
  const season = Number((request.query as { season?: string }).season);
  if (!Number.isInteger(showId) || showId < 1 || !Number.isInteger(season)) {
    reply.code(400);
    return { error: "Invalid show id or season" };
  }
  let showName = resolveCommunityShowName(showId, undefined);
  if (!showName) {
    try {
      const d = await fetchShow(showId);
      showName = d.name?.trim() ?? "";
    } catch {
      showName = "";
    }
  }
  const episodes = seasonEpisodeRatingsForChart(showId, season);
  return { tvmazeShowId: showId, showName, season, episodes };
});

app.get("/api/community/shows/:showId/episode-ratings-seasons", async (request, reply) => {
  const showId = Number((request.params as { showId: string }).showId);
  if (!Number.isInteger(showId) || showId < 1) {
    reply.code(400);
    return { error: "Invalid show id" };
  }
  const rows = db
    .prepare(
      `SELECT DISTINCT season FROM episodes_cache
       WHERE tvmaze_show_id = ?
         AND airdate IS NOT NULL AND trim(airdate) != ''
         AND date(airdate) <= date('now')
       ORDER BY season ASC`,
    )
    .all(showId) as { season: number }[];
  return { seasons: rows.map((r) => Number(r.season)) };
});

app.get("/api/community/shows/:showId/episode-rating", async (request, reply) => {
  const showId = Number((request.params as { showId: string }).showId);
  const ep = Number((request.query as { episodeId?: string }).episodeId);
  if (!Number.isInteger(showId) || showId < 1 || !Number.isInteger(ep) || ep < 1) {
    reply.code(400);
    return { error: "Invalid show or episode" };
  }
  const agg = db
    .prepare(
      `SELECT AVG(rating) AS a, COUNT(*) AS c FROM community_episode_ratings WHERE tvmaze_show_id = ? AND tvmaze_episode_id = ?`,
    )
    .get(showId, ep) as { a: number | null; c: number } | undefined;
  const sid = sessionUserIdFromRequest(request);
  let myRating: number | null = null;
  if (sid) {
    const m = db
      .prepare(
        `SELECT rating FROM community_episode_ratings WHERE user_id = ? AND tvmaze_show_id = ? AND tvmaze_episode_id = ?`,
      )
      .get(sid, showId, ep) as { rating: number } | undefined;
    if (m) myRating = m.rating;
  }
  const canRate = episodeHasAiredUtc(showId, ep);
  return {
    tvmazeShowId: showId,
    tvmazeEpisodeId: ep,
    canRate,
    myRating,
    avgRating:
      agg?.a != null && Number(agg.c) > 0 ? Math.round(Number(agg.a) * 100) / 100 : null,
    ratingCount: Number(agg?.c) || 0,
  };
});

app.put("/api/community/episode-ratings", async (request, reply) => {
  const uid = sessionRegisteredUserId(request, reply);
  if (!uid) return;
  if (!assertFullSocialAccess(reply, uid)) return;
  const body = (request.body ?? {}) as { tvmazeShowId?: number; tvmazeEpisodeId?: number; rating?: number };
  const showId = Number(body.tvmazeShowId);
  const ep = Number(body.tvmazeEpisodeId);
  const rating = Number(body.rating);
  if (!Number.isInteger(showId) || showId < 1 || !Number.isInteger(ep) || ep < 1) {
    reply.code(400);
    return { error: "Invalid show or episode" };
  }
  if (!Number.isInteger(rating) || rating < 1 || rating > 5) {
    reply.code(400);
    return { error: "Rating must be between 1 and 5" };
  }
  if (!episodeHasAiredUtc(showId, ep)) {
    reply.code(400);
    return { error: "Ratings open after the episode’s listed air date" };
  }
  const label = await resolveCommunityEpisodeLabel(showId, ep);
  if (!label) {
    reply.code(400);
    return { error: "Episode not found for this show" };
  }
  db.prepare(
    `INSERT INTO community_episode_ratings (user_id, tvmaze_show_id, tvmaze_episode_id, rating, updated_at)
     VALUES (?, ?, ?, ?, datetime('now'))
     ON CONFLICT(user_id, tvmaze_show_id, tvmaze_episode_id) DO UPDATE SET
       rating = excluded.rating,
       updated_at = excluded.updated_at`,
  ).run(uid, showId, ep, rating);
  const agg = db
    .prepare(
      `SELECT AVG(rating) AS a, COUNT(*) AS c FROM community_episode_ratings WHERE tvmaze_show_id = ? AND tvmaze_episode_id = ?`,
    )
    .get(showId, ep) as { a: number | null; c: number };
  return {
    tvmazeShowId: showId,
    tvmazeEpisodeId: ep,
    canRate: true,
    myRating: rating,
    avgRating: agg.a != null ? Math.round(Number(agg.a) * 100) / 100 : null,
    ratingCount: Number(agg.c) || 0,
  };
});

/**
 * After Tasks → Watched: save episode rating and/or publish a Community review in one shot.
 * - One review post per user per episode (`tag=episode_review`): create or update body on repeat submit.
 * - Rating-only updates `community_episode_ratings` without creating a post.
 * - Written review without a new rating uses the stored rating for the header line when present.
 */
app.post("/api/community/post-watch-review", async (request, reply) => {
  const uid = sessionRegisteredUserId(request, reply);
  if (!uid) return;
  if (!assertFullSocialAccess(reply, uid)) return;
  const body = (request.body ?? {}) as {
    tvmazeShowId?: number;
    tvmazeEpisodeId?: number;
    rating?: number | null;
    reviewText?: string | null;
  };
  const showId = Number(body.tvmazeShowId);
  const ep = Number(body.tvmazeEpisodeId);
  const ratingRaw = body.rating;
  const reviewRaw = typeof body.reviewText === "string" ? body.reviewText : "";
  const reviewTrim = reviewRaw.trim().slice(0, 4000);

  if (!Number.isInteger(showId) || showId < 1 || !Number.isInteger(ep) || ep < 1) {
    reply.code(400);
    return { error: "Invalid show or episode" };
  }

  let ratingNum: number | null = null;
  if (ratingRaw != null && String(ratingRaw).trim() !== "") {
    const n = Number(ratingRaw);
    if (!Number.isInteger(n) || n < 1 || n > 5) {
      reply.code(400);
      return { error: "Rating must be between 1 and 5" };
    }
    ratingNum = n;
  }
  const hasRating = ratingNum != null;

  if (!hasRating && !reviewTrim) {
    reply.code(400);
    return { error: "Provide a rating and/or a written review" };
  }

  if (!episodeHasAiredUtc(showId, ep)) {
    reply.code(400);
    return { error: "Ratings open after the episode’s listed air date" };
  }

  const label = await resolveCommunityEpisodeLabel(showId, ep);
  if (!label) {
    reply.code(400);
    return { error: "Episode not found for this show" };
  }

  let ratingOut: {
    tvmazeShowId: number;
    tvmazeEpisodeId: number;
    myRating: number;
    avgRating: number | null;
    ratingCount: number;
  } | null = null;

  if (hasRating && ratingNum != null) {
    db.prepare(
      `INSERT INTO community_episode_ratings (user_id, tvmaze_show_id, tvmaze_episode_id, rating, updated_at)
       VALUES (?, ?, ?, ?, datetime('now'))
       ON CONFLICT(user_id, tvmaze_show_id, tvmaze_episode_id) DO UPDATE SET
         rating = excluded.rating,
         updated_at = excluded.updated_at`,
    ).run(uid, showId, ep, ratingNum);
    const agg = db
      .prepare(
        `SELECT AVG(rating) AS a, COUNT(*) AS c FROM community_episode_ratings WHERE tvmaze_show_id = ? AND tvmaze_episode_id = ?`,
      )
      .get(showId, ep) as { a: number | null; c: number };
    ratingOut = {
      tvmazeShowId: showId,
      tvmazeEpisodeId: ep,
      myRating: ratingNum,
      avgRating: agg.a != null ? Math.round(Number(agg.a) * 100) / 100 : null,
      ratingCount: Number(agg.c) || 0,
    };
  }

  let postFormatted: ReturnType<typeof formatCommunityPost> | null = null;
  let postUpdated = false;

  if (reviewTrim) {
    const stored = db
      .prepare(
        `SELECT rating FROM community_episode_ratings WHERE user_id = ? AND tvmaze_show_id = ? AND tvmaze_episode_id = ?`,
      )
      .get(uid, showId, ep) as { rating: number } | undefined;
    const starsN = ratingNum ?? stored?.rating ?? null;
    const starsStr =
      starsN != null && starsN >= 1 && starsN <= 5
        ? `${"\u2605".repeat(starsN)}${"\u2606".repeat(5 - starsN)}`
        : "";
    const ratingLine =
      starsN != null
        ? `<p><strong>Episode rating:</strong> ${starsN}/5 <span aria-hidden="true">${starsStr}</span></p>`
        : "";
    const paras = reviewTrim
      .split(/\n+/)
      .map((line) => sanitizeCommunityHtml(line))
      .filter((x) => stripHtml(x).length > 0)
      .map((line) => `<p>${line}</p>`)
      .join("");
    const bodyHtml = ratingLine + paras;
    if (!stripHtml(bodyHtml)) {
      reply.code(400);
      return { error: "Review cannot be empty" };
    }

    let showDetail: Awaited<ReturnType<typeof fetchShow>>;
    try {
      showDetail = await fetchShow(showId);
    } catch {
      reply.code(400);
      return { error: "Could not verify show with TVMaze" };
    }
    const showName = showDetail.name?.trim() || "Unknown show";

    const existing = db
      .prepare(
        `SELECT id FROM community_posts WHERE user_id = ? AND tvmaze_show_id = ? AND tvmaze_episode_id = ? AND tag = 'episode_review' AND deleted_at IS NULL`,
      )
      .get(uid, showId, ep) as { id: string } | undefined;

    const tx = db.transaction(() => {
      if (existing) {
        db.prepare(
          `UPDATE community_posts SET body_html = ?, edited_at = datetime('now'), edited_by_user_id = ?, show_name = ?, episode_label = ? WHERE id = ?`,
        ).run(bodyHtml, uid, showName, label, existing.id);
        postUpdated = true;
      } else {
        const id = uuidv4();
        db.prepare(
          `INSERT INTO community_posts (id, user_id, tvmaze_show_id, show_name, tvmaze_episode_id, episode_label, body_html, is_spoiler, parent_post_id, tag)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
        ).run(id, uid, showId, showName, ep, label, bodyHtml, 0, null, "episode_review");
      }
    });
    tx();

    const postId = existing?.id ?? (db.prepare(`SELECT id FROM community_posts WHERE user_id = ? AND tvmaze_show_id = ? AND tvmaze_episode_id = ? AND tag = 'episode_review' AND deleted_at IS NULL`).get(uid, showId, ep) as { id: string } | undefined)?.id;
    if (!postId) {
      reply.code(500);
      return { error: "Could not save review" };
    }

    if (!existing) {
      const author = db
        .prepare(`SELECT display_name, username FROM users WHERE id = ?`)
        .get(uid) as { display_name: string | null; username: string | null } | undefined;
      const authorLabel = authorPublicHandle(author ?? { display_name: null, username: null });
      await notifyCommunityThreadSubscribers({
        tvmazeShowId: showId,
        showName,
        authorUserId: uid,
        authorLabel,
        tvmazeEpisodeId: ep,
        episodeLabel: label,
      });
      await notifyCommunityMentionedUsers({
        bodyHtml,
        previousBodyHtml: null,
        taggerUserId: uid,
        taggerLabel: authorLabel,
        tvmazeShowId: showId,
        showName,
        tvmazeEpisodeId: ep,
        postId,
      });
    }

    const row = db
      .prepare(
        `SELECT p.id, p.user_id, p.tvmaze_show_id, p.show_name, p.tvmaze_episode_id, p.episode_label,
                p.body_html, p.is_spoiler, p.created_at, p.edited_at, p.edited_by_user_id,
                p.parent_post_id, p.tag,
                au.display_name AS authorDisplayName, au.username AS authorUsername, au.avatar_data_url AS authorAvatarDataUrl,
                eu.display_name AS editorDisplayName, eu.username AS editorUsername
         FROM community_posts p
         JOIN users au ON au.id = p.user_id
         LEFT JOIN users eu ON eu.id = p.edited_by_user_id
         WHERE p.id = ?`,
      )
      .get(postId) as CommunityPostRow & { parent_post_id: string | null; tag: string | null };
    if (row) postFormatted = formatCommunityPost(row as CommunityPostRow);
  }

  return {
    ok: true,
    rating: ratingOut,
    post: postFormatted,
    postUpdated,
  };
});

app.post("/api/community/posts", async (request, reply) => {
  const uid = sessionRegisteredUserId(request, reply);
  if (!uid) return;
  if (!assertFullSocialAccess(reply, uid)) return;
  const body = (request.body ?? {}) as {
    tvmazeShowId?: number;
    bodyHtml?: string;
    isSpoiler?: boolean;
    tvmazeEpisodeId?: number | null;
    episodeLabel?: string | null;
    parentPostId?: string | null;
    tag?: string | null;
  };
  const tvmazeShowId = Number(body.tvmazeShowId);
  if (!Number.isInteger(tvmazeShowId) || tvmazeShowId < 1) {
    reply.code(400);
    return { error: "tvmazeShowId required" };
  }
  const bodyHtml = sanitizeCommunityHtml(typeof body.bodyHtml === "string" ? body.bodyHtml : "");
  if (!stripHtml(bodyHtml)) {
    reply.code(400);
    return { error: "Post cannot be empty" };
  }
  let episodeLabel: string | null = null;
  let tvmazeEpisodeId: number | null = null;
  if (body.tvmazeEpisodeId != null) {
    const ep = Number(body.tvmazeEpisodeId);
    if (Number.isInteger(ep) && ep > 0) {
      const label = await resolveCommunityEpisodeLabel(tvmazeShowId, ep);
      if (!label) {
        reply.code(400);
        return { error: "Episode not found or does not belong to this show" };
      }
      tvmazeEpisodeId = ep;
      episodeLabel = label;
    }
  } else if (typeof body.episodeLabel === "string" && body.episodeLabel.trim()) {
    episodeLabel = body.episodeLabel.trim().slice(0, 48);
  }
  let showDetail;
  try {
    showDetail = await fetchShow(tvmazeShowId);
  } catch {
    reply.code(400);
    return { error: "Could not verify show with TVMaze" };
  }
  const showName = showDetail.name?.trim() || "Unknown show";
  const id = uuidv4();
  const isSpoiler = body.isSpoiler === true ? 1 : 0;
  const parentPostId = typeof body.parentPostId === "string" && body.parentPostId.trim() ? body.parentPostId.trim() : null;
  const allowedTags = ["theory", "spoiler-free", "hot-take"];
  const tag = typeof body.tag === "string" && allowedTags.includes(body.tag) ? body.tag : null;

  db.prepare(
    `INSERT INTO community_posts (id, user_id, tvmaze_show_id, show_name, tvmaze_episode_id, episode_label, body_html, is_spoiler, parent_post_id, tag)
     VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
  ).run(id, uid, tvmazeShowId, showName, tvmazeEpisodeId, episodeLabel, bodyHtml, isSpoiler, parentPostId, tag);

  const author = db
    .prepare(`SELECT display_name, username FROM users WHERE id = ?`)
    .get(uid) as { display_name: string | null; username: string | null };
  const authorLabel = authorPublicHandle(author);
  await notifyCommunityThreadSubscribers({
    tvmazeShowId,
    showName,
    authorUserId: uid,
    authorLabel,
    tvmazeEpisodeId,
    episodeLabel,
  });
  await notifyCommunityMentionedUsers({
    bodyHtml,
    previousBodyHtml: null,
    taggerUserId: uid,
    taggerLabel: authorLabel,
    tvmazeShowId,
    showName,
    tvmazeEpisodeId,
    postId: id,
  });
  if (parentPostId) {
    const parentPost = db.prepare(`SELECT user_id FROM community_posts WHERE id = ?`).get(parentPostId) as { user_id: string } | undefined;
    if (parentPost && parentPost.user_id !== uid) {
      let url = tvmazeEpisodeId != null
        ? `/?communityShow=${tvmazeShowId}&communityEpisode=${tvmazeEpisodeId}`
        : `/?communityShow=${tvmazeShowId}`;
      url += `&communityPostId=${encodeURIComponent(id)}`;
      await sendWebPushToUser(parentPost.user_id, {
        title: "New reply to your post",
        body: `${authorLabel} replied to your post in ${showName}`,
        url,
      }, { kind: "communityThreadNewPost" });
      insertActivityNotification({
        recipientUserId: parentPost.user_id,
        kind: "community_reply",
        title: "New reply",
        summary: `${authorLabel} replied in ${showName}`,
        url,
        actorUserId: uid,
        sourcePostId: id,
      });
    }
  }

  const row = db
    .prepare(
      `SELECT p.id, p.user_id, p.tvmaze_show_id, p.show_name, p.tvmaze_episode_id, p.episode_label,
              p.body_html, p.is_spoiler, p.created_at, p.edited_at, p.edited_by_user_id,
              p.parent_post_id, p.tag,
              au.display_name AS authorDisplayName, au.username AS authorUsername, au.avatar_data_url AS authorAvatarDataUrl,
              eu.display_name AS editorDisplayName, eu.username AS editorUsername
       FROM community_posts p
       JOIN users au ON au.id = p.user_id
       LEFT JOIN users eu ON eu.id = p.edited_by_user_id
       WHERE p.id = ?`,
    )
    .get(id) as CommunityPostRow | undefined;
  reply.code(201);
  return { post: row ? formatCommunityPost(row) : { id } };
});

app.patch("/api/community/posts/:postId", async (request, reply) => {
  const { postId } = request.params as { postId: string };
  const sid = sessionUserIdFromRequest(request);
  const admin = isRequestAdmin(request);
  if (!sid && !admin) {
    reply.code(401);
    return { error: "Sign in required" };
  }
  const cur = db.prepare(`SELECT * FROM community_posts WHERE id = ? AND deleted_at IS NULL`).get(postId) as
    | {
        id: string;
        user_id: string;
        tvmaze_show_id: number;
        show_name: string;
        body_html: string;
        is_spoiler: number;
        tvmaze_episode_id: number | null;
        episode_label: string | null;
      }
    | undefined;
  if (!cur) {
    reply.code(404);
    return { error: "Post not found" };
  }
  const owner = sid === cur.user_id;
  if (!owner && !admin) {
    reply.code(403);
    return { error: "Forbidden" };
  }
  if (owner && !admin) {
    const reg = sessionRegisteredUserId(request, reply);
    if (!reg) return;
    if (!assertFullSocialAccess(reply, reg)) return;
  }
  const body = (request.body ?? {}) as {
    bodyHtml?: string;
    isSpoiler?: boolean;
    tvmazeShowId?: number;
    tvmazeEpisodeId?: number | null;
  };
  let newShowId = cur.tvmaze_show_id;
  let newShowName = cur.show_name;
  if (admin && body.tvmazeShowId != null) {
    const mv = Number(body.tvmazeShowId);
    if (!Number.isInteger(mv) || mv < 1) {
      reply.code(400);
      return { error: "Invalid tvmazeShowId" };
    }
    try {
      const det = await fetchShow(mv);
      newShowId = mv;
      newShowName = det.name?.trim() || "Unknown show";
    } catch {
      reply.code(400);
      return { error: "Could not verify show with TVMaze" };
    }
  } else if (!admin && body.tvmazeShowId != null && body.tvmazeShowId !== cur.tvmaze_show_id) {
    reply.code(403);
    return { error: "Only admins can move posts to another show" };
  }
  const movedShow = newShowId !== cur.tvmaze_show_id;

  let finHtml = cur.body_html;
  if (typeof body.bodyHtml === "string") {
    const cleaned = sanitizeCommunityHtml(body.bodyHtml);
    if (!stripHtml(cleaned)) {
      reply.code(400);
      return { error: "Post cannot be empty" };
    }
    finHtml = cleaned;
  }
  const bodyChanged = finHtml !== cur.body_html;

  let finSpoiler = cur.is_spoiler;
  if (typeof body.isSpoiler === "boolean") {
    finSpoiler = body.isSpoiler ? 1 : 0;
  }
  const spoilerChanged = typeof body.isSpoiler === "boolean" && finSpoiler !== cur.is_spoiler;

  let finEpId: number | null = cur.tvmaze_episode_id;
  let finEpLabel: string | null = cur.episode_label;
  let episodeChanged = false;
  if ("tvmazeEpisodeId" in body && body.tvmazeEpisodeId === null) {
    finEpId = null;
    finEpLabel = null;
    episodeChanged = cur.tvmaze_episode_id != null || cur.episode_label != null;
  } else if (typeof body.tvmazeEpisodeId === "number" && Number.isInteger(body.tvmazeEpisodeId) && body.tvmazeEpisodeId > 0) {
    const label = await resolveCommunityEpisodeLabel(newShowId, body.tvmazeEpisodeId);
    if (!label) {
      reply.code(400);
      return { error: "Episode not found or does not belong to this show" };
    }
    finEpId = body.tvmazeEpisodeId;
    finEpLabel = label;
    episodeChanged = finEpId !== cur.tvmaze_episode_id || finEpLabel !== cur.episode_label;
  } else if (movedShow && (cur.tvmaze_episode_id != null || cur.episode_label)) {
    finEpId = null;
    finEpLabel = null;
    episodeChanged = true;
  }

  const touchEdit = bodyChanged || spoilerChanged || movedShow || episodeChanged;
  const editorId = sid || null;

  if (touchEdit) {
    db.prepare(
      `UPDATE community_posts SET
        body_html = ?,
        is_spoiler = ?,
        tvmaze_show_id = ?,
        show_name = ?,
        tvmaze_episode_id = ?,
        episode_label = ?,
        edited_at = datetime('now'),
        edited_by_user_id = ?
       WHERE id = ?`,
    ).run(finHtml, finSpoiler, newShowId, newShowName, finEpId, finEpLabel, editorId, postId);

    logCommunityModeration({
      postId,
      actorUserId: editorId,
      action: "post_edit",
      detail: {
        bodyChanged,
        spoilerChanged,
        movedShow: movedShow ? { fromShowId: cur.tvmaze_show_id, toShowId: newShowId } : undefined,
        episodeChanged,
      },
    });

    if (bodyChanged && editorId) {
      const taggerRow = db
        .prepare(`SELECT display_name, username FROM users WHERE id = ?`)
        .get(editorId) as { display_name: string | null; username: string | null } | undefined;
      const taggerLabel = taggerRow ? authorPublicHandle(taggerRow) : "Someone";
      await notifyCommunityMentionedUsers({
        bodyHtml: finHtml,
        previousBodyHtml: cur.body_html,
        taggerUserId: editorId,
        taggerLabel,
        tvmazeShowId: newShowId,
        showName: newShowName,
        tvmazeEpisodeId: finEpId,
        postId,
      });
    }
  }

  const row = db
    .prepare(
      `SELECT p.id, p.user_id, p.tvmaze_show_id, p.show_name, p.tvmaze_episode_id, p.episode_label,
              p.body_html, p.is_spoiler, p.created_at, p.edited_at, p.edited_by_user_id,
              au.display_name AS authorDisplayName, au.username AS authorUsername, au.avatar_data_url AS authorAvatarDataUrl,
              eu.display_name AS editorDisplayName, eu.username AS editorUsername
       FROM community_posts p
       JOIN users au ON au.id = p.user_id
       LEFT JOIN users eu ON eu.id = p.edited_by_user_id
       WHERE p.id = ?`,
    )
    .get(postId) as CommunityPostRow;
  return { post: formatCommunityPost(row) };
});

app.delete("/api/community/posts/:postId", async (request, reply) => {
  const { postId } = request.params as { postId: string };
  const sid = sessionUserIdFromRequest(request);
  const admin = isRequestAdmin(request);
  if (!sid && !admin) {
    reply.code(401);
    return { error: "Sign in required" };
  }
  const post = db
    .prepare(`SELECT user_id, tvmaze_show_id FROM community_posts WHERE id = ? AND deleted_at IS NULL`)
    .get(postId) as { user_id: string; tvmaze_show_id: number } | undefined;
  if (!post) {
    reply.code(404);
    return { error: "Not found" };
  }
  if (post.user_id !== sid && !admin) {
    reply.code(403);
    return { error: "Forbidden" };
  }
  if (!admin) {
    const reg = sessionRegisteredUserId(request, reply);
    if (!reg) return;
    if (!assertFullSocialAccess(reply, reg)) return;
  }
  logCommunityModeration({
    postId,
    actorUserId: sid || null,
    action: "post_delete",
    detail: { authorUserId: post.user_id, tvmazeShowId: post.tvmaze_show_id },
  });
  db.prepare(`UPDATE community_posts SET deleted_at = datetime('now') WHERE id = ?`).run(postId);
  return { ok: true };
});

app.get("/api/community/thread-subscriptions/:showId", async (request, reply) => {
  const showId = Number((request.params as { showId: string }).showId);
  if (!Number.isInteger(showId) || showId < 1) {
    reply.code(400);
    return { error: "Invalid show id" };
  }
  const sid = sessionUserIdFromRequest(request);
  if (!sid) {
    return { subscribed: false };
  }
  const sub = db
    .prepare(`SELECT 1 FROM community_thread_push_subs WHERE user_id = ? AND tvmaze_show_id = ?`)
    .get(sid, showId);
  return { subscribed: Boolean(sub) };
});

app.post("/api/community/thread-subscriptions", async (request, reply) => {
  const uid = sessionRegisteredUserId(request, reply);
  if (!uid) return;
  if (!assertFullSocialAccess(reply, uid)) return;
  const body = (request.body ?? {}) as { tvmazeShowId?: number };
  const showId = Number(body.tvmazeShowId);
  if (!Number.isInteger(showId) || showId < 1) {
    reply.code(400);
    return { error: "tvmazeShowId required" };
  }
  const hasPosts = db
    .prepare(`SELECT 1 FROM community_posts WHERE tvmaze_show_id = ? AND deleted_at IS NULL LIMIT 1`)
    .get(showId);
  const onMyList = db
    .prepare(`SELECT 1 FROM show_subscriptions WHERE user_id = ? AND tvmaze_show_id = ? LIMIT 1`)
    .get(uid, showId);
  if (!hasPosts && !onMyList) {
    reply.code(400);
    return { error: "Add this show to My List, or wait for the first post, to subscribe to thread alerts" };
  }
  const id = uuidv4();
  try {
    db.prepare(`INSERT INTO community_thread_push_subs (id, user_id, tvmaze_show_id) VALUES (?, ?, ?)`).run(id, uid, showId);
  } catch {
    /* unique */
  }
  return { ok: true, subscribed: true };
});

app.delete("/api/community/thread-subscriptions/:showId", async (request, reply) => {
  const uid = sessionRegisteredUserId(request, reply);
  if (!uid) return;
  if (!assertFullSocialAccess(reply, uid)) return;
  const showId = Number((request.params as { showId: string }).showId);
  if (!Number.isInteger(showId) || showId < 1) {
    reply.code(400);
    return { error: "Invalid show id" };
  }
  db.prepare(`DELETE FROM community_thread_push_subs WHERE user_id = ? AND tvmaze_show_id = ?`).run(uid, showId);
  return { ok: true, subscribed: false };
});

app.get("/api/dm/ws", { websocket: true }, (socket: WebSocket, request: FastifyRequest) => {
  const uid = getRegisteredSessionUserId(request);
  if (!uid) {
    socket.close(4401, "Unauthorized");
    return;
  }
  if (!hasFullSocialAccess(uid)) {
    socket.close(4403, UNLOCK_SOCIAL_FEATURES_MESSAGE);
    return;
  }
  registerDmSocket(uid, socket);
  socket.on("message", (data) => {
    handleDmClientSocketMessage(uid, data);
  });
  socket.on("close", () => unregisterDmSocket(uid, socket));
  socket.on("error", () => unregisterDmSocket(uid, socket));
});

app.get("/api/community/live-rooms/summary", async (_request: FastifyRequest, reply) => {
  const summary = getLiveRoomSummary();
  const enriched = summary.rooms.slice(0, 8).map((r) => {
    let showName = "";
    let episodeLabel = "";
    const showRow = db
      .prepare(`SELECT name FROM shows WHERE tvmaze_id = ?`)
      .get(r.showId) as { name: string } | undefined;
    if (showRow) showName = showRow.name;
    if (r.tvmazeEpisodeId != null) {
      const epRow = db
        .prepare(
          `SELECT season, number, name FROM episodes_cache WHERE tvmaze_show_id = ? AND tvmaze_episode_id = ?`,
        )
        .get(r.showId, r.tvmazeEpisodeId) as { season: number; number: number; name: string | null } | undefined;
      if (epRow) {
        episodeLabel =
          "S" + String(epRow.season).padStart(2, "0") + "E" + String(epRow.number).padStart(2, "0");
      }
    }
    return {
      showId: r.showId,
      tvmazeEpisodeId: r.tvmazeEpisodeId,
      showName,
      episodeLabel,
      viewerCount: r.viewerCount,
      liveAirNight: r.liveAirNight,
    };
  });
  return reply.send({ totalViewers: summary.totalViewers, rooms: enriched });
});

/** Ephemeral live rail + “watching now” presence for a thread (show + episode scope). */
app.get("/api/community/thread-live/ws", { websocket: true }, (socket: WebSocket, request: FastifyRequest) => {
  const uid = getRegisteredSessionUserId(request);
  if (!uid) {
    socket.close(4401, "Unauthorized");
    return;
  }
  const q = request.query as { showId?: string; episode?: string };
  const parsed = parseThreadLiveRoomQuery(q);
  if (!parsed.ok) {
    socket.close(4400, parsed.error);
    return;
  }
  registerCommunityThreadLiveSocket(uid, socket, parsed.roomKey);
  socket.on("message", (data) => {
    handleCommunityThreadLiveMessage(uid, socket, data);
  });
  socket.on("close", () => unregisterCommunityThreadLiveSocket(socket));
  socket.on("error", () => unregisterCommunityThreadLiveSocket(socket));
});

app.get("/api/dm/unread", async (request, reply) => {
  const uid = sessionRegisteredUserId(request, reply);
  if (!uid) return;
  if (!assertFullSocialAccess(reply, uid)) return;
  return { total: getDmUnreadTotal(uid) };
});

app.get("/api/dm/threads", async (request, reply) => {
  const uid = sessionRegisteredUserId(request, reply);
  if (!uid) return;
  if (!assertFullSocialAccess(reply, uid)) return;
  return { threads: listDmThreadsForUser(uid), groups: listDmGroupsForUser(uid) };
});

app.post("/api/dm/threads", async (request, reply) => {
  const uid = sessionRegisteredUserId(request, reply);
  if (!uid) return;
  if (!assertFullSocialAccess(reply, uid)) return;
  const body = (request.body ?? {}) as { otherUserId?: string };
  const other = typeof body.otherUserId === "string" ? body.otherUserId.trim() : "";
  if (!other || other === uid) {
    reply.code(400);
    return { error: "Invalid recipient" };
  }
  const exists = db.prepare(`SELECT 1 FROM users WHERE id = ?`).get(other);
  if (!exists) {
    reply.code(404);
    return { error: "User not found" };
  }
  const threadId = getOrCreateDmThread(uid, other);
  return { threadId };
});

app.get("/api/dm/threads/:threadId/messages", async (request, reply) => {
  const uid = sessionRegisteredUserId(request, reply);
  if (!uid) return;
  if (!assertFullSocialAccess(reply, uid)) return;
  const { threadId } = request.params as { threadId: string };
  const inThread = db
    .prepare(`SELECT 1 FROM dm_threads WHERE id = ? AND (user_low = ? OR user_high = ?)`)
    .get(threadId, uid, uid);
  if (!inThread) {
    reply.code(404);
    return { error: "Thread not found" };
  }
  const q = request.query as { limit?: string; before?: string };
  const limitRaw = Number(q.limit);
  const limit = Number.isFinite(limitRaw) ? limitRaw : 50;
  const beforeId = typeof q.before === "string" && q.before.trim() ? q.before.trim() : null;
  const raw = listDmMessages(threadId, uid, limit, beforeId);
  const chronological = raw.slice().reverse();
  const otherLastReadAt = getOtherParticipantLastReadAt(threadId, uid);
  const messages = enrichMessagesWithReadState(chronological, uid, otherLastReadAt);
  const pair = db
    .prepare(`SELECT user_low AS low, user_high AS high FROM dm_threads WHERE id = ?`)
    .get(threadId) as { low: string; high: string } | undefined;
  const otherUserId = pair ? (pair.low === uid ? pair.high : pair.low) : null;
  let otherAvatarDataUrl: string | null = null;
  let otherDisplayName: string | null = null;
  let otherUsername: string | null = null;
  if (otherUserId) {
    const ur = db
      .prepare(
        `SELECT display_name AS displayName, username, avatar_data_url AS avatarDataUrl FROM users WHERE id = ?`,
      )
      .get(otherUserId) as { displayName: string | null; username: string | null; avatarDataUrl: string | null } | undefined;
    otherAvatarDataUrl = ur?.avatarDataUrl ?? null;
    otherDisplayName = ur?.displayName ?? null;
    otherUsername = ur?.username ?? null;
  }
  return { messages, otherLastReadAt, otherUserId, otherAvatarDataUrl, otherDisplayName, otherUsername };
});

app.post("/api/dm/threads/:threadId/messages", async (request, reply) => {
  const uid = sessionRegisteredUserId(request, reply);
  if (!uid) return;
  if (!assertFullSocialAccess(reply, uid)) return;
  const { threadId } = request.params as { threadId: string };
  const body = (request.body ?? {}) as { body?: string };
  const text = typeof body.body === "string" ? body.body : "";
  const row = sendDmMessage(uid, threadId, text);
  if (!row) {
    reply.code(400);
    return { error: "Could not send (empty message or no access)" };
  }
  return { message: row };
});

app.post("/api/dm/threads/:threadId/read", async (request, reply) => {
  const uid = sessionRegisteredUserId(request, reply);
  if (!uid) return;
  if (!assertFullSocialAccess(reply, uid)) return;
  const { threadId } = request.params as { threadId: string };
  const member = db
    .prepare(`SELECT 1 FROM dm_threads WHERE id = ? AND (user_low = ? OR user_high = ?)`)
    .get(threadId, uid, uid);
  if (!member) {
    reply.code(404);
    return { error: "Thread not found" };
  }
  markDmThreadRead(threadId, uid);
  return { ok: true };
});

app.post("/api/dm/threads/:threadId/unread", async (request, reply) => {
  const uid = sessionRegisteredUserId(request, reply);
  if (!uid) return;
  if (!assertFullSocialAccess(reply, uid)) return;
  const { threadId } = request.params as { threadId: string };
  const member = db
    .prepare(`SELECT 1 FROM dm_threads WHERE id = ? AND (user_low = ? OR user_high = ?)`)
    .get(threadId, uid, uid);
  if (!member) {
    reply.code(404);
    return { error: "Thread not found" };
  }
  markDmThreadUnread(threadId, uid);
  return { ok: true };
});

app.delete("/api/dm/threads/:threadId", async (request, reply) => {
  const uid = sessionRegisteredUserId(request, reply);
  if (!uid) return;
  if (!assertFullSocialAccess(reply, uid)) return;
  const { threadId } = request.params as { threadId: string };
  const ok = deleteDmThreadAsMember(threadId, uid);
  if (!ok) {
    reply.code(404);
    return { error: "Thread not found" };
  }
  return { ok: true };
});

app.post("/api/dm/groups", async (request, reply) => {
  const uid = sessionRegisteredUserId(request, reply);
  if (!uid) return;
  if (!assertFullSocialAccess(reply, uid)) return;
  const body = (request.body ?? {}) as { name?: string; memberUserIds?: unknown };
  const name = typeof body.name === "string" ? body.name : "";
  const raw = body.memberUserIds;
  const memberUserIds = Array.isArray(raw) ? raw.filter((x): x is string => typeof x === "string") : [];
  try {
    const groupId = createDmGroup(uid, name, memberUserIds);
    return { groupId };
  } catch (e) {
    const msg = e instanceof Error ? e.message : "Could not create group";
    reply.code(400);
    return { error: msg };
  }
});

app.get("/api/dm/groups/:groupId", async (request, reply) => {
  const uid = sessionRegisteredUserId(request, reply);
  if (!uid) return;
  if (!assertFullSocialAccess(reply, uid)) return;
  const { groupId } = request.params as { groupId: string };
  const detail = getDmGroupDetail(groupId, uid);
  if (!detail) {
    reply.code(404);
    return { error: "Group not found" };
  }
  return detail;
});

app.patch("/api/dm/groups/:groupId", async (request, reply) => {
  const uid = sessionRegisteredUserId(request, reply);
  if (!uid) return;
  if (!assertFullSocialAccess(reply, uid)) return;
  const { groupId } = request.params as { groupId: string };
  const body = (request.body ?? {}) as { name?: string; avatarDataUrl?: string | null };
  const hasName = typeof body.name === "string";
  const hasAvatar = "avatarDataUrl" in body;
  if (!hasName && !hasAvatar) {
    reply.code(400);
    return { error: "Nothing to update" };
  }
  const r = patchDmGroup(groupId, uid, {
    rawName: hasName ? body.name : undefined,
    avatarDataUrl: hasAvatar ? body.avatarDataUrl : undefined,
  });
  if ("error" in r) {
    const code =
      r.error.includes("not found") ? 404 : r.error.includes("Nothing") || r.error.includes("Avatar") ? 400 : 403;
    reply.code(code);
    return { error: r.error };
  }
  return { ok: true };
});

app.post("/api/dm/groups/:groupId/members", async (request, reply) => {
  const uid = sessionRegisteredUserId(request, reply);
  if (!uid) return;
  if (!assertFullSocialAccess(reply, uid)) return;
  const { groupId } = request.params as { groupId: string };
  const body = (request.body ?? {}) as { userIds?: unknown };
  const raw = body.userIds;
  const userIds = Array.isArray(raw) ? raw.filter((x): x is string => typeof x === "string") : [];
  const r = addDmGroupMembers(groupId, uid, userIds);
  if ("error" in r) {
    reply.code(400);
    return { error: r.error };
  }
  return { ok: true };
});

app.delete("/api/dm/groups/:groupId/members/:memberUserId", async (request, reply) => {
  const uid = sessionRegisteredUserId(request, reply);
  if (!uid) return;
  if (!assertFullSocialAccess(reply, uid)) return;
  const { groupId, memberUserId } = request.params as { groupId: string; memberUserId: string };
  const r = removeDmGroupMember(groupId, uid, memberUserId);
  if ("error" in r) {
    reply.code(400);
    return { error: r.error };
  }
  return { ok: true };
});

app.get("/api/dm/groups/:groupId/messages", async (request, reply) => {
  const uid = sessionRegisteredUserId(request, reply);
  if (!uid) return;
  if (!assertFullSocialAccess(reply, uid)) return;
  const { groupId } = request.params as { groupId: string };
  const q = request.query as { limit?: string };
  const limitRaw = Number(q.limit);
  const limit = Number.isFinite(limitRaw) ? limitRaw : 80;
  const messages = listDmGroupMessagesForApi(groupId, uid, limit);
  if (messages.length === 0) {
    const ok = db.prepare(`SELECT 1 FROM dm_group_members WHERE group_id = ? AND user_id = ?`).get(groupId, uid);
    if (!ok) {
      reply.code(404);
      return { error: "Group not found" };
    }
  }
  return { messages, isGroup: true as const };
});

app.post("/api/dm/groups/:groupId/messages", async (request, reply) => {
  const uid = sessionRegisteredUserId(request, reply);
  if (!uid) return;
  if (!assertFullSocialAccess(reply, uid)) return;
  const { groupId } = request.params as { groupId: string };
  const body = (request.body ?? {}) as { body?: string };
  const text = typeof body.body === "string" ? body.body : "";
  const row = sendDmGroupMessage(uid, groupId, text);
  if (!row) {
    reply.code(400);
    return { error: "Could not send (empty message or no access)" };
  }
  return { message: row };
});

app.post("/api/dm/groups/:groupId/read", async (request, reply) => {
  const uid = sessionRegisteredUserId(request, reply);
  if (!uid) return;
  if (!assertFullSocialAccess(reply, uid)) return;
  const { groupId } = request.params as { groupId: string };
  markDmGroupRead(groupId, uid);
  return { ok: true };
});

app.post("/api/dm/groups/:groupId/unread", async (request, reply) => {
  const uid = sessionRegisteredUserId(request, reply);
  if (!uid) return;
  if (!assertFullSocialAccess(reply, uid)) return;
  const { groupId } = request.params as { groupId: string };
  markDmGroupUnread(groupId, uid);
  return { ok: true };
});

app.delete("/api/dm/groups/:groupId", async (request, reply) => {
  const uid = sessionRegisteredUserId(request, reply);
  if (!uid) return;
  if (!assertFullSocialAccess(reply, uid)) return;
  const { groupId } = request.params as { groupId: string };
  const ok = leaveOrDeleteDmGroup(groupId, uid);
  if (!ok) {
    reply.code(404);
    return { error: "Group not found" };
  }
  return { ok: true };
});

/** Search registered members by username (substring match). Requires a signed-in registered account. */
app.get("/api/community/users/search", async (request, reply) => {
  const uid = sessionRegisteredUserId(request, reply);
  if (!uid) return;
  if (!assertFullSocialAccess(reply, uid)) return;
  const raw = (request.query as { q?: string }).q;
  const q = typeof raw === "string" ? raw.trim().slice(0, 64) : "";
  if (q.length < 1) {
    return { users: [] as { id: string; username: string; displayName: string | null }[] };
  }
  const rows = db
    .prepare(
      `SELECT id, username, display_name AS displayName, avatar_data_url AS avatarDataUrl
       FROM users
       WHERE username IS NOT NULL AND TRIM(username) != ''
         AND id != ?
         AND (instr(lower(username), lower(?)) > 0 OR instr(lower(COALESCE(display_name, '')), lower(?)) > 0)
       ORDER BY
         CASE WHEN lower(trim(username)) = lower(?) THEN 0 ELSE 1 END,
         CASE WHEN instr(lower(username), lower(?)) = 1 THEN 0 ELSE 1 END,
         length(username) ASC,
         username ASC
       LIMIT 20`,
    )
    .all(uid, q, q, q, q) as { id: string; username: string; displayName: string | null; avatarDataUrl: string | null }[];
  return {
    users: rows.map((r) => ({
      id: r.id,
      username: r.username,
      displayName: r.displayName,
      avatarDataUrl: r.avatarDataUrl ?? null,
    })),
  };
});

app.get("/api/community/challenges", async (request, reply) => {
  const viewer = getRegisteredSessionUserId(request);
  const rows = db
    .prepare(
      `SELECT c.*,
              (SELECT COUNT(*) FROM community_watch_challenge_participants p WHERE p.challenge_id = c.id) AS participant_count
       FROM community_watch_challenges c
       ORDER BY
         CASE WHEN date(c.deadline_airdate) >= date('now') THEN 0 ELSE 1 END,
         datetime(c.created_at) DESC
       LIMIT 40`,
    )
    .all() as (ChallengeRow & { participant_count: number })[];

  const joinedSet = new Set<string>();
  if (viewer && rows.length) {
    const ids = rows.map((r) => r.id);
    const placeholders = ids.map(() => "?").join(", ");
    const jrows = db
      .prepare(
        `SELECT challenge_id FROM community_watch_challenge_participants WHERE user_id = ? AND challenge_id IN (${placeholders})`,
      )
      .all(viewer, ...ids) as { challenge_id: string }[];
    for (const j of jrows) joinedSet.add(j.challenge_id);
  }

  const challenges = rows.map((c) => {
    const myProgress =
      viewer && joinedSet.has(c.id)
        ? challengeProgressParts(viewer, {
            tvmaze_target_show_id: c.tvmaze_target_show_id,
            deadline_airdate: c.deadline_airdate,
          })
        : undefined;
    return {
      id: c.id,
      title: c.title,
      summary: c.summary,
      tvmazeTargetShowId: c.tvmaze_target_show_id,
      targetShowName: c.target_show_name,
      tvmazeDeadlineShowId: c.tvmaze_deadline_show_id,
      tvmazeDeadlineEpisodeId: c.tvmaze_deadline_episode_id,
      deadlineShowName: c.deadline_show_name,
      deadlineEpisodeLabel: c.deadline_episode_label,
      deadlineAirdate: c.deadline_airdate,
      createdAt: c.created_at,
      active: challengeIsActive(c.deadline_airdate),
      participantCount: Number(c.participant_count ?? 0),
      joined: viewer ? joinedSet.has(c.id) : false,
      myProgress,
    };
  });

  return { challenges };
});

app.get("/api/community/challenges/:challengeId", async (request, reply) => {
  const { challengeId } = request.params as { challengeId: string };
  const viewer = getRegisteredSessionUserId(request);
  const c = db
    .prepare(`SELECT * FROM community_watch_challenges WHERE id = ?`)
    .get(challengeId) as ChallengeRow | undefined;
  if (!c) {
    reply.code(404);
    return { error: "Challenge not found" };
  }

  const participantCount = Number(
    (
      db
        .prepare(`SELECT COUNT(*) AS c FROM community_watch_challenge_participants WHERE challenge_id = ?`)
        .get(challengeId) as { c: number }
    ).c ?? 0,
  );

  let joined = false;
  if (viewer) {
    const j = db
      .prepare(
        `SELECT 1 FROM community_watch_challenge_participants WHERE challenge_id = ? AND user_id = ?`,
      )
      .get(challengeId, viewer);
    joined = Boolean(j);
  }

  const myProgress =
    viewer && joined
      ? challengeProgressParts(viewer, {
          tvmaze_target_show_id: c.tvmaze_target_show_id,
          deadline_airdate: c.deadline_airdate,
        })
      : null;

  const parts = db
    .prepare(
      `SELECT user_id, joined_at FROM community_watch_challenge_participants WHERE challenge_id = ?`,
    )
    .all(challengeId) as { user_id: string; joined_at: string }[];

  type LB = {
    userId: string;
    handle: string;
    joinedAt: string;
    completed: number;
    eligible: number;
    percent: number;
    finished: boolean;
  };

  const leaderboard: LB[] = [];
  for (const p of parts) {
    const u = db
      .prepare(`SELECT display_name, username FROM users WHERE id = ?`)
      .get(p.user_id) as { display_name: string | null; username: string | null } | undefined;
    const prog = challengeProgressParts(p.user_id, {
      tvmaze_target_show_id: c.tvmaze_target_show_id,
      deadline_airdate: c.deadline_airdate,
    });
    leaderboard.push({
      userId: p.user_id,
      handle: authorPublicHandle({ display_name: u?.display_name ?? null, username: u?.username ?? null }),
      joinedAt: p.joined_at,
      completed: prog.completed,
      eligible: prog.eligible,
      percent: prog.percent,
      finished: prog.finished,
    });
  }

  leaderboard.sort((a, b) => {
    if (b.percent !== a.percent) return b.percent - a.percent;
    if (b.completed !== a.completed) return b.completed - a.completed;
    return String(a.joinedAt).localeCompare(String(b.joinedAt));
  });

  return {
    challenge: {
      id: c.id,
      title: c.title,
      summary: c.summary,
      tvmazeTargetShowId: c.tvmaze_target_show_id,
      targetShowName: c.target_show_name,
      tvmazeDeadlineShowId: c.tvmaze_deadline_show_id,
      tvmazeDeadlineEpisodeId: c.tvmaze_deadline_episode_id,
      deadlineShowName: c.deadline_show_name,
      deadlineEpisodeLabel: c.deadline_episode_label,
      deadlineAirdate: c.deadline_airdate,
      createdAt: c.created_at,
      active: challengeIsActive(c.deadline_airdate),
      participantCount,
      joined,
      myProgress,
    },
    leaderboard,
  };
});

app.post("/api/community/challenges", async (request, reply) => {
  const uid = sessionRegisteredUserId(request, reply);
  if (!uid) return;
  if (!assertFullSocialAccess(reply, uid)) return;
  const body = (request.body ?? {}) as {
    title?: string;
    summary?: string;
    tvmazeTargetShowId?: number;
    tvmazeDeadlineShowId?: number;
    tvmazeDeadlineEpisodeId?: number;
  };
  const title = typeof body.title === "string" ? body.title.trim().slice(0, 200) : "";
  if (title.length < 3) {
    reply.code(400);
    return { error: "Title must be at least 3 characters" };
  }
  const summary =
    typeof body.summary === "string" && body.summary.trim() ? body.summary.trim().slice(0, 2000) : null;
  if (
    typeof body.tvmazeTargetShowId !== "number" ||
    !Number.isInteger(body.tvmazeTargetShowId) ||
    typeof body.tvmazeDeadlineShowId !== "number" ||
    !Number.isInteger(body.tvmazeDeadlineShowId) ||
    typeof body.tvmazeDeadlineEpisodeId !== "number" ||
    !Number.isInteger(body.tvmazeDeadlineEpisodeId)
  ) {
    reply.code(400);
    return { error: "tvmazeTargetShowId, tvmazeDeadlineShowId, and tvmazeDeadlineEpisodeId required" };
  }

  try {
    await refreshShowEpisodes(body.tvmazeTargetShowId);
    await refreshShowEpisodes(body.tvmazeDeadlineShowId);
  } catch (err) {
    app.log.warn({ err }, "refreshShowEpisodes for challenge create");
  }

  const targetShow = await fetchShow(body.tvmazeTargetShowId);
  const deadlineShow = await fetchShow(body.tvmazeDeadlineShowId);

  const epRow = db
    .prepare(
      `SELECT date(airdate) AS airdate, name, season, number FROM episodes_cache
       WHERE tvmaze_show_id = ? AND tvmaze_episode_id = ?`,
    )
    .get(body.tvmazeDeadlineShowId, body.tvmazeDeadlineEpisodeId) as
    | { airdate: string; name: string; season: number; number: number }
    | undefined;

  if (!epRow?.airdate || !String(epRow.airdate).trim()) {
    reply.code(400);
    return {
      error:
        "Deadline episode not in cache or has no air date — open the show in the app or run Refresh all show data, then pick an episode with a listed air date.",
    };
  }

  const deadlineYmd = String(epRow.airdate).trim().slice(0, 10);
  const eligible = countEligibleChallengeEpisodes(body.tvmazeTargetShowId, deadlineYmd);
  if (eligible < 1) {
    reply.code(400);
    return { error: "No eligible target-show episodes on or before that deadline — check show cache and dates." };
  }

  const epLabel = `S${epRow.season}E${epRow.number} — ${epRow.name || "Episode"}`;
  const id = uuidv4();
  db.prepare(
    `INSERT INTO community_watch_challenges (
       id, title, summary, tvmaze_target_show_id, target_show_name,
       tvmaze_deadline_show_id, tvmaze_deadline_episode_id,
       deadline_show_name, deadline_episode_label, deadline_airdate, created_by_user_id
     ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
  ).run(
    id,
    title,
    summary,
    body.tvmazeTargetShowId,
    targetShow.name,
    body.tvmazeDeadlineShowId,
    body.tvmazeDeadlineEpisodeId,
    deadlineShow.name,
    epLabel,
    deadlineYmd,
    uid,
  );

  reply.code(201);
  return { id };
});

app.post("/api/community/challenges/:challengeId/join", async (request, reply) => {
  const uid = sessionRegisteredUserId(request, reply);
  if (!uid) return;
  if (!assertFullSocialAccess(reply, uid)) return;
  const { challengeId } = request.params as { challengeId: string };
  const c = db.prepare(`SELECT deadline_airdate FROM community_watch_challenges WHERE id = ?`).get(challengeId) as
    | { deadline_airdate: string }
    | undefined;
  if (!c) {
    reply.code(404);
    return { error: "Challenge not found" };
  }
  if (!challengeIsActive(c.deadline_airdate)) {
    reply.code(400);
    return { error: "This challenge has ended" };
  }
  try {
    db.prepare(`INSERT INTO community_watch_challenge_participants (challenge_id, user_id) VALUES (?, ?)`).run(
      challengeId,
      uid,
    );
  } catch (e) {
    const msg = e instanceof Error ? e.message : "";
    if (msg.includes("UNIQUE")) {
      reply.code(409);
      return { error: "Already joined" };
    }
    throw e;
  }
  reply.code(201);
  return { ok: true };
});

app.delete("/api/community/challenges/:challengeId/join", async (request, reply) => {
  const uid = sessionRegisteredUserId(request, reply);
  if (!uid) return;
  if (!assertFullSocialAccess(reply, uid)) return;
  const { challengeId } = request.params as { challengeId: string };
  const r = db
    .prepare(`DELETE FROM community_watch_challenge_participants WHERE challenge_id = ? AND user_id = ?`)
    .run(challengeId, uid);
  if (r.changes === 0) {
    reply.code(404);
    return { error: "Not participating" };
  }
  return { ok: true };
});

/** Read-only profile for community member cards (no calendar token, timezone, or admin flags). */
app.get("/api/community/users/:userId/profile", async (request, reply) => {
  const { userId } = request.params as { userId: string };
  if (!userId || typeof userId !== "string" || userId.length > 80) {
    reply.code(400);
    return { error: "Invalid user id" };
  }
  const row = db
    .prepare(
      `SELECT id, display_name AS displayName, avatar_data_url AS avatarDataUrl, username,
              about_me AS aboutMe, age, sex, favorite_show AS favoriteShow, created_at AS createdAt
       FROM users WHERE id = ?`,
    )
    .get(userId) as Record<string, unknown> | undefined;
  if (!row) {
    reply.code(404);
    return { error: "User not found" };
  }
  const watchStats = await buildPublicProfileWatchStats(userId);

  const viewerId = getRegisteredSessionUserId(request);
  let viewerCompatibility: { percent: number; showsInCommon: number; mutualEpisodesRated: number } | null = null;
  if (viewerId && viewerId !== userId) {
    viewerCompatibility = computeViewerShowCompatibility(viewerId, userId);
  }

  const postsCount = (db.prepare(
    `SELECT COUNT(*) as c FROM community_posts WHERE user_id = ? AND deleted_at IS NULL`
  ).get(userId) as { c: number }).c;

  const recentPosts = db.prepare(
    `SELECT cp.id, cp.show_name AS showName, cp.episode_label AS episodeLabel,
            cp.body_html AS bodyHtml, cp.created_at AS createdAt, cp.tvmaze_show_id AS tvmazeShowId
     FROM community_posts cp
     WHERE cp.user_id = ? AND cp.deleted_at IS NULL
     ORDER BY cp.created_at DESC LIMIT 3`
  ).all(userId);

  return {
    ...row,
    watchStats,
    viewerCompatibility,
    postsCount,
    recentPosts,
    ...viewerRolePayloadForUser(userId),
  };
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

function readTemplateHtml(name: string): string {
  return fs.readFileSync(path.join(templatesDir, name), "utf8");
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
app.get("/embed-ratings.html", async (_req, reply) => {
  reply.header("Cache-Control", "no-store, max-age=0");
  reply.type("text/html; charset=utf-8").send(readPublicHtml("embed-ratings.html"));
});
app.get("/beta.html", async (_req, reply) => {
  reply.header("Cache-Control", "no-store, max-age=0");
  reply.type("text/html; charset=utf-8").send(readPublicHtml("beta.html"));
});
app.get("/beta", async (_req, reply) => reply.redirect("/beta.html", 302));
app.get("/admin.html", async (request, reply) => {
  if (replyForbiddenUnlessAdminPage(request, reply)) return;
  reply.header("Cache-Control", "no-store, max-age=0");
  reply.type("text/html; charset=utf-8").send(readPublicHtml("admin.html"));
});
app.get("/admin", async (request, reply) => {
  if (replyForbiddenUnlessAdminPage(request, reply)) return;
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

app.get("/galaxy-inspiration-bg.png", async (_req, reply) => {
  const p = path.join(publicDir, "galaxy-inspiration-bg.png");
  if (!fs.existsSync(p)) {
    reply.code(404);
    return "Not found";
  }
  reply.header("Cache-Control", "public, max-age=86400");
  return reply.type("image/png").send(fs.readFileSync(p));
});

cron.schedule("5 * * * *", async () => {
  try {
    await refreshAllSubscribedShows();
    const n = await runDailyNotifications();
    if (n.length) {
      app.log.info({ count: n.length }, "notifications recorded");
    }
    const personPushes = await runPersonNewProjectNotifications();
    if (personPushes.length > 0) {
      app.log.info({ count: personPushes.length }, "person new-project pushes sent");
    }
    const nudges = await runTaskNudgeNotifications();
    if (nudges > 0) {
      app.log.info({ count: nudges }, "task nudge pushes sent");
    }
    await refreshAllCastCache();
  } catch (err) {
    app.log.error(err, "scheduled job failed");
  }
});

cron.schedule("15,45 * * * *", async () => {
  try {
    const result = await pollRssFeeds();
    app.log.info(result, "RSS poll completed");
  } catch (err) {
    app.log.error(err, "RSS poll job failed");
  }
});

cron.schedule("30 */6 * * *", async () => {
  try {
    const { pollGoogleNewsForTopShows } = await import("./breakingNews.js");
    const result = await pollGoogleNewsForTopShows();
    app.log.info(result, "Google News poll completed");
  } catch (err) {
    app.log.error(err, "Google News poll failed");
  }
});

await app.listen({ port: PORT, host: "0.0.0.0" });
app.log.info(`Airalert V1 http://localhost:${PORT}`);

/** Railway sends SIGTERM when replacing the container; close Fastify cleanly so sockets/DB don't hang. */
const shutdown = async (signal: string) => {
  app.log.info({ signal }, "shutdown signal received");
  try {
    await app.close();
  } catch (err) {
    app.log.error(err, "error during app.close");
  }
  process.exit(0);
};
process.once("SIGTERM", () => void shutdown("SIGTERM"));
process.once("SIGINT", () => void shutdown("SIGINT"));

// Backfill show_image_url for existing subscriptions missing it
(async () => {
  const missing = db
    .prepare(`SELECT DISTINCT tvmaze_show_id FROM show_subscriptions WHERE show_image_url IS NULL`)
    .all() as { tvmaze_show_id: number }[];
  if (!missing.length) return;
  app.log.info({ count: missing.length }, "Backfilling show images");
  const updateStmt = db.prepare(`UPDATE show_subscriptions SET show_image_url = ? WHERE tvmaze_show_id = ? AND show_image_url IS NULL`);
  for (const row of missing) {
    try {
      const show = await fetchShow(row.tvmaze_show_id);
      const url = show.image?.original ?? show.image?.medium ?? null;
      if (url) updateStmt.run(url, row.tvmaze_show_id);
    } catch { /* TVMaze unavailable */ }
    await new Promise(r => setTimeout(r, 300));
  }
  app.log.info("Show image backfill complete");
})();
