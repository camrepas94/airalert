import fs from "node:fs";
import path from "node:path";
import crypto from "node:crypto";
import Fastify from "fastify";
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
import { refreshAllSubscribedShows, runDailyNotifications } from "./jobs.js";
import { configureWebPush, getVapidPublicKey } from "./push.js";
import { computeRecommendedShows } from "./recommend.js";

const PORT = Number(process.env.PORT) || 3000;
const publicDir = path.join(process.cwd(), "public");

const webPushReady = configureWebPush();

const app = Fastify({
  logger: true,
  routerOptions: {
    ignoreTrailingSlash: true,
  },
});

await app.register(cors, { origin: true });

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

app.get("/api/health", async () => ({
  ok: true,
  /** Open in a browser after deploy to confirm DB is on a volume (looksEphemeral should be false). */
  sqlite: getSqlitePersistenceInfo(),
}));

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
  const countRow = db.prepare(`SELECT COUNT(*) AS count FROM users`).get() as { count: number };
  if (Number(countRow?.count || 0) === 1) {
    const existing = db
      .prepare(
        `SELECT id, timezone, reminder_hour_local AS reminderHourLocal, calendar_token AS calendarToken
         FROM users ORDER BY created_at DESC LIMIT 1`,
      )
      .get() as { id: string; timezone: string; reminderHourLocal: number; calendarToken: string } | undefined;
    if (existing) {
      return { ...existing, reused: true };
    }
  }
  const created = createUserRecord(timezone, reminderHourLocal);
  reply.code(201);
  return { ...created, reused: false };
});

app.get("/api/users/:id", async (request, reply) => {
  const { id } = request.params as { id: string };
  const row = db
    .prepare(`SELECT id, timezone, reminder_hour_local AS reminderHourLocal, calendar_token AS calendarToken, created_at AS createdAt FROM users WHERE id = ?`)
    .get(id) as Record<string, unknown> | undefined;
  if (!row) {
    reply.code(404);
    return { error: "User not found" };
  }
  return row;
});

app.patch("/api/users/:id", async (request, reply) => {
  const { id } = request.params as { id: string };
  const body = (request.body ?? {}) as { timezone?: string; reminderHourLocal?: number };
  const existing = db.prepare(`SELECT id FROM users WHERE id = ?`).get(id);
  if (!existing) {
    reply.code(404);
    return { error: "User not found" };
  }
  if (typeof body.timezone === "string" && body.timezone.trim()) {
    db.prepare(`UPDATE users SET timezone = ? WHERE id = ?`).run(body.timezone.trim(), id);
  }
  if (typeof body.reminderHourLocal === "number" && Number.isInteger(body.reminderHourLocal)) {
    const h = Math.min(23, Math.max(0, body.reminderHourLocal));
    db.prepare(`UPDATE users SET reminder_hour_local = ? WHERE id = ?`).run(h, id);
  }
  const row = db
    .prepare(
      `SELECT id, timezone, reminder_hour_local AS reminderHourLocal, calendar_token AS calendarToken FROM users WHERE id = ?`,
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
  const rows = db
    .prepare(
      `SELECT id, tvmaze_show_id AS tvmazeShowId, show_name AS showName, created_at AS createdAt
       FROM show_subscriptions WHERE user_id = ? ORDER BY created_at DESC`,
    )
    .all(userId) as {
    id: string;
    tvmazeShowId: number;
    showName: string;
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

app.post("/api/users/:userId/subscriptions", async (request, reply) => {
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
      `INSERT INTO show_subscriptions (id, user_id, tvmaze_show_id, show_name, platform_note) VALUES (?, ?, ?, ?, ?)`,
    ).run(id, userId, show.id, show.name, null);
  } catch (e) {
    const msg = e instanceof Error ? e.message : "";
    if (msg.includes("UNIQUE")) {
      reply.code(409);
      return { error: "Already subscribed" };
    }
    throw e;
  }
  reply.code(201);
  return { id, tvmazeShowId: show.id, showName: show.name };
});

app.delete("/api/subscriptions/:subscriptionId", async (request, reply) => {
  const { subscriptionId } = request.params as { subscriptionId: string };
  const r = db.prepare(`DELETE FROM show_subscriptions WHERE id = ?`).run(subscriptionId);
  if (r.changes === 0) {
    reply.code(404);
    return { error: "Not found" };
  }
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

/** Admin / dev: refresh episode cache + run notification pass. */
app.post("/api/jobs/run", async () => {
  const refreshed = await refreshAllSubscribedShows();
  const notifications = await runDailyNotifications();
  return { refreshed, notificationsCreated: notifications.length, notifications };
});

app.get("/api/users/:userId/notifications", async (request, reply) => {
  const { userId } = request.params as { userId: string };
  const u = db.prepare(`SELECT id FROM users WHERE id = ?`).get(userId);
  if (!u) {
    reply.code(404);
    return { error: "User not found" };
  }
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
  } catch (err) {
    app.log.error(err, "scheduled job failed");
  }
});

await app.listen({ port: PORT, host: "0.0.0.0" });
app.log.info(`Airalert V1 http://localhost:${PORT}`);
