import crypto from "node:crypto";
import { v4 as uuidv4 } from "uuid";
import { db } from "./db.js";
import { hashPassword } from "./password.js";
import { fetchShow, fetchEpisodeWithSummary } from "./tvmaze.js";
import { safeTodayInTimeZone } from "./time.js";
import {
  BOT_SCHEDULE_TZ,
  DEFAULT_WEEKDAYS_JSON,
  PERSONA_KEYS,
  type PersonaKey,
  parseAllowedWeekdaysJson,
  serializeAllowedWeekdays,
  isBotPostingDayAllowed,
  getBotWeekStartYmd,
  hasWeeklyCapacity,
  plainPostHtml,
  stripHtmlSummary,
  templateGeneratePostBody,
  templateGenerateReplyBody,
} from "./communityBotLogic.js";

export {
  BOT_SCHEDULE_TZ,
  DEFAULT_WEEKDAYS_JSON,
  PERSONA_KEYS,
  type PersonaKey,
  parseAllowedWeekdaysJson,
  serializeAllowedWeekdays,
  isBotPostingDayAllowed,
  getBotWeekStartYmd,
  hasWeeklyCapacity,
  plainPostHtml,
  templateGeneratePostBody,
  templateGenerateReplyBody,
} from "./communityBotLogic.js";

export type CommunityBotListItem = {
  userId: string;
  username: string;
  displayName: string | null;
  enabled: boolean;
  personaKey: string;
  personaPrompt: string | null;
  postsPerWeekMax: number;
  repliesPerWeekMax: number;
  allowedWeekdays: number[];
  subscriptionCount: number;
  postsThisWeek: number;
  repliesThisWeek: number;
  createdAt: string;
  updatedAt: string;
};

export type DryRunItem = {
  botUserId: string;
  username: string;
  showId: number;
  showName: string;
  episodeId: number;
  episodeLabel: string;
  episodeTitle: string;
  airdate: string | null;
  bodyPreview: string;
  skippedReason?: string;
};

export type BotRunResult = {
  botsChecked: number;
  postsCreated: number;
  repliesCreated: number;
  skipped: string[];
  errors: string[];
};

type EpisodeCandidate = {
  tvmazeShowId: number;
  showName: string;
  tvmazeEpisodeId: number;
  episodeLabel: string;
  episodeTitle: string;
  airdate: string | null;
};

type BotRow = {
  user_id: string;
  username: string;
  display_name: string | null;
  enabled: number;
  persona_key: string;
  persona_prompt: string | null;
  posts_per_week_max: number;
  replies_per_week_max: number;
  allowed_weekdays_json: string;
  created_at: string;
  updated_at: string;
};

const USERNAME_RE = /^[a-zA-Z0-9._-]{3,32}$/;

export function countWeeklyBotRuns(userId: string, kind: "post" | "reply", weekStartYmd: string): number {
  const row = db
    .prepare(
      `SELECT COUNT(*) AS c FROM community_bot_runs
       WHERE user_id = ? AND kind = ? AND date(created_at) >= date(?)`,
    )
    .get(userId, kind, weekStartYmd) as { c: number };
  return row?.c ?? 0;
}

function formatEpisodeLabel(season: number | null, number: number | null, name: string): string {
  if (season != null && number != null) {
    const base = `S${season}E${number}`;
    const trimmed = name?.trim() ?? "";
    if (trimmed && trimmed !== `Episode ${number}`) return `${base} · ${trimmed}`;
    return base;
  }
  return name?.trim() || "Episode";
}

async function generateWithOpenAI(opts: {
  kind: "post" | "reply";
  personaKey: string;
  personaPrompt: string | null;
  showName?: string;
  episodeLabel?: string;
  episodeSummary?: string | null;
  humanSnippet?: string;
}): Promise<string | null> {
  const apiKey = process.env.OPENAI_API_KEY?.trim();
  if (!apiKey) return null;
  const model = process.env.OPENAI_MODEL?.trim() || "gpt-4o-mini";
  const persona = opts.personaPrompt?.trim() || `You are a ${opts.personaKey} TV fan on a community app. Keep posts short, casual, no hashtags.`;
  let userPrompt: string;
  if (opts.kind === "post") {
    userPrompt = `Write one short community post (1-3 sentences, under 280 chars) reacting to ${opts.episodeLabel} of ${opts.showName}. Episode summary: ${stripHtmlSummary(opts.episodeSummary) || "unknown"}. Do not spoil beyond the summary.`;
  } else {
    userPrompt = `Write one short friendly reply (under 180 chars) to this comment: "${opts.humanSnippet?.slice(0, 200) ?? ""}"`;
  }
  const res = await fetch("https://api.openai.com/v1/chat/completions", {
    method: "POST",
    headers: {
      Authorization: `Bearer ${apiKey}`,
      "Content-Type": "application/json",
    },
    body: JSON.stringify({
      model,
      messages: [
        { role: "system", content: persona },
        { role: "user", content: userPrompt },
      ],
      max_tokens: 120,
      temperature: 0.9,
    }),
  });
  if (!res.ok) return null;
  const data = (await res.json()) as { choices?: { message?: { content?: string } }[] };
  const text = data.choices?.[0]?.message?.content?.trim();
  return text ? text.slice(0, 500) : null;
}

async function generatePostBody(opts: {
  personaKey: string;
  personaPrompt: string | null;
  showName: string;
  episodeLabel: string;
  episodeSummary?: string | null;
}): Promise<string> {
  const llm = await generateWithOpenAI({ kind: "post", ...opts });
  if (llm) return llm;
  return templateGeneratePostBody(opts);
}

async function generateReplyBody(opts: {
  personaKey: string;
  personaPrompt: string | null;
  humanSnippet: string;
}): Promise<string> {
  const llm = await generateWithOpenAI({ kind: "reply", ...opts });
  if (llm) return llm;
  return templateGenerateReplyBody(opts);
}

function botAlreadyPostedOnEpisode(userId: string, showId: number, episodeId: number): boolean {
  const row = db
    .prepare(
      `SELECT 1 FROM community_posts
       WHERE user_id = ? AND tvmaze_show_id = ? AND tvmaze_episode_id = ? AND deleted_at IS NULL
       LIMIT 1`,
    )
    .get(userId, showId, episodeId);
  return Boolean(row);
}

function listEpisodeCandidatesForBot(userId: string, lookbackDays = 14): EpisodeCandidate[] {
  const today = safeTodayInTimeZone(BOT_SCHEDULE_TZ);
  const rows = db
    .prepare(
      `SELECT e.tvmaze_show_id AS showId, ss.show_name AS showName, e.tvmaze_episode_id AS episodeId,
              e.name AS episodeTitle, e.season, e.number, date(e.airdate) AS airdate
       FROM show_subscriptions ss
       INNER JOIN episodes_cache e ON e.tvmaze_show_id = ss.tvmaze_show_id
       WHERE ss.user_id = ?
         AND e.airdate IS NOT NULL AND trim(e.airdate) != ''
         AND date(e.airdate) >= date(?, '-' || ? || ' days')
         AND date(e.airdate) <= date(?)
       ORDER BY e.airdate DESC, e.season DESC, e.number DESC`,
    )
    .all(userId, today, lookbackDays, today) as {
    showId: number;
    showName: string;
    episodeId: number;
    episodeTitle: string;
    season: number | null;
    number: number | null;
    airdate: string | null;
  }[];

  const out: EpisodeCandidate[] = [];
  for (const r of rows) {
    if (botAlreadyPostedOnEpisode(userId, r.showId, r.episodeId)) continue;
    out.push({
      tvmazeShowId: r.showId,
      showName: r.showName?.trim() || "Unknown show",
      tvmazeEpisodeId: r.episodeId,
      episodeLabel: formatEpisodeLabel(r.season, r.number, r.episodeTitle),
      episodeTitle: r.episodeTitle?.trim() || "Episode",
      airdate: r.airdate,
    });
  }
  return out;
}

function pickRandomCandidate(candidates: EpisodeCandidate[]): EpisodeCandidate | null {
  if (!candidates.length) return null;
  return candidates[crypto.randomInt(0, candidates.length)];
}

function getBotRow(userId: string): BotRow | null {
  return db
    .prepare(
      `SELECT p.user_id, u.username, u.display_name, p.enabled, p.persona_key, p.persona_prompt,
              p.posts_per_week_max, p.replies_per_week_max, p.allowed_weekdays_json, p.created_at, p.updated_at
       FROM community_bot_profiles p
       INNER JOIN users u ON u.id = p.user_id
       WHERE p.user_id = ? AND u.is_community_bot = 1`,
    )
    .get(userId) as BotRow | null;
}

function listEnabledBots(): BotRow[] {
  return db
    .prepare(
      `SELECT p.user_id, u.username, u.display_name, p.enabled, p.persona_key, p.persona_prompt,
              p.posts_per_week_max, p.replies_per_week_max, p.allowed_weekdays_json, p.created_at, p.updated_at
       FROM community_bot_profiles p
       INNER JOIN users u ON u.id = p.user_id
       WHERE u.is_community_bot = 1 AND p.enabled = 1
       ORDER BY u.username ASC`,
    )
    .all() as BotRow[];
}

function rowToListItem(row: BotRow, weekStart: string): CommunityBotListItem {
  const subCount = (
    db.prepare(`SELECT COUNT(*) AS c FROM show_subscriptions WHERE user_id = ?`).get(row.user_id) as { c: number }
  ).c;
  return {
    userId: row.user_id,
    username: row.username,
    displayName: row.display_name,
    enabled: row.enabled === 1,
    personaKey: row.persona_key,
    personaPrompt: row.persona_prompt,
    postsPerWeekMax: row.posts_per_week_max,
    repliesPerWeekMax: row.replies_per_week_max,
    allowedWeekdays: parseAllowedWeekdaysJson(row.allowed_weekdays_json),
    subscriptionCount: subCount,
    postsThisWeek: countWeeklyBotRuns(row.user_id, "post", weekStart),
    repliesThisWeek: countWeeklyBotRuns(row.user_id, "reply", weekStart),
    createdAt: row.created_at,
    updatedAt: row.updated_at,
  };
}

export function listCommunityBots(): CommunityBotListItem[] {
  const weekStart = getBotWeekStartYmd();
  const rows = db
    .prepare(
      `SELECT p.user_id, u.username, u.display_name, p.enabled, p.persona_key, p.persona_prompt,
              p.posts_per_week_max, p.replies_per_week_max, p.allowed_weekdays_json, p.created_at, p.updated_at
       FROM community_bot_profiles p
       INNER JOIN users u ON u.id = p.user_id
       WHERE u.is_community_bot = 1
       ORDER BY u.username ASC`,
    )
    .all() as BotRow[];
  return rows.map((r) => rowToListItem(r, weekStart));
}

function randomBotPassword(): string {
  return crypto.randomBytes(12).toString("base64url").slice(0, 16);
}

export function createCommunityBot(input: {
  username: string;
  displayName?: string | null;
  personaKey?: string;
  personaPrompt?: string | null;
  postsPerWeekMax?: number;
  repliesPerWeekMax?: number;
  allowedWeekdays?: number[];
}): { userId: string; username: string; password: string } {
  const username = input.username.trim();
  if (!USERNAME_RE.test(username)) {
    throw new Error("Username must be 3–32 characters (letters, numbers, . _ -)");
  }
  const existing = db
    .prepare(`SELECT id FROM users WHERE username IS NOT NULL AND lower(trim(username)) = lower(?)`)
    .get(username) as { id: string } | undefined;
  if (existing) throw new Error("Username already taken");

  const personaKey = PERSONA_KEYS.includes(input.personaKey as PersonaKey) ? input.personaKey! : "fan";
  const postsPerWeekMax = clampInt(input.postsPerWeekMax, 0, 21, 3);
  const repliesPerWeekMax = clampInt(input.repliesPerWeekMax, 0, 50, 5);
  const allowedWeekdaysJson = serializeAllowedWeekdays(input.allowedWeekdays ?? parseAllowedWeekdaysJson(DEFAULT_WEEKDAYS_JSON));

  const id = uuidv4();
  const password = randomBotPassword();
  const clipped = password.slice(0, 256);
  const email = `bot+${username.toLowerCase()}@bots.internal.airalert`;
  const displayName = input.displayName?.trim() || username;

  db.prepare(
    `INSERT INTO users (id, timezone, reminder_hour_local, calendar_token, username, password_hash, password_plain_admin,
      is_admin, email, email_verified, auth_provider, is_test_account, is_community_bot, display_name)
     VALUES (?, 'America/Los_Angeles', 8, ?, ?, ?, ?, 0, ?, 1, 'local', 1, 1, ?)`,
  ).run(id, uuidv4(), username, hashPassword(clipped), clipped, email, displayName);

  db.prepare(
    `INSERT INTO community_bot_profiles (user_id, enabled, persona_key, persona_prompt, posts_per_week_max, replies_per_week_max, allowed_weekdays_json)
     VALUES (?, 1, ?, ?, ?, ?, ?)`,
  ).run(
    id,
    personaKey,
    input.personaPrompt?.trim() || null,
    postsPerWeekMax,
    repliesPerWeekMax,
    allowedWeekdaysJson,
  );

  return { userId: id, username, password };
}

function clampInt(value: number | undefined, min: number, max: number, fallback: number): number {
  if (typeof value !== "number" || !Number.isInteger(value)) return fallback;
  return Math.min(max, Math.max(min, value));
}

export function updateCommunityBot(
  userId: string,
  patch: {
    enabled?: boolean;
    displayName?: string | null;
    personaKey?: string;
    personaPrompt?: string | null;
    postsPerWeekMax?: number;
    repliesPerWeekMax?: number;
    allowedWeekdays?: number[];
  },
): CommunityBotListItem | null {
  const row = getBotRow(userId);
  if (!row) return null;

  const personaKey =
    patch.personaKey && PERSONA_KEYS.includes(patch.personaKey as PersonaKey) ? patch.personaKey : row.persona_key;
  const postsPerWeekMax =
    patch.postsPerWeekMax != null ? clampInt(patch.postsPerWeekMax, 0, 21, row.posts_per_week_max) : row.posts_per_week_max;
  const repliesPerWeekMax =
    patch.repliesPerWeekMax != null
      ? clampInt(patch.repliesPerWeekMax, 0, 50, row.replies_per_week_max)
      : row.replies_per_week_max;
  const allowedWeekdaysJson =
    patch.allowedWeekdays != null
      ? serializeAllowedWeekdays(patch.allowedWeekdays)
      : row.allowed_weekdays_json;
  const enabled = patch.enabled != null ? (patch.enabled ? 1 : 0) : row.enabled;

  db.prepare(
    `UPDATE community_bot_profiles SET enabled = ?, persona_key = ?, persona_prompt = ?,
      posts_per_week_max = ?, replies_per_week_max = ?, allowed_weekdays_json = ?, updated_at = datetime('now')
     WHERE user_id = ?`,
  ).run(
    enabled,
    personaKey,
    patch.personaPrompt !== undefined ? patch.personaPrompt?.trim() || null : row.persona_prompt,
    postsPerWeekMax,
    repliesPerWeekMax,
    allowedWeekdaysJson,
    userId,
  );

  if (patch.displayName !== undefined) {
    db.prepare(`UPDATE users SET display_name = ? WHERE id = ?`).run(patch.displayName?.trim() || null, userId);
  }

  return rowToListItem(getBotRow(userId)!, getBotWeekStartYmd());
}

export function deleteCommunityBot(userId: string): boolean {
  const row = getBotRow(userId);
  if (!row) return false;
  db.prepare(`DELETE FROM community_bot_profiles WHERE user_id = ?`).run(userId);
  db.prepare(`UPDATE users SET is_community_bot = 0 WHERE id = ?`).run(userId);
  return true;
}

function insertBotCommunityPost(opts: {
  userId: string;
  showId: number;
  showName: string;
  episodeId: number;
  episodeLabel: string;
  bodyText: string;
  personaKey: string;
  episodeSummary?: string | null;
  runKind: "post" | "dry_run_post";
}): string | null {
  const bodyHtml = plainPostHtml(opts.bodyText);
  const runId = uuidv4();

  if (opts.runKind === "dry_run_post") {
    db.prepare(
      `INSERT INTO community_bot_runs (id, user_id, kind, tvmaze_show_id, tvmaze_episode_id, detail_json)
       VALUES (?, ?, 'dry_run_post', ?, ?, ?)`,
    ).run(
      runId,
      opts.userId,
      opts.showId,
      opts.episodeId,
      JSON.stringify({ bodyPreview: opts.bodyText, episodeLabel: opts.episodeLabel, showName: opts.showName }),
    );
    return null;
  }

  const postId = uuidv4();
  db.prepare(
    `INSERT INTO community_posts (id, user_id, tvmaze_show_id, show_name, tvmaze_episode_id, episode_label, body_html, is_spoiler, parent_post_id, tag)
     VALUES (?, ?, ?, ?, ?, ?, ?, 0, NULL, 'hot-take')`,
  ).run(postId, opts.userId, opts.showId, opts.showName, opts.episodeId, opts.episodeLabel, bodyHtml);

  db.prepare(
    `INSERT INTO community_bot_post_context (post_id, episode_summary, persona_key)
     VALUES (?, ?, ?)`,
  ).run(postId, opts.episodeSummary?.slice(0, 4000) ?? null, opts.personaKey);

  db.prepare(
    `INSERT INTO community_bot_runs (id, user_id, kind, tvmaze_show_id, tvmaze_episode_id, community_post_id, detail_json)
     VALUES (?, ?, 'post', ?, ?, ?, ?)`,
  ).run(
    runId,
    opts.userId,
    opts.showId,
    opts.episodeId,
    postId,
    JSON.stringify({ bodyPreview: opts.bodyText.slice(0, 200) }),
  );

  return postId;
}

function insertBotReply(opts: {
  userId: string;
  parentPostId: string;
  showId: number;
  showName: string;
  episodeId: number | null;
  episodeLabel: string | null;
  bodyText: string;
  runKind: "reply" | "dry_run_reply";
}): string | null {
  const bodyHtml = plainPostHtml(opts.bodyText);
  const runId = uuidv4();

  if (opts.runKind === "dry_run_reply") {
    db.prepare(
      `INSERT INTO community_bot_runs (id, user_id, kind, tvmaze_show_id, tvmaze_episode_id, detail_json)
       VALUES (?, ?, 'dry_run_reply', ?, ?, ?)`,
    ).run(
      runId,
      opts.userId,
      opts.showId,
      opts.episodeId,
      JSON.stringify({ bodyPreview: opts.bodyText, parentPostId: opts.parentPostId }),
    );
    return null;
  }

  const postId = uuidv4();
  db.prepare(
    `INSERT INTO community_posts (id, user_id, tvmaze_show_id, show_name, tvmaze_episode_id, episode_label, body_html, is_spoiler, parent_post_id, tag)
     VALUES (?, ?, ?, ?, ?, ?, ?, 0, ?, NULL)`,
  ).run(
    postId,
    opts.userId,
    opts.showId,
    opts.showName,
    opts.episodeId,
    opts.episodeLabel,
    bodyHtml,
    opts.parentPostId,
  );

  db.prepare(
    `INSERT INTO community_bot_runs (id, user_id, kind, tvmaze_show_id, tvmaze_episode_id, community_post_id, detail_json)
     VALUES (?, ?, 'reply', ?, ?, ?, ?)`,
  ).run(
    runId,
    opts.userId,
    opts.showId,
    opts.episodeId,
    postId,
    JSON.stringify({ bodyPreview: opts.bodyText.slice(0, 200), parentPostId: opts.parentPostId }),
  );

  return postId;
}

async function planBotPost(bot: BotRow, dryRun: boolean): Promise<DryRunItem | null> {
  const allowed = parseAllowedWeekdaysJson(bot.allowed_weekdays_json);
  if (!isBotPostingDayAllowed(allowed)) {
    return {
      botUserId: bot.user_id,
      username: bot.username,
      showId: 0,
      showName: "",
      episodeId: 0,
      episodeLabel: "",
      episodeTitle: "",
      airdate: null,
      bodyPreview: "",
      skippedReason: "Not an allowed posting day",
    };
  }

  const weekStart = getBotWeekStartYmd();
  const postsThisWeek = countWeeklyBotRuns(bot.user_id, "post", weekStart);
  if (!hasWeeklyCapacity(postsThisWeek, bot.posts_per_week_max)) {
    return {
      botUserId: bot.user_id,
      username: bot.username,
      showId: 0,
      showName: "",
      episodeId: 0,
      episodeLabel: "",
      episodeTitle: "",
      airdate: null,
      bodyPreview: "",
      skippedReason: "Weekly post cap reached",
    };
  }

  const candidates = listEpisodeCandidatesForBot(bot.user_id);
  const picked = pickRandomCandidate(candidates);
  if (!picked) {
    return {
      botUserId: bot.user_id,
      username: bot.username,
      showId: 0,
      showName: "",
      episodeId: 0,
      episodeLabel: "",
      episodeTitle: "",
      airdate: null,
      bodyPreview: "",
      skippedReason: "No eligible episodes (add show subscriptions or wait for recent airdates)",
    };
  }

  let episodeSummary: string | null = null;
  try {
    const ep = await fetchEpisodeWithSummary(picked.tvmazeEpisodeId);
    if (ep?.summary) episodeSummary = ep.summary;
    if (ep && ep.showId !== picked.tvmazeShowId) {
      return {
        botUserId: bot.user_id,
        username: bot.username,
        showId: picked.tvmazeShowId,
        showName: picked.showName,
        episodeId: picked.tvmazeEpisodeId,
        episodeLabel: picked.episodeLabel,
        episodeTitle: picked.episodeTitle,
        airdate: picked.airdate,
        bodyPreview: "",
        skippedReason: "Episode does not belong to subscribed show",
      };
    }
  } catch {
    /* template fallback without summary */
  }

  const bodyText = await generatePostBody({
    personaKey: bot.persona_key,
    personaPrompt: bot.persona_prompt,
    showName: picked.showName,
    episodeLabel: picked.episodeLabel,
    episodeSummary,
  });

  insertBotCommunityPost({
    userId: bot.user_id,
    showId: picked.tvmazeShowId,
    showName: picked.showName,
    episodeId: picked.tvmazeEpisodeId,
    episodeLabel: picked.episodeLabel,
    bodyText,
    personaKey: bot.persona_key,
    episodeSummary,
    runKind: dryRun ? "dry_run_post" : "post",
  });

  return {
    botUserId: bot.user_id,
    username: bot.username,
    showId: picked.tvmazeShowId,
    showName: picked.showName,
    episodeId: picked.tvmazeEpisodeId,
    episodeLabel: picked.episodeLabel,
    episodeTitle: picked.episodeTitle,
    airdate: picked.airdate,
    bodyPreview: bodyText,
  };
}

export async function dryRunCommunityBot(userId: string): Promise<DryRunItem[]> {
  const bot = getBotRow(userId);
  if (!bot) throw new Error("Community bot not found");
  const item = await planBotPost(bot, true);
  return item ? [item] : [];
}

export function communityBotsEnabled(): boolean {
  return process.env.COMMUNITY_BOTS_ENABLED === "1" || process.env.COMMUNITY_BOTS_ENABLED === "true";
}

export async function runCommunityBotSeedPosts(opts?: { userId?: string; force?: boolean }): Promise<BotRunResult> {
  const result: BotRunResult = { botsChecked: 0, postsCreated: 0, repliesCreated: 0, skipped: [], errors: [] };
  if (!opts?.force && !communityBotsEnabled()) {
    result.skipped.push("COMMUNITY_BOTS_ENABLED is not set");
    return result;
  }

  const bots = opts?.userId
    ? (() => {
        const b = getBotRow(opts.userId);
        return b && b.enabled === 1 ? [b] : [];
      })()
    : listEnabledBots();

  for (const bot of bots) {
    result.botsChecked++;
    try {
      const item = await planBotPost(bot, false);
      if (!item) continue;
      if (item.skippedReason) {
        result.skipped.push(`${bot.username}: ${item.skippedReason}`);
      } else if (item.bodyPreview) {
        result.postsCreated++;
      }
    } catch (err) {
      result.errors.push(`${bot.username}: ${err instanceof Error ? err.message : String(err)}`);
    }
  }

  return result;
}

type PendingReply = {
  replyId: string;
  parentPostId: string;
  humanUserId: string;
  showId: number;
  showName: string;
  episodeId: number | null;
  episodeLabel: string | null;
  bodyHtml: string;
};

function listPendingRepliesForBot(botUserId: string): PendingReply[] {
  return db
    .prepare(
      `SELECT r.id AS replyId, r.parent_post_id AS parentPostId, r.user_id AS humanUserId,
              r.tvmaze_show_id AS showId, r.show_name AS showName, r.tvmaze_episode_id AS episodeId,
              r.episode_label AS episodeLabel, r.body_html AS bodyHtml
       FROM community_posts top
       INNER JOIN community_posts r ON r.parent_post_id = top.id
       INNER JOIN users hu ON hu.id = r.user_id
       WHERE top.user_id = ?
         AND top.deleted_at IS NULL
         AND r.deleted_at IS NULL
         AND hu.is_community_bot = 0
         AND NOT EXISTS (
           SELECT 1 FROM community_posts br
           WHERE br.parent_post_id = r.id AND br.user_id = ? AND br.deleted_at IS NULL
         )
       ORDER BY r.created_at ASC
       LIMIT 20`,
    )
    .all(botUserId, botUserId) as PendingReply[];
}

function stripHtmlBody(html: string): string {
  return html.replace(/<[^>]+>/g, " ").replace(/\s+/g, " ").trim();
}

async function planBotReply(bot: BotRow, pending: PendingReply, dryRun: boolean): Promise<boolean> {
  const snippet = stripHtmlBody(pending.bodyHtml);
  const bodyText = await generateReplyBody({
    personaKey: bot.persona_key,
    personaPrompt: bot.persona_prompt,
    humanSnippet: snippet,
  });

  insertBotReply({
    userId: bot.user_id,
    parentPostId: pending.replyId,
    showId: pending.showId,
    showName: pending.showName,
    episodeId: pending.episodeId,
    episodeLabel: pending.episodeLabel,
    bodyText,
    runKind: dryRun ? "dry_run_reply" : "reply",
  });
  return true;
}

export async function runCommunityBotReplies(opts?: { userId?: string; force?: boolean }): Promise<BotRunResult> {
  const result: BotRunResult = { botsChecked: 0, postsCreated: 0, repliesCreated: 0, skipped: [], errors: [] };
  if (!opts?.force && !communityBotsEnabled()) {
    result.skipped.push("COMMUNITY_BOTS_ENABLED is not set");
    return result;
  }

  const weekStart = getBotWeekStartYmd();
  const bots = opts?.userId
    ? (() => {
        const b = getBotRow(opts.userId);
        return b && b.enabled === 1 ? [b] : [];
      })()
    : listEnabledBots();

  for (const bot of bots) {
    result.botsChecked++;
    const repliesThisWeek = countWeeklyBotRuns(bot.user_id, "reply", weekStart);
    if (!hasWeeklyCapacity(repliesThisWeek, bot.replies_per_week_max)) {
      result.skipped.push(`${bot.username}: Weekly reply cap reached`);
      continue;
    }

    const allowed = parseAllowedWeekdaysJson(bot.allowed_weekdays_json);
    if (!isBotPostingDayAllowed(allowed)) {
      result.skipped.push(`${bot.username}: Not an allowed reply day`);
      continue;
    }

    const pending = listPendingRepliesForBot(bot.user_id);
    if (!pending.length) {
      result.skipped.push(`${bot.username}: No pending human replies`);
      continue;
    }

    let remaining = bot.replies_per_week_max - repliesThisWeek;
    for (const p of pending) {
      if (remaining <= 0) break;
      try {
        await planBotReply(bot, p, false);
        result.repliesCreated++;
        remaining--;
      } catch (err) {
        result.errors.push(`${bot.username}: ${err instanceof Error ? err.message : String(err)}`);
      }
    }
  }

  return result;
}

export async function ensureBotShowVerified(showId: number): Promise<{ id: number; name: string }> {
  const show = await fetchShow(showId);
  return { id: show.id, name: show.name?.trim() || "Unknown show" };
}
