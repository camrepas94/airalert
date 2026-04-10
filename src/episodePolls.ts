import { db } from "./db.js";
import { normalizeEpisodeAirdate } from "./time.js";

/** UTC ms at midnight UTC on the episode listing air date (same anchor as live “air night”). */
export function episodeAirStartUtcMs(showId: number, tvmazeEpisodeId: number): number | null {
  const row = db
    .prepare(`SELECT airdate FROM episodes_cache WHERE tvmaze_show_id = ? AND tvmaze_episode_id = ?`)
    .get(showId, tvmazeEpisodeId) as { airdate: string | null } | undefined;
  const ymd = normalizeEpisodeAirdate(row?.airdate ?? null);
  if (!ymd) return null;
  const m = /^(\d{4})-(\d{2})-(\d{2})$/.exec(ymd);
  if (!m) return null;
  const y = Number(m[1]);
  const mo = Number(m[2]);
  const d = Number(m[3]);
  return Date.UTC(y, mo - 1, d, 0, 0, 0, 0);
}

/** True while viewers can still create polls and vote (before the episode’s listed air date, UTC). */
export function isEpisodePollVotingOpen(showId: number, tvmazeEpisodeId: number): boolean {
  const t = episodeAirStartUtcMs(showId, tvmazeEpisodeId);
  if (t == null) return false;
  return Date.now() < t;
}

export const POLL_MAX_QUESTION = 220;
export const POLL_MAX_OPTIONS = 8;
export const POLL_MIN_OPTIONS = 2;
export const POLL_MAX_OPTION_LEN = 96;
export const POLL_MAX_POLLS_PER_EPISODE = 15;

export function normalizePollOptions(raw: unknown): string[] | null {
  if (!Array.isArray(raw)) return null;
  if (raw.length < POLL_MIN_OPTIONS || raw.length > POLL_MAX_OPTIONS) return null;
  const out: string[] = [];
  for (const x of raw) {
    if (typeof x !== "string") return null;
    const t = x.replace(/\r\n/g, "\n").trim();
    if (!t || t.length > POLL_MAX_OPTION_LEN) return null;
    out.push(t);
  }
  if (new Set(out.map((s) => s.toLowerCase())).size !== out.length) return null;
  return out;
}
