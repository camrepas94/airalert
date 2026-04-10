import { db } from "./db.js";
import { normalizeEpisodeAirdate } from "./time.js";

/** Episode listing date has passed (UTC calendar compare). */
export function episodeHasAiredUtc(showId: number, tvmazeEpisodeId: number): boolean {
  const row = db
    .prepare(`SELECT airdate FROM episodes_cache WHERE tvmaze_show_id = ? AND tvmaze_episode_id = ?`)
    .get(showId, tvmazeEpisodeId) as { airdate: string | null } | undefined;
  const ymd = normalizeEpisodeAirdate(row?.airdate ?? null);
  if (!ymd) return false;
  const today = new Date().toISOString().slice(0, 10);
  return ymd <= today;
}
