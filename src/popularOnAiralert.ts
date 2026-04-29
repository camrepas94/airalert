/**
 * "Popular on AirAlert" — internal shelf only. Candidates are shows that at least
 * one user has in `show_subscriptions`. No external popularity APIs.
 *
 * Ranking (higher score first):
 *   score = adderCount * W_ADD + ratingBonus
 *   ratingBonus = 0  if ratingN < MIN_RATING_N
 *                 (avgRating - 1) * log2(1 + ratingN) * W_R   otherwise
 *
 * - adderCount: rows in show_subscriptions per tvmaze_show_id (one per user+show; equals
 *   number of users who added the show).
 * - avgRating, ratingN: from community_episode_ratings (1–5 star episode ratings) aggregated per show.
 *
 * W_ADD = 1000, W_R = 42, MIN_RATING_N = 3
 *   → adoption dominates: +1 user is +1000, while a strong rating signal adds at most on the
 *     order of 4 * log2(1+N) * 42 (e.g. ~400–900 for healthy N), so broad adoption beats
 *     a single perfect rating unless weights were equal (they are not).
 */
import { db } from "./db.js";
import { fetchShow, fetchPreviousEpisodeAirdates } from "./tvmaze.js";

export const POPULAR_ON_AIRALERT_WEIGHTS = {
  W_ADD: 1000,
  W_R: 42,
  MIN_RATING_N: 3,
} as const;

function plainSummaryHtml(s: string | null | undefined): string | null {
  if (!s) return null;
  const t = String(s)
    .replace(/<[^>]*>/g, " ")
    .replace(/\s+/g, " ")
    .trim();
  if (!t) return null;
  return t.length > 140 ? t.slice(0, 137) + "…" : t;
}

export function popularOnAiralertScore(adderCount: number, avgRating: number | null, ratingN: number): number {
  const { W_ADD, W_R, MIN_RATING_N } = POPULAR_ON_AIRALERT_WEIGHTS;
  let score = adderCount * W_ADD;
  if (ratingN >= MIN_RATING_N && avgRating != null) {
    const ar = Number(avgRating);
    if (Number.isFinite(ar)) {
      score += (ar - 1) * Math.log2(1 + ratingN) * W_R;
    }
  }
  return score;
}

export type PopularOnAiralertShow = {
  id: number;
  name: string;
  network: string | null;
  premiered: string | null;
  image: string | null;
  lastAiredDate: string | null;
  genres: string[];
  summary: string | null;
  adderCount: number;
  airalertAvgRating: number | null;
  airalertRatingCount: number;
  popularScore: number;
};

/**
 * @param opts.excludeUserId — omit shows this user already subscribes to (discovery shelf).
 */
export async function computePopularOnAiralert(opts: {
  excludeUserId: string | null;
  limit: number;
}): Promise<{
  shows: PopularOnAiralertShow[];
  shelfState: "ok" | "early" | "empty";
  weights: typeof POPULAR_ON_AIRALERT_WEIGHTS;
}> {
  const { excludeUserId, limit } = opts;
  const cap = Math.min(100, Math.max(1, limit));

  const adderRows = db
    .prepare(
      `SELECT tvmaze_show_id AS id, COUNT(*) AS adder_count
       FROM show_subscriptions
       GROUP BY tvmaze_show_id`,
    )
    .all() as { id: number; adder_count: number }[];

  if (adderRows.length === 0) {
    return { shows: [], shelfState: "empty", weights: POPULAR_ON_AIRALERT_WEIGHTS };
  }

  const distinctShowCount = adderRows.length;
  const shelfState: "ok" | "early" = distinctShowCount >= 6 ? "ok" : "early";

  const ratingRows = db
    .prepare(
      `SELECT tvmaze_show_id AS id,
              AVG(rating) AS avg_r,
              COUNT(*) AS rating_n
       FROM community_episode_ratings
       GROUP BY tvmaze_show_id`,
    )
    .all() as { id: number; avg_r: number; rating_n: number }[];

  const ratingByShow = new Map<number, { avg: number; n: number }>();
  for (const r of ratingRows) {
    ratingByShow.set(Number(r.id), { avg: Number(r.avg_r), n: Number(r.rating_n) || 0 });
  }

  let excluded = new Set<number>();
  if (excludeUserId) {
    const u = db.prepare(`SELECT id FROM users WHERE id = ?`).get(excludeUserId);
    if (u) {
      const mine = db
        .prepare(`SELECT tvmaze_show_id FROM show_subscriptions WHERE user_id = ?`)
        .all(excludeUserId) as { tvmaze_show_id: number }[];
      excluded = new Set(mine.map((m) => Number(m.tvmaze_show_id)));
    }
  }

  type Row = { id: number; adder_count: number; popularScore: number; avg: number | null; ratingN: number };
  const scored: Row[] = [];
  for (const r of adderRows) {
    const id = Number(r.id);
    if (!Number.isInteger(id) || id < 1) continue;
    const adderCount = Math.max(1, Math.floor(Number(r.adder_count) || 0));
    const rt = ratingByShow.get(id);
    const avg = rt != null ? rt.avg : null;
    const ratingN = rt != null ? rt.n : 0;
    const popularScore = popularOnAiralertScore(adderCount, avg, ratingN);
    scored.push({ id, adder_count: adderCount, popularScore, avg, ratingN });
  }

  scored.sort((a, b) => {
    if (b.popularScore !== a.popularScore) return b.popularScore - a.popularScore;
    if (b.adder_count !== a.adder_count) return b.adder_count - a.adder_count;
    const ab = a.avg != null ? a.avg : 0;
    const bb = b.avg != null ? b.avg : 0;
    if (bb !== ab) return bb - ab;
    return a.id - b.id;
  });

  const candidates: Row[] = [];
  for (const s of scored) {
    if (excluded.has(s.id)) continue;
    candidates.push(s);
    if (candidates.length >= cap) break;
  }

  if (candidates.length === 0) {
    return { shows: [], shelfState, weights: POPULAR_ON_AIRALERT_WEIGHTS };
  }

  const top = candidates;
  const ids = top.map((t) => t.id);
  const lastAiredById = await fetchPreviousEpisodeAirdates(ids);

  const fetched = await Promise.all(
    top.map(async (row) => {
      const id = row.id;
      let name = "";
      let network: string | null = null;
      let premiered: string | null = null;
      let image: string | null = null;
      let genres: string[] = [];
      let summary: string | null = null;

      try {
        const d = await fetchShow(id);
        name = d.name?.trim() || "";
        network = d.network?.name ?? d.webChannel?.name ?? null;
        premiered = d.premiered ?? null;
        image = d.image?.medium ?? null;
        const rawG = d.genres;
        genres = Array.isArray(rawG)
          ? rawG.map((x) => String(x)).filter((x) => x.length > 0).slice(0, 6)
          : [];
        summary = plainSummaryHtml(d.summary ?? null);
      } catch {
        const nameRow = db
          .prepare(
            `SELECT show_name, show_image_url
             FROM show_subscriptions
             WHERE tvmaze_show_id = ?
             ORDER BY datetime(created_at) DESC
             LIMIT 1`,
          )
          .get(id) as { show_name: string; show_image_url: string | null } | undefined;
        name = nameRow?.show_name?.trim() || "Show " + id;
        image = nameRow?.show_image_url?.trim() || null;
      }

      if (!name) name = "Show " + id;

      const avgR = row.avg != null && Number.isFinite(row.avg) ? Math.round(row.avg * 100) / 100 : null;

      return {
        id,
        name,
        network,
        premiered,
        image,
        lastAiredDate: lastAiredById.get(id) ?? null,
        genres,
        summary,
        adderCount: row.adder_count,
        airalertAvgRating: avgR,
        airalertRatingCount: row.ratingN,
        popularScore: Math.round(row.popularScore * 100) / 100,
      };
    }),
  );

  const shows: PopularOnAiralertShow[] = fetched;

  shows.sort((a, b) => b.popularScore - a.popularScore);
  return { shows, shelfState, weights: POPULAR_ON_AIRALERT_WEIGHTS };
}
