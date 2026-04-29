/**
 * "Popular on AirAlert" — internal shelf only. Candidates are shows that at least
 * one user has in `show_subscriptions`. No external popularity APIs.
 *
 * Ranking (descending — best first):
 *
 * 1. **Rated vs unrated** — Any show with at least one `community_episode_ratings` row ranks
 *    above shows with zero ratings. Add-count alone cannot push an unrated show above a rated one.
 *
 * 2. **Primary (among rated)** — Bayesian-smoothed mean rating (IMDb-style):
 *        R_b = (n × R + m × C) / (n + m)
 *    where R = observed average (1–5), n = rating count for the show,
 *    C = global mean across all AirAlert episode ratings, m = prior strength (pulls low-n
 *    shows toward C so one 5★ cannot dominate a show with many strong ratings).
 *
 * 3. **Secondary** — `adderCount` (distinct users with the show on My List), then stable `id`.
 *
 * 4. **Unrated shows** — Sorted only by `adderCount` (they appear after all rated shows).
 */
import { db } from "./db.js";
import { fetchShow, fetchPreviousEpisodeAirdates } from "./tvmaze.js";

export const POPULAR_ON_AIRALERT_CONFIG = {
  /**
   * Prior strength `m` in R_b = (n×R + m×C)/(n+m). Larger = stronger shrink toward global
   * mean for small sample sizes (reduces single-rating outliers).
   */
  BAYESIAN_PRIOR_M: 14,
  /** When there are no ratings in the DB yet, use this as global prior mean C (1–5 scale). */
  FALLBACK_GLOBAL_MEAN: 3,
} as const;

export type PopularOnAiralertWeights = typeof POPULAR_ON_AIRALERT_CONFIG & {
  /** Observed global mean from `community_episode_ratings` when available. */
  globalMeanRating: number;
  globalRatingCount: number;
};

function plainSummaryHtml(s: string | null | undefined): string | null {
  if (!s) return null;
  const t = String(s)
    .replace(/<[^>]*>/g, " ")
    .replace(/\s+/g, " ")
    .trim();
  if (!t) return null;
  return t.length > 140 ? t.slice(0, 137) + "…" : t;
}

/** Bayesian average on 1–5 scale; requires n >= 1 and finite avg. */
export function bayesianSmoothedRating(
  avgRating: number,
  ratingN: number,
  globalMean: number,
  priorM: number,
): number {
  const n = Math.max(0, Math.floor(ratingN));
  const m = Math.max(0.001, priorM);
  if (n < 1) return globalMean;
  const r = Number(avgRating);
  if (!Number.isFinite(r)) return globalMean;
  return (n * r + m * globalMean) / (n + m);
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
  /** Bayesian-smoothed AirAlert rating (1–5); null when there are no episode ratings. */
  popularScore: number | null;
};

type ScoredRow = {
  id: number;
  adder_count: number;
  avg: number | null;
  ratingN: number;
  bayesian: number | null;
};

function comparePopularRows(a: ScoredRow, b: ScoredRow): number {
  const aRated = a.ratingN >= 1 && a.bayesian != null;
  const bRated = b.ratingN >= 1 && b.bayesian != null;
  if (aRated !== bRated) return aRated ? -1 : 1;
  if (aRated && bRated) {
    const br = (b.bayesian ?? 0) - (a.bayesian ?? 0);
    if (Math.abs(br) > 1e-9) return br;
    const ac = b.adder_count - a.adder_count;
    if (ac !== 0) return ac;
    return a.id - b.id;
  }
  const ac = b.adder_count - a.adder_count;
  if (ac !== 0) return ac;
  return a.id - b.id;
}

/**
 * @param opts.excludeUserId — omit shows this user already subscribes to (discovery shelf).
 */
export async function computePopularOnAiralert(opts: {
  excludeUserId: string | null;
  limit: number;
}): Promise<{
  shows: PopularOnAiralertShow[];
  shelfState: "ok" | "early" | "empty";
  weights: PopularOnAiralertWeights;
}> {
  const { excludeUserId, limit } = opts;
  const cap = Math.min(100, Math.max(1, limit));
  const { BAYESIAN_PRIOR_M, FALLBACK_GLOBAL_MEAN } = POPULAR_ON_AIRALERT_CONFIG;

  const adderRows = db
    .prepare(
      `SELECT tvmaze_show_id AS id, COUNT(*) AS adder_count
       FROM show_subscriptions
       GROUP BY tvmaze_show_id`,
    )
    .all() as { id: number; adder_count: number }[];

  if (adderRows.length === 0) {
    const weights: PopularOnAiralertWeights = {
      ...POPULAR_ON_AIRALERT_CONFIG,
      globalMeanRating: FALLBACK_GLOBAL_MEAN,
      globalRatingCount: 0,
    };
    return { shows: [], shelfState: "empty", weights };
  }

  const distinctShowCount = adderRows.length;
  const shelfState: "ok" | "early" = distinctShowCount >= 6 ? "ok" : "early";

  const globalRow = db
    .prepare(`SELECT AVG(rating) AS g_avg, COUNT(*) AS g_n FROM community_episode_ratings`)
    .get() as { g_avg: number | null; g_n: number } | undefined;
  const globalN = Math.max(0, Math.floor(Number(globalRow?.g_n ?? 0)));
  const globalMean =
    globalN > 0 && globalRow?.g_avg != null && Number.isFinite(Number(globalRow.g_avg))
      ? Number(globalRow.g_avg)
      : FALLBACK_GLOBAL_MEAN;

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

  const scored: ScoredRow[] = [];
  for (const r of adderRows) {
    const id = Number(r.id);
    if (!Number.isInteger(id) || id < 1) continue;
    const adderCount = Math.max(1, Math.floor(Number(r.adder_count) || 0));
    const rt = ratingByShow.get(id);
    const avg = rt != null ? rt.avg : null;
    const ratingN = rt != null ? rt.n : 0;
    let bayesian: number | null = null;
    if (ratingN >= 1 && avg != null && Number.isFinite(avg)) {
      bayesian = bayesianSmoothedRating(avg, ratingN, globalMean, BAYESIAN_PRIOR_M);
    }
    scored.push({ id, adder_count: adderCount, avg, ratingN, bayesian });
  }

  scored.sort(comparePopularRows);

  const candidates: ScoredRow[] = [];
  for (const s of scored) {
    if (excluded.has(s.id)) continue;
    candidates.push(s);
    if (candidates.length >= cap) break;
  }

  const weights: PopularOnAiralertWeights = {
    ...POPULAR_ON_AIRALERT_CONFIG,
    globalMeanRating: Math.round(globalMean * 10000) / 10000,
    globalRatingCount: globalN,
  };

  if (candidates.length === 0) {
    return { shows: [], shelfState, weights };
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

      const avgR =
        row.avg != null && Number.isFinite(row.avg) ? Math.round(row.avg * 100) / 100 : null;
      const ps =
        row.bayesian != null && Number.isFinite(row.bayesian)
          ? Math.round(row.bayesian * 10000) / 10000
          : null;

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
        popularScore: ps,
      };
    }),
  );

  const shows: PopularOnAiralertShow[] = fetched;
  return { shows, shelfState, weights };
}
