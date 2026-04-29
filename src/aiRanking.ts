/**
 * AI-assisted ranking layer for recommendations and trending.
 *
 * Adds thematic similarity (TF-IDF from show summaries), rating-weighted genre
 * preferences, and watch-depth signals on top of the existing scoring pipeline.
 *
 * Structured so vector embeddings can replace the text similarity later —
 * swap out `scoreSummarySimilarity` with an embedding-cosine function and the
 * rest of the pipeline stays unchanged.
 */

import { db } from "./db.js";
import type { TvmazeShowDetail } from "./tvmaze.js";

// ─── Text processing ─────────────────────────────────────────────────────────

function stripHtml(html: string): string {
  return html
    .replace(/<[^>]*>/g, " ")
    .replace(/&[a-zA-Z]+;/g, " ")
    .replace(/&#\d+;/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

const SUMMARY_STOP_WORDS = new Set([
  "the", "a", "an", "and", "or", "but", "in", "on", "at", "to", "for",
  "of", "with", "by", "from", "as", "is", "was", "are", "were", "been",
  "be", "have", "has", "had", "do", "does", "did", "will", "would",
  "could", "should", "may", "might", "shall", "can", "not", "no",
  "it", "its", "they", "them", "their", "he", "she", "his", "her",
  "him", "we", "us", "our", "you", "your", "who", "whom", "which",
  "that", "this", "these", "those", "what", "when", "where", "how",
  "all", "each", "every", "both", "few", "more", "most", "other",
  "some", "such", "than", "too", "very", "just", "also", "about",
  "after", "before", "between", "through", "during", "above", "below",
  "into", "out", "off", "over", "under", "again", "then", "once",
  "here", "there", "only", "own", "same", "so", "while", "because",
  "if", "until", "one", "two", "three", "new", "old", "first",
  "last", "long", "great", "little", "right", "big", "high", "small",
  "large", "next", "early", "young", "important", "public", "bad",
  "show", "series", "season", "episode", "episodes", "follows",
  "story", "stories", "based", "set", "life", "lives", "world",
  "group", "find", "finds", "must", "take", "takes", "come", "comes",
  "make", "makes", "get", "gets", "go", "goes", "see", "sees",
  "know", "knows", "think", "way", "back", "like", "still", "even",
  "well", "many", "much", "good", "being", "around",
  "another", "any", "become", "becomes", "face", "faces",
  "need", "needs", "turn", "turns", "help", "helps", "try", "tries",
  "keep", "keeps", "work", "works", "start", "starts", "part",
  "end", "day", "days", "time", "year", "years", "man", "woman",
  "people", "thing", "things", "place",
]);

function tokenizeSummary(text: string): string[] {
  return text
    .toLowerCase()
    .replace(/[^a-z0-9\s-]/g, " ")
    .split(/\s+/)
    .map((t) => t.trim())
    .filter((t) => t.length >= 3 && !SUMMARY_STOP_WORDS.has(t));
}

// ─── Theme profile (centroid-based keyword extraction) ────────────────────────

export type ThemeProfile = {
  /** Normalised keyword weights (unit-length vector). */
  keywords: Map<string, number>;
  docCount: number;
};

/**
 * Build a thematic profile from show summaries.
 *
 * Keywords appearing across more of the user's shows are weighted higher —
 * they represent consistent taste signals. Engagement-weighted: shows the user
 * watches more deeply contribute more to the profile.
 *
 * The resulting keyword map is L2-normalised so `scoreSummarySimilarity` can
 * compute a cosine-like similarity directly.
 */
export function buildThemeProfile(
  summaries: { text: string; weight: number }[],
): ThemeProfile {
  if (summaries.length === 0) return { keywords: new Map(), docCount: 0 };

  const keywords = new Map<string, number>();

  for (const { text, weight } of summaries) {
    const seen = new Set(tokenizeSummary(stripHtml(text)));
    for (const t of seen) {
      keywords.set(t, (keywords.get(t) ?? 0) + weight);
    }
  }

  let mag = 0;
  for (const v of keywords.values()) mag += v * v;
  mag = Math.sqrt(mag);

  if (mag > 0) {
    for (const [k, v] of keywords) keywords.set(k, v / mag);
  }

  return { keywords, docCount: summaries.length };
}

/**
 * Score a candidate show's summary against the user's theme profile.
 *
 * Returns a 0–1 cosine-like similarity: the dot product of the (normalised)
 * profile vector and the candidate's binary keyword vector, divided by the
 * candidate's magnitude. Higher means the candidate's themes overlap more
 * with what the user consistently watches.
 *
 * Drop-in replacement point for embeddings: swap this function with a
 * vector-cosine implementation and the rest of the pipeline stays the same.
 */
export function scoreSummarySimilarity(
  candidateSummary: string | null | undefined,
  profile: ThemeProfile,
): number {
  if (!candidateSummary || profile.keywords.size === 0) return 0;

  const tokens = new Set(tokenizeSummary(stripHtml(candidateSummary)));
  if (tokens.size < 2) return 0;

  let dot = 0;
  for (const t of tokens) {
    const w = profile.keywords.get(t);
    if (w != null) dot += w;
  }
  if (dot <= 0) return 0;

  return dot / Math.sqrt(tokens.size);
}

// ─── Rating-weighted genre preferences ───────────────────────────────────────

/**
 * Build genre preferences weighted by the user's episode ratings.
 *
 * Shows the user rates 4–5 boost their genres (+); shows rated 1–2 dampen
 * them (−). More ratings on a show increase confidence.
 */
export function buildRatingWeightedGenres(
  userId: string,
  showGenres: Map<number, string[]>,
): Map<string, number> {
  const out = new Map<string, number>();
  try {
    const rows = db
      .prepare(
        `SELECT tvmaze_show_id AS sid, AVG(rating) AS avgRating, COUNT(*) AS cnt
         FROM community_episode_ratings
         WHERE user_id = ?
         GROUP BY tvmaze_show_id`,
      )
      .all(userId) as { sid: number; avgRating: number; cnt: number }[];

    for (const r of rows) {
      const genres = showGenres.get(Number(r.sid));
      if (!genres) continue;
      const ratingSignal = (r.avgRating - 2.5) / 2.5; // −1 … +1
      const confidence = Math.min(r.cnt / 3, 1.5);
      const w = ratingSignal * confidence;
      for (const g of genres) {
        out.set(g, (out.get(g) ?? 0) + w);
      }
    }
  } catch {
    /* ratings table may not exist yet */
  }
  return out;
}

// ─── Watch depth ─────────────────────────────────────────────────────────────

/**
 * Per-show engagement depth: completed episodes / total tasks (0–1).
 * A depth of 0.8 means the user completed 80 % of available episode tasks.
 */
export function buildWatchDepthMap(userId: string): Map<number, number> {
  const out = new Map<number, number>();
  try {
    const rows = db
      .prepare(
        `SELECT tvmaze_show_id AS sid,
                COUNT(CASE WHEN completed_at IS NOT NULL THEN 1 END) AS completed,
                COUNT(*) AS total
         FROM watch_tasks
         WHERE user_id = ?
         GROUP BY tvmaze_show_id`,
      )
      .all(userId) as { sid: number; completed: number; total: number }[];

    for (const r of rows) {
      if (r.total === 0) continue;
      out.set(Number(r.sid), Math.min(1, r.completed / Math.max(r.total, 1)));
    }
  } catch {
    /* table may not have rows yet */
  }
  return out;
}

// ─── Composite AI profile ────────────────────────────────────────────────────

export type AIEnrichedProfile = {
  themeProfile: ThemeProfile;
  ratingWeightedGenres: Map<string, number>;
  watchDepth: Map<number, number>;
};

/**
 * Build the full AI-enriched profile from the user's subscribed show details.
 *
 * Show summaries are weighted by watch depth so deeply-watched shows
 * influence the theme profile more than shows the user barely engaged with.
 *
 * Result is cached in-memory (15 min TTL) so both trending and recommended
 * pipelines can share the same profile without redundant computation.
 */
export function buildAIEnrichedProfile(
  userId: string,
  details: TvmazeShowDetail[],
): AIEnrichedProfile {
  const cached = aiProfileCache.get(userId);
  if (cached && cached.expiresAt > Date.now()) return cached.profile;

  const watchDepth = buildWatchDepthMap(userId);

  const summaries = details
    .filter((d) => d.summary)
    .map((d) => ({
      text: d.summary!,
      weight: 1 + (watchDepth.get(d.id) ?? 0),
    }));
  const themeProfile = buildThemeProfile(summaries);

  const showGenres = new Map<number, string[]>();
  for (const d of details) {
    showGenres.set(
      d.id,
      (d.genres ?? []).map((g) => g.trim().toLowerCase()).filter((g) => g.length >= 2),
    );
  }
  const ratingWeightedGenres = buildRatingWeightedGenres(userId, showGenres);

  const profile: AIEnrichedProfile = { themeProfile, ratingWeightedGenres, watchDepth };
  aiProfileCache.set(userId, { profile, expiresAt: Date.now() + AI_PROFILE_TTL_MS });
  return profile;
}

// ─── AI scoring ──────────────────────────────────────────────────────────────

export type AIScoreBreakdown = {
  summarySimilarity: number;
  ratingGenreBoost: number;
  aiRelevanceScore: number;
  themeKeywordsMatched: string[];
  weakMatch: boolean;
};

/**
 * Tuning knobs kept together so they're easy to adjust.
 *
 * Recommended mode uses summary similarity + rating-weighted genres.
 * Trending mode uses only rating-weighted genres (candidate summaries are
 * not fetched for trending to avoid extra API calls).
 */
export const AI_WEIGHTS = {
  REC_SUMMARY_WEIGHT: 34,
  REC_RATING_GENRE_WEIGHT: 10,
  REC_WEAK_THRESHOLD: 0.038,
  /** Applied when summary AI match is weak and base taste score is not strong. */
  REC_WEAK_MULT: 0.64,

  TREND_RATING_GENRE_WEIGHT: 1.5,
} as const;

/**
 * Compute the AI relevance score for a candidate show.
 *
 * - **recommended**: full scoring — summary similarity + rating-weighted genres.
 * - **trending**: lightweight — rating-weighted genres only (no summary fetch).
 *
 * Returns an additive score plus a `weakMatch` flag that the caller can use
 * to apply a multiplicative penalty on borderline candidates.
 */
export function computeAIScore(
  candidateSummary: string | null | undefined,
  candidateGenres: string[],
  aiProfile: AIEnrichedProfile | null,
  mode: "trending" | "recommended",
): AIScoreBreakdown {
  const empty: AIScoreBreakdown = {
    summarySimilarity: 0,
    ratingGenreBoost: 0,
    aiRelevanceScore: 0,
    themeKeywordsMatched: [],
    weakMatch: false,
  };
  if (!aiProfile) return empty;

  const summarySim =
    mode === "recommended"
      ? scoreSummarySimilarity(candidateSummary, aiProfile.themeProfile)
      : 0;

  let ratingGenreBoost = 0;
  for (const g of candidateGenres) {
    const rw = aiProfile.ratingWeightedGenres.get(g);
    if (rw != null) ratingGenreBoost += rw;
  }

  const themeKeywordsMatched: string[] = [];
  if (mode === "recommended" && candidateSummary && aiProfile.themeProfile.keywords.size > 0) {
    const tokens = new Set(tokenizeSummary(stripHtml(candidateSummary)));
    for (const t of tokens) {
      const w = aiProfile.themeProfile.keywords.get(t);
      if (w != null && w > 0.05) themeKeywordsMatched.push(t);
    }
    themeKeywordsMatched.sort(
      (a, b) =>
        (aiProfile.themeProfile.keywords.get(b) ?? 0) -
        (aiProfile.themeProfile.keywords.get(a) ?? 0),
    );
  }

  const simWeight = mode === "recommended" ? AI_WEIGHTS.REC_SUMMARY_WEIGHT : 0;
  const rgWeight =
    mode === "recommended"
      ? AI_WEIGHTS.REC_RATING_GENRE_WEIGHT
      : AI_WEIGHTS.TREND_RATING_GENRE_WEIGHT;

  const aiRelevanceScore = summarySim * simWeight + Math.max(0, ratingGenreBoost) * rgWeight;

  const weakMatch =
    mode === "recommended" && summarySim < AI_WEIGHTS.REC_WEAK_THRESHOLD && ratingGenreBoost <= 0;

  return {
    summarySimilarity: summarySim,
    ratingGenreBoost,
    aiRelevanceScore,
    themeKeywordsMatched: themeKeywordsMatched.slice(0, 8),
    weakMatch,
  };
}

// ─── Cache ───────────────────────────────────────────────────────────────────

const AI_PROFILE_TTL_MS = 15 * 60 * 1000;
const aiProfileCache = new Map<string, { profile: AIEnrichedProfile; expiresAt: number }>();

/**
 * Clear the cached AI profile for a user (e.g. after they subscribe/unsubscribe).
 * Call with no argument to clear all profiles.
 */
export function clearAIProfileCache(userId?: string): void {
  if (userId) aiProfileCache.delete(userId);
  else aiProfileCache.clear();
}
