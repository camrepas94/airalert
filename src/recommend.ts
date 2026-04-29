import {
  fetchShow,
  searchShowsMerged,
  searchShowsPlain,
  fetchPreviousEpisodeAirdates,
  scanShowsCatalogForGenreFit,
  scanShowsCatalogForTrending,
  type TvmazeShowListItem,
} from "./tvmaze.js";
import { db } from "./db.js";
import { parseOnboardingPrefsJson, type OnboardingPrefs } from "./onboardingPrefs.js";
import {
  buildAIEnrichedProfile,
  computeAIScore,
  clearAIProfileCache,
  AI_WEIGHTS,
  type AIEnrichedProfile,
} from "./aiRanking.js";

export { clearAIProfileCache };

type ShowDetail = Awaited<ReturnType<typeof fetchShow>>;

const STOP_WORDS = new Set([
  "the",
  "and",
  "for",
  "with",
  "from",
  "that",
  "this",
  "show",
  "series",
  "real",
  "life",
  "story",
  "tales",
  "chronicles",
]);

function tokenizeTitle(name: string): string[] {
  return name
    .toLowerCase()
    .split(/[^a-z0-9]+/)
    .map((t) => t.trim())
    .filter((t) => t.length >= 4 && !STOP_WORDS.has(t));
}

/** Longest common prefix across show names, trimmed at a word boundary when possible. */
function commonTitlePrefix(names: string[]): string {
  if (names.length < 2) return "";
  const lower = names.map((n) => n.toLowerCase());
  let s = lower[0];
  for (let i = 1; i < lower.length; i++) {
    const t = lower[i];
    let j = 0;
    while (j < s.length && j < t.length && s[j] === t[j]) j++;
    s = s.slice(0, j);
  }
  s = s.trimEnd();
  const sp = s.lastIndexOf(" ");
  if (sp >= 6) s = s.slice(0, sp);
  return s.trim();
}

// ─── Taste profile ────────────────────────────────────────────────────────────

/** Genres from subscribed shows (lowercase) for overlap scoring. */
export function buildUserGenreSet(details: ShowDetail[]): Set<string> {
  const s = new Set<string>();
  for (const d of details) {
    for (const g of d.genres ?? []) {
      const k = g.trim().toLowerCase();
      if (k.length >= 2) s.add(k);
    }
  }
  return s;
}

/** Subscription genre counts (lowercase key) for weighted overlap. */
export function buildUserGenreWeights(details: ShowDetail[]): Map<string, number> {
  const m = new Map<string, number>();
  for (const d of details) {
    for (const g of d.genres ?? []) {
      const k = g.trim().toLowerCase();
      if (k.length < 2) continue;
      m.set(k, (m.get(k) ?? 0) + 1);
    }
  }
  return m;
}

function buildUserNetworkCounts(details: ShowDetail[]): Map<string, number> {
  const m = new Map<string, number>();
  for (const d of details) {
    const n = d.network?.name ?? d.webChannel?.name ?? "";
    const t = n.trim();
    if (t.length < 2) continue;
    m.set(t, (m.get(t) ?? 0) + 1);
  }
  return m;
}

/** TVMaze `type` counts across subscriptions (e.g. Scripted, Reality) — lowercase keys. */
function buildUserTypeWeights(details: ShowDetail[]): Map<string, number> {
  const m = new Map<string, number>();
  for (const d of details) {
    const t = (d.type ?? "").trim().toLowerCase();
    if (t.length < 2) continue;
    m.set(t, (m.get(t) ?? 0) + 1);
  }
  return m;
}

/**
 * Full user taste profile built from their subscribed show metadata.
 * Captures genre distribution, network affinity, show-type preference,
 * and engagement signals from watch_tasks / ratings.
 */
export type UserTasteProfile = {
  genreWeights: Map<string, number>;
  networkCounts: Map<string, number>;
  typeWeights: Map<string, number>;
  totalShows: number;
  /** Normalised genre fractions: genre -> (count / totalShows). For penalty math. */
  genreFractions: Map<string, number>;
  /** Genres explicitly absent from the user's list. */
  antiGenres: Set<string>;
  /** Shows with high engagement (completed tasks or ratings). */
  engagedShowIds: Set<number>;
  /** Genre weights boosted by engagement (completed tasks, ratings). */
  engagedGenreBoost: Map<string, number>;
};

function loadOnboardingPrefs(userId: string): OnboardingPrefs {
  const row = db
    .prepare(`SELECT onboarding_prefs_json FROM users WHERE id = ?`)
    .get(userId) as { onboarding_prefs_json: string | null } | undefined;
  return parseOnboardingPrefsJson(row?.onboarding_prefs_json ?? null);
}

/** TVMaze search works better with human-readable genre labels. */
function genreQueryDisplayName(k: string): string {
  if (!k || k.length < 2) return k;
  return k
    .split(/[-\s]+/)
    .map((p) => (p ? p.charAt(0).toUpperCase() + p.slice(1).toLowerCase() : p))
    .join(k.includes("-") ? "-" : " ");
}

function buildSyntheticTasteFromOnboarding(prefs: OnboardingPrefs): UserTasteProfile | null {
  if (prefs.favoriteGenres.length === 0 && prefs.favoriteNetworks.length === 0) return null;
  const genreWeights = new Map<string, number>();
  for (const g of prefs.favoriteGenres) {
    genreWeights.set(g, (genreWeights.get(g) ?? 0) + 3);
  }
  const networkCounts = new Map<string, number>();
  for (const n of prefs.favoriteNetworks) {
    networkCounts.set(n, (networkCounts.get(n) ?? 0) + 2);
  }
  const totalShows = 1;
  const genreFractions = new Map<string, number>();
  for (const [g, c] of genreWeights) genreFractions.set(g, c / totalShows);
  return {
    genreWeights,
    networkCounts,
    typeWeights: new Map(),
    totalShows,
    genreFractions,
    antiGenres: new Set(),
    engagedShowIds: new Set(),
    engagedGenreBoost: new Map(),
  };
}

function mergeOnboardingIntoProfile(base: UserTasteProfile, prefs: OnboardingPrefs): UserTasteProfile {
  const genreWeights = new Map(base.genreWeights);
  for (const g of prefs.favoriteGenres) {
    genreWeights.set(g, (genreWeights.get(g) ?? 0) + 2);
  }
  const networkCounts = new Map(base.networkCounts);
  for (const n of prefs.favoriteNetworks) {
    networkCounts.set(n, (networkCounts.get(n) ?? 0) + 1);
  }
  const totalShows = base.totalShows;
  const genreFractions = new Map<string, number>();
  for (const [g, c] of genreWeights) genreFractions.set(g, c / Math.max(totalShows, 1));
  return {
    ...base,
    genreWeights,
    networkCounts,
    genreFractions,
  };
}

function buildGenreQueriesFromProfile(profile: UserTasteProfile): {
  genreQueries: string[];
  sharedNetworkQueries: string[];
  queriesUsed: string[];
} {
  const genreQueries = [...profile.genreWeights.entries()]
    .sort((a, b) => b[1] - a[1] || a[0].localeCompare(b[0]))
    .map(([g]) => genreQueryDisplayName(g))
    .slice(0, 8);
  const sharedNetworkQueries: string[] = [];
  for (const [name, c] of profile.networkCounts) {
    if (c >= 1 && name.length >= 3) sharedNetworkQueries.push(name);
  }
  sharedNetworkQueries.sort((a, b) => a.localeCompare(b));
  const queriesUsed: string[] = [];
  for (const g of genreQueries) queriesUsed.push(`Genre: ${g}`);
  for (const n of sharedNetworkQueries.slice(0, 4)) queriesUsed.push(`Network: ${n}`);
  return { genreQueries, sharedNetworkQueries, queriesUsed };
}

const ALL_COMMON_GENRES = new Set([
  "drama", "comedy", "action", "romance", "thriller", "horror", "science-fiction",
  "fantasy", "crime", "mystery", "adventure", "family", "children", "music",
  "war", "history", "western", "sports", "nature", "food", "travel", "anime",
  "medical", "legal", "espionage", "supernatural",
]);

export function buildUserTasteProfile(details: ShowDetail[], userId: string): UserTasteProfile {
  const genreWeights = buildUserGenreWeights(details);
  const networkCounts = buildUserNetworkCounts(details);
  const typeWeights = buildUserTypeWeights(details);
  const totalShows = details.length || 1;

  const genreFractions = new Map<string, number>();
  for (const [g, c] of genreWeights) {
    genreFractions.set(g, c / totalShows);
  }

  const antiGenres = new Set<string>();
  if (details.length >= 3) {
    for (const g of ALL_COMMON_GENRES) {
      if (!genreWeights.has(g)) antiGenres.add(g);
    }
  }

  const engagedShowIds = new Set<number>();
  const engagedGenreBoost = new Map<string, number>();

  try {
    const completedRows = db
      .prepare(
        `SELECT DISTINCT tvmaze_show_id AS sid FROM watch_tasks
         WHERE user_id = ? AND completed_at IS NOT NULL`,
      )
      .all(userId) as { sid: number }[];
    for (const r of completedRows) {
      const id = Number(r.sid);
      if (Number.isInteger(id) && id > 0) engagedShowIds.add(id);
    }
  } catch { /* table may not have rows yet */ }

  try {
    const ratedRows = db
      .prepare(
        `SELECT DISTINCT tvmaze_show_id AS sid FROM community_episode_ratings
         WHERE user_id = ?`,
      )
      .all(userId) as { sid: number }[];
    for (const r of ratedRows) {
      const id = Number(r.sid);
      if (Number.isInteger(id) && id > 0) engagedShowIds.add(id);
    }
  } catch { /* table may not exist */ }

  for (const d of details) {
    if (!engagedShowIds.has(d.id)) continue;
    for (const g of d.genres ?? []) {
      const k = g.trim().toLowerCase();
      if (k.length < 2) continue;
      engagedGenreBoost.set(k, (engagedGenreBoost.get(k) ?? 0) + 1);
    }
  }

  return {
    genreWeights,
    networkCounts,
    typeWeights,
    totalShows,
    genreFractions,
    antiGenres,
    engagedShowIds,
    engagedGenreBoost,
  };
}

// ─── Scoring constants ────────────────────────────────────────────────────────

const W = {
  GENRE_OVERLAP:         50,
  GENRE_DISTINCT:        22,
  ENGAGED_GENRE_BOOST:   14,
  NETWORK_MATCH_MULTI:   30,
  NETWORK_MATCH_SINGLE:  14,
  TYPE_MATCH:            28,
  CATALOG_FIT:           10,
  SEARCH_HIT:             2,
  COLLAB_SCALE:          22,
  COLLAB_MAX:            92,
  RUNNING_BONUS:          6,
  IN_DEV_BONUS:           2,
  ANTI_GENRE_PENALTY:   -48,
  CHILDREN_PENALTY:     -72,
  /** Collab-only / search-only matches without genres — crush ghost recommendations */
  NO_OVERLAP_PENALTY:     0.035,
  /** Recommended row — fewer, higher-quality picks */
  MIN_RECOMMEND_SCORE:   46,
  POPULARITY_LEAK_PENALTY: -55,
  TYPE_MISMATCH_SCRIPTED_VS_REALITY: -88,
  TYPE_MISMATCH_SOFT: -38,
  SINGLE_WEAK_GENRE_MULT: 0.72,
} as const;

const TW = {
  GENRE_FIT_MULT:         1.6,
  NETWORK_MATCH_BONUS:   2.8,
  TYPE_MATCH_BONUS:      2.0,
  ENGAGED_GENRE_BONUS:   1.2,
  ANTI_GENRE_PENALTY:    0.15,
  CHILDREN_PENALTY:      0.08,
  BASE_RATING_MULT:      1.25,
  PERSONAL_RATING_MULT:  1.0,
  GENERAL_MIN_RATING:    7.5,
} as const;

// ─── Penalty / guardrail logic ────────────────────────────────────────────────

const CHILDREN_FAMILY_GENRES = new Set(["children", "family"]);

function hasChildrenGenre(genres: string[]): boolean {
  return genres.some((g) => CHILDREN_FAMILY_GENRES.has(g.trim().toLowerCase()));
}

function userWatchesChildrens(profile: UserTasteProfile): boolean {
  for (const g of CHILDREN_FAMILY_GENRES) {
    if ((profile.genreWeights.get(g) ?? 0) >= 1) return true;
  }
  return false;
}

/** How strongly user's subscriptions imply Scripted vs Reality vs other (TVMaze `type`). */
function typeDistribution(profile: UserTasteProfile): {
  scripted: number;
  reality: number;
  total: number;
  scriptedShare: number;
  realityShare: number;
} {
  let scripted = 0;
  let reality = 0;
  let total = 0;
  for (const [k, v] of profile.typeWeights) {
    total += v;
    if (k === "scripted") scripted += v;
    if (k === "reality") reality += v;
  }
  const safe = Math.max(total, 1);
  return {
    scripted,
    reality,
    total,
    scriptedShare: scripted / safe,
    realityShare: reality / safe,
  };
}

/**
 * Penalize scripted↔reality mismatch when the user's list is strongly skewed one way.
 * Requires multiple overlapping genres before we allow a cross-format pick.
 */
function scriptedRealityMismatchPenalty(
  profile: UserTasteProfile,
  candidateType: string,
  distinctGenreMatches: number,
  genreOverlap: number,
): number {
  const t = candidateType.trim().toLowerCase();
  if (t !== "scripted" && t !== "reality") return 0;
  const { scriptedShare, realityShare, total } = typeDistribution(profile);
  if (total < 3) return 0;

  const strongGenreBridge = distinctGenreMatches >= 2 || genreOverlap >= 4;

  // Mostly scripted subscriptions → Reality candidates need strong genre proof
  if (
    t === "reality" &&
    scriptedShare >= 0.62 &&
    realityShare <= 0.12 &&
    !strongGenreBridge
  ) {
    return W.TYPE_MISMATCH_SCRIPTED_VS_REALITY;
  }
  // Mostly reality → scripted picks without overlap are often wrong-fit
  if (
    t === "scripted" &&
    realityShare >= 0.52 &&
    scriptedShare <= 0.22 &&
    distinctGenreMatches < 2 &&
    genreOverlap < 3
  ) {
    return W.TYPE_MISMATCH_SOFT;
  }
  return 0;
}

/** Collab should not float unrelated titles when genre overlap is absent. */
function collaborativeTasteFactor(
  collabRaw: number,
  distinctGenreMatches: number,
  genreOverlap: number,
  typeBonus: number,
): number {
  if (collabRaw < 1) return 1;
  if (distinctGenreMatches >= 2 || genreOverlap >= 3.5) return 1;
  if (distinctGenreMatches === 1 && (typeBonus > 0 || genreOverlap >= 1.8)) return 0.92;
  if (distinctGenreMatches === 0 && genreOverlap === 0) return 0.22;
  if (distinctGenreMatches === 1) return 0.58;
  return 0.78;
}

/** Catalog/search popularity without any weighted genre overlap — usually junk fits. */
function popularityWithoutTastePenalty(
  catalogFit: number,
  searchHits: number,
  distinctGenreMatches: number,
  genreOverlap: number,
): number {
  if (distinctGenreMatches > 0 || genreOverlap > 0.01) return 0;
  const leak = catalogFit * 9 + searchHits * 14;
  if (leak < 8) return 0;
  return Math.min(72, leak + 18);
}

function computeTasteConfidence(
  profile: UserTasteProfile,
  distinctGenreMatches: number,
  genreOverlap: number,
  networkBonus: number,
  typeBonus: number,
): number {
  const gw = [...profile.genreWeights.values()].reduce((a, b) => a + b, 0) || 1;
  const overlapNorm = Math.min(1, genreOverlap / Math.max(2.5, gw * 0.38));
  let c = distinctGenreMatches * 19 + overlapNorm * 44;
  if (networkBonus > 0) c += 10;
  if (typeBonus > 0) c += 12;
  return Math.min(100, Math.round(c * 10) / 10);
}

export type QualityGateResult = { ok: boolean; reason: string };

/** Minimum belief before a title is shown in Recommended (not Trending). */
export function passesRecommendedQualityGate(
  d: DebugScoreInfo,
  aiSummarySimilarity: number,
  finalScoreAfterAi: number,
): QualityGateResult {
  const g = d.distinctGenreMatches;
  const go = d.genreOverlap;
  const strongGenres = g >= 2 || go >= 4;
  const moderateGenres = g >= 1 && (go >= 2 || d.networkBonus > 0);
  const strongAI = aiSummarySimilarity >= 0.072;
  const decentAI = aiSummarySimilarity >= 0.048;
  const collabPlusTaste =
    d.collabBonus > 38 && g >= 1 && (go >= 1.2 || d.typeBonus > 0 || d.networkBonus > 0);
  const bridgeNetType = d.networkBonus > 0 && d.typeBonus > 0 && g >= 1;

  if (strongGenres) return { ok: true, reason: "strong_genre_overlap" };
  if (bridgeNetType && go >= 1) return { ok: true, reason: "network+type+genre" };
  if (moderateGenres && decentAI) return { ok: true, reason: "genre+summary" };
  if (strongAI && g >= 1) return { ok: true, reason: "summary+taste" };
  if (aiSummarySimilarity >= 0.095 && g >= 1) return { ok: true, reason: "strong_summary" };
  if (collabPlusTaste) return { ok: true, reason: "social+taste_aligned" };
  if (d.tasteConfidence >= 62 && g >= 1 && go >= 2) return { ok: true, reason: "taste_confidence" };
  if (finalScoreAfterAi >= 118 && g >= 1) return { ok: true, reason: "high_composite_score" };

  return { ok: false, reason: "insufficient_taste_alignment" };
}

/** Count how many of a show's genres are in the user's anti-genre set. */
function antiGenreCount(genres: string[], profile: UserTasteProfile): number {
  let c = 0;
  for (const g of genres) {
    if (profile.antiGenres.has(g.trim().toLowerCase())) c++;
  }
  return c;
}

// ─── Debug info type ──────────────────────────────────────────────────────────

export type DebugScoreInfo = {
  genreOverlap: number;
  distinctGenreMatches: number;
  engagedGenreBoost: number;
  networkBonus: number;
  typeBonus: number;
  catalogFit: number;
  searchHits: number;
  collabBonus: number;
  collabTasteFactor: number;
  runningBonus: number;
  antiGenrePenalty: number;
  childrenPenalty: number;
  typeMismatchPenalty: number;
  popularityLeakPenalty: number;
  noOverlapPenalty: boolean;
  rawScore: number;
  finalScore: number;
  tasteConfidence: number;
  matchedGenres: string[];
  matchedNetwork: string | null;
  matchedType: string | null;
  aiSummarySimilarity?: number;
  aiRatingGenreBoost?: number;
  aiRelevanceScore?: number;
  aiThemeKeywords?: string[];
  aiWeakMatch?: boolean;
  qualityGateReason?: string;
  filteredOut?: boolean;
};

// ─── Collaborative filtering ─────────────────────────────────────────────────

/**
 * Co-subscription strength: for each show Y, count how many **other users** (who share at least one
 * of your subscribed shows) also subscribe to Y. Surfaces "people who watch what you watch also watch…".
 */
export function collaborativeShowScores(userId: string): Map<number, number> {
  const out = new Map<number, number>();
  const rows = db
    .prepare(
      `SELECT o.tvmaze_show_id AS showId, COUNT(*) AS w
       FROM show_subscriptions o
       WHERE o.user_id IN (
         SELECT DISTINCT s.user_id FROM show_subscriptions s
         WHERE s.user_id != ?
           AND s.tvmaze_show_id IN (SELECT tvmaze_show_id FROM show_subscriptions WHERE user_id = ?)
       )
       AND o.tvmaze_show_id NOT IN (SELECT tvmaze_show_id FROM show_subscriptions WHERE user_id = ?)
       GROUP BY o.tvmaze_show_id
       ORDER BY w DESC
       LIMIT 220`,
    )
    .all(userId, userId, userId) as { showId: number; w: number }[];

  for (const r of rows) {
    const id = Number(r.showId);
    const w = Number(r.w);
    if (!Number.isInteger(id) || id < 1 || !Number.isFinite(w)) continue;
    out.set(id, w);
  }
  return out;
}

// ─── Query builders ──────────────────────────────────────────────────────────

/**
 * Primary: TVMaze genre names from your subscriptions (search uses the same strings TVMaze lists).
 * Secondary: networks shared by 2+ subscribed shows (only used if genre search is thin).
 */
export function buildGenreFirstQueries(details: ShowDetail[]): {
  genreQueries: string[];
  sharedNetworkQueries: string[];
  queriesUsed: string[];
} {
  const genreCounts = new Map<string, number>();
  const netCounts = new Map<string, number>();

  for (const d of details) {
    for (const g of d.genres ?? []) {
      const raw = g.trim();
      if (raw.length >= 2) genreCounts.set(raw, (genreCounts.get(raw) ?? 0) + 1);
    }
    const net = d.network?.name ?? d.webChannel?.name ?? "";
    if (net.trim()) {
      const n = net.trim();
      netCounts.set(n, (netCounts.get(n) ?? 0) + 1);
    }
  }

  const genreQueries = [...genreCounts.entries()]
    .sort((a, b) => b[1] - a[1] || a[0].localeCompare(b[0]))
    .map(([g]) => g)
    .slice(0, 7);

  const sharedNetworkQueries: string[] = [];
  for (const [name, c] of netCounts) {
    if (c >= 2 && name.length >= 3) sharedNetworkQueries.push(name);
  }

  const queriesUsed: string[] = [];
  for (const g of genreQueries) queriesUsed.push(`Genre: ${g}`);
  for (const n of sharedNetworkQueries.slice(0, 2)) queriesUsed.push(`Network: ${n}`);

  return { genreQueries, sharedNetworkQueries, queriesUsed };
}

/** Legacy mixed query builder — used only when subscribed shows have no genre metadata. */
export function buildRecommendationQueries(details: ShowDetail[]): string[] {
  const queries: string[] = [];
  if (details.length === 0) return queries;

  const genreCounts = new Map<string, number>();
  const wordCounts = new Map<string, number>();
  const netCounts = new Map<string, number>();

  for (const d of details) {
    for (const g of d.genres ?? []) {
      const k = g.trim().toLowerCase();
      if (k.length >= 3) genreCounts.set(k, (genreCounts.get(k) ?? 0) + 1);
    }
    for (const w of tokenizeTitle(d.name)) {
      wordCounts.set(w, (wordCounts.get(w) ?? 0) + 1);
    }
    const net = d.network?.name ?? d.webChannel?.name ?? "";
    if (net.trim()) {
      const n = net.trim();
      netCounts.set(n, (netCounts.get(n) ?? 0) + 1);
    }
  }

  const topGenres = [...genreCounts.entries()]
    .sort((a, b) => b[1] - a[1] || a[0].localeCompare(b[0]))
    .map(([g]) => g)
    .slice(0, 3);
  queries.push(...topGenres);

  for (const [name, c] of netCounts) {
    if (c >= 2 && name.length >= 3) queries.push(name);
  }

  const topWords = [...wordCounts.entries()]
    .sort((a, b) => b[1] - a[1] || a[0].localeCompare(b[0]))
    .map(([w]) => w)
    .slice(0, 4);
  queries.push(...topWords);

  const prefix = commonTitlePrefix(details.map((d) => d.name));
  if (prefix.length >= 8) queries.push(prefix);

  const seen = new Set<string>();
  const out: string[] = [];
  for (const q of queries) {
    const k = q.trim().toLowerCase();
    if (k.length < 3 || seen.has(k)) continue;
    seen.add(k);
    out.push(q.trim());
    if (out.length >= 7) break;
  }
  return out;
}

export async function fetchShowDetailsForRecommend(showIds: number[]): Promise<ShowDetail[]> {
  const unique = [...new Set(showIds)].slice(0, 15);
  const out: ShowDetail[] = [];
  for (let i = 0; i < unique.length; i += 6) {
    const chunk = unique.slice(i, i + 6);
    const part = await Promise.all(
      chunk.map(async (id) => {
        try {
          return await fetchShow(id);
        } catch {
          return null;
        }
      }),
    );
    for (const p of part) {
      if (p) out.push(p);
    }
  }
  return out;
}

/** True if YYYY-MM-DD is within the last `months` calendar months (from today, local). */
function isAirdateWithinLastMonths(yyyyMmDd: string | null | undefined, months: number): boolean {
  if (!yyyyMmDd || typeof yyyyMmDd !== "string") return false;
  const m = yyyyMmDd.trim().match(/^(\d{4})-(\d{2})-(\d{2})/);
  if (!m) return false;
  const y = Number(m[1]);
  const mo = Number(m[2]);
  const day = Number(m[3]);
  if (!Number.isFinite(y) || !Number.isFinite(mo) || !Number.isFinite(day)) return false;
  const ep = new Date(y, mo - 1, day);
  const cutoff = new Date();
  cutoff.setHours(0, 0, 0, 0);
  cutoff.setMonth(cutoff.getMonth() - months);
  return ep.getTime() >= cutoff.getTime();
}

export type RecommendedShowHit = {
  id: number;
  name: string;
  network: string | null;
  premiered: string | null;
  image: string | null;
  lastAiredDate: string | null;
  matchScore: number;
  _debug?: DebugScoreInfo;
};

// ─── Recommend: score a single enriched candidate ────────────────────────────

function scoreRecommendedCandidate(
  d: ShowDetail,
  catalogFit: number,
  searchHits: number,
  collabRaw: number,
  profile: UserTasteProfile,
): { matchScore: number; debug: DebugScoreInfo } {
  const genres = (d.genres ?? []).map((g) => g.trim().toLowerCase()).filter((g) => g.length >= 2);
  const netTrim = (d.network?.name ?? d.webChannel?.name ?? "").trim();
  const typeKey = (d.type ?? "").trim().toLowerCase();

  let genreOverlap = 0;
  let distinctGenreMatches = 0;
  const matchedGenres: string[] = [];
  for (const g of genres) {
    const w = profile.genreWeights.get(g);
    if (w != null && w > 0) {
      distinctGenreMatches++;
      matchedGenres.push(g);
    }
    genreOverlap += w ?? 0;
  }

  let engagedGenreBoost = 0;
  for (const g of genres) {
    engagedGenreBoost += profile.engagedGenreBoost.get(g) ?? 0;
  }

  const netHits = netTrim ? (profile.networkCounts.get(netTrim) ?? 0) : 0;
  const networkBonus = netHits >= 2 ? W.NETWORK_MATCH_MULTI : netHits === 1 ? W.NETWORK_MATCH_SINGLE : 0;
  const matchedNetwork = netHits > 0 ? netTrim : null;

  const typeCount = typeKey ? (profile.typeWeights.get(typeKey) ?? 0) : 0;
  const typeBonus = typeCount > 0 ? typeCount * W.TYPE_MATCH : 0;
  const matchedType = typeCount > 0 ? typeKey : null;

  const runningBonus = d.status === "Running" ? W.RUNNING_BONUS : d.status === "In Development" ? W.IN_DEV_BONUS : 0;

  const collabTasteFactor = collaborativeTasteFactor(
    collabRaw,
    distinctGenreMatches,
    genreOverlap,
    typeBonus,
  );
  let collabBonus =
    Math.min(W.COLLAB_MAX, Math.sqrt(collabRaw + 0.15) * W.COLLAB_SCALE) * collabTasteFactor;

  let antiGenrePenalty = 0;
  const badCount = antiGenreCount(d.genres ?? [], profile);
  if (badCount > 0 && profile.totalShows >= 3) {
    antiGenrePenalty = badCount * W.ANTI_GENRE_PENALTY;
  }

  let childrenPenalty = 0;
  if (hasChildrenGenre(d.genres ?? []) && !userWatchesChildrens(profile)) {
    childrenPenalty = W.CHILDREN_PENALTY;
  }

  const typeMismatchPenalty = scriptedRealityMismatchPenalty(
    profile,
    typeKey,
    distinctGenreMatches,
    genreOverlap,
  );

  let popularityLeakPenalty = popularityWithoutTastePenalty(
    catalogFit,
    searchHits,
    distinctGenreMatches,
    genreOverlap,
  );
  if (popularityLeakPenalty !== 0) {
    popularityLeakPenalty = -Math.min(72, popularityLeakPenalty);
  }

  let rawScore =
    genreOverlap * W.GENRE_OVERLAP +
    distinctGenreMatches * W.GENRE_DISTINCT +
    engagedGenreBoost * W.ENGAGED_GENRE_BOOST +
    catalogFit * W.CATALOG_FIT +
    searchHits * W.SEARCH_HIT +
    collabBonus +
    typeBonus +
    networkBonus +
    runningBonus +
    antiGenrePenalty +
    childrenPenalty +
    typeMismatchPenalty +
    popularityLeakPenalty;

  let noOverlapPenalty = false;
  if (
    genreOverlap === 0 &&
    catalogFit <= 0 &&
    networkBonus === 0 &&
    typeBonus === 0 &&
    collabRaw < 2
  ) {
    rawScore *= W.NO_OVERLAP_PENALTY;
    noOverlapPenalty = true;
  }

  /** Single overlapping genre with a broad catalog genre ("Drama") — dampen unless overlap mass is real */
  const genreMass = [...profile.genreWeights.values()].reduce((a, b) => a + b, 0);
  if (
    profile.totalShows >= 5 &&
    distinctGenreMatches === 1 &&
    genreOverlap < Math.min(2.8, genreMass * 0.09)
  ) {
    rawScore *= W.SINGLE_WEAK_GENRE_MULT;
  }

  const tasteConfidence = computeTasteConfidence(
    profile,
    distinctGenreMatches,
    genreOverlap,
    networkBonus,
    typeBonus,
  );

  const finalScore = rawScore;

  return {
    matchScore: finalScore,
    debug: {
      genreOverlap,
      distinctGenreMatches,
      engagedGenreBoost,
      networkBonus,
      typeBonus,
      catalogFit,
      searchHits,
      collabBonus,
      collabTasteFactor,
      runningBonus,
      antiGenrePenalty,
      childrenPenalty,
      typeMismatchPenalty,
      popularityLeakPenalty,
      noOverlapPenalty,
      rawScore,
      finalScore,
      tasteConfidence,
      matchedGenres,
      matchedNetwork,
      matchedType,
    },
  };
}

// ─── Recommended shows ───────────────────────────────────────────────────────

/**
 * Suggest shows using:
 * - **Collaborative filtering**: other users who share your shows — what else they subscribe to.
 * - **Genre + catalog fit** (wider TVMaze `/shows` sample) and **TVMaze type** (Scripted / Reality / …) vs your list.
 * - **Genre / network search** when metadata exists; title heuristics as fallback.
 * - **Engagement signals**: episodes marked watched, episode ratings boost genre affinity.
 * - **Guardrails**: children/family content penalised if user never watches it; anti-genre penalties;
 *   minimum relevance threshold removes weak matches.
 */
export async function computeRecommendedShows(
  userId: string,
  subscribedShowIds: number[],
  opts?: { debug?: boolean },
): Promise<{
  shows: RecommendedShowHit[];
  queriesUsed: string[];
}> {
  const subSet = new Set(subscribedShowIds);
  const prefs = loadOnboardingPrefs(userId);

  let profile: UserTasteProfile;
  let collaborative: Map<number, number>;
  let details: ShowDetail[];
  let genreQueries: string[];
  let sharedNetworkQueries: string[];
  let planLines: string[];

  let aiProfile: AIEnrichedProfile | null = null;

  if (subscribedShowIds.length === 0) {
    const syn = buildSyntheticTasteFromOnboarding(prefs);
    if (!syn) {
      return {
        shows: [],
        queriesUsed: [
          "Add favorite genres or streaming services in Personalize — then we can suggest titles here.",
        ],
      };
    }
    profile = syn;
    collaborative = new Map();
    details = [];
    const q = buildGenreQueriesFromProfile(profile);
    genreQueries = q.genreQueries;
    sharedNetworkQueries = q.sharedNetworkQueries;
    planLines = q.queriesUsed;
  } else {
    details = await fetchShowDetailsForRecommend(subscribedShowIds);
    if (details.length === 0) {
      return { shows: [], queriesUsed: [] };
    }
    profile = buildUserTasteProfile(details, userId);
    if (
      subscribedShowIds.length < 5 &&
      (prefs.favoriteGenres.length > 0 || prefs.favoriteNetworks.length > 0)
    ) {
      profile = mergeOnboardingIntoProfile(profile, prefs);
    }
    collaborative = collaborativeShowScores(userId);
    aiProfile = buildAIEnrichedProfile(userId, details);
    const q = buildGenreFirstQueries(details);
    genreQueries = q.genreQueries;
    sharedNetworkQueries = q.sharedNetworkQueries;
    planLines = q.queriesUsed;
  }

  const queriesUsed: string[] = [];

  if (subscribedShowIds.length === 0) {
    queriesUsed.push(
      "Recommendations from your onboarding taste picks — add shows anytime to refine further.",
    );
  } else if (collaborative.size > 0) {
    const topCo = [...collaborative.values()].reduce((a, b) => Math.max(a, b), 0);
    queriesUsed.push(
      `Viewers like you: up to ${collaborative.size} candidate shows from overlapping subscriptions (max ${topCo} co-subscribers per title)`,
    );
  }

  type Agg = {
    id: number;
    name: string;
    network: string | null;
    premiered: string | null;
    image: string | null;
    searchHits: number;
    catalogFit: number;
    collabScore: number;
  };

  const candidates = new Map<number, Agg>();

  if (profile.genreWeights.size > 0) {
    queriesUsed.push("Catalog: broader show pages, scored by weighted genre overlap");
    const catalogMatches = await scanShowsCatalogForGenreFit(profile.genreWeights, subSet, {
      pageRanges: [
        [0, 18],
        [32, 62],
        [88, 118],
      ],
      concurrency: 8,
    });
    for (const [id, { show, genreFit }] of catalogMatches) {
      candidates.set(id, {
        id,
        name: show.name,
        network: show.network?.name ?? show.webChannel?.name ?? null,
        premiered: show.premiered ?? null,
        image: show.image?.medium ?? null,
        searchHits: 0,
        catalogFit: genreFit,
        collabScore: collaborative.get(id) ?? 0,
      });
    }
  }

  for (const [id, w] of collaborative) {
    if (subSet.has(id)) continue;
    const cur = candidates.get(id);
    if (cur) {
      cur.collabScore = w;
    } else {
      candidates.set(id, {
        id,
        name: "",
        network: null,
        premiered: null,
        image: null,
        searchHits: 0,
        catalogFit: 0,
        collabScore: w,
      });
    }
  }

  queriesUsed.push(...planLines);

  async function addSearchHits(plan: { q: string; weight: number; merged: boolean }[]) {
    await Promise.all(
      plan.map(async ({ q, weight, merged }) => {
        const hits = merged ? await searchShowsMerged(q) : await searchShowsPlain(q);
        for (const h of hits) {
          const id = h.show.id;
          if (subSet.has(id)) continue;
          let cur = candidates.get(id);
          if (!cur) {
            cur = {
              id,
              name: h.show.name,
              network: h.show.network?.name ?? h.show.webChannel?.name ?? null,
              premiered: h.show.premiered ?? null,
              image: h.show.image?.medium ?? null,
              searchHits: 0,
              catalogFit: 0,
              collabScore: collaborative.get(id) ?? 0,
            };
            candidates.set(id, cur);
          }
          cur.searchHits += weight;
        }
      }),
    );
  }

  const useHeuristicFallback = genreQueries.length === 0;

  if (!useHeuristicFallback) {
    await addSearchHits(genreQueries.map((q) => ({ q, weight: 2, merged: false })));
    if (candidates.size < 14 && sharedNetworkQueries.length > 0) {
      await addSearchHits(sharedNetworkQueries.slice(0, 3).map((q) => ({ q, weight: 1, merged: false })));
    }
    if (profile.genreWeights.size > 0 && candidates.size < 18) {
      await addSearchHits(genreQueries.slice(0, 5).map((q) => ({ q, weight: 1, merged: true })));
    }
  } else {
    queriesUsed.push("Heuristic queries (no genre tags on subscriptions — title/network search)");
    let fallback: string[];
    if (details.length > 0) {
      fallback = buildRecommendationQueries(details);
    } else {
      const genQs = prefs.favoriteGenres.slice(0, 6).map((g) => genreQueryDisplayName(g));
      const netQs = prefs.favoriteNetworks.slice(0, 4);
      fallback = genQs.length > 0 ? genQs : netQs.length > 0 ? netQs : ["drama"];
    }
    queriesUsed.push(...fallback.map((q) => `Search: ${q}`));
    await addSearchHits(fallback.map((q) => ({ q, weight: 1, merged: true })));
  }

  if (candidates.size === 0) {
    return { shows: [], queriesUsed };
  }

  let ordered = [...candidates.values()].sort(
    (a, b) =>
      b.collabScore * 20 +
        b.catalogFit * 14 +
        b.searchHits * 3 -
        (a.collabScore * 20 + a.catalogFit * 14 + a.searchHits * 3) ||
      a.name.localeCompare(b.name),
  );
  ordered = ordered.slice(0, 72);

  type Row = Agg & { matchScore: number; _debug?: DebugScoreInfo };
  const enriched: Row[] = [];
  const debugRecommended = opts?.debug === true;

  /** Taste-confidence gate needs real subscription signals; onboarding-only lists use score threshold only. */
  const applyQualityGate = subscribedShowIds.length >= 1;

  for (let i = 0; i < ordered.length; i += 6) {
    const chunk = ordered.slice(i, i + 6);
    const part = await Promise.all(
      chunk.map(async (c): Promise<Row | null> => {
        try {
          const d = await fetchShow(c.id);
          const { matchScore, debug } = scoreRecommendedCandidate(
            d,
            c.catalogFit,
            c.searchHits,
            c.collabScore ?? 0,
            profile,
          );

          const candidateGenres = (d.genres ?? [])
            .map((g) => g.trim().toLowerCase())
            .filter((g) => g.length >= 2);
          const aiResult = computeAIScore(d.summary, candidateGenres, aiProfile, "recommended");

          let finalScore = matchScore + aiResult.aiRelevanceScore;
          if (aiResult.weakMatch && matchScore < 88) {
            finalScore *= AI_WEIGHTS.REC_WEAK_MULT;
          }

          let qualityGateReason = "skipped_gate";
          if (applyQualityGate) {
            const gate = passesRecommendedQualityGate(debug, aiResult.summarySimilarity, finalScore);
            qualityGateReason = gate.reason;
            if (!gate.ok) {
              if (debugRecommended) {
                return {
                  ...c,
                  matchScore: finalScore,
                  _debug: {
                    ...debug,
                    finalScore,
                    aiSummarySimilarity: aiResult.summarySimilarity,
                    aiRatingGenreBoost: aiResult.ratingGenreBoost,
                    aiRelevanceScore: aiResult.aiRelevanceScore,
                    aiThemeKeywords: aiResult.themeKeywordsMatched,
                    aiWeakMatch: aiResult.weakMatch,
                    qualityGateReason,
                    filteredOut: true,
                  },
                  name: d.name ?? c.name,
                  network: (d.network?.name ?? d.webChannel?.name ?? "").trim() || c.network,
                  premiered: d.premiered ?? c.premiered,
                  image: d.image?.medium ?? c.image,
                };
              }
              return null;
            }
          }

          if (finalScore < W.MIN_RECOMMEND_SCORE) {
            if (debugRecommended) {
              return {
                ...c,
                matchScore: finalScore,
                _debug: {
                  ...debug,
                  finalScore,
                  aiSummarySimilarity: aiResult.summarySimilarity,
                  aiRatingGenreBoost: aiResult.ratingGenreBoost,
                  aiRelevanceScore: aiResult.aiRelevanceScore,
                  aiThemeKeywords: aiResult.themeKeywordsMatched,
                  aiWeakMatch: aiResult.weakMatch,
                  qualityGateReason: `below_min_score(${W.MIN_RECOMMEND_SCORE})`,
                  filteredOut: true,
                },
                name: d.name ?? c.name,
                network: (d.network?.name ?? d.webChannel?.name ?? "").trim() || c.network,
                premiered: d.premiered ?? c.premiered,
                image: d.image?.medium ?? c.image,
              };
            }
            return null;
          }

          return {
            ...c,
            matchScore: finalScore,
            _debug: {
              ...debug,
              finalScore,
              aiSummarySimilarity: aiResult.summarySimilarity,
              aiRatingGenreBoost: aiResult.ratingGenreBoost,
              aiRelevanceScore: aiResult.aiRelevanceScore,
              aiThemeKeywords: aiResult.themeKeywordsMatched,
              aiWeakMatch: aiResult.weakMatch,
              qualityGateReason,
              filteredOut: false,
            },
            name: d.name ?? c.name,
            network: (d.network?.name ?? d.webChannel?.name ?? "").trim() || c.network,
            premiered: d.premiered ?? c.premiered,
            image: d.image?.medium ?? c.image,
          };
        } catch {
          const cr = c.collabScore ?? 0;
          return {
            ...c,
            matchScore: Math.sqrt(cr + 0.1) * 28 + c.catalogFit * 16 + c.searchHits * 2,
          };
        }
      }),
    );
    for (const row of part) {
      if (row != null) enriched.push(row);
    }
  }

  const filtered = enriched.filter((e) => e.matchScore >= W.MIN_RECOMMEND_SCORE && !e._debug?.filteredOut);
  filtered.sort((a, b) => b.matchScore - a.matchScore || a.name.localeCompare(b.name));
  const top = filtered.slice(0, 18);

  const lastAiredById = await fetchPreviousEpisodeAirdates(top.map((s) => s.id));

  let shows: RecommendedShowHit[] = top.map((s) => {
    const hit: RecommendedShowHit = {
      id: s.id,
      name: s.name,
      network: s.network,
      premiered: s.premiered,
      image: s.image,
      lastAiredDate: lastAiredById.get(s.id) ?? null,
      matchScore: s.matchScore,
    };
    if (debugRecommended && s._debug) hit._debug = s._debug;
    return hit;
  });

  shows.sort((a, b) => {
    if (b.matchScore !== a.matchScore) return b.matchScore - a.matchScore;
    if (a.lastAiredDate && b.lastAiredDate) return b.lastAiredDate.localeCompare(a.lastAiredDate);
    if (a.lastAiredDate && !b.lastAiredDate) return -1;
    if (!a.lastAiredDate && b.lastAiredDate) return 1;
    return a.name.localeCompare(b.name);
  });

  shows = shows.slice(0, 10);

  return { shows, queriesUsed };
}

// ─── Trending shows ──────────────────────────────────────────────────────────

/**
 * Up to **25** **streaming** (TVMaze `webChannel`), **running** shows: personalized by genre,
 * network, and show-type overlap with the user's subscriptions, with guardrails against
 * irrelevant categories.
 * Only includes shows whose **previous aired episode** was within the last **4 months**.
 */
export async function computeTrendingShows(
  subscribedShowIds: number[],
  userId?: string,
): Promise<RecommendedShowHit[]> {
  const subSet = new Set(subscribedShowIds);

  let profile: UserTasteProfile | null = null;
  let userGenreWeights = new Map<string, number>();
  let aiProfile: AIEnrichedProfile | null = null;
  if (subscribedShowIds.length > 0) {
    const details = await fetchShowDetailsForRecommend(subscribedShowIds);
    userGenreWeights = buildUserGenreWeights(details);
    profile = buildUserTasteProfile(details, userId ?? "");
    if (userId) {
      aiProfile = buildAIEnrichedProfile(userId, details);
    }
  } else if (userId) {
    const prefs = loadOnboardingPrefs(userId);
    const syn = buildSyntheticTasteFromOnboarding(prefs);
    if (syn) {
      profile = syn;
      userGenreWeights = syn.genreWeights;
    }
  }

  const catalogMatches = await scanShowsCatalogForTrending(userGenreWeights, subSet, {
    pageRanges: [
      [0, 45],
      [48, 100],
      [115, 185],
    ],
    concurrency: 8,
  });

  type ScoredEntry = { show: TvmazeShowListItem; trendScore: number };
  const rescored: ScoredEntry[] = [];

  for (const [, entry] of catalogMatches) {
    const show = entry.show;
    let score = entry.trendScore;

    if (
      profile &&
      (subscribedShowIds.length === 0 || profile.totalShows >= 2)
    ) {
      const p = profile;
      const genres = (show.genres ?? []).map((g) => g.trim().toLowerCase()).filter((g) => g.length >= 2);
      const netName = (show.network?.name ?? show.webChannel?.name ?? "").trim();
      const typeKey = (show.type ?? "").trim().toLowerCase();

      let genreFit = 0;
      for (const g of genres) {
        const w = p.genreWeights.get(g);
        if (w != null && w > 0) genreFit += w;
      }
      if (genreFit > 0) {
        score += genreFit * TW.GENRE_FIT_MULT;
      }

      let engagedFit = 0;
      for (const g of genres) {
        engagedFit += p.engagedGenreBoost.get(g) ?? 0;
      }
      if (engagedFit > 0) {
        score += engagedFit * TW.ENGAGED_GENRE_BONUS;
      }

      if (netName && (p.networkCounts.get(netName) ?? 0) >= 1) {
        score += (p.networkCounts.get(netName) ?? 0) * TW.NETWORK_MATCH_BONUS;
      }

      if (typeKey && (p.typeWeights.get(typeKey) ?? 0) >= 1) {
        score += (p.typeWeights.get(typeKey) ?? 0) * TW.TYPE_MATCH_BONUS;
      }

      const distinctGenreHits = genres.filter((g) => (p.genreWeights.get(g) ?? 0) > 0).length;
      const typeMis = scriptedRealityMismatchPenalty(p, typeKey, distinctGenreHits, genreFit);
      if (typeMis < 0) {
        score += typeMis * 0.42;
      }

      if (hasChildrenGenre(show.genres ?? []) && !userWatchesChildrens(p)) {
        score *= TW.CHILDREN_PENALTY;
      }

      const badCount = antiGenreCount(show.genres ?? [], p);
      if (badCount > 0 && genreFit === 0) {
        score *= Math.pow(TW.ANTI_GENRE_PENALTY, badCount);
      }

      if (aiProfile) {
        const aiResult = computeAIScore(null, genres, aiProfile, "trending");
        score += aiResult.aiRelevanceScore;
      }

      // Timely/social lean — but not a free pass for cold genre fit on rich profiles
      if (subscribedShowIds.length >= 5 && genreFit === 0 && badCount === 0) {
        const typed = typeKey && (p.typeWeights.get(typeKey) ?? 0) >= 1;
        if (!typed) score *= 0.44;
      }
    }

    rescored.push({ show, trendScore: score });
  }

  rescored.sort((a, b) => b.trendScore - a.trendScore || a.show.name.localeCompare(b.show.name));

  const RECENT_MONTHS = 4;
  const TARGET = 25;
  const CHUNK = 40;
  const MAX_SCAN = 320;

  const selected: { show: TvmazeShowListItem; trendScore: number; lastAired: string | null }[] = [];
  let offset = 0;

  while (selected.length < TARGET && offset < rescored.length && offset < MAX_SCAN) {
    const chunk = rescored.slice(offset, offset + CHUNK);
    offset += CHUNK;
    if (chunk.length === 0) break;

    const lastAiredById = await fetchPreviousEpisodeAirdates(chunk.map((c) => c.show.id));

    for (const c of chunk) {
      const d = lastAiredById.get(c.show.id) ?? null;
      if (!isAirdateWithinLastMonths(d, RECENT_MONTHS)) continue;
      selected.push({ show: c.show, trendScore: c.trendScore, lastAired: d });
      if (selected.length >= TARGET) break;
    }
  }

  return selected.map(({ show, trendScore, lastAired }) => ({
    id: show.id,
    name: show.name,
    network: show.webChannel?.name ?? show.network?.name ?? null,
    premiered: show.premiered ?? null,
    image: show.image?.medium ?? null,
    lastAiredDate: lastAired,
    matchScore: trendScore,
  }));
}
