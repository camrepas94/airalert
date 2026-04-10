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
 * Co-subscription strength: for each show Y, count how many **other users** (who share at least one
 * of your subscribed shows) also subscribe to Y. Surfaces “people who watch what you watch also watch…”.
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
};

/**
 * Suggest shows using:
 * - **Collaborative filtering**: other users who share your shows — what else they subscribe to.
 * - **Genre + catalog fit** (wider TVMaze `/shows` sample) and **TVMaze type** (Scripted / Reality / …) vs your list.
 * - **Genre / network search** when metadata exists; title heuristics as fallback.
 */
export async function computeRecommendedShows(
  userId: string,
  subscribedShowIds: number[],
): Promise<{
  shows: RecommendedShowHit[];
  queriesUsed: string[];
}> {
  const subSet = new Set(subscribedShowIds);
  if (subscribedShowIds.length === 0) {
    return { shows: [], queriesUsed: [] };
  }

  const details = await fetchShowDetailsForRecommend(subscribedShowIds);
  if (details.length === 0) {
    return { shows: [], queriesUsed: [] };
  }

  const collaborative = collaborativeShowScores(userId);

  const userGenreWeights = buildUserGenreWeights(details);
  const userNetCounts = buildUserNetworkCounts(details);
  const userTypeWeights = buildUserTypeWeights(details);
  const { genreQueries, sharedNetworkQueries, queriesUsed: planLines } = buildGenreFirstQueries(details);
  const queriesUsed: string[] = [];

  if (collaborative.size > 0) {
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

  if (userGenreWeights.size > 0) {
    queriesUsed.push("Catalog: broader TVMaze pages, scored by weighted genre overlap");
    const catalogMatches = await scanShowsCatalogForGenreFit(userGenreWeights, subSet, {
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
    if (userGenreWeights.size > 0 && candidates.size < 18) {
      await addSearchHits(genreQueries.slice(0, 5).map((q) => ({ q, weight: 1, merged: true })));
    }
  } else {
    queriesUsed.push("Heuristic queries (no genre tags on subscriptions — title/network search)");
    const fallback = buildRecommendationQueries(details);
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

  type Row = Agg & { matchScore: number };
  const enriched: Row[] = [];

  for (let i = 0; i < ordered.length; i += 6) {
    const chunk = ordered.slice(i, i + 6);
    const part = await Promise.all(
      chunk.map(async (c): Promise<Row> => {
        try {
          const d = await fetchShow(c.id);
          const net = d.network?.name ?? d.webChannel?.name ?? "";
          const netTrim = net.trim();

          let genreOverlap = 0;
          let distinctGenreMatches = 0;
          for (const g of d.genres ?? []) {
            const k = g.trim().toLowerCase();
            if (k.length < 2) continue;
            const w = userGenreWeights.get(k);
            if (w != null && w > 0) {
              distinctGenreMatches += 1;
            }
            genreOverlap += w ?? 0;
          }

          const typeKey = (d.type ?? "").trim().toLowerCase();
          const typeBonus = typeKey ? (userTypeWeights.get(typeKey) ?? 0) * 20 : 0;

          const netHits = netTrim ? (userNetCounts.get(netTrim) ?? 0) : 0;
          const sharedNetBonus = netHits >= 2 ? 10 : netHits === 1 ? 4 : 0;
          const runningBonus =
            d.status === "Running" ? 6 : d.status === "In Development" ? 2 : 0;

          const collabRaw = c.collabScore ?? 0;
          const collabBonus = Math.min(100, Math.sqrt(collabRaw + 0.15) * 24);

          let matchScore =
            genreOverlap * 36 +
            distinctGenreMatches * 14 +
            c.catalogFit * 10 +
            c.searchHits * 3 +
            collabBonus +
            typeBonus +
            sharedNetBonus +
            runningBonus;

          if (genreOverlap === 0 && c.catalogFit <= 0 && c.searchHits > 0 && collabRaw < 2) {
            matchScore *= 0.1;
          }

          return {
            ...c,
            matchScore,
            name: d.name ?? c.name,
            network: netTrim || c.network,
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
    enriched.push(...part);
  }

  enriched.sort((a, b) => b.matchScore - a.matchScore || a.name.localeCompare(b.name));
  const top = enriched.slice(0, 22);

  const lastAiredById = await fetchPreviousEpisodeAirdates(top.map((s) => s.id));

  const shows: RecommendedShowHit[] = top.map((s) => ({
    id: s.id,
    name: s.name,
    network: s.network,
    premiered: s.premiered,
    image: s.image,
    lastAiredDate: lastAiredById.get(s.id) ?? null,
    matchScore: s.matchScore,
  }));

  shows.sort((a, b) => {
    if (b.matchScore !== a.matchScore) return b.matchScore - a.matchScore;
    if (a.lastAiredDate && b.lastAiredDate) return b.lastAiredDate.localeCompare(a.lastAiredDate);
    if (a.lastAiredDate && !b.lastAiredDate) return -1;
    if (!a.lastAiredDate && b.lastAiredDate) return 1;
    return a.name.localeCompare(b.name);
  });

  return { shows: shows.slice(0, 14), queriesUsed };
}

/**
 * Up to **25** **streaming** (TVMaze `webChannel`), **running** shows: personalized by genre overlap
 * with your subscriptions when possible, plus **high-rated** streaming titles for everyone.
 * Only includes shows whose **previous aired episode** (TVMaze embed) was within the last **4 months**.
 */
export async function computeTrendingShows(subscribedShowIds: number[]): Promise<RecommendedShowHit[]> {
  const subSet = new Set(subscribedShowIds);

  let userGenreWeights = new Map<string, number>();
  if (subscribedShowIds.length > 0) {
    const details = await fetchShowDetailsForRecommend(subscribedShowIds);
    userGenreWeights = buildUserGenreWeights(details);
  }

  const catalogMatches = await scanShowsCatalogForTrending(userGenreWeights, subSet, {
    pageRanges: [
      [0, 45],
      [48, 100],
      [115, 185],
    ],
    concurrency: 8,
  });

  const sorted = [...catalogMatches.values()].sort(
    (a, b) => b.trendScore - a.trendScore || a.show.name.localeCompare(b.show.name),
  );

  const RECENT_MONTHS = 4;
  const TARGET = 25;
  const CHUNK = 40;
  const MAX_SCAN = 320;

  const selected: { show: TvmazeShowListItem; trendScore: number; lastAired: string | null }[] = [];
  let offset = 0;

  while (selected.length < TARGET && offset < sorted.length && offset < MAX_SCAN) {
    const chunk = sorted.slice(offset, offset + CHUNK);
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
