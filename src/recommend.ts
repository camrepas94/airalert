import {
  fetchShow,
  searchShowsMerged,
  searchShowsPlain,
  fetchPreviousEpisodeAirdates,
  scanShowsCatalogForGenreFit,
  scanShowsCatalogForTrending,
} from "./tvmaze.js";

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
 * Suggest shows using **catalog genre fit** (paginated `/shows` includes real `genres` fields),
 * plus plain TVMaze search for genres/networks (no `real`/`the` query variants that bias titles).
 * Title-heavy heuristics run only when subscriptions have no usable genre metadata.
 */
export async function computeRecommendedShows(subscribedShowIds: number[]): Promise<{
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

  const userGenreWeights = buildUserGenreWeights(details);
  const userNetCounts = buildUserNetworkCounts(details);
  const { genreQueries, sharedNetworkQueries, queriesUsed: planLines } = buildGenreFirstQueries(details);
  const queriesUsed: string[] = [];

  type Agg = {
    id: number;
    name: string;
    network: string | null;
    premiered: string | null;
    image: string | null;
    searchHits: number;
    catalogFit: number;
  };

  const candidates = new Map<number, Agg>();

  if (userGenreWeights.size > 0) {
    queriesUsed.push("Catalog: sampled TVMaze pages, scored by true genre overlap (not title search)");
    const catalogMatches = await scanShowsCatalogForGenreFit(userGenreWeights, subSet, {
      pageRanges: [
        [0, 9],
        [48, 59],
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
    if (candidates.size < 12 && sharedNetworkQueries.length > 0) {
      await addSearchHits(sharedNetworkQueries.slice(0, 2).map((q) => ({ q, weight: 1, merged: false })));
    }
    if (userGenreWeights.size > 0 && candidates.size < 14) {
      await addSearchHits(genreQueries.slice(0, 4).map((q) => ({ q, weight: 1, merged: true })));
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
      b.catalogFit * 14 + b.searchHits * 3 - (a.catalogFit * 14 + a.searchHits * 3) ||
      a.name.localeCompare(b.name),
  );
  ordered = ordered.slice(0, 48);

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
          for (const g of d.genres ?? []) {
            const k = g.trim().toLowerCase();
            genreOverlap += userGenreWeights.get(k) ?? 0;
          }

          const netHits = netTrim ? (userNetCounts.get(netTrim) ?? 0) : 0;
          const sharedNetBonus = netHits >= 2 ? 8 : netHits === 1 ? 3 : 0;
          const runningBonus =
            d.status === "Running" ? 5 : d.status === "In Development" ? 2 : 0;

          let matchScore =
            genreOverlap * 22 +
            c.catalogFit * 6 +
            c.searchHits * 3 +
            sharedNetBonus +
            runningBonus;

          if (genreOverlap === 0 && c.catalogFit <= 0 && c.searchHits > 0) {
            matchScore *= 0.12;
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
          return {
            ...c,
            matchScore: c.catalogFit * 16 + c.searchHits * 2,
          };
        }
      }),
    );
    enriched.push(...part);
  }

  enriched.sort((a, b) => b.matchScore - a.matchScore || a.name.localeCompare(b.name));
  const top = enriched.slice(0, 18);

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
 * Popular catalog picks whose **genres** overlap your subscriptions, boosted by TVMaze rating.
 */
export async function computeTrendingShows(subscribedShowIds: number[]): Promise<RecommendedShowHit[]> {
  const subSet = new Set(subscribedShowIds);
  if (subscribedShowIds.length === 0) {
    return [];
  }

  const details = await fetchShowDetailsForRecommend(subscribedShowIds);
  if (details.length === 0) {
    return [];
  }

  const userGenreWeights = buildUserGenreWeights(details);
  if (userGenreWeights.size === 0) {
    return [];
  }

  const catalogMatches = await scanShowsCatalogForTrending(userGenreWeights, subSet, {
    pageRanges: [
      [0, 26],
      [45, 62],
      [110, 128],
    ],
    concurrency: 8,
  });

  const sorted = [...catalogMatches.values()].sort((a, b) => b.trendScore - a.trendScore || a.show.name.localeCompare(b.show.name));
  const top = sorted.slice(0, 22);

  const hits: RecommendedShowHit[] = top.map(({ show, trendScore }) => ({
    id: show.id,
    name: show.name,
    network: show.network?.name ?? show.webChannel?.name ?? null,
    premiered: show.premiered ?? null,
    image: show.image?.medium ?? null,
    lastAiredDate: null,
    matchScore: trendScore,
  }));

  const lastAiredById = await fetchPreviousEpisodeAirdates(hits.map((h) => h.id));

  return hits.map((h) => ({
    ...h,
    lastAiredDate: lastAiredById.get(h.id) ?? null,
  }));
}
