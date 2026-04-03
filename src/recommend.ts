import { fetchShow, searchShowsMerged, fetchPreviousEpisodeAirdates } from "./tvmaze.js";

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
 * Suggest shows using genres, shared networks, repeated title words, and common title prefix
 * from the user's current subscriptions (TVMaze metadata only; fast search, no catalog scan).
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

  const queriesUsed = buildRecommendationQueries(details);
  if (queriesUsed.length === 0) {
    return { shows: [], queriesUsed: [] };
  }

  const scored = new Map<
    number,
    {
      id: number;
      name: string;
      network: string | null;
      premiered: string | null;
      image: string | null;
      matchScore: number;
    }
  >();

  await Promise.all(
    queriesUsed.map(async (q) => {
      const hits = await searchShowsMerged(q);
      for (const h of hits) {
        const id = h.show.id;
        if (subSet.has(id)) continue;
        const cur = scored.get(id);
        if (cur) cur.matchScore += 1;
        else {
          scored.set(id, {
            id,
            name: h.show.name,
            network: h.show.network?.name ?? h.show.webChannel?.name ?? null,
            premiered: h.show.premiered ?? null,
            image: h.show.image?.medium ?? null,
            matchScore: 1,
          });
        }
      }
    }),
  );

  let list = [...scored.values()].sort((a, b) => b.matchScore - a.matchScore || a.name.localeCompare(b.name)).slice(0, 3);

  const lastAiredById = await fetchPreviousEpisodeAirdates(list.map((s) => s.id));

  const shows: RecommendedShowHit[] = list.map((s) => ({
    id: s.id,
    name: s.name,
    network: s.network,
    premiered: s.premiered,
    image: s.image,
    lastAiredDate: lastAiredById.get(s.id) ?? null,
    matchScore: s.matchScore,
  }));

  shows.sort((a, b) => {
    if (a.lastAiredDate && b.lastAiredDate) return b.lastAiredDate.localeCompare(a.lastAiredDate);
    if (a.lastAiredDate && !b.lastAiredDate) return -1;
    if (!a.lastAiredDate && b.lastAiredDate) return 1;
    if (b.matchScore !== a.matchScore) return b.matchScore - a.matchScore;
    return a.name.localeCompare(b.name);
  });

  return { shows, queriesUsed };
}
