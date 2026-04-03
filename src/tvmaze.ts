const BASE = "https://api.tvmaze.com";
const PREV_EP_CACHE_TTL_MS = 6 * 60 * 60 * 1000;
const previousEpisodeAirdateCache = new Map<number, { value: string | null; expiresAt: number }>();

export type TvmazeShowSearch = {
  score?: number;
  show: {
    id: number;
    name: string;
    premiered?: string | null;
    network?: { name: string } | null;
    webChannel?: { name: string } | null;
    image?: { medium?: string; original?: string } | null;
  };
};

/** Filter by broadcaster: both network and webChannel names (streaming) are checked; tokens must all appear somewhere in that combined string. */
export function showMatchesNetworkFilter(show: TvmazeShowSearch["show"], networkNeedle: string): boolean {
  const raw = networkNeedle
    .trim()
    .toLowerCase()
    .replace(/\+/g, " ")
    .replace(/[^\w\s-]/g, " ");
  if (!raw) return true;
  const netFull = `${show.network?.name ?? ""} ${show.webChannel?.name ?? ""}`
    .toLowerCase()
    .replace(/\+/g, " ")
    .replace(/\s+/g, " ")
    .trim();
  if (!netFull) return false;
  return raw
    .split(/\s+/)
    .filter((t) => t.length > 0)
    .every((t) => netFull.includes(t));
}

/** Re-rank TVMaze search hits so tokens match title, network, or premiere year (e.g. "Valley Bravo"). */
export function rankSearchResults(results: TvmazeShowSearch[], query: string): TvmazeShowSearch[] {
  const tokens = query
    .toLowerCase()
    .split(/\s+/)
    .map((t) => t.trim())
    .filter((t) => t.length > 0);
  if (tokens.length === 0) return results;

  const scored = results.map((r) => {
    const net = r.show.network?.name ?? r.show.webChannel?.name ?? "";
    const hay = `${r.show.name} ${net} ${r.show.premiered ?? ""}`.toLowerCase();
    let matches = 0;
    for (const t of tokens) {
      if (hay.includes(t)) matches++;
    }
    const tvmazeScore = r.score ?? 0;
    return { r, matches, tvmazeScore };
  });

  scored.sort((a, b) => {
    if (b.matches !== a.matches) return b.matches - a.matches;
    return b.tvmazeScore - a.tvmazeScore;
  });

  return scored.map((s) => s.r);
}

export type TvmazeEpisode = {
  id: number;
  name: string;
  season: number;
  number: number;
  airdate: string | null;
  airtime: string;
  airstamp: string | null;
  runtime: number | null;
};

function unwrap<T>(res: Response): Promise<T> {
  if (!res.ok) {
    throw new Error(`TVMaze HTTP ${res.status}: ${res.statusText}`);
  }
  return res.json() as Promise<T>;
}

export async function searchShows(query: string): Promise<TvmazeShowSearch[]> {
  const q = encodeURIComponent(query.trim());
  const res = await fetch(`${BASE}/search/shows?q=${q}`);
  return unwrap<TvmazeShowSearch[]>(res);
}

type TvmazeShowListItem = {
  id: number;
  name: string;
  type?: string;
  status?: string | null;
  genres?: string[];
  premiered?: string | null;
  network?: { name: string } | null;
  webChannel?: { name: string } | null;
  image?: { medium?: string; original?: string } | null;
};

function listItemToSearchHit(show: TvmazeShowListItem): TvmazeShowSearch {
  return {
    score: 0,
    show: {
      id: show.id,
      name: show.name,
      premiered: show.premiered ?? null,
      network: show.network ?? null,
      webChannel: show.webChannel ?? null,
      image: show.image ?? null,
    },
  };
}

/**
 * TVMaze `/search/shows` returns at most ~10 hits per query. Run several related
 * queries and dedupe so short queries (e.g. "housewives") surface more franchises.
 */
export async function searchShowsMerged(query: string): Promise<TvmazeShowSearch[]> {
  const q = query.trim();
  if (!q) return [];
  const variants = new Set<string>([q]);
  const lower = q.toLowerCase();
  if (!lower.includes("real")) {
    variants.add(`real ${q}`);
  }
  if (!lower.startsWith("the ")) {
    variants.add(`the ${q}`);
    if (!lower.includes("real")) {
      variants.add(`the real ${q}`);
    }
  }
  const byId = new Map<number, TvmazeShowSearch>();
  const settled = await Promise.allSettled([...variants].map(async (v) => searchShows(v)));
  for (const s of settled) {
    if (s.status !== "fulfilled") continue;
    for (const h of s.value) {
      if (!byId.has(h.show.id)) byId.set(h.show.id, h);
    }
  }
  return [...byId.values()];
}

/** Single `/search/shows` call — use for genre/network strings so we do not add `real`/`the` title variants. */
export async function searchShowsPlain(query: string): Promise<TvmazeShowSearch[]> {
  return searchShows(query);
}

export async function fetchShowsCatalogPage(page: number): Promise<TvmazeShowListItem[]> {
  const res = await fetch(`${BASE}/shows?page=${page}`);
  if (res.status === 404) return [];
  if (!res.ok) {
    throw new Error(`TVMaze shows?page=${page} HTTP ${res.status}: ${res.statusText}`);
  }
  const rows = (await res.json()) as TvmazeShowListItem[];
  return Array.isArray(rows) ? rows : [];
}

/**
 * Sample catalog pages; score each show by **genre overlap** with the user's weighted genre map
 * (counts from subscriptions). Uses list payloads which include `genres` — no per-show fetch.
 */
export async function scanShowsCatalogForGenreFit(
  userGenreWeights: Map<string, number>,
  excludeIds: Set<number>,
  opts: { pageRanges: [number, number][]; concurrency: number },
): Promise<Map<number, { show: TvmazeShowListItem; genreFit: number }>> {
  if (userGenreWeights.size === 0) return new Map();

  const pages: number[] = [];
  for (const [a, b] of opts.pageRanges) {
    const lo = Math.min(a, b);
    const hi = Math.max(a, b);
    for (let p = lo; p <= hi; p++) pages.push(p);
  }
  const uniquePages = [...new Set(pages)].sort((x, y) => x - y);
  const out = new Map<number, { show: TvmazeShowListItem; genreFit: number }>();

  for (let i = 0; i < uniquePages.length; i += opts.concurrency) {
    const batch = uniquePages.slice(i, i + opts.concurrency);
    const settled = await Promise.allSettled(batch.map((page) => fetchShowsCatalogPage(page)));
    for (const s of settled) {
      if (s.status !== "fulfilled") continue;
      for (const show of s.value) {
        if (!show?.id || excludeIds.has(show.id)) continue;
        let fit = 0;
        for (const g of show.genres ?? []) {
          const k = g.trim().toLowerCase();
          if (k.length < 2) continue;
          fit += userGenreWeights.get(k) ?? 0;
        }
        if (fit <= 0) continue;
        const prev = out.get(show.id);
        if (!prev || fit > prev.genreFit) {
          out.set(show.id, { show, genreFit: fit });
        }
      }
    }
    await new Promise((r) => setTimeout(r, 6));
  }

  return out;
}

/**
 * Scan paginated `GET /shows?page=` (full catalog) for titles containing `needle`.
 * Needed because `/search/shows` is capped (~10) and relevance can omit matches
 * (e.g. "Salt Lake City" is far into the catalog for substring "housewives").
 */
export async function scanShowsCatalogForNeedle(
  needle: string,
  opts: { maxPages: number; concurrency: number },
): Promise<TvmazeShowSearch[]> {
  const n = needle.trim().toLowerCase();
  if (n.length < 2) return [];

  const out: TvmazeShowSearch[] = [];
  const seen = new Set<number>();

  for (let start = 0; start < opts.maxPages; start += opts.concurrency) {
    const batchPages: number[] = [];
    for (let i = 0; i < opts.concurrency && start + i < opts.maxPages; i++) {
      batchPages.push(start + i);
    }
    if (batchPages.length === 0) break;

    const results = await Promise.all(
      batchPages.map(async (page) => {
        const res = await fetch(`${BASE}/shows?page=${page}`);
        return { page, res };
      }),
    );

    let catalogEnded = false;
    for (const { res } of results) {
      if (res.status === 404) {
        catalogEnded = true;
        continue;
      }
      if (!res.ok) continue;
      const rows = (await res.json()) as TvmazeShowListItem[];
      if (!Array.isArray(rows)) continue;
      for (const show of rows) {
        if (!show.name?.toLowerCase().includes(n)) continue;
        if (seen.has(show.id)) continue;
        seen.add(show.id);
        out.push(listItemToSearchHit(show));
      }
    }

    if (catalogEnded) break;

    await new Promise((r) => setTimeout(r, 5));
  }

  return out;
}

export type SearchShowsWithCatalogOptions = {
  /** Skip `/shows` scan; only merged `/search/shows` (~10× per variant). */
  skipCatalog?: boolean;
  /** Max catalog pages to scan (each page ~240–250 shows). Default 280. */
  catalogMaxPages?: number;
  /** Parallel catalog fetches per batch. Default 12. */
  catalogConcurrency?: number;
};

/**
 * Merged multi-query search plus optional full-catalog substring pass so users
 * are not limited to TVMaze's ~10 search hits.
 */
export async function searchShowsWithCatalog(
  query: string,
  options?: SearchShowsWithCatalogOptions,
): Promise<TvmazeShowSearch[]> {
  const merged = await searchShowsMerged(query);
  const byId = new Map<number, TvmazeShowSearch>();
  for (const h of merged) byId.set(h.show.id, h);

  if (options?.skipCatalog) {
    return [...byId.values()];
  }

  const needle = query.trim();
  if (needle.length < 2) {
    return [...byId.values()];
  }

  const maxPages = Math.min(400, Math.max(20, options?.catalogMaxPages ?? 140));
  const concurrency = Math.min(20, Math.max(4, options?.catalogConcurrency ?? 12));

  const catalog = await scanShowsCatalogForNeedle(needle, { maxPages, concurrency });
  for (const h of catalog) {
    if (!byId.has(h.show.id)) byId.set(h.show.id, h);
  }
  return [...byId.values()];
}

export async function fetchShowEpisodes(showId: number): Promise<TvmazeEpisode[]> {
  const res = await fetch(`${BASE}/shows/${showId}/episodes?specials=1`);
  return unwrap<TvmazeEpisode[]>(res);
}

export type TvmazeShowDetail = {
  id: number;
  name: string;
  premiered?: string | null;
  status?: string | null;
  summary?: string | null;
  genres?: string[];
  network?: { name: string } | null;
  webChannel?: { name: string } | null;
  image?: { medium?: string; original?: string } | null;
};

export async function fetchShow(showId: number): Promise<TvmazeShowDetail> {
  const res = await fetch(`${BASE}/shows/${showId}`);
  return unwrap<TvmazeShowDetail>(res);
}

/** Last aired calendar date (YYYY-MM-DD) from embedded previous episode, or null. */
export async function fetchPreviousEpisodeAirdate(showId: number): Promise<string | null> {
  const cached = previousEpisodeAirdateCache.get(showId);
  if (cached && cached.expiresAt > Date.now()) return cached.value;

  const u = new URL(`${BASE}/shows/${showId}`);
  u.searchParams.append("embed[]", "previousepisode");
  const res = await fetch(u.href);
  if (!res.ok) {
    previousEpisodeAirdateCache.set(showId, { value: null, expiresAt: Date.now() + PREV_EP_CACHE_TTL_MS });
    return null;
  }
  const data = (await res.json()) as {
    _embedded?: { previousepisode?: { airdate?: string | null } };
  };
  const raw = data._embedded?.previousepisode?.airdate;
  if (raw == null) {
    previousEpisodeAirdateCache.set(showId, { value: null, expiresAt: Date.now() + PREV_EP_CACHE_TTL_MS });
    return null;
  }
  const s = String(raw).trim();
  if (!s) {
    previousEpisodeAirdateCache.set(showId, { value: null, expiresAt: Date.now() + PREV_EP_CACHE_TTL_MS });
    return null;
  }
  const m = s.match(/^(\d{4}-\d{2}-\d{2})/);
  const out = m ? m[1] : null;
  previousEpisodeAirdateCache.set(showId, { value: out, expiresAt: Date.now() + PREV_EP_CACHE_TTL_MS });
  return out;
}

/** Batch-fetch previous-episode airdates; runs in chunks to limit parallel requests. */
export async function fetchPreviousEpisodeAirdates(
  showIds: number[],
  chunkSize = 20,
): Promise<Map<number, string | null>> {
  const out = new Map<number, string | null>();
  const uniqueIds = [...new Set(showIds)];
  const toFetch: number[] = [];
  const now = Date.now();

  for (const id of uniqueIds) {
    const cached = previousEpisodeAirdateCache.get(id);
    if (cached && cached.expiresAt > now) {
      out.set(id, cached.value);
      continue;
    }
    toFetch.push(id);
  }

  for (let i = 0; i < toFetch.length; i += chunkSize) {
    const chunk = toFetch.slice(i, i + chunkSize);
    const results = await Promise.all(
      chunk.map(async (id) => {
        try {
          const d = await fetchPreviousEpisodeAirdate(id);
          return [id, d] as const;
        } catch {
          return [id, null] as const;
        }
      }),
    );
    for (const [id, d] of results) out.set(id, d);
  }
  return out;
}
