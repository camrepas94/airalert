/**
 * Breaking News duplicate-story pass for Community ticker + show news feeds.
 *
 * Matching (conservative — same show alone is NOT enough):
 * - Normalized URL equality (tracking params stripped)
 * - Same TVMaze show_id + Jaccard(word) similarity + time window
 * - Very high title similarity across outlets even if show_id mismatched
 *
 * Canonical pick order:
 * 1) Higher source tier (trusted entertainment trades first)
 * 2) Higher DB relevance score
 * 3) Longer headline (more complete), capped
 * 4) Fresher created_at
 */

export interface BreakingNewsDedupeRow {
  id: string;
  headline: string;
  source: string;
  url: string;
  show_id: number | null;
  show_name: string | null;
  created_at: string;
  score: number;
  snippet?: string | null;
}

const STOPWORDS = new Set([
  "the",
  "a",
  "an",
  "to",
  "of",
  "in",
  "for",
  "on",
  "with",
  "at",
  "by",
  "from",
  "as",
  "is",
  "it",
  "and",
  "or",
  "but",
  "tv",
  "show",
  "series",
  "season",
  "episode",
  "new",
  "exclusive",
  "report",
  "reports",
  "breaking",
  "update",
  "heres",
  "here",
  "how",
  "what",
  "why",
  "after",
  "before",
  "about",
  "into",
  "over",
  "more",
  "all",
  "out",
  "up",
  "gets",
  "get",
  "got",
  "his",
  "her",
  "their",
  "they",
  "she",
  "he",
  "that",
  "this",
  "than",
  "then",
  "not",
  "are",
  "was",
  "were",
  "has",
  "have",
  "had",
  "will",
  "can",
  "may",
  "just",
  "into",
  "also",
]);

/** Outlet preference (higher = preferred as canonical). */
const SOURCE_TIER: Record<string, number> = {
  Deadline: 100,
  Variety: 98,
  TVLine: 96,
  "Entertainment Weekly": 88,
  People: 85,
  "Page Six": 78,
  TMZ: 72,
  "Google News": 70,
};

function sourceTier(source: string): number {
  const t = SOURCE_TIER[source.trim()];
  if (t != null) return t;
  const lower = source.toLowerCase();
  for (const [k, v] of Object.entries(SOURCE_TIER)) {
    if (k.toLowerCase() === lower) return v;
  }
  return 50;
}

/** Strip tracking params, lowercase host, trim trailing slash on path. */
export function normalizeBreakingNewsUrl(raw: string): string {
  const s = raw.trim();
  if (!s) return "";
  try {
    const u = new URL(s);
    u.hostname = u.hostname.toLowerCase().replace(/^www\./, "");
    const drop = [
      "utm_source",
      "utm_medium",
      "utm_campaign",
      "utm_term",
      "utm_content",
      "fbclid",
      "gclid",
      "mc_cid",
      "mc_eid",
      "igshid",
    ];
    for (const k of drop) {
      u.searchParams.delete(k);
    }
    u.hash = "";
    let path = u.pathname;
    if (path.length > 1 && path.endsWith("/")) path = path.slice(0, -1);
    const search = u.search ? u.search : "";
    return `${u.protocol}//${u.hostname}${path}${search}`;
  } catch {
    return s.toLowerCase();
  }
}

function normalizeHeadlineForTokens(headline: string): Set<string> {
  const h = String(headline ?? "");
  const nf = h.normalize("NFKD").replace(/\p{M}/gu, "");
  const lower = nf.toLowerCase();
  const cleaned = lower
    .replace(/[''`´]/g, "'")
    .replace(/[\u2018\u2019]/g, "'")
    .replace(/[\u2013\u2014]/g, "-")
    .replace(/[^a-z0-9\s'-]/g, " ")
    .replace(/\s+/g, " ")
    .trim();
  const tokens = new Set<string>();
  for (const w of cleaned.split(" ")) {
    const t = w.replace(/^-+|-+$/g, "");
    if (t.length < 2) continue;
    if (STOPWORDS.has(t)) continue;
    tokens.add(t);
  }
  return tokens;
}

/** Jaccard similarity on word sets (0–1). */
export function headlineTokenJaccard(a: string, b: string): number {
  const A = normalizeHeadlineForTokens(a);
  const B = normalizeHeadlineForTokens(b);
  if (A.size === 0 && B.size === 0) return 1;
  if (A.size === 0 || B.size === 0) return 0;
  let inter = 0;
  for (const x of A) {
    if (B.has(x)) inter += 1;
  }
  const union = A.size + B.size - inter;
  return union <= 0 ? 0 : inter / union;
}

function sqliteTimeToMs(created_at: string): number {
  const iso = created_at.includes("T") ? created_at : created_at.replace(" ", "T");
  const withZ = /[zZ]|[+-][0-9]{2}:?[0-9]{2}$/.test(iso) ? iso : `${iso}Z`;
  const ms = Date.parse(withZ);
  return Number.isFinite(ms) ? ms : 0;
}

function hoursApart(a: BreakingNewsDedupeRow, b: BreakingNewsDedupeRow): number {
  return Math.abs(sqliteTimeToMs(a.created_at) - sqliteTimeToMs(b.created_at)) / 3600000;
}

export type DuplicateReason =
  | "normalized_url"
  | "same_show_high_title_sim"
  | "same_show_time_title"
  | "near_identical_title"
  | "transitive_cluster";

function duplicatePairReason(x: BreakingNewsDedupeRow, y: BreakingNewsDedupeRow): DuplicateReason | null {
  const nu = normalizeBreakingNewsUrl(x.url);
  const nv = normalizeBreakingNewsUrl(y.url);
  if (nu && nv && nu === nv) return "normalized_url";

  const j = headlineTokenJaccard(x.headline, y.headline);
  const h = hoursApart(x, y);

  const sameShow =
    x.show_id != null &&
    y.show_id != null &&
    Number.isFinite(x.show_id) &&
    Number.isFinite(y.show_id) &&
    x.show_id === y.show_id;

  if (sameShow) {
    if (j >= 0.82) return "same_show_high_title_sim";
    if (j >= 0.68 && h <= 8) return "same_show_time_title";
    return null;
  }

  // Different or unknown show: only collapse near-identical headlines (same viral story picked up everywhere)
  if (j >= 0.88 && h <= 14) return "near_identical_title";
  if (j >= 0.93) return "near_identical_title";

  return null;
}

class UnionFind {
  readonly parent: number[];
  constructor(n: number) {
    this.parent = Array.from({ length: n }, (_, i) => i);
  }
  find(i: number): number {
    if (this.parent[i] !== i) this.parent[i] = this.find(this.parent[i]);
    return this.parent[i];
  }
  union(i: number, j: number): void {
    const ri = this.find(i);
    const rj = this.find(j);
    if (ri !== rj) this.parent[ri] = rj;
  }
}

/** Higher return value = better canonical row. */
function compareCanonical(a: BreakingNewsDedupeRow, b: BreakingNewsDedupeRow): number {
  const ta = sourceTier(a.source);
  const tb = sourceTier(b.source);
  if (ta !== tb) return ta - tb;
  const ra = Number(a.score) || 0;
  const rb = Number(b.score) || 0;
  if (ra !== rb) return ra - rb;
  const la = (a.headline || "").length;
  const lb = (b.headline || "").length;
  if (la !== lb) return la - lb;
  const sa = (a.snippet && String(a.snippet).trim().length) || 0;
  const sb = (b.snippet && String(b.snippet).trim().length) || 0;
  if (sa !== sb) return sa - sb;
  return sqliteTimeToMs(a.created_at) - sqliteTimeToMs(b.created_at);
}

function pickCanonical(cluster: BreakingNewsDedupeRow[]): BreakingNewsDedupeRow {
  let best = cluster[0];
  for (let i = 1; i < cluster.length; i++) {
    if (compareCanonical(cluster[i], best) > 0) best = cluster[i];
  }
  return best;
}

function explainDrop(
  row: BreakingNewsDedupeRow,
  cluster: BreakingNewsDedupeRow[],
  canon: BreakingNewsDedupeRow,
): { reason: DuplicateReason; detail: string } {
  for (const m of cluster) {
    if (m.id === row.id) continue;
    const r = duplicatePairReason(row, m);
    if (r) {
      return {
        reason: r,
        detail: `pair=${m.id.slice(0, 8)}… j=${headlineTokenJaccard(row.headline, canon.headline).toFixed(2)}`,
      };
    }
  }
  const d = duplicatePairReason(row, canon);
  if (d) return { reason: d, detail: `direct j=${headlineTokenJaccard(row.headline, canon.headline).toFixed(2)}` };
  return { reason: "transitive_cluster", detail: `j=${headlineTokenJaccard(row.headline, canon.headline).toFixed(2)}` };
}

export interface DedupeResult {
  kept: BreakingNewsDedupeRow[];
  dropped: { id: string; reason: DuplicateReason; keptId: string; detail?: string }[];
}

/**
 * Cluster duplicate rows (union-find on duplicate pairs), keep one canonical per cluster.
 * `rows` should be newest-first; output `kept` preserves relative order of first-seen canonicals.
 */
export function dedupeBreakingNewsCandidates(rows: BreakingNewsDedupeRow[], opts?: { log?: boolean }): DedupeResult {
  const log = opts?.log === true || process.env.DEBUG_BREAKING_DEDUPE === "1";
  const n = rows.length;
  if (n <= 1) return { kept: [...rows], dropped: [] };

  const uf = new UnionFind(n);
  for (let i = 0; i < n; i++) {
    for (let j = i + 1; j < n; j++) {
      const reason = duplicatePairReason(rows[i], rows[j]);
      if (reason) uf.union(i, j);
    }
  }

  const rootToMembers = new Map<number, number[]>();
  for (let i = 0; i < n; i++) {
    const r = uf.find(i);
    if (!rootToMembers.has(r)) rootToMembers.set(r, []);
    rootToMembers.get(r)!.push(i);
  }

  const kept: BreakingNewsDedupeRow[] = [];
  const dropped: DedupeResult["dropped"] = [];

  for (const [, indices] of rootToMembers) {
    const cluster = indices.map((i) => rows[i]);
    const canon = pickCanonical(cluster);
    kept.push(canon);
    for (const row of cluster) {
      if (row.id === canon.id) continue;
      const { reason, detail } = explainDrop(row, cluster, canon);
      dropped.push({
        id: row.id,
        reason,
        keptId: canon.id,
        detail: `${detail} | keep="${canon.headline.slice(0, 56)}${canon.headline.length > 56 ? "…" : ""}"`,
      });
      if (log) {
        console.debug(
          `[breaking-dedupe] drop id=${row.id} (${row.source}) reason=${reason} keep=${canon.id} (${canon.source}) ${detail}`,
        );
      }
    }
  }

  // Restore original newest-first order among canonicals
  kept.sort((a, b) => sqliteTimeToMs(b.created_at) - sqliteTimeToMs(a.created_at));

  return { kept, dropped };
}
