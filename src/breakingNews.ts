import { db } from "./db.js";
import { v4 as uuid } from "uuid";

/* ── RSS Feed Sources ─────────────────────────────────────────── */
const RSS_FEEDS: { name: string; url: string }[] = [
  { name: "Deadline", url: "https://deadline.com/feed/" },
  { name: "TVLine", url: "https://tvline.com/feed/" },
  { name: "Variety", url: "https://variety.com/feed/" },
  { name: "Entertainment Weekly", url: "https://ew.com/feed/" },
  { name: "People", url: "https://people.com/feed/" },
  { name: "Page Six", url: "https://pagesix.com/feed/" },
  { name: "TMZ", url: "https://www.tmz.com/rss.xml" },
];

const DRAMA_KEYWORDS = [
  "fired",
  "arrested",
  "cheating",
  "affair",
  "feud",
  "lawsuit",
  "split",
  "divorce",
  "exposed",
  "leaked",
  "scandal",
  "meltdown",
  "beef",
  "caught",
  "quit",
  "walked off",
  "confrontation",
  "cancelled",
  "canceled",
  "renewed",
  "axed",
  "renewed",
  "returning",
  "premiere",
  "finale",
  "spinoff",
  "spin-off",
  "reboot",
];

/* ── Cast Cache ───────────────────────────────────────────────── */

/**
 * Fetch cast for a single show from TVMaze and cache it.
 * Returns the cast rows inserted.
 */
export async function fetchAndCacheShowCast(
  tvmazeShowId: number,
): Promise<{ personName: string; characterName: string | null }[]> {
  const url = `https://api.tvmaze.com/shows/${tvmazeShowId}/cast`;
  try {
    const res = await fetch(url);
    if (!res.ok) return [];
    const data = (await res.json()) as {
      person: { id: number; name: string };
      character: { name: string } | null;
    }[];

    const insert = db.prepare(
      `INSERT OR REPLACE INTO cast_cache (tvmaze_show_id, tvmaze_person_id, person_name, character_name, fetched_at)
       VALUES (?, ?, ?, ?, datetime('now'))`,
    );
    const results: { personName: string; characterName: string | null }[] = [];
    const tx = db.transaction(() => {
      for (const entry of data) {
        const pName = entry.person?.name;
        const cName = entry.character?.name || null;
        if (!pName) continue;
        insert.run(tvmazeShowId, entry.person.id, pName, cName);
        results.push({ personName: pName, characterName: cName });
      }
    });
    tx();
    return results;
  } catch (e) {
    console.error(`[cast-cache] Failed to fetch cast for show ${tvmazeShowId}:`, e);
    return [];
  }
}

/**
 * Refresh cast cache for all subscribed shows.
 * Processes in batches of 10 with 1s delay between batches to respect TVMaze rate limits.
 * Only refreshes shows whose cast data is older than 24 hours.
 */
export async function refreshAllCastCache(): Promise<number> {
  const shows = db
    .prepare(
      `SELECT DISTINCT ss.tvmaze_show_id
       FROM show_subscriptions ss
       LEFT JOIN cast_cache cc ON cc.tvmaze_show_id = ss.tvmaze_show_id
       GROUP BY ss.tvmaze_show_id
       HAVING cc.tvmaze_show_id IS NULL
          OR MAX(cc.fetched_at) < datetime('now', '-24 hours')`,
    )
    .all() as { tvmaze_show_id: number }[];

  let refreshed = 0;
  const batchSize = 10;
  for (let i = 0; i < shows.length; i += batchSize) {
    const batch = shows.slice(i, i + batchSize);
    await Promise.all(batch.map((s) => fetchAndCacheShowCast(s.tvmaze_show_id)));
    refreshed += batch.length;
    if (i + batchSize < shows.length) {
      await new Promise((r) => setTimeout(r, 1000));
    }
  }
  console.log(`[cast-cache] Refreshed cast for ${refreshed} shows`);
  return refreshed;
}

/* ── RSS Parsing (lightweight, no external XML lib) ────────── */

interface FeedItem {
  title: string;
  link: string;
  description: string;
}

function extractTag(xml: string, tag: string): string {
  const open = xml.indexOf(`<${tag}`);
  if (open === -1) return "";
  const contentStart = xml.indexOf(">", open);
  if (contentStart === -1) return "";
  const close = xml.indexOf(`</${tag}>`, contentStart);
  if (close === -1) return "";
  let content = xml.slice(contentStart + 1, close).trim();
  if (content.startsWith("<![CDATA[") && content.endsWith("]]>")) {
    content = content.slice(9, -3);
  }
  return content;
}

function parseRssFeed(xml: string): FeedItem[] {
  const items: FeedItem[] = [];
  let cursor = 0;
  while (true) {
    const itemStart = xml.indexOf("<item", cursor);
    if (itemStart === -1) break;
    const itemEnd = xml.indexOf("</item>", itemStart);
    if (itemEnd === -1) break;
    const itemXml = xml.slice(itemStart, itemEnd + 7);
    const title = extractTag(itemXml, "title")
      .replace(/<[^>]*>/g, "")
      .trim();
    const link = extractTag(itemXml, "link").trim();
    const desc = extractTag(itemXml, "description")
      .replace(/<[^>]*>/g, "")
      .trim();
    if (title && link) {
      items.push({ title, link, description: desc });
    }
    cursor = itemEnd + 7;
  }
  return items;
}

/* ── Relevance Scoring ─────────────────────────────────────── */

interface ShowInfo {
  tvmazeShowId: number;
  showName: string;
  network: string | null;
}

interface CastMember {
  tvmazeShowId: number;
  personName: string;
}

function scoreHeadline(
  headline: string,
  shows: ShowInfo[],
  cast: CastMember[],
): { score: number; matchedShowId: number | null; matchedShowName: string | null } {
  const hl = headline.toLowerCase();
  let bestScore = 0;
  let matchedShowId: number | null = null;
  let matchedShowName: string | null = null;

  for (const show of shows) {
    if (show.showName.length >= 3 && hl.includes(show.showName.toLowerCase())) {
      if (100 > bestScore) {
        bestScore = 100;
        matchedShowId = show.tvmazeShowId;
        matchedShowName = show.showName;
      }
    }
    if (show.network && show.network.length >= 3) {
      const networkLower = show.network.toLowerCase();
      if (hl.includes(networkLower)) {
        const hasDramaKw = DRAMA_KEYWORDS.some((kw) => hl.includes(kw));
        if (hasDramaKw && 50 > bestScore) {
          bestScore = 50;
          matchedShowId = show.tvmazeShowId;
          matchedShowName = show.showName;
        }
      }
    }
  }

  for (const member of cast) {
    const parts = member.personName.toLowerCase().split(" ");
    if (parts.length >= 2 && hl.includes(member.personName.toLowerCase())) {
      if (80 > bestScore) {
        bestScore = 80;
        matchedShowId = member.tvmazeShowId;
        const show = shows.find((s) => s.tvmazeShowId === member.tvmazeShowId);
        matchedShowName = show?.showName || null;
      }
    }
  }

  if (bestScore < 30) {
    const hasDramaKw = DRAMA_KEYWORDS.some((kw) => hl.includes(kw));
    if (hasDramaKw) bestScore = Math.max(bestScore, 30);
  }

  return { score: bestScore, matchedShowId, matchedShowName };
}

/* ── Main RSS Poll Job ─────────────────────────────────────── */

export async function pollRssFeeds(): Promise<{
  fetched: number;
  autoPublished: number;
  pendingReview: number;
  discarded: number;
}> {
  const shows = db
    .prepare(
      `SELECT DISTINCT ss.tvmaze_show_id as tvmazeShowId, ss.show_name as showName,
              ec.network
       FROM show_subscriptions ss
       LEFT JOIN episodes_cache ec ON ec.tvmaze_show_id = ss.tvmaze_show_id
       GROUP BY ss.tvmaze_show_id`,
    )
    .all() as ShowInfo[];

  const cast = db
    .prepare(`SELECT tvmaze_show_id as tvmazeShowId, person_name as personName FROM cast_cache`)
    .all() as CastMember[];

  const existingUrls = new Set(
    (db.prepare(`SELECT url FROM breaking_news`).all() as { url: string }[]).map((r) => r.url),
  );

  let fetched = 0;
  let autoPublished = 0;
  let pendingReview = 0;
  let discarded = 0;

  const insert = db.prepare(
    `INSERT OR IGNORE INTO breaking_news (id, headline, snippet, source, url, show_id, show_name, score, status, created_at)
     VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, datetime('now'))`,
  );

  for (const feed of RSS_FEEDS) {
    try {
      const controller = new AbortController();
      const timeout = setTimeout(() => controller.abort(), 15000);
      const res = await fetch(feed.url, {
        signal: controller.signal,
        headers: { "User-Agent": "AirAlert/1.0 (TV show tracker)" },
      });
      clearTimeout(timeout);
      if (!res.ok) continue;

      const xml = await res.text();
      const items = parseRssFeed(xml).slice(0, 20);

      for (const item of items) {
        if (existingUrls.has(item.link)) continue;
        existingUrls.add(item.link);
        fetched++;

        const { score, matchedShowId, matchedShowName } = scoreHeadline(
          item.title,
          shows,
          cast,
        );

        if (score < 40) {
          discarded++;
          continue;
        }

        const snippet = item.description.length > 150
          ? item.description.slice(0, 147) + "..."
          : item.description;

        const status = score >= 80 ? "auto" : "pending";
        if (status === "auto") autoPublished++;
        else pendingReview++;

        insert.run(
          uuid(),
          item.title,
          snippet || null,
          feed.name,
          item.link,
          matchedShowId,
          matchedShowName,
          score,
          status,
        );
      }
    } catch (e) {
      console.error(`[rss-poll] Failed to fetch ${feed.name} (${feed.url}):`, e);
    }
  }

  db.prepare(
    `DELETE FROM breaking_news WHERE created_at < datetime('now', '-48 hours')`,
  ).run();

  console.log(
    `[rss-poll] Done: ${fetched} new items, ${autoPublished} auto-published, ${pendingReview} pending review, ${discarded} discarded`,
  );
  return { fetched, autoPublished, pendingReview, discarded };
}

/* ── Ticker Data (what the frontend fetches) ───────────────── */

export interface TickerItem {
  type: "admin" | "breaking" | "airing" | "stat";
  emoji: string;
  text: string;
  source?: string;
  url?: string;
  /** TVMaze show id when known — client can load poster from api.tvmaze.com */
  tvmazeShowId?: number;
  showName?: string;
}

export function getTickerItems(): TickerItem[] {
  const items: TickerItem[] = [];

  const admin = db
    .prepare(`SELECT message FROM admin_ticker_message WHERE id = 1`)
    .get() as { message: string | null } | undefined;
  if (admin?.message) {
    items.push({
      type: "admin",
      emoji: "\u{1F6A8}",
      text: admin.message,
    });
  }

  const news = db
    .prepare(
      `SELECT headline, source, url, show_name, show_id FROM breaking_news
       WHERE status IN ('auto', 'approved')
         AND created_at > datetime('now', '-48 hours')
       ORDER BY created_at DESC LIMIT 15`,
    )
    .all() as {
      headline: string;
      source: string;
      url: string;
      show_name: string | null;
      show_id: number | null;
    }[];
  for (const n of news) {
    items.push({
      type: "breaking",
      emoji: "\u{1F534}",
      text: n.headline,
      source: n.source,
      url: n.url,
      tvmazeShowId: n.show_id != null && Number.isFinite(n.show_id) ? n.show_id : undefined,
      showName: n.show_name ?? undefined,
    });
  }

  const airingToday = db
    .prepare(
      `SELECT DISTINCT ec.tvmaze_show_id, ec.network, ss.show_name, ec.season, ec.number, ec.name
       FROM episodes_cache ec
       JOIN show_subscriptions ss ON ss.tvmaze_show_id = ec.tvmaze_show_id
       WHERE ec.airdate = date('now')
       ORDER BY ss.show_name
       LIMIT 10`,
    )
    .all() as {
      tvmaze_show_id: number;
      network: string;
      show_name: string;
      season: number;
      number: number;
      name: string;
    }[];
  for (const ep of airingToday) {
    const label = `S${String(ep.season).padStart(2, "0")}E${String(ep.number).padStart(2, "0")}`;
    items.push({
      type: "airing",
      emoji: "\u{1F4FA}",
      text: `${ep.show_name} ${label} "${ep.name}" airs tonight`,
      tvmazeShowId: ep.tvmaze_show_id,
      showName: ep.show_name,
    });
  }

  if (items.length === 0) {
    const userCount = (db.prepare(`SELECT COUNT(*) as c FROM users WHERE username IS NOT NULL`).get() as { c: number }).c;
    const postCount = (db.prepare(`SELECT COUNT(*) as c FROM community_posts WHERE deleted_at IS NULL`).get() as { c: number }).c;
    items.push({
      type: "stat",
      emoji: "\u{1F4AC}",
      text: `${userCount} members \u2022 ${postCount} community posts`,
    });
  }

  return items;
}

/* ── Google News RSS for top followed shows ────────────── */

export async function pollGoogleNewsForTopShows(): Promise<{
  fetched: number;
  autoPublished: number;
  pendingReview: number;
}> {
  const topShows = db
    .prepare(
      `SELECT tvmaze_show_id, show_name, COUNT(*) as followers
       FROM show_subscriptions
       GROUP BY tvmaze_show_id
       ORDER BY followers DESC
       LIMIT 10`,
    )
    .all() as { tvmaze_show_id: number; show_name: string; followers: number }[];

  const existingUrls = new Set(
    (db.prepare(`SELECT url FROM breaking_news`).all() as { url: string }[]).map((r) => r.url),
  );

  const shows = db
    .prepare(
      `SELECT DISTINCT ss.tvmaze_show_id as tvmazeShowId, ss.show_name as showName, ec.network
       FROM show_subscriptions ss
       LEFT JOIN episodes_cache ec ON ec.tvmaze_show_id = ss.tvmaze_show_id
       GROUP BY ss.tvmaze_show_id`,
    )
    .all() as { tvmazeShowId: number; showName: string; network: string | null }[];

  const cast = db
    .prepare(`SELECT tvmaze_show_id as tvmazeShowId, person_name as personName FROM cast_cache`)
    .all() as { tvmazeShowId: number; personName: string }[];

  const insert = db.prepare(
    `INSERT OR IGNORE INTO breaking_news (id, headline, snippet, source, url, show_id, show_name, score, status, created_at)
     VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, datetime('now'))`,
  );

  let fetched = 0;
  let autoPublished = 0;
  let pendingReview = 0;

  for (const show of topShows) {
    const q = encodeURIComponent(`"${show.show_name}" TV show`);
    const feedUrl = `https://news.google.com/rss/search?q=${q}&hl=en-US&gl=US&ceid=US:en`;
    try {
      const controller = new AbortController();
      const timeout = setTimeout(() => controller.abort(), 15000);
      const res = await fetch(feedUrl, {
        signal: controller.signal,
        headers: { "User-Agent": "AirAlert/1.0 (TV show tracker)" },
      });
      clearTimeout(timeout);
      if (!res.ok) continue;

      const xml = await res.text();
      const items = parseRssFeed(xml).slice(0, 10);

      for (const item of items) {
        if (existingUrls.has(item.link)) continue;
        existingUrls.add(item.link);

        const { score } = scoreHeadline(item.title, shows, cast);
        const effectiveScore = Math.max(score, 80);

        const snippet = item.description.length > 150
          ? item.description.slice(0, 147) + "..."
          : item.description;

        const status = effectiveScore >= 80 ? "auto" : "pending";
        if (status === "auto") autoPublished++;
        else pendingReview++;
        fetched++;

        insert.run(
          uuid(),
          item.title,
          snippet || null,
          "Google News",
          item.link,
          show.tvmaze_show_id,
          show.show_name,
          effectiveScore,
          status,
        );
      }
      await new Promise((r) => setTimeout(r, 500));
    } catch (e) {
      console.error(`[google-news] Failed for "${show.show_name}":`, e);
    }
  }

  console.log(`[google-news] Done: ${fetched} new, ${autoPublished} auto-published, ${pendingReview} pending`);
  return { fetched, autoPublished, pendingReview };
}
