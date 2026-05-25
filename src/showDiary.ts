import { db } from "./db.js";
import { getMyShowsAnalytics, type MyShowsAnalytics } from "./myShowsAnalytics.js";

export type ShowDiaryDayEntry = {
  taskId: string;
  tvmazeShowId: number;
  tvmazeEpisodeId: number;
  showName: string;
  episodeLabel: string;
  completedAt: string;
  rating: number | null;
  showImageUrl: string | null;
  reviewExcerpt: string | null;
  reviewPostId: string | null;
};

export type ShowDiaryCalendarDay = {
  date: string;
  day: number;
  entryCount: number;
  topRating: number | null;
  showImageUrl: string | null;
  entries: ShowDiaryDayEntry[];
};

export type ShowDiaryWeekSummary = {
  episodeCount: number;
  averageRating: number | null;
  topShowName: string | null;
  topShowCount: number;
};

export type ShowDiaryWeeklyBar = {
  weekLabel: string;
  episodeCount: number;
};

export type ShowDiaryTopRatedItem = {
  tvmazeShowId: number;
  tvmazeEpisodeId: number;
  showName: string;
  episodeLabel: string;
  rating: number;
  showImageUrl: string | null;
  completedAt: string | null;
};

export type ShowDiaryPayload = {
  year: number;
  month: number;
  monthLabel: string;
  timezone: string;
  stats: MyShowsAnalytics;
  weekSummary: ShowDiaryWeekSummary;
  weeklyBars: ShowDiaryWeeklyBar[];
  calendarDays: ShowDiaryCalendarDay[];
  topRated: ShowDiaryTopRatedItem[];
};

function userTimezone(userId: string): string {
  const row = db.prepare(`SELECT timezone FROM users WHERE id = ?`).get(userId) as { timezone: string | null } | undefined;
  const tz = row?.timezone && String(row.timezone).trim();
  return tz || "UTC";
}

function localDateKey(iso: string, timeZone: string): string | null {
  if (!iso) return null;
  const d = new Date(iso);
  if (!Number.isFinite(d.getTime())) return null;
  try {
    return new Intl.DateTimeFormat("en-CA", { timeZone, year: "numeric", month: "2-digit", day: "2-digit" }).format(d);
  } catch {
    return d.toISOString().slice(0, 10);
  }
}

function weekBoundsLocal(timeZone: string): { start: string; end: string } {
  const now = new Date();
  const today = localDateKey(now.toISOString(), timeZone) ?? now.toISOString().slice(0, 10);
  const parts = today.split("-").map(Number);
  const noon = new Date(Date.UTC(parts[0], parts[1] - 1, parts[2], 12, 0, 0));
  const dow = noon.getUTCDay();
  const sun = new Date(noon);
  sun.setUTCDate(sun.getUTCDate() - dow);
  const sat = new Date(sun);
  sat.setUTCDate(sat.getUTCDate() + 6);
  const fmt = (d: Date) => {
    try {
      return new Intl.DateTimeFormat("en-CA", { timeZone, year: "numeric", month: "2-digit", day: "2-digit" }).format(d);
    } catch {
      return d.toISOString().slice(0, 10);
    }
  };
  return { start: fmt(sun), end: fmt(sat) };
}

function monthLabel(year: number, month: number): string {
  const d = new Date(Date.UTC(year, month - 1, 1, 12, 0, 0));
  return new Intl.DateTimeFormat("en-US", { month: "long", year: "numeric", timeZone: "UTC" }).format(d).toUpperCase();
}

function daysInMonth(year: number, month: number): number {
  return new Date(Date.UTC(year, month, 0)).getUTCDate();
}

function stripHtmlToText(html: string): string {
  return String(html)
    .replace(/<[^>]+>/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

function buildWeekSummary(
  entries: { date: string; showName: string; rating: number | null }[],
  weekStart: string,
  weekEnd: string,
): ShowDiaryWeekSummary {
  const inWeek = entries.filter((e) => e.date >= weekStart && e.date <= weekEnd);
  const episodeCount = inWeek.length;
  const ratings = inWeek.map((e) => e.rating).filter((r): r is number => r != null && Number.isFinite(r));
  const averageRating =
    ratings.length > 0
      ? Math.round((ratings.reduce((a, b) => a + b, 0) / ratings.length) * 10) / 10
      : null;
  const byShow = new Map<string, number>();
  for (const e of inWeek) {
    byShow.set(e.showName, (byShow.get(e.showName) ?? 0) + 1);
  }
  let topShowName: string | null = null;
  let topShowCount = 0;
  for (const [name, c] of byShow) {
    if (c > topShowCount) {
      topShowName = name;
      topShowCount = c;
    }
  }
  return { episodeCount, averageRating, topShowName, topShowCount };
}

function buildWeeklyBars(
  entries: { date: string }[],
  timeZone: string,
): ShowDiaryWeeklyBar[] {
  const now = new Date();
  const bars: ShowDiaryWeeklyBar[] = [];
  for (let w = 4; w >= 0; w--) {
    const end = new Date(now);
    end.setDate(end.getDate() - w * 7);
    const start = new Date(end);
    start.setDate(start.getDate() - 6);
    const startKey = localDateKey(start.toISOString(), timeZone);
    const endKey = localDateKey(end.toISOString(), timeZone);
    if (!startKey || !endKey) continue;
    const count = entries.filter((e) => e.date >= startKey && e.date <= endKey).length;
    const label =
      w === 0
        ? "This wk"
        : w === 1
          ? "Last wk"
          : "-" + w + "w";
    bars.push({ weekLabel: label, episodeCount: count });
  }
  return bars;
}

export function getShowDiary(userId: string, year: number, month: number): ShowDiaryPayload {
  const tz = userTimezone(userId);
  const y = Math.floor(year);
  const m = Math.floor(month);
  if (!Number.isFinite(y) || !Number.isFinite(m) || m < 1 || m > 12) {
    throw new Error("Invalid year or month");
  }

  const stats = getMyShowsAnalytics(userId);
  const { start: weekStart, end: weekEnd } = weekBoundsLocal(tz);

  const imageMap = new Map<number, string | null>();
  const imgRows = db
    .prepare(`SELECT tvmaze_show_id AS sid, show_image_url AS url FROM show_subscriptions WHERE user_id = ?`)
    .all(userId) as { sid: number; url: string | null }[];
  for (const r of imgRows) imageMap.set(r.sid, r.url);

  const taskRows = db
    .prepare(
      `SELECT wt.id AS taskId, wt.tvmaze_show_id AS tvmazeShowId, wt.tvmaze_episode_id AS tvmazeEpisodeId,
              wt.show_name AS showName, wt.episode_label AS episodeLabel, wt.completed_at AS completedAt,
              cer.rating AS userRating
       FROM watch_tasks wt
       LEFT JOIN community_episode_ratings cer
         ON cer.user_id = wt.user_id
        AND cer.tvmaze_show_id = wt.tvmaze_show_id
        AND cer.tvmaze_episode_id = wt.tvmaze_episode_id
       WHERE wt.user_id = ? AND wt.completed_at IS NOT NULL
       ORDER BY datetime(wt.completed_at) DESC
       LIMIT 800`,
    )
    .all(userId) as {
    taskId: string;
    tvmazeShowId: number;
    tvmazeEpisodeId: number;
    showName: string;
    episodeLabel: string;
    completedAt: string;
    userRating: number | null;
  }[];

  const reviewRows = db
    .prepare(
      `SELECT id, tvmaze_show_id AS tvmazeShowId, tvmaze_episode_id AS tvmazeEpisodeId, body_html AS bodyHtml
       FROM community_posts
       WHERE user_id = ? AND deleted_at IS NULL AND tag = 'episode_review'`,
    )
    .all(userId) as { id: string; tvmazeShowId: number; tvmazeEpisodeId: number; bodyHtml: string }[];

  const reviewByEp = new Map<string, { id: string; excerpt: string }>();
  for (const r of reviewRows) {
    const key = `${r.tvmazeShowId}:${r.tvmazeEpisodeId}`;
    const text = stripHtmlToText(r.bodyHtml);
    reviewByEp.set(key, { id: r.id, excerpt: text.length > 220 ? text.slice(0, 217) + "…" : text });
  }

  const monthPrefix = `${y}-${String(m).padStart(2, "0")}`;
  const allDated: { date: string; showName: string; rating: number | null }[] = [];

  const byDate = new Map<string, ShowDiaryDayEntry[]>();
  for (const t of taskRows) {
    const date = localDateKey(t.completedAt, tz);
    if (!date) continue;
    allDated.push({
      date,
      showName: t.showName,
      rating: t.userRating != null && Number.isFinite(Number(t.userRating)) ? Number(t.userRating) : null,
    });
    if (!date.startsWith(monthPrefix)) continue;
    const rev = reviewByEp.get(`${t.tvmazeShowId}:${t.tvmazeEpisodeId}`);
    const rating =
      t.userRating != null && Number.isFinite(Number(t.userRating)) ? Number(t.userRating) : null;
    const entry: ShowDiaryDayEntry = {
      taskId: t.taskId,
      tvmazeShowId: t.tvmazeShowId,
      tvmazeEpisodeId: t.tvmazeEpisodeId,
      showName: t.showName,
      episodeLabel: t.episodeLabel,
      completedAt: t.completedAt,
      rating,
      showImageUrl: imageMap.get(t.tvmazeShowId) ?? null,
      reviewExcerpt: rev?.excerpt ?? null,
      reviewPostId: rev?.id ?? null,
    };
    const list = byDate.get(date) ?? [];
    list.push(entry);
    byDate.set(date, list);
  }

  const dim = daysInMonth(y, m);
  const calendarDays: ShowDiaryCalendarDay[] = [];
  for (let day = 1; day <= dim; day++) {
    const date = `${monthPrefix}-${String(day).padStart(2, "0")}`;
    const entries = byDate.get(date) ?? [];
    if (!entries.length) continue;
    const ratings = entries.map((e) => e.rating).filter((r): r is number => r != null);
    const topRating = ratings.length ? Math.max(...ratings) : null;
    const showImageUrl = entries.find((e) => e.showImageUrl)?.showImageUrl ?? null;
    calendarDays.push({
      date,
      day,
      entryCount: entries.length,
      topRating,
      showImageUrl,
      entries,
    });
  }

  const topRatedRows = db
    .prepare(
      `SELECT cer.tvmaze_show_id AS tvmazeShowId, cer.tvmaze_episode_id AS tvmazeEpisodeId,
              cer.rating AS rating, wt.show_name AS showName, wt.episode_label AS episodeLabel,
              wt.completed_at AS completedAt
       FROM community_episode_ratings cer
       INNER JOIN watch_tasks wt
         ON wt.user_id = cer.user_id
        AND wt.tvmaze_show_id = cer.tvmaze_show_id
        AND wt.tvmaze_episode_id = cer.tvmaze_episode_id
        AND wt.completed_at IS NOT NULL
       WHERE cer.user_id = ?
       ORDER BY cer.rating DESC, datetime(wt.completed_at) DESC
       LIMIT 8`,
    )
    .all(userId) as {
    tvmazeShowId: number;
    tvmazeEpisodeId: number;
    rating: number;
    showName: string;
    episodeLabel: string;
    completedAt: string | null;
  }[];

  const topRated: ShowDiaryTopRatedItem[] = topRatedRows.map((r) => ({
    tvmazeShowId: r.tvmazeShowId,
    tvmazeEpisodeId: r.tvmazeEpisodeId,
    showName: r.showName,
    episodeLabel: r.episodeLabel,
    rating: Number(r.rating),
    showImageUrl: imageMap.get(r.tvmazeShowId) ?? null,
    completedAt: r.completedAt,
  }));

  return {
    year: y,
    month: m,
    monthLabel: monthLabel(y, m),
    timezone: tz,
    stats,
    weekSummary: buildWeekSummary(allDated, weekStart, weekEnd),
    weeklyBars: buildWeeklyBars(allDated, tz),
    calendarDays,
    topRated,
  };
}
