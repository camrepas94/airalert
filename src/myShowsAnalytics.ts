import { db } from "./db.js";

export type MyShowsAnalytics = {
  showsWatching: number;
  episodesWatched: number;
  episodesRemaining: number;
  episodesThisWeek: number;
  skippedEpisodes: number;
  averageRating: number | null;
  totalHoursWatched: number;
  streakDays: number;
  streakLabel: string;
};

function userTimezone(userId: string): string {
  const row = db.prepare(`SELECT timezone FROM users WHERE id = ?`).get(userId) as { timezone: string | null } | undefined;
  const tz = row?.timezone && String(row.timezone).trim();
  return tz || "UTC";
}

/** YYYY-MM-DD in user timezone. */
function localDateKey(d: Date, timeZone: string): string {
  try {
    return new Intl.DateTimeFormat("en-CA", { timeZone, year: "numeric", month: "2-digit", day: "2-digit" }).format(d);
  } catch {
    return d.toISOString().slice(0, 10);
  }
}

function weekBoundsLocal(timeZone: string): { start: string; end: string } {
  const now = new Date();
  const today = localDateKey(now, timeZone);
  const parts = today.split("-").map(Number);
  const noon = new Date(Date.UTC(parts[0], parts[1] - 1, parts[2], 12, 0, 0));
  const dow = noon.getUTCDay();
  const sun = new Date(noon);
  sun.setUTCDate(sun.getUTCDate() - dow);
  const sat = new Date(sun);
  sat.setUTCDate(sat.getUTCDate() + 6);
  return { start: localDateKey(sun, timeZone), end: localDateKey(sat, timeZone) };
}

function computeStreak(completedDates: string[]): { streakDays: number; streakLabel: string } {
  if (!completedDates.length) {
    return { streakDays: 0, streakLabel: "Start your watch streak today" };
  }
  const unique = [...new Set(completedDates)].sort((a, b) => b.localeCompare(a));
  const today = unique[0];
  let streak = 1;
  let prev = today;
  for (let i = 1; i < unique.length; i++) {
    const cur = unique[i];
    const prevMs = Date.parse(prev + "T12:00:00");
    const curMs = Date.parse(cur + "T12:00:00");
    const diffDays = Math.round((prevMs - curMs) / 86400000);
    if (diffDays === 1) {
      streak++;
      prev = cur;
    } else if (diffDays === 0) {
      continue;
    } else {
      break;
    }
  }
  const label =
    streak >= 2
      ? `Watched ${streak} days in a row`
      : streak === 1
        ? "Watched today — keep it going"
        : "Start your watch streak today";
  return { streakDays: streak, streakLabel: label };
}

export function getMyShowsAnalytics(userId: string): MyShowsAnalytics {
  const tz = userTimezone(userId);
  const { start: weekStart, end: weekEnd } = weekBoundsLocal(tz);

  const showsWatching = (
    db.prepare(`SELECT COUNT(*) AS c FROM show_subscriptions WHERE user_id = ?`).get(userId) as { c: number }
  ).c;

  const watched = (
    db
      .prepare(`SELECT COUNT(*) AS c FROM watch_tasks WHERE user_id = ? AND completed_at IS NOT NULL`)
      .get(userId) as { c: number }
  ).c;

  const remaining = (
    db
      .prepare(
        `SELECT COUNT(*) AS c FROM watch_tasks WHERE user_id = ? AND completed_at IS NULL AND dismissed_at IS NULL`,
      )
      .get(userId) as { c: number }
  ).c;

  const skippedEpisodes = (
    db.prepare(`SELECT COUNT(*) AS c FROM watch_tasks WHERE user_id = ? AND dismissed_at IS NOT NULL`).get(userId) as {
      c: number;
    }
  ).c;

  const episodesThisWeek = (
    db
      .prepare(
        `SELECT COUNT(*) AS c FROM watch_tasks
         WHERE user_id = ? AND airdate IS NOT NULL
           AND date(airdate) >= date(?) AND date(airdate) <= date(?)`,
      )
      .get(userId, weekStart, weekEnd) as { c: number }
  ).c;

  const avgRow = db
    .prepare(`SELECT AVG(rating) AS avg FROM community_episode_ratings WHERE user_id = ?`)
    .get(userId) as { avg: number | null } | undefined;
  const averageRating =
    avgRow?.avg != null && Number.isFinite(Number(avgRow.avg)) ? Math.round(Number(avgRow.avg) * 10) / 10 : null;

  const hoursRow = db
    .prepare(
      `SELECT COALESCE(SUM(CAST(ec.runtime AS INTEGER)), 0) AS mins
       FROM watch_tasks wt
       INNER JOIN episodes_cache ec ON ec.tvmaze_episode_id = wt.tvmaze_episode_id
       WHERE wt.user_id = ? AND wt.completed_at IS NOT NULL AND ec.runtime IS NOT NULL AND ec.runtime > 0`,
    )
    .get(userId) as { mins: number } | undefined;
  const totalHoursWatched = Math.round(((Number(hoursRow?.mins) || 0) / 60) * 10) / 10;

  const dateRows = db
    .prepare(
      `SELECT DISTINCT date(completed_at) AS d
       FROM watch_tasks
       WHERE user_id = ? AND completed_at IS NOT NULL
       ORDER BY d DESC
       LIMIT 120`,
    )
    .all(userId) as { d: string }[];
  const { streakDays, streakLabel } = computeStreak(dateRows.map((r) => String(r.d || "").slice(0, 10)).filter(Boolean));

  return {
    showsWatching,
    episodesWatched: watched,
    episodesRemaining: remaining,
    episodesThisWeek,
    skippedEpisodes,
    averageRating,
    totalHoursWatched,
    streakDays,
    streakLabel,
  };
}
