import { randomUUID } from "node:crypto";
import { db } from "./db.js";
import { fetchShowEpisodes } from "./tvmaze.js";
import { normalizeEpisodeAirdate, safeTodayInTimeZone } from "./time.js";
import { sendWebPushToUser } from "./push.js";

type SubscriptionRow = {
  id: string;
  user_id: string;
  tvmaze_show_id: number;
  show_name: string;
};

type UserRow = {
  id: string;
  timezone: string;
};

/** Upsert episodes from TVMaze into cache for one show. */
export async function refreshShowEpisodes(tvmazeShowId: number): Promise<number> {
  const episodes = await fetchShowEpisodes(tvmazeShowId);
  const upsert = db.prepare(`
    INSERT INTO episodes_cache (
      tvmaze_show_id, tvmaze_episode_id, name, season, number, airdate, airtime, runtime, network, updated_at
    ) VALUES (
      @tvmaze_show_id, @tvmaze_episode_id, @name, @season, @number, @airdate, @airtime, @runtime, @network, datetime('now')
    )
    ON CONFLICT(tvmaze_show_id, tvmaze_episode_id) DO UPDATE SET
      name = excluded.name,
      season = excluded.season,
      number = excluded.number,
      airdate = excluded.airdate,
      airtime = excluded.airtime,
      runtime = excluded.runtime,
      network = excluded.network,
      updated_at = datetime('now')
  `);

  const tx = db.transaction((rows: typeof episodes) => {
    for (const ep of rows) {
      upsert.run({
        tvmaze_show_id: tvmazeShowId,
        tvmaze_episode_id: ep.id,
        name: ep.name || "TBA",
        season: ep.season,
        number: ep.number,
        airdate: normalizeEpisodeAirdate(ep.airdate),
        airtime: ep.airtime || "",
        runtime: ep.runtime,
        network: null,
      });
    }
  });

  tx(episodes);
  return episodes.length;
}

/** Refresh cache for every show that appears in any subscription. */
export async function refreshAllSubscribedShows(): Promise<{ showId: number; count: number }[]> {
  const rows = db
    .prepare(`SELECT DISTINCT tvmaze_show_id FROM show_subscriptions`)
    .all() as { tvmaze_show_id: number }[];

  const results: { showId: number; count: number }[] = [];
  for (const r of rows) {
    const count = await refreshShowEpisodes(r.tvmaze_show_id);
    results.push({ showId: r.tvmaze_show_id, count });
  }
  return results;
}

export type DryRunNotification = {
  userId: string;
  showName: string;
  episodeLabel: string;
  airdate: string;
  tvmazeEpisodeId: number;
};

/**
 * Record notifications for episodes airing "today" in each user's timezone.
 * Logs to DB (dry_run channel). Sends Web Push when VAPID is configured and the user has subscribed.
 */
export async function runDailyNotifications(): Promise<DryRunNotification[]> {
  const subs = db.prepare(`SELECT id, user_id, tvmaze_show_id, show_name FROM show_subscriptions`).all() as SubscriptionRow[];

  const users = new Map<string, UserRow>();
  for (const u of db.prepare(`SELECT id, timezone FROM users`).all() as UserRow[]) {
    users.set(u.id, u);
  }

  const insertLog = db.prepare(`
    INSERT OR IGNORE INTO notification_log (id, user_id, tvmaze_episode_id, show_name, episode_label, airdate, channel)
    VALUES (@id, @user_id, @tvmaze_episode_id, @show_name, @episode_label, @airdate, 'dry_run')
  `);

  const findEpisodes = db.prepare(`
    SELECT tvmaze_episode_id, name, season, number, date(airdate) AS airdate
    FROM episodes_cache
    WHERE tvmaze_show_id = ? AND date(airdate) IS NOT NULL AND date(airdate) = date(?)
  `);

  const created: DryRunNotification[] = [];

  for (const sub of subs) {
    const user = users.get(sub.user_id);
    if (!user) continue;
    const today = safeTodayInTimeZone(user.timezone);
    const eps = findEpisodes.all(sub.tvmaze_show_id, today) as {
      tvmaze_episode_id: number;
      name: string;
      season: number;
      number: number;
      airdate: string;
    }[];

    for (const ep of eps) {
      const episodeLabel = `S${ep.season}E${ep.number} — ${ep.name}`;
      const id = randomUUID();
      const result = insertLog.run({
        id,
        user_id: sub.user_id,
        tvmaze_episode_id: ep.tvmaze_episode_id,
        show_name: sub.show_name,
        episode_label: episodeLabel,
        airdate: ep.airdate,
      });
      if (result.changes > 0) {
        created.push({
          userId: sub.user_id,
          showName: sub.show_name,
          episodeLabel,
          airdate: ep.airdate,
          tvmazeEpisodeId: ep.tvmaze_episode_id,
        });
        await sendWebPushToUser(sub.user_id, {
          title: sub.show_name,
          body: `Airs today: ${episodeLabel}`,
          url: "/",
        });
      }
    }
  }

  return created;
}
