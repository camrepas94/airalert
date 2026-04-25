import { randomUUID } from "node:crypto";
import { db } from "./db.js";
import { fetchPersonShowCreditsMerged, fetchShowEpisodes } from "./tvmaze.js";
import { normalizeEpisodeAirdate, safeTodayInTimeZone } from "./time.js";
import { getPushPrefsForUser, sendWebPushToUser } from "./push.js";

type SubscriptionRow = {
  id: string;
  user_id: string;
  tvmaze_show_id: number;
  show_name: string;
  binge_later: number | null;
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
  const subs = db
    .prepare(`SELECT id, user_id, tvmaze_show_id, show_name, binge_later FROM show_subscriptions`)
    .all() as SubscriptionRow[];

  const users = new Map<string, UserRow>();
  for (const u of db.prepare(`SELECT id, timezone FROM users`).all() as UserRow[]) {
    users.set(u.id, u);
  }

  const insertLog = db.prepare(`
    INSERT OR IGNORE INTO notification_log (id, user_id, tvmaze_episode_id, show_name, episode_label, airdate, channel)
    VALUES (@id, @user_id, @tvmaze_episode_id, @show_name, @episode_label, @airdate, 'dry_run')
  `);

  const insertWatchTask = db.prepare(`
    INSERT OR IGNORE INTO watch_tasks (id, user_id, tvmaze_show_id, tvmaze_episode_id, show_name, episode_label, airdate)
    VALUES (@id, @user_id, @tvmaze_show_id, @tvmaze_episode_id, @show_name, @episode_label, @airdate)
  `);

  const findEpisodes = db.prepare(`
    SELECT tvmaze_episode_id, name, season, number, date(airdate) AS airdate
    FROM episodes_cache
    WHERE tvmaze_show_id = ? AND date(airdate) IS NOT NULL AND date(airdate) = date(?)
  `);

  const created: DryRunNotification[] = [];

  for (const sub of subs) {
    if (sub.binge_later === 1) continue;
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
        insertWatchTask.run({
          id: randomUUID(),
          user_id: sub.user_id,
          tvmaze_show_id: sub.tvmaze_show_id,
          tvmaze_episode_id: ep.tvmaze_episode_id,
          show_name: sub.show_name,
          episode_label: episodeLabel,
          airdate: ep.airdate,
        });
        await sendWebPushToUser(
          sub.user_id,
          {
            title: sub.show_name,
            body: `Airs today: ${episodeLabel}`,
            url: `/?tab=tasks&taskEpisode=${ep.tvmaze_episode_id}`,
          },
          { kind: "episodeAirsToday" },
        );
      }
    }
  }

  return created;
}

type UserNudgeRow = {
  id: string;
  timezone: string;
  task_nudge_days_after_air: number;
};

/**
 * For users who enabled “still watching?” nudges: if an episode aired N calendar days ago
 * (in their timezone) and the task is still open, send one follow-up push.
 */
export async function runTaskNudgeNotifications(): Promise<number> {
  const users = db
    .prepare(
      `SELECT id, timezone, task_nudge_days_after_air FROM users
       WHERE task_nudge_days_after_air IS NOT NULL AND task_nudge_days_after_air IN (1, 3, 7)`,
    )
    .all() as UserNudgeRow[];

  const selectDue = db.prepare(`
    SELECT w.id, w.user_id, w.show_name, w.episode_label
    FROM watch_tasks w
    WHERE w.user_id = ?
      AND w.completed_at IS NULL
      AND w.dismissed_at IS NULL
      AND w.nudge_sent_at IS NULL
      AND date(w.airdate, '+' || ? || ' days') = date(?)
      AND NOT EXISTS (
        SELECT 1 FROM show_subscriptions s
        WHERE s.user_id = w.user_id AND s.tvmaze_show_id = w.tvmaze_show_id AND IFNULL(s.binge_later, 0) = 1
      )
  `);

  const markNudged = db.prepare(`UPDATE watch_tasks SET nudge_sent_at = datetime('now') WHERE id = ? AND user_id = ?`);

  let sent = 0;
  for (const u of users) {
    const today = safeTodayInTimeZone(u.timezone);
    const offset = String(u.task_nudge_days_after_air);
    const rows = selectDue.all(u.id, offset, today) as {
      id: string;
      user_id: string;
      show_name: string;
      episode_label: string;
    }[];

    for (const row of rows) {
      const prefs = getPushPrefsForUser(row.user_id);
      if (!prefs.taskStillWatching) {
        markNudged.run(row.id, row.user_id);
        continue;
      }
      await sendWebPushToUser(
        row.user_id,
        {
          title: "Still watching?",
          body: `Have you watched ${row.show_name} yet?!`,
          url: "/?tab=tasks",
        },
        { kind: "taskStillWatching" },
      );
      markNudged.run(row.id, row.user_id);
      sent += 1;
    }
  }

  return sent;
}

/** After a user follows someone, record all current TVMaze credits so we only notify on future additions. */
export async function baselinePersonCreditsForPerson(personId: number): Promise<void> {
  const credits = await fetchPersonShowCreditsMerged(personId);
  const ins = db.prepare(
    `INSERT OR IGNORE INTO person_credited_shows (tvmaze_person_id, tvmaze_show_id, show_name) VALUES (?, ?, ?)`,
  );
  const tx = db.transaction((rows: typeof credits) => {
    for (const c of rows) ins.run(personId, c.tvmazeShowId, c.showName);
  });
  tx(credits);
}

export type PersonProjectNotificationSent = {
  userId: string;
  tvmazePersonId: number;
  personName: string;
  tvmazeShowId: number;
  showName: string;
};

/**
 * For each followed person, refresh cast+crew from TVMaze; when a new show appears in their credits,
 * notify every user following that person.
 */
export async function runPersonNewProjectNotifications(): Promise<PersonProjectNotificationSent[]> {
  const people = db
    .prepare(`SELECT DISTINCT tvmaze_person_id FROM user_person_follows`)
    .all() as { tvmaze_person_id: number }[];

  const insertSnap = db.prepare(
    `INSERT OR IGNORE INTO person_credited_shows (tvmaze_person_id, tvmaze_show_id, show_name) VALUES (?, ?, ?)`,
  );

  const out: PersonProjectNotificationSent[] = [];

  for (const p of people) {
    let credits: { tvmazeShowId: number; showName: string }[];
    try {
      credits = await fetchPersonShowCreditsMerged(p.tvmaze_person_id);
    } catch (err) {
      console.warn("[jobs] person credits fetch failed", p.tvmaze_person_id, err);
      await new Promise((r) => setTimeout(r, 400));
      continue;
    }

    const nameRow = db
      .prepare(`SELECT person_name FROM user_person_follows WHERE tvmaze_person_id = ? LIMIT 1`)
      .get(p.tvmaze_person_id) as { person_name: string } | undefined;
    const personName = nameRow?.person_name?.trim() || "Someone";

    for (const c of credits) {
      const r = insertSnap.run(p.tvmaze_person_id, c.tvmazeShowId, c.showName);
      if (r.changes === 0) continue;

      const followers = db
        .prepare(`SELECT user_id FROM user_person_follows WHERE tvmaze_person_id = ?`)
        .all(p.tvmaze_person_id) as { user_id: string }[];

      for (const f of followers) {
        await sendWebPushToUser(
          f.user_id,
          {
            title: `${personName} — new project`,
            body: `${c.showName} is now on their TVMaze credits.`,
            url: `/?communityShow=${c.tvmazeShowId}`,
          },
          { kind: "personNewProject" },
        );
        out.push({
          userId: f.user_id,
          tvmazePersonId: p.tvmaze_person_id,
          personName,
          tvmazeShowId: c.tvmazeShowId,
          showName: c.showName,
        });
      }
    }

    await new Promise((res) => setTimeout(res, 200));
  }

  return out;
}
