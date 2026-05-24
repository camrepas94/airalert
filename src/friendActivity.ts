import { v4 as uuidv4 } from "uuid";
import { db } from "./db.js";

export type FriendActivityType =
  | "rating"
  | "episode_watched"
  | "thread_joined"
  | "thread_reply"
  | "thread_post";

export type RecordFriendActivityInput = {
  userId: string;
  activityType: FriendActivityType;
  tvmazeShowId?: number | null;
  showName?: string | null;
  showImageUrl?: string | null;
  tvmazeEpisodeId?: number | null;
  episodeLabel?: string | null;
  threadPostId?: string | null;
  rating?: number | null;
};

export type FriendActivityFeedItem = {
  id: string;
  userId: string;
  username: string | null;
  displayName: string | null;
  avatarDataUrl: string | null;
  activityType: FriendActivityType;
  tvmazeShowId: number | null;
  showName: string | null;
  showImageUrl: string | null;
  tvmazeEpisodeId: number | null;
  episodeLabel: string | null;
  threadPostId: string | null;
  rating: number | null;
  createdAt: string;
};

function resolveShowImageUrl(userId: string, tvmazeShowId: number): string | null {
  const sub = db
    .prepare(
      `SELECT show_image_url AS url FROM show_subscriptions WHERE user_id = ? AND tvmaze_show_id = ? LIMIT 1`,
    )
    .get(userId, tvmazeShowId) as { url: string | null } | undefined;
  if (sub?.url && String(sub.url).trim()) return String(sub.url).trim();
  const any = db
    .prepare(`SELECT show_image_url AS url FROM show_subscriptions WHERE tvmaze_show_id = ? AND show_image_url IS NOT NULL LIMIT 1`)
    .get(tvmazeShowId) as { url: string | null } | undefined;
  if (any?.url && String(any.url).trim()) return String(any.url).trim();
  return null;
}

/** Persist a friend-activity event (best-effort; never throws to callers). */
export function recordFriendActivity(input: RecordFriendActivityInput): void {
  try {
    const userId = String(input.userId || "").trim();
    if (!userId) return;
    const activityType = input.activityType;
    const allowed: FriendActivityType[] = [
      "rating",
      "episode_watched",
      "thread_joined",
      "thread_reply",
      "thread_post",
    ];
    if (!allowed.includes(activityType)) return;

    let tvmazeShowId: number | null =
      input.tvmazeShowId != null && Number.isInteger(Number(input.tvmazeShowId)) && Number(input.tvmazeShowId) > 0
        ? Number(input.tvmazeShowId)
        : null;
    const showName =
      input.showName != null && String(input.showName).trim() ? String(input.showName).trim().slice(0, 200) : null;
    let showImageUrl =
      input.showImageUrl != null && String(input.showImageUrl).trim()
        ? String(input.showImageUrl).trim().slice(0, 2048)
        : null;
    if (!showImageUrl && tvmazeShowId) showImageUrl = resolveShowImageUrl(userId, tvmazeShowId);

    const tvmazeEpisodeId =
      input.tvmazeEpisodeId != null && Number.isInteger(Number(input.tvmazeEpisodeId)) && Number(input.tvmazeEpisodeId) > 0
        ? Number(input.tvmazeEpisodeId)
        : null;
    const episodeLabel =
      input.episodeLabel != null && String(input.episodeLabel).trim()
        ? String(input.episodeLabel).trim().slice(0, 80)
        : null;
    const threadPostId =
      input.threadPostId != null && String(input.threadPostId).trim()
        ? String(input.threadPostId).trim().slice(0, 80)
        : null;
    const rating =
      input.rating != null && Number.isInteger(Number(input.rating)) && Number(input.rating) >= 1 && Number(input.rating) <= 5
        ? Number(input.rating)
        : null;

    db.prepare(
      `INSERT INTO friend_activity_events (
         id, user_id, activity_type, tvmaze_show_id, show_name, show_image_url,
         tvmaze_episode_id, episode_label, thread_post_id, rating, created_at
       ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, datetime('now'))`,
    ).run(
      uuidv4(),
      userId,
      activityType,
      tvmazeShowId,
      showName,
      showImageUrl,
      tvmazeEpisodeId,
      episodeLabel,
      threadPostId,
      rating,
    );
  } catch {
    /* best-effort */
  }
}

export function countUserShowPosts(userId: string, tvmazeShowId: number): number {
  const row = db
    .prepare(
      `SELECT COUNT(*) AS c FROM community_posts
       WHERE user_id = ? AND tvmaze_show_id = ? AND deleted_at IS NULL`,
    )
    .get(userId, tvmazeShowId) as { c: number } | undefined;
  return Number(row?.c) || 0;
}

export function recordCommunityPostActivity(
  userId: string,
  tvmazeShowId: number,
  showName: string,
  showImageUrl: string | null,
  parentPostId: string | null,
  postId: string,
  episodeId: number | null,
  episodeLabel: string | null,
  priorPostCountOnShow: number,
): void {
  let activityType: FriendActivityType;
  if (parentPostId) {
    activityType = "thread_reply";
  } else if (priorPostCountOnShow === 0) {
    activityType = "thread_joined";
  } else {
    activityType = "thread_post";
  }
  recordFriendActivity({
    userId,
    activityType,
    tvmazeShowId,
    showName,
    showImageUrl,
    tvmazeEpisodeId: episodeId,
    episodeLabel,
    threadPostId: postId,
  });
}

export function getFriendsActivityFeed(
  viewerId: string,
  opts?: { limit?: number; before?: string | null },
): { items: FriendActivityFeedItem[]; followingCount: number } {
  const limitRaw = opts?.limit;
  const limit = Number.isFinite(limitRaw) ? Math.min(60, Math.max(1, Math.floor(Number(limitRaw)))) : 30;
  const followingCount = (
    db.prepare(`SELECT COUNT(*) AS c FROM user_follows WHERE follower_id = ?`).get(viewerId) as { c: number }
  ).c;

  if (followingCount === 0) {
    return { items: [], followingCount: 0 };
  }

  const before = opts?.before && String(opts.before).trim() ? String(opts.before).trim() : null;
  const params: (string | number)[] = [viewerId];
  let beforeClause = "";
  if (before) {
    beforeClause = ` AND datetime(e.created_at) < datetime(?)`;
    params.push(before);
  }
  params.push(limit);

  const rows = db
    .prepare(
      `SELECT e.id, e.user_id AS userId, e.activity_type AS activityType,
              e.tvmaze_show_id AS tvmazeShowId, e.show_name AS showName, e.show_image_url AS showImageUrl,
              e.tvmaze_episode_id AS tvmazeEpisodeId, e.episode_label AS episodeLabel,
              e.thread_post_id AS threadPostId, e.rating, e.created_at AS createdAt,
              u.username, u.display_name AS displayName, u.avatar_data_url AS avatarDataUrl
       FROM friend_activity_events e
       INNER JOIN user_follows uf ON uf.followed_id = e.user_id AND uf.follower_id = ?
       INNER JOIN users u ON u.id = e.user_id
       WHERE 1=1${beforeClause}
       ORDER BY datetime(e.created_at) DESC
       LIMIT ?`,
    )
    .all(...params) as FriendActivityFeedItem[];

  return { items: rows, followingCount };
}
