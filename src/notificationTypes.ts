export type NotificationCategory = "activity" | "direct_message" | "show_alert" | "system_account" | "live_room";

export type NotificationSurface = "activity_dropdown" | "inbox" | "tasks" | "community" | "profile_account" | "push_only";

export type PushNotificationKind =
  | "episodeAirsToday"
  | "taskStillWatching"
  | "dmMessage"
  | "communityMention"
  | "communityReply"
  | "communityThreadNewPost"
  | "personNewProject";

/** Row `kind` in `activity_notifications` (human/social activity inbox, not `notification_log`). */
export type ActivityNotificationKind =
  | "community_mention"
  | "community_reply"
  | "group_chat_invite"
  /** Internal: beta feedback submitted — recipient `user_id` must be admin only (insert path enforces). */
  | "beta_feedback_admin";

export const ACTIVITY_NOTIFICATION_KINDS = new Set<ActivityNotificationKind>([
  "community_mention",
  "community_reply",
  "group_chat_invite",
  "beta_feedback_admin",
]);

export const PUSH_NOTIFICATION_KINDS = new Set<PushNotificationKind>([
  "episodeAirsToday",
  "taskStillWatching",
  "dmMessage",
  "communityMention",
  "communityReply",
  "communityThreadNewPost",
  "personNewProject",
]);

export function pushNotificationCategory(kind: PushNotificationKind): NotificationCategory {
  if (kind === "dmMessage") return "direct_message";
  if (kind === "communityMention" || kind === "communityReply" || kind === "communityThreadNewPost") return "activity";
  if (kind === "episodeAirsToday" || kind === "taskStillWatching" || kind === "personNewProject") return "show_alert";
  return "system_account";
}

export function pushNotificationSurface(kind: PushNotificationKind): NotificationSurface {
  if (kind === "dmMessage") return "inbox";
  if (kind === "communityMention" || kind === "communityReply") return "activity_dropdown";
  if (kind === "communityThreadNewPost" || kind === "personNewProject") return "community";
  if (kind === "episodeAirsToday" || kind === "taskStillWatching") return "tasks";
  return "push_only";
}

