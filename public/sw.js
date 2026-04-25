/* global self */

self.addEventListener("push", (event) => {
  event.waitUntil(
    (async () => {
      let data = { title: "AirAlert", body: "New episode reminder", url: "/" };
      try {
        if (event.data) {
          const j = await event.data.json();
          if (j && typeof j === "object") {
            data = { ...data, ...j };
          }
        }
      } catch {
        /* ignore */
      }
      await self.registration.showNotification(data.title, {
        body: data.body,
        data: {
          url: data.url || "/",
          kind: data.kind || null,
          category: data.category || null,
          surface: data.surface || null,
        },
      });
      await sendAnalyticsFromSw("notification.shown", data.url || "/", {
        notificationKind: data.kind || null,
        notificationCategory: data.category || null,
        notificationSurface: data.surface || null,
      });
    })(),
  );
});

async function sendAnalyticsFromSw(name, url, metadata) {
  const payload = {
    event: {
      name,
      sourceScreen: "service_worker",
      targetType: "notification",
      targetId: notificationTargetId(url),
      metadata: {
        destination: notificationDestination(url),
        ...(metadata || {}),
      },
    },
  };
  try {
    await fetch("/api/analytics/events", {
      method: "POST",
      credentials: "include",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload),
    });
  } catch {
    /* analytics must never break notification behavior */
  }
}

function notificationDestination(raw) {
  try {
    const u = new URL(raw || "/", self.location.origin);
    if (u.searchParams.has("communityShow")) return "community_thread";
    if (u.searchParams.has("dmThread")) return "dm_thread";
    if (u.searchParams.get("openInbox") === "1") return "dm_inbox";
    if (u.searchParams.has("taskEpisode")) return "tasks";
    if (u.searchParams.get("tab") === "tasks") return "tasks";
    if (u.searchParams.get("tab") === "settings") return "settings";
    if (u.searchParams.get("profile") === "account") return "profile_account";
    return "app";
  } catch {
    return "unknown";
  }
}

function notificationTargetId(raw) {
  try {
    const u = new URL(raw || "/", self.location.origin);
    return u.searchParams.get("communityPostId") || u.searchParams.get("communityEpisode") || u.searchParams.get("communityShow") || u.searchParams.get("dmThread") || u.searchParams.get("taskEpisode") || u.searchParams.get("tab") || "app";
  } catch {
    return "unknown";
  }
}

self.addEventListener("notificationclick", (event) => {
  event.notification.close();
  const raw = event.notification.data && event.notification.data.url ? event.notification.data.url : "/";
  const abs = new URL(raw, self.location.origin).href;
  const notificationData = event.notification.data || {};
  event.waitUntil(
    sendAnalyticsFromSw("notification.tapped", raw, {
      notificationKind: notificationData.kind || null,
      notificationCategory: notificationData.category || null,
      notificationSurface: notificationData.surface || null,
    }).then(() => self.clients.matchAll({ type: "window", includeUncontrolled: true })).then((clients) => {
      for (const c of clients) {
        if (c.url && new URL(c.url).origin === self.location.origin) {
          try {
            c.postMessage({
              type: "airalert.analytics",
              name: "notification.tapped",
              targetType: "notification",
              targetId: notificationTargetId(raw),
              metadata: {
                destination: notificationDestination(raw),
                notificationKind: notificationData.kind || null,
                notificationCategory: notificationData.category || null,
                notificationSurface: notificationData.surface || null,
              },
            });
          } catch {
            /* ignore */
          }
          c.focus();
          c.navigate(abs);
          return;
        }
      }
      return self.clients.openWindow(abs);
    }),
  );
});
