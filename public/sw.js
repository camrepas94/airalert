/* global self */

self.addEventListener("push", (event) => {
  event.waitUntil(
    (async () => {
      let data = { title: "Airalert", body: "New episode reminder", url: "/" };
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
        data: { url: data.url || "/" },
      });
    })(),
  );
});

self.addEventListener("notificationclick", (event) => {
  event.notification.close();
  const raw = event.notification.data && event.notification.data.url ? event.notification.data.url : "/";
  const abs = new URL(raw, self.location.origin).href;
  event.waitUntil(
    self.clients.matchAll({ type: "window", includeUncontrolled: true }).then((clients) => {
      for (const c of clients) {
        if (c.url && new URL(c.url).origin === self.location.origin) {
          c.focus();
          c.navigate(abs);
          return;
        }
      }
      return self.clients.openWindow(abs);
    }),
  );
});
