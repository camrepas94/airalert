/* global self */

self.addEventListener("push", (event) => {
  let data = { title: "Airalert", body: "New episode reminder", url: "/" };
  try {
    if (event.data) {
      const j = event.data.json();
      if (j && typeof j === "object") {
        data = { ...data, ...j };
      }
    }
  } catch {
    /* ignore */
  }
  event.waitUntil(
    self.registration.showNotification(data.title, {
      body: data.body,
      data: { url: data.url || "/" },
    }),
  );
});

self.addEventListener("notificationclick", (event) => {
  event.notification.close();
  const url = event.notification.data && event.notification.data.url ? event.notification.data.url : "/";
  event.waitUntil(self.clients.openWindow(url));
});
