// bunnychat service worker — enables real Web Push notifications (delivery
// even when the app/tab isn't open), which a plain `new Notification()`
// call from page JS cannot do since that only works while the page's own
// JS is actually running.

self.addEventListener('install', function(event) {
  self.skipWaiting();
});

self.addEventListener('activate', function(event) {
  event.waitUntil(self.clients.claim());
});

self.addEventListener('push', function(event) {
  var data = {};
  try {
    data = event.data ? event.data.json() : {};
  } catch (e) {
    data = { title: 'bunnychat 🐰💬', body: event.data ? event.data.text() : '' };
  }

  event.waitUntil(
    self.clients.matchAll({ type: 'window', includeUncontrolled: true }).then(function(clientList) {
      // If a tab for this app is already open and focused, skip the
      // notification — the in-page notice already covers that case, and
      // popping a redundant OS notification while someone's actively
      // looking at the chat is just noise.
      var anyFocused = clientList.some(function(c) { return c.focused; });
      if (anyFocused) return;

      return self.registration.showNotification(data.title || 'bunnychat 🐰💬', {
        body: data.body || 'new message',
        icon: 'bunnylogo.png',
        badge: 'bunnylogo.png',
        data: { roomCode: data.roomCode || null },
        tag: 'bunnychat-message',
        renotify: true
      });
    })
  );
});

self.addEventListener('notificationclick', function(event) {
  event.notification.close();
  event.waitUntil(
    self.clients.matchAll({ type: 'window', includeUncontrolled: true }).then(function(clientList) {
      for (var i = 0; i < clientList.length; i++) {
        if ('focus' in clientList[i]) return clientList[i].focus();
      }
      if (self.clients.openWindow) return self.clients.openWindow('/');
    })
  );
});
