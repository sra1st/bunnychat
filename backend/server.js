const http = require('http');
const WebSocket = require('ws');
const webpush = require('web-push');

// ── Web Push setup ──────────────────────────────────────────────────────
// These are real keys generated for this deployment. VAPID_PRIVATE_KEY
// should be set as an environment variable on Render (never commit a
// private key to the repo) — this hardcoded value is only a fallback so
// the server doesn't crash if the env var is missing.
function cleanKey(v) {
  // Strip whitespace/newlines and any stray surrounding quotes — the most
  // common ways a pasted env var value ends up subtly wrong on Render.
  if (typeof v !== 'string') return v;
  return v.trim().replace(/^['"]|['"]$/g, '');
}
const VAPID_PUBLIC_KEY = cleanKey(process.env.VAPID_PUBLIC_KEY) || 'BDNp9cW764pLS8BjU0m5tjc1khyDWzDk--OZReiUavkExKBcJPblV6ifT-7oZz1tB-dt9x-3zANtqWdgN1C97zs';
const VAPID_PRIVATE_KEY = cleanKey(process.env.VAPID_PRIVATE_KEY) || '-w1v8VYjQkUDE9phIn7Sf8ZrQb7AeIbgfhvemYiIfjg';

// A bad/misconfigured VAPID key must NEVER take down the whole server —
// chat, WebSockets, everything else should keep working even if push
// notifications end up disabled. Only the push feature itself degrades.
let pushEnabled = true;
try {
  webpush.setVapidDetails('mailto:admin@bunnychat.netlify.app', VAPID_PUBLIC_KEY, VAPID_PRIVATE_KEY);
} catch (e) {
  pushEnabled = false;
  console.error('bunnychat: VAPID key setup failed, push notifications are DISABLED. Chat still works normally. Error:', e.message);
}

// Allow the frontend origin to call the subscribe endpoint.
const ALLOWED_ORIGIN = process.env.ALLOWED_ORIGIN || 'https://bunnychat.netlify.app';

function setCors(res) {
  res.setHeader('Access-Control-Allow-Origin', ALLOWED_ORIGIN);
  res.setHeader('Access-Control-Allow-Methods', 'POST, DELETE, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type');
}

function readJsonBody(req, callback) {
  let body = '';
  let tooBig = false;
  req.on('data', function(chunk) {
    body += chunk;
    if (body.length > 64 * 1024) { tooBig = true; req.destroy(); }
  });
  req.on('end', function() {
    if (tooBig) return callback(new Error('payload too large'));
    try {
      callback(null, JSON.parse(body || '{}'));
    } catch (e) {
      callback(e);
    }
  });
}

const server = http.createServer((req, res) => {
  const url = req.url.split('?')[0];

  if (req.method === 'OPTIONS') {
    setCors(res);
    res.writeHead(204);
    res.end();
    return;
  }

  if (req.method === 'POST' && url === '/subscribe') {
    setCors(res);
    if (!pushEnabled) {
      res.writeHead(503, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ ok: false, error: 'push notifications are not configured on this server' }));
      return;
    }
    readJsonBody(req, function(err, data) {
      if (err) { res.writeHead(400); res.end('bad request'); return; }
      const roomCode = data.roomCode;
      const clientId = data.clientId;
      const subscription = data.subscription;
      if (!isNonEmptyString(roomCode, MAX_CODE_LEN) || !isNonEmptyString(clientId, MAX_ID_LEN) ||
          !subscription || typeof subscription !== 'object' || !subscription.endpoint) {
        res.writeHead(400); res.end('invalid subscription'); return;
      }
      const room = getRoom(roomCode);
      room.pushSubs[clientId] = subscription;
      res.writeHead(200, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ ok: true }));
    });
    return;
  }

  if (req.method === 'DELETE' && url === '/subscribe') {
    setCors(res);
    readJsonBody(req, function(err, data) {
      if (err) { res.writeHead(400); res.end('bad request'); return; }
      const roomCode = data.roomCode;
      const clientId = data.clientId;
      if (isNonEmptyString(roomCode, MAX_CODE_LEN) && isNonEmptyString(clientId, MAX_ID_LEN) && rooms[roomCode]) {
        delete rooms[roomCode].pushSubs[clientId];
      }
      res.writeHead(200, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ ok: true }));
    });
    return;
  }

  res.writeHead(200, { 'Content-Type': 'text/plain' });
  res.end('bunny chat server running');
});

const wss = new WebSocket.Server({
  server: server,
  clientTracking: true,
  // Reject any single WS frame bigger than this — stops one client from
  // flooding the process (and every peer's bandwidth) with giant payloads.
  maxPayload: 64 * 1024
});

wss.on('error', function(err) {
  console.log('WSS ERROR:', err);
});

const rooms = {};

// How long we hold off announcing a departure, in case it's just a page
// refresh reconnecting rather than someone actually leaving.
const RECONNECT_GRACE_MS = 5000;

// Cap how many chat messages a room remembers server-side, so a
// long-running room's memory footprint stays bounded.
const MAX_ROOM_MESSAGES = 200;

// ── input limits ────────────────────────────────────────────────────────
const MAX_NAME_LEN = 20;
const MAX_CODE_LEN = 40;
const MAX_TEXT_LEN = 2000;
const MAX_EMOJI_LEN = 32; // generous enough for multi-codepoint/ZWJ emoji
const MAX_ID_LEN = 128;

function isNonEmptyString(v, maxLen) {
  return typeof v === 'string' && v.length > 0 && v.length <= maxLen;
}

function isString(v, maxLen) {
  return typeof v === 'string' && v.length <= maxLen;
}

// Only pass through the handful of primitive fields we expect on a
// replyTo object, so a malformed/malicious payload can't inject
// arbitrary nested data into what gets broadcast and stored.
function sanitizeReplyTo(replyTo) {
  if (!replyTo || typeof replyTo !== 'object') return null;
  if (!isNonEmptyString(replyTo.id, MAX_ID_LEN)) return null;
  return {
    id: replyTo.id,
    name: isString(replyTo.name, MAX_NAME_LEN) ? replyTo.name : '',
    text: isString(replyTo.text, MAX_TEXT_LEN) ? replyTo.text : '',
    time: isString(replyTo.time, 64) ? replyTo.time : ''
  };
}

function getRoom(code) {
  if (!rooms[code]) {
    rooms[code] = {
      clients: [],
      messageSenders: {},
      // Which ws is currently "the" connection for a given clientId.
      activeByClientId: {},
      // clientIds whose departure announcement is delayed, in case they
      // reconnect (e.g. a page refresh) before the grace period ends.
      pendingLeaves: {},
      // Server-side message history so anyone who joins (even late) sees
      // what was already said — capped, oldest dropped first.
      messages: [],
      messagesById: {},
      // Web Push subscriptions, keyed by clientId.
      pushSubs: {}
    };
  }
  return rooms[code];
}

function clearPendingLeave(room, clientId) {
  const pending = room.pendingLeaves[clientId];
  if (pending) {
    clearTimeout(pending.timer);
    delete room.pendingLeaves[clientId];
  }
}

function safeSend(client, payload) {
  if (client && client.readyState === WebSocket.OPEN) {
    try {
      client.send(JSON.stringify(payload));
    } catch (e) {
      console.log('SEND ERROR:', e.message);
    }
  }
}

function broadcast(roomCode, payload, exceptClient) {
  const room = rooms[roomCode];
  if (!room) return;

  room.clients.forEach(function(client) {
    if (client !== exceptClient) {
      safeSend(client, payload);
    }
  });
}

function broadcastPresence(roomCode) {
  const room = rooms[roomCode];
  if (!room) return;

  const names = room.clients.map(function(c) { return c.userName; }).filter(Boolean);
  room.clients.forEach(function(client) {
    safeSend(client, {
      type: 'presence',
      count: room.clients.length,
      names: names
    });
  });
}

function addRoomMessage(room, messageObj) {
  room.messages.push(messageObj);
  room.messagesById[messageObj.id] = messageObj;
  while (room.messages.length > MAX_ROOM_MESSAGES) {
    const dropped = room.messages.shift();
    if (dropped) delete room.messagesById[dropped.id];
  }
}

function sendPushToRoom(roomCode, room, senderClientId, payload) {
  if (!pushEnabled) return;
  const subEntries = Object.keys(room.pushSubs);
  subEntries.forEach(function(clientId) {
    if (clientId === senderClientId) return;
    const subscription = room.pushSubs[clientId];
    webpush.sendNotification(subscription, JSON.stringify(payload)).catch(function(err) {
      // 410/404 means the subscription is gone (uninstalled, permissions
      // revoked, etc) — stop trying to push to it.
      if (err && (err.statusCode === 410 || err.statusCode === 404)) {
        delete room.pushSubs[clientId];
      } else {
        console.log('PUSH ERROR:', err && err.message);
      }
    });
  });
}

function removeFromRoom(ws, immediate) {
  if (!ws.roomCode || !rooms[ws.roomCode]) return;

  const roomCode = ws.roomCode;
  const room = rooms[roomCode];

  room.clients = room.clients.filter(c => c !== ws);
  broadcastPresence(roomCode);

  const clientId = ws.clientId;
  const userName = ws.userName;
  const isCurrentlyActive = !!(clientId && room.activeByClientId[clientId] === ws);

  if (isCurrentlyActive) {
    delete room.activeByClientId[clientId];
  }

  if (immediate) {
    // Explicit "hop off" (the leave-button click) — the ONLY event that
    // announces a departure to the room and tears down the push
    // subscription. A merely dropped connection — refresh, backgrounded
    // or closed installed app, phone restart, network blip — must never
    // look like a "hop off" to everyone else, and must keep the person
    // eligible for push notifications until they actually choose to leave.
    if (clientId) {
      clearPendingLeave(room, clientId);
      delete room.pushSubs[clientId];
    }
    if (userName) {
      broadcast(roomCode, { type: 'left', name: userName }, ws);
    }
    if (room.clients.length === 0) delete rooms[roomCode];
  } else if (clientId && isCurrentlyActive) {
    // Passive disconnect of the currently-active connection for this
    // identity. We keep a short window purely to avoid rebroadcasting a
    // duplicate "hopped in" message if they reconnect quickly (e.g. a
    // page refresh) — nothing more. No departure is ever announced here,
    // and the push subscription is left untouched, so notifications keep
    // working no matter how long they're away for.
    room.pendingLeaves[clientId] = {
      timer: setTimeout(function() {
        delete room.pendingLeaves[clientId];
        if (room.clients.length === 0 && Object.keys(room.pendingLeaves).length === 0) {
          delete rooms[roomCode];
        }
      }, RECONNECT_GRACE_MS)
    };
  } else {
    // Either no clientId to track, or a newer connection for this
    // identity already took over (this close was processed after that
    // newer join) — nothing to announce or schedule, just clean up if
    // the room is now genuinely empty.
    if (room.clients.length === 0 && Object.keys(room.pendingLeaves).length === 0) {
      delete rooms[roomCode];
    }
  }

  ws.roomCode = null;
}

wss.on('connection', function(ws, request) {

  console.log('WebSocket connected from:', request.socket.remoteAddress);

  ws.roomCode = null;
  ws.userName = null;
  ws.hasJoined = false;
  ws.isAlive = true;

  ws.on('pong', function() {
    ws.isAlive = true;
  });

  ws.on('message', function(data) {

    let msg;

    try {
      msg = JSON.parse(data.toString());
    } catch(e) {
      return;
    }

    if (!msg || typeof msg !== 'object' || typeof msg.type !== 'string') return;


    if (msg.type === 'join') {

      if (ws.hasJoined) return; // one join per connection
      if (!isNonEmptyString(msg.name, MAX_NAME_LEN)) return;
      if (!isNonEmptyString(msg.code, MAX_CODE_LEN)) return;

      ws.roomCode = msg.code;
      ws.userName = msg.name;
      ws.hasJoined = true;
      ws.clientId = isNonEmptyString(msg.clientId, MAX_ID_LEN) ? msg.clientId : null;

      const room = getRoom(ws.roomCode);

      // A reconnect is either: we were mid-way through delaying its
      // departure announcement, or its previous connection hasn't closed
      // yet (this new one arrived first) — either way, same identity.
      let isReconnect = false;
      if (ws.clientId) {
        if (room.pendingLeaves[ws.clientId]) {
          clearPendingLeave(room, ws.clientId);
          isReconnect = true;
        }
        if (room.activeByClientId[ws.clientId]) {
          isReconnect = true;
        }
        room.activeByClientId[ws.clientId] = ws;
      }

      room.clients.push(ws);

      broadcastPresence(ws.roomCode);

      // Send this client the room's existing message history so joining
      // late (or reconnecting) still shows what was already said.
      safeSend(ws, { type: 'history', messages: room.messages });

      if (!isReconnect) {
        room.clients.forEach(function(client) {
          if (client !== ws) {
            safeSend(client, {
              type:'system',
              text: msg.name + ' hopped in'
            });
          }
        });
      }

      return;
    }


    if (msg.type === 'typing') {

      if (!ws.roomCode) return;

      broadcast(ws.roomCode, {
        type:'typing',
        name: ws.userName,
        isTyping: !!msg.isTyping
      }, ws);

      return;
    }


    if (msg.type === 'message') {

      if (!ws.roomCode) return;
      if (!isNonEmptyString(msg.text, MAX_TEXT_LEN)) return;
      if (msg.id !== undefined && !isNonEmptyString(msg.id, MAX_ID_LEN)) return;

      const id = msg.id || (
        'm_' +
        Date.now() +
        '_' +
        Math.random().toString(36).slice(2)
      );

      const room = rooms[ws.roomCode];

      room.messageSenders[id] = ws;

      const time = msg.time || new Date().toISOString();
      const replyTo = sanitizeReplyTo(msg.replyTo);

      addRoomMessage(room, {
        id: id,
        name: ws.userName,
        text: msg.text,
        time: time,
        replyTo: replyTo,
        reactions: {}
      });

      broadcast(ws.roomCode, {
        type:'message',
        id:id,
        name:ws.userName,
        text:msg.text,
        time:time,
        replyTo: replyTo
      }, ws);

      sendPushToRoom(ws.roomCode, room, ws.clientId, {
        title: 'bunnychat 🐰💬',
        body: ws.userName + ': ' + msg.text,
        roomCode: ws.roomCode
      });

      return;
    }


    if (msg.type === 'reaction') {

      if (!ws.roomCode) return;
      if (!isNonEmptyString(msg.messageId, MAX_ID_LEN)) return;
      if (!isNonEmptyString(msg.emoji, MAX_EMOJI_LEN)) return;
      const action = msg.action === 'remove' ? 'remove' : 'add';

      const room = rooms[ws.roomCode];
      const targetMessage = room && room.messagesById[msg.messageId];
      if (targetMessage) {
        if (!targetMessage.reactions) targetMessage.reactions = {};
        const emoji = msg.emoji;
        const who = ws.userName;
        if (action === 'remove') {
          if (targetMessage.reactions[emoji]) {
            targetMessage.reactions[emoji] = targetMessage.reactions[emoji].filter(function(u) { return u !== who; });
            if (!targetMessage.reactions[emoji].length) delete targetMessage.reactions[emoji];
          }
        } else {
          if (!targetMessage.reactions[emoji]) targetMessage.reactions[emoji] = [];
          if (targetMessage.reactions[emoji].indexOf(who) === -1) {
            targetMessage.reactions[emoji].push(who);
          }
        }
      }

      broadcast(ws.roomCode, {
        type:'reaction',
        messageId:msg.messageId,
        emoji:msg.emoji,
        userName:ws.userName,
        action:action
      }, ws);

      return;
    }


    if (msg.type === 'read') {

      if (!ws.roomCode) return;
      if (!isNonEmptyString(msg.id, MAX_ID_LEN)) return;

      const room = rooms[ws.roomCode];

      const sender = room.messageSenders[msg.id];

      if (sender) {
        safeSend(sender,{
          type:'read',
          id:msg.id,
          by:ws.userName
        });
      }

      return;
    }


    if (msg.type === 'leave') {
      removeFromRoom(ws, true);
      return;
    }

  });


  ws.on('close', function() {
    console.log('WebSocket disconnected:', ws.userName);
    removeFromRoom(ws);
  });


  ws.on('error', function(err) {
    console.log('CLIENT ERROR:', err.message);
    removeFromRoom(ws);
  });

});

// Every 30s, ping each connection. If one didn't respond to the *previous*
// ping (isAlive still false), it's dead — terminate it so removeFromRoom
// runs and, after the grace period, peers get notified it's really gone.
// Without this, a crashed browser or dropped network (as opposed to a
// clean tab close, which sends a proper close frame) could sit forever
// without the server ever knowing the connection is gone.
const HEARTBEAT_INTERVAL_MS = 30000;
const heartbeatInterval = setInterval(function() {
  wss.clients.forEach(function(ws) {
    if (ws.isAlive === false) {
      return ws.terminate();
    }
    ws.isAlive = false;
    ws.ping();
  });
}, HEARTBEAT_INTERVAL_MS);

wss.on('close', function() {
  clearInterval(heartbeatInterval);
});


const PORT = process.env.PORT || 3000;

server.listen(PORT, function() {
  console.log('Server listening on port', PORT);
  console.log('VAPID public key:', VAPID_PUBLIC_KEY);
});
