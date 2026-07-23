
const http = require('http');
const https = require('https');
const WebSocket = require('ws');

const server = http.createServer((req, res) => {
  res.writeHead(200, { 'Content-Type': 'text/plain' });
  res.end('bunny chat server running');
});

const wss = new WebSocket.Server({ server });

const rooms = {};
const MAX_MESSAGE_SENDERS = 500;
const HEARTBEAT_INTERVAL = 30000;

var heartbeatInterval = setInterval(function() {
  wss.clients.forEach(function(ws) {
    if (ws.isAlive === false) {
      return ws.terminate();
    }
    ws.isAlive = false;
    ws.ping();
  });
}, HEARTBEAT_INTERVAL);

wss.on('close', function() {
  clearInterval(heartbeatInterval);
});

function isString(value) {
  return typeof value === 'string';
}

function isBoolean(value) {
  return typeof value === 'boolean';
}

function isValidName(name) {
  return isString(name) && name.trim().length >= 1 && name.trim().length <= 20;
}

function isValidCode(code) {
  return isString(code) && code.trim().length >= 1 && code.trim().length <= 40;
}

function isValidText(text) {
  return isString(text) && text.trim().length > 0 && text.trim().length <= 800;
}

function isValidMessageId(id) {
  return isString(id) && id.length > 0 && id.length <= 128;
}

function isValidEmoji(emoji) {
  return isString(emoji) && emoji.length > 0 && emoji.length <= 4;
}

function pruneOldMessageSenders(room) {
  var keys = Object.keys(room.messageSenders);
  if (keys.length <= MAX_MESSAGE_SENDERS) return;
  keys.sort(function(a, b) {
    return room.messageSenders[a].time - room.messageSenders[b].time;
  });
  for (var i = 0; i < keys.length - MAX_MESSAGE_SENDERS; i++) {
    delete room.messageSenders[keys[i]];
  }
}

function getRoom(code) {
  if (!rooms[code]) {
    rooms[code] = { clients: [], messageSenders: {}, cleanupTimer: null };
  }
  return rooms[code];
}

// Soft-delete: keep empty rooms alive for 30 min so rejoining users find the same room
function scheduleRoomCleanup(roomCode) {
  var room = rooms[roomCode];
  if (!room) return;
  if (room.cleanupTimer) clearTimeout(room.cleanupTimer);
  room.cleanupTimer = setTimeout(function() {
    if (rooms[roomCode] && rooms[roomCode].clients.length === 0) {
      delete rooms[roomCode];
    }
  }, 30 * 60 * 1000);
}

function safeSend(client, payload) {
  if (client && client.readyState === WebSocket.OPEN) {
    try {
      client.send(JSON.stringify(payload));
    } catch (error) {
      console.error('WebSocket send failed:', error);
      try { client.terminate(); } catch (e) {}
    }
  }
}

function broadcast(roomCode, payload, exceptClient) {
  var room = rooms[roomCode];
  if (!room) return;
  room.clients.forEach(function(client) {
    if (client !== exceptClient) {
      safeSend(client, payload);
    }
  });
}

function broadcastPresence(roomCode) {
  var room = rooms[roomCode];
  if (!room) return;
  var count = room.clients.length;
  room.clients.forEach(function(client) {
    safeSend(client, { type: 'presence', count: count });
  });
}

function removeFromRoom(ws, notifyLeft) {
  if (!ws.roomCode || !rooms[ws.roomCode]) return;

  var roomCode = ws.roomCode;
  var room = rooms[roomCode];
  room.clients = room.clients.filter(function(c) { return c !== ws; });

  if (notifyLeft && ws.userName) {
    broadcast(roomCode, { type: 'left', name: ws.userName }, ws);
  }

  if (room.clients.length === 0) {
    // Don't delete immediately — wait 30 min so rejoining with same code works
    scheduleRoomCleanup(roomCode);
  } else {
    broadcastPresence(roomCode);
  }

  ws.roomCode = null;
}

wss.on('connection', function(ws) {
  ws.roomCode = null;
  ws.userName = null;
  ws.leaveAnnounced = false;
  ws.isAlive = true;

  ws.on('pong', function() {
    ws.isAlive = true;
  });

  ws.on('message', function(data) {
    var msg;
    try { msg = JSON.parse(data.toString()); } catch(e) { return; }

    if (msg.type === 'join') {
      if (!isValidName(msg.name) || !isValidCode(msg.code)) return;
      ws.roomCode = msg.code.trim();
      ws.userName = msg.name.trim();
      ws.leaveAnnounced = false;

      var room = getRoom(ws.roomCode);

      // Cancel any pending room deletion since someone is joining
      if (room.cleanupTimer) {
        clearTimeout(room.cleanupTimer);
        room.cleanupTimer = null;
      }

      room.clients.push(ws);
      broadcastPresence(ws.roomCode);

      // Tell existing users that this person joined
      room.clients.forEach(function(client) {
        if (client !== ws) {
          safeSend(client, { type: 'system', text: ws.userName + ' hopped in' });
        }
      });

      // Tell the new joiner who is already in the room
      var existingNames = room.clients
        .filter(function(c) { return c !== ws && c.userName && c.readyState === WebSocket.OPEN; })
        .map(function(c) { return c.userName; });
      if (existingNames.length > 0) {
        safeSend(ws, { type: 'system', text: existingNames.join(' & ') + ' is already here 🐇' });
      }

      return;
    }

    if (msg.type === 'typing') {
      if (!ws.roomCode || !isBoolean(msg.isTyping)) return;
      broadcast(ws.roomCode, { type: 'typing', name: ws.userName, isTyping: !!msg.isTyping }, ws);
      return;
    }

    if (msg.type === 'message' && ws.roomCode) {
      if (!isValidMessageId(msg.id) || !isValidText(msg.text)) return;
      var room = rooms[ws.roomCode];
      if (!room) return;

      var messageId = msg.id;
      var time = isString(msg.time) ? msg.time : new Date().toISOString();
      room.messageSenders[messageId] = { sender: ws, time: Date.now() };
      pruneOldMessageSenders(room);

      var replyTo = null;
      if (msg.replyTo && isValidMessageId(msg.replyTo.id) && isString(msg.replyTo.name) && isString(msg.replyTo.text) && isString(msg.replyTo.time)) {
        replyTo = {
          id: msg.replyTo.id,
          name: msg.replyTo.name,
          text: msg.replyTo.text,
          time: msg.replyTo.time
        };
      }

      broadcast(ws.roomCode, {
        type: 'message',
        id: messageId,
        name: ws.userName,
        text: msg.text,
        time: time,
        replyTo: replyTo
      }, ws);
      return;
    }

    if (msg.type === 'reaction' && ws.roomCode) {
      if (!isValidMessageId(msg.messageId) || !isValidEmoji(msg.emoji)) return;
      var action = msg.action === 'remove' ? 'remove' : 'add';
      broadcast(ws.roomCode, {
        type: 'reaction',
        messageId: msg.messageId,
        emoji: msg.emoji,
        userName: ws.userName,
        action: action
      }, ws);
      return;
    }

    if (msg.type === 'read' && ws.roomCode && isValidMessageId(msg.id)) {
      var currentRoom = rooms[ws.roomCode];
      if (!currentRoom) return;
      var senderEntry = currentRoom.messageSenders[msg.id];
      var sender = senderEntry && senderEntry.sender;
      if (sender && sender !== ws) {
        safeSend(sender, { type: 'read', id: msg.id, by: ws.userName });
      }
      return;
    }

    if (msg.type === 'leave') {
      if (!ws.leaveAnnounced) {
        ws.leaveAnnounced = true;
        removeFromRoom(ws, true);
      }
      return;
    }
  });

  ws.on('close', function() {
    if (!ws.leaveAnnounced) {
      ws.leaveAnnounced = true;
      removeFromRoom(ws, true);
    } else {
      removeFromRoom(ws, false);
    }
  });

  ws.on('error', function() {
    if (!ws.leaveAnnounced) {
      ws.leaveAnnounced = true;
      removeFromRoom(ws, true);
    } else {
      removeFromRoom(ws, false);
    }
  });
});

const PORT = process.env.PORT || 3000;
server.listen(PORT, function() {
  console.log('Server listening on port ' + PORT);
});

var keepAliveUrl = process.env.KEEP_ALIVE_URL;
if (keepAliveUrl) {
  setInterval(function() {
    var client = keepAliveUrl.startsWith('https:') ? https : http;
    client.get(keepAliveUrl, function(res) {
      console.log('Keep-alive ping:', res.statusCode);
    }).on('error', function(e) {
      console.log('Ping error:', e.message);
    });
  }, 10 * 60 * 1000);
}
