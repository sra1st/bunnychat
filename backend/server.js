const http = require('http');
const https = require('https');
const WebSocket = require('ws');

const server = http.createServer((req, res) => {
  res.writeHead(200, { 'Content-Type': 'text/plain' });
  res.end('bunny chat server running');
});

const wss = new WebSocket.Server({ server });

const rooms = {};

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
    client.send(JSON.stringify(payload));
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

  ws.on('message', function(data) {
    var msg;
    try { msg = JSON.parse(data.toString()); } catch(e) { return; }

    if (msg.type === 'join') {
      ws.roomCode = msg.code;
      ws.userName = msg.name;
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
          safeSend(client, { type: 'system', text: msg.name + ' hopped in' });
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
      if (!ws.roomCode) return;
      broadcast(ws.roomCode, { type: 'typing', name: ws.userName, isTyping: !!msg.isTyping }, ws);
      return;
    }

    if (msg.type === 'message' && ws.roomCode) {
      var room = rooms[ws.roomCode];
      if (!room) return;

      var messageId = msg.id || ('m_' + Date.now() + '_' + Math.random().toString(36).slice(2, 8));
      var time = msg.time || new Date().toISOString();
      room.messageSenders[messageId] = ws;

      broadcast(ws.roomCode, {
        type: 'message',
        id: messageId,
        name: ws.userName,
        text: msg.text,
        time: time,
        replyTo: msg.replyTo || null
      }, ws);
      return;
    }

    if (msg.type === 'reaction' && ws.roomCode) {
      broadcast(ws.roomCode, {
        type: 'reaction',
        messageId: msg.messageId,
        emoji: msg.emoji,
        userName: ws.userName,
        action: msg.action || 'add'
      }, ws);
      return;
    }

    if (msg.type === 'read' && ws.roomCode && msg.id) {
      var currentRoom = rooms[ws.roomCode];
      if (!currentRoom) return;
      var sender = currentRoom.messageSenders[msg.id];
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

// Keep-alive ping every 10 mins to prevent Render free tier spin-down
setInterval(function() {
  https.get('https://chat-backend-2ri0.onrender.com', function(res) {
    console.log('Keep-alive ping:', res.statusCode);
  }).on('error', function(e) {
    console.log('Ping error:', e.message);
  });
}, 10 * 60 * 1000);