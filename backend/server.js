const http = require('http');
const WebSocket = require('ws');

const server = http.createServer((req, res) => {
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
      messageSenders: {}
    };
  }
  return rooms[code];
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

  room.clients.forEach(function(client) {
    safeSend(client, {
      type: 'presence',
      count: room.clients.length
    });
  });
}

function removeFromRoom(ws) {
  if (!ws.roomCode || !rooms[ws.roomCode]) return;

  const roomCode = ws.roomCode;
  const room = rooms[roomCode];

  room.clients = room.clients.filter(c => c !== ws);

  if (ws.userName) {
    broadcast(roomCode, {
      type: 'left',
      name: ws.userName
    }, ws);
  }

  broadcastPresence(roomCode);

  if (room.clients.length === 0) {
    delete rooms[roomCode];
  }

  ws.roomCode = null;
}

wss.on('connection', function(ws, request) {

  console.log('WebSocket connected from:', request.socket.remoteAddress);

  ws.roomCode = null;
  ws.userName = null;
  ws.hasJoined = false;

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

      const room = getRoom(ws.roomCode);

      room.clients.push(ws);

      broadcastPresence(ws.roomCode);

      room.clients.forEach(function(client) {
        if (client !== ws) {
          safeSend(client, {
            type:'system',
            text: msg.name + ' hopped in'
          });
        }
      });

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

      broadcast(ws.roomCode, {
        type:'message',
        id:id,
        name:ws.userName,
        text:msg.text,
        time:msg.time || new Date().toISOString(),
        replyTo: sanitizeReplyTo(msg.replyTo)
      }, ws);

      return;
    }


    if (msg.type === 'reaction') {

      if (!ws.roomCode) return;
      if (!isNonEmptyString(msg.messageId, MAX_ID_LEN)) return;
      if (!isNonEmptyString(msg.emoji, MAX_EMOJI_LEN)) return;
      const action = msg.action === 'remove' ? 'remove' : 'add';

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
      removeFromRoom(ws);
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


const PORT = process.env.PORT || 3000;

server.listen(PORT, function() {
  console.log('Server listening on port', PORT);
});
