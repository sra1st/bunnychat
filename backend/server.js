const http = require('http');
const WebSocket = require('ws');

const server = http.createServer((req, res) => {
  res.writeHead(200, { 'Content-Type': 'text/plain' });
  res.end('bunny chat server running');
});

const wss = new WebSocket.Server({
  server: server,
  clientTracking: true
});

wss.on('error', function(err) {
  console.log('WSS ERROR:', err);
});

const rooms = {};

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
    client.send(JSON.stringify(payload));
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

  ws.on('message', function(data) {

    let msg;

    try {
      msg = JSON.parse(data.toString());
    } catch(e) {
      return;
    }


    if (msg.type === 'join') {

      ws.roomCode = msg.code;
      ws.userName = msg.name;

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
        replyTo:msg.replyTo || null
      }, ws);

      return;
    }


    if (msg.type === 'reaction') {

      if (!ws.roomCode) return;

      broadcast(ws.roomCode, {
        type:'reaction',
        messageId:msg.messageId,
        emoji:msg.emoji,
        userName:ws.userName,
        action:msg.action || 'add'
      }, ws);

      return;
    }


    if (msg.type === 'read') {

      if (!ws.roomCode) return;

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