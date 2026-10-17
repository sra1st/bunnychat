# bunnychat

Real-time web chat app with private rooms, group chat, presence and push notifications.

**Live demo:** https://bunnychat.netlify.app

![Home screen](docs/Screenshot From 2026-10-17 13-54-02.png)
![Demo](ezgif-103862e847dd483c.gif)

## Features
- Join a room with your name and a secret code
- Real-time messaging over WebSockets, with group sender labels
- Swipe to reply on mobile
- Online presence and persistent room history (last 200 messages)
- Web Push notifications (service worker, VAPID)
- Input validation and payload size limits on the server
- Mobile-first UI, including an iOS keyboard fix
- [Describe the encryption accurately, or remove this line]

## Tech stack
[Frontend: HTML/CSS/JavaScript, or your actual framework] · Node.js + WebSocket server · Netlify (frontend) · Render (backend)

## Run locally
```bash
git clone https://github.com/sra1st/bunnychat.git
cd bunnychat/backend
npm install
node server.js
```
Then open `frontend/index.html` in a browser. [Confirm these steps.]

## Project structure
- `frontend/` — web client
- `backend/` — WebSocket server (`server.js`)

## Author
adi 
