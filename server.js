// H5 五子棋 · 实时联机服务器
// 单文件 Node 服务：托管 public/ 静态资源 + WebSocket 房间管理
// 设计要点：所有「写操作」（落子/悔棋/认输/重开）都在服务端校验，前端只读 + 同步，
//          这样既能防作弊，也避免前端直接写导致的状态不一致。

const http = require('http');
const fs = require('fs');
const path = require('path');
const crypto = require('crypto');
const { WebSocketServer } = require('ws');

const PORT = process.env.PORT || 3000;
const PUBLIC = path.join(__dirname, 'public');
const N = 15;                 // 15×15 棋盘
const ROOM_TTL = 10 * 60 * 1000; // 双方都离线后 10 分钟回收房间

const rooms = new Map();
let pidCounter = 0;

// ---------- 工具 ----------
function genRoomId() {
  const chars = 'ABCDEFGHJKLMNPQRSTUVWXYZ23456789'; // 去掉易混字符 I O 0 1
  let id;
  do {
    id = '';
    for (let i = 0; i < 4; i++) id += chars[Math.floor(Math.random() * chars.length)];
  } while (rooms.has(id));
  return id;
}
const inB = (r, c) => r >= 0 && r < N && c >= 0 && c < N;

// 五连判定：以最后一手 (r,c,p) 为中心向四个方向延展，>=5 即胜，返回连线坐标（取前 5 个）
function checkWin(board, r, c, p) {
  const dirs = [[0, 1], [1, 0], [1, 1], [1, -1]];
  for (const [dr, dc] of dirs) {
    const line = [[r, c]];
    let i = 1;
    while (inB(r + dr * i, c + dc * i) && board[(r + dr * i) * N + (c + dc * i)] === p) { line.push([r + dr * i, c + dc * i]); i++; }
    i = 1;
    while (inB(r - dr * i, c - dc * i) && board[(r - dr * i) * N + (c - dc * i)] === p) { line.unshift([r - dr * i, c - dc * i]); i++; }
    if (line.length >= 5) return line.slice(0, 5);
  }
  return null;
}

function makeRoom() {
  const id = genRoomId();
  const room = {
    id, board: new Array(N * N).fill(0), moves: [], turn: 1,
    status: 'waiting', winner: 0, winLine: [], undoReq: null,
    players: [], createdAt: Date.now()
  };
  rooms.set(id, room);
  return room;
}

// 对外暴露的房间状态（不含 ws 句柄）
function publicState(room) {
  return {
    id: room.id, board: room.board, moves: room.moves, turn: room.turn,
    status: room.status, winner: room.winner, winLine: room.winLine,
    players: room.players.map(p => ({ id: p.id, color: p.color, name: p.name, online: p.online })),
    undoPending: !!room.undoReq,
    undoFrom: room.undoReq ? room.undoReq.from : null
  };
}

function send(ws, obj) { try { if (ws.readyState === ws.OPEN) ws.send(JSON.stringify(obj)); } catch (e) {} }
function broadcast(room, obj) { room.players.forEach(p => send(p.ws, obj)); }

// ---------- 消息处理 ----------
function handle(ws, m) {
  switch (m.type) {
    case 'create': {
      const room = makeRoom();
      const p = { id: ws.pid, ws, color: 1, name: (m.name || '黑棋').slice(0, 8), online: true };
      room.players.push(p);
      ws.roomId = room.id; ws.color = 1; ws.name = p.name;
      send(ws, { type: 'created', roomId: room.id, you: 1, pid: ws.pid, state: publicState(room) });
      break;
    }
    case 'join': {
      const room = rooms.get(String(m.roomId || '').toUpperCase());
      if (!room) { send(ws, { type: 'error', msg: '房间不存在，请检查房间号' }); return; }
      // 优先回到自己离线前的座位（断线重连），否则补一个新座位
      let slot = room.players.find(p => !p.online);
      if (slot) {
        slot.ws = ws; slot.online = true; ws.color = slot.color; ws.name = slot.name;
      } else if (room.players.length < 2) {
        const color = room.players.length === 0 ? 1 : 2;
        const p = { id: ws.pid, ws, color, name: (color === 1 ? '黑棋' : '白棋'), online: true };
        room.players.push(p); ws.color = color; ws.name = p.name;
      } else {
        send(ws, { type: 'error', msg: '房间已满' }); return;
      }
      ws.roomId = room.id;
      if (room.players.length === 2 && room.status === 'waiting') room.status = 'playing';
      send(ws, { type: 'joined', you: ws.color, pid: ws.pid, state: publicState(room) });
      broadcast(room, { type: 'state', state: publicState(room) });
      break;
    }
    case 'place': {
      const room = rooms.get(ws.roomId); if (!room || room.status !== 'playing') return;
      const me = room.players.find(p => p.id === ws.pid); if (!me) return;
      if (room.turn !== me.color) return;                 // 非当前回合
      const { r, c } = m;
      if (!inB(r, c) || room.board[r * N + c] !== 0) return; // 越界或已有子
      room.board[r * N + c] = me.color;
      room.moves.push({ r, c, p: me.color });
      const line = checkWin(room.board, r, c, me.color);
      if (line) { room.status = 'over'; room.winner = me.color; room.winLine = line; }
      else { room.turn = me.color === 1 ? 2 : 1; }
      broadcast(room, { type: 'state', state: publicState(room) });
      break;
    }
    case 'undo_request': {
      const room = rooms.get(ws.roomId); if (!room || room.status !== 'playing') return;
      const me = room.players.find(p => p.id === ws.pid); if (!me) return;
      if (room.moves.length === 0) return;
      room.undoReq = { from: me.color };
      broadcast(room, { type: 'state', state: publicState(room) });
      break;
    }
    case 'undo_respond': {
      const room = rooms.get(ws.roomId); if (!room || !room.undoReq) return;
      if (m.accept) {
        const last = room.moves.pop();
        if (last) { room.board[last.r * N + last.c] = 0; room.turn = last.p; } // 退回请求方回合
      }
      room.undoReq = null;
      broadcast(room, { type: 'state', state: publicState(room) });
      break;
    }
    case 'resign': {
      const room = rooms.get(ws.roomId); if (!room || room.status !== 'playing') return;
      const me = room.players.find(p => p.id === ws.pid); if (!me) return;
      room.status = 'over'; room.winner = me.color === 1 ? 2 : 1; room.winLine = [];
      broadcast(room, { type: 'state', state: publicState(room) });
      break;
    }
    case 'restart': {
      const room = rooms.get(ws.roomId); if (!room) return;
      room.board = new Array(N * N).fill(0);
      room.moves = []; room.winLine = []; room.undoReq = null; room.winner = 0;
      room.status = 'playing';
      // 交换黑白，败方先手
      room.players.forEach(p => { p.color = p.color === 1 ? 2 : 1; p.ws.color = p.color; });
      room.turn = 1;
      broadcast(room, { type: 'state', state: publicState(room) });
      break;
    }
    case 'emoji':
    case 'chat': {
      const room = rooms.get(ws.roomId); if (!room) return;
      broadcast(room, { type: 'emoji', from: ws.color, name: ws.name, text: String(m.text || '').slice(0, 30) });
      break;
    }
    case 'name': {
      const room = rooms.get(ws.roomId); if (!room) return;
      ws.name = String(m.name || ws.name).slice(0, 8);
      const p = room.players.find(p => p.ws === ws); if (p) p.name = ws.name;
      broadcast(room, { type: 'state', state: publicState(room) });
      break;
    }
  }
}

function leaveRoom(ws) {
  const room = rooms.get(ws.roomId); if (!room) return;
  const p = room.players.find(p => p.ws === ws); if (p) p.online = false;
  if (room.players.every(pl => !pl.online)) {
    setTimeout(() => { if (room.players.every(pl => !pl.online)) rooms.delete(room.id); }, ROOM_TTL);
  } else {
    broadcast(room, { type: 'state', state: publicState(room) });
  }
}

// ---------- HTTP 静态服务 ----------
const MIME = { '.html': 'text/html; charset=utf-8', '.js': 'text/javascript; charset=utf-8', '.css': 'text/css; charset=utf-8', '.png': 'image/png', '.ico': 'image/x-icon', '.svg': 'image/svg+xml', '.json': 'application/json' };
const server = http.createServer((req, res) => {
  let urlPath = decodeURIComponent(req.url.split('?')[0]);
  if (urlPath === '/' || urlPath === '') urlPath = '/index.html';
  const filePath = path.join(PUBLIC, path.normalize(urlPath));
  if (!filePath.startsWith(PUBLIC)) { res.writeHead(403); res.end('Forbidden'); return; }
  fs.readFile(filePath, (err, data) => {
    if (err) { res.writeHead(404); res.end('Not found'); return; }
    res.writeHead(200, { 'Content-Type': MIME[path.extname(filePath).toLowerCase()] || 'application/octet-stream' });
    res.end(data);
  });
});

// ---------- WebSocket ----------
const wss = new WebSocketServer({ server });
wss.on('connection', (ws) => {
  ws.pid = 'P' + (++pidCounter) + crypto.randomBytes(2).toString('hex');
  ws.roomId = null; ws.color = 0; ws.name = '玩家';
  ws.on('message', (raw) => {
    let m; try { m = JSON.parse(raw); } catch (e) { return; }
    handle(ws, m);
  });
  ws.on('close', () => leaveRoom(ws));
  ws.on('error', () => {});
});

server.listen(PORT, () => {
  console.log('五子棋 H5 服务已启动： http://localhost:' + PORT + '  (WebSocket 同端口)');
});
