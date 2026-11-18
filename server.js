// سرور بدون وابستگی: فایل‌های استاتیک + WebSocket برای اتاق زنده
"use strict";
const http = require("http");
const fs = require("fs");
const path = require("path");
const crypto = require("crypto");

const PORT = process.env.PORT || 3000;
const PUB = __dirname;
const FILES = {
  "/": ["index.html", "text/html; charset=utf-8"],
  "/index.html": ["index.html", "text/html; charset=utf-8"],
  "/room-shim.js": ["room-shim.js", "application/javascript; charset=utf-8"],
};
const MAX_LOBBY = 150, MAX_PRIVATE = 8, MAX_ROOMS_PER_CLIENT = 4, MAX_BYTES = 4096;

const server = http.createServer((req, res) => {
  const url = (req.url || "/").split("?")[0];
  if (url === "/healthz") { res.writeHead(200); return res.end("ok"); }
  const f = FILES[url];
  if (!f) { res.writeHead(404); return res.end("not found"); }
  fs.readFile(path.join(PUB, f[0]), (err, buf) => {
    if (err) { res.writeHead(500); return res.end("error"); }
    res.writeHead(200, { "Content-Type": f[1], "Cache-Control": "no-cache" });
    res.end(buf);
  });
});

/* ---------- WebSocket (RFC 6455، حداقل لازم) ---------- */
const GUID = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11";
function frame(op, payload) {
  const len = payload.length;
  let head;
  if (len < 126) head = Buffer.from([0x80 | op, len]);
  else if (len < 65536) { head = Buffer.alloc(4); head[0] = 0x80 | op; head[1] = 126; head.writeUInt16BE(len, 2); }
  else { head = Buffer.alloc(10); head[0] = 0x80 | op; head[1] = 127; head.writeBigUInt64BE(BigInt(len), 2); }
  return Buffer.concat([head, payload]);
}

const rooms = new Map();   // name -> Set<Client>
const dirty = new Set();   // rooms whose peer list must be re-sent
let nextId = 1;

class Client {
  constructor(sock) {
    this.sock = sock;
    this.id = "p" + (nextId++).toString(36) + crypto.randomBytes(3).toString("hex");
    this.rooms = new Set();
    this.pres = new Map();
    this.buf = Buffer.alloc(0);
    this.frag = null;
    this.alive = true;
    this.tokens = 80; this.stamp = Date.now();
    this.closed = false;
  }
  allow() {
    const now = Date.now();
    this.tokens = Math.min(80, this.tokens + (now - this.stamp) * 0.04);
    this.stamp = now;
    if (this.tokens < 1) return false;
    this.tokens -= 1; return true;
  }
  sendJSON(o) { this.sendRaw(JSON.stringify(o)); }
  sendRaw(str) { if (!this.closed && this.sock.writable) this.sock.write(frame(1, Buffer.from(str))); }
  close() {
    if (this.closed) return;
    this.closed = true;
    for (const r of this.rooms) leaveRoom(this, r);
    try { this.sock.destroy(); } catch (_) {}
  }
}

function validRoom(n) { return n === "lobby" || /^p:[a-z0-9]{1,40}$/.test(n); }
function joinRoom(c, name) {
  if (!validRoom(name) || c.rooms.has(name) || c.rooms.size >= MAX_ROOMS_PER_CLIENT) return;
  let set = rooms.get(name);
  if (!set) rooms.set(name, (set = new Set()));
  if (set.size >= (name === "lobby" ? MAX_LOBBY : MAX_PRIVATE)) { c.sendJSON({ t: "full", room: name }); return; }
  set.add(c); c.rooms.add(name); c.pres.set(name, {});
  dirty.add(name);
}
function leaveRoom(c, name) {
  const set = rooms.get(name);
  if (!set) return;
  set.delete(c); c.rooms.delete(name); c.pres.delete(name);
  if (set.size === 0) { rooms.delete(name); dirty.delete(name); } else dirty.add(name);
}
function mergePresence(cur, patch) {
  const next = Object.assign({}, cur);
  for (const k of Object.keys(patch).slice(0, 64)) {
    if (patch[k] === null) delete next[k]; else next[k] = patch[k];
  }
  return Buffer.byteLength(JSON.stringify(next)) <= MAX_BYTES ? next : cur;
}

function onMessage(c, text) {
  let m;
  try { m = JSON.parse(text); } catch (_) { return; }
  if (!m || typeof m !== "object") return;
  const room = m.room;
  if (m.t === "join") return joinRoom(c, room);
  if (m.t === "leave") return leaveRoom(c, room);
  if (!c.rooms.has(room)) return;
  if (!c.allow()) return;
  if (m.t === "pres" && m.patch && typeof m.patch === "object" && !Array.isArray(m.patch)) {
    c.pres.set(room, mergePresence(c.pres.get(room), m.patch));
    dirty.add(room);
  } else if (m.t === "emit" && typeof m.topic === "string" && /^[a-z][a-z0-9_.-]{0,47}$/.test(m.topic)) {
    if (Buffer.byteLength(JSON.stringify(m.data === undefined ? null : m.data)) > MAX_BYTES) return;
    const out = JSON.stringify({ t: "msg", room, topic: m.topic, data: m.data, peer: c.id });
    for (const o of rooms.get(room)) o.sendRaw(out);
  }
}

setInterval(() => {
  for (const name of dirty) {
    const set = rooms.get(name);
    if (!set) continue;
    const list = [];
    for (const c of set) list.push({ peer: c.id, presence: c.pres.get(name) || {} });
    const out = JSON.stringify({ t: "peers", room: name, list });
    for (const c of set) c.sendRaw(out);
  }
  dirty.clear();
}, 100);

function parse(c) {
  for (;;) {
    const b = c.buf;
    if (b.length < 2) return;
    const fin = (b[0] & 0x80) !== 0, op = b[0] & 0x0f, masked = (b[1] & 0x80) !== 0;
    let len = b[1] & 0x7f, off = 2;
    if (len === 126) { if (b.length < 4) return; len = b.readUInt16BE(2); off = 4; }
    else if (len === 127) { if (b.length < 10) return; len = Number(b.readBigUInt64BE(2)); off = 10; }
    if (len > 16384 || !masked) return c.close();
    if (b.length < off + 4 + len) return;
    const mask = b.subarray(off, off + 4);
    const data = Buffer.from(b.subarray(off + 4, off + 4 + len));
    for (let i = 0; i < data.length; i++) data[i] ^= mask[i & 3];
    c.buf = b.subarray(off + 4 + len);
    if (op === 8) { try { c.sock.write(frame(8, Buffer.alloc(0))); } catch (_) {} return c.close(); }
    if (op === 9) { c.sock.write(frame(10, data)); continue; }
    if (op === 10) { c.alive = true; continue; }
    if (op === 1 || op === 2) c.frag = { op, parts: [data] };
    else if (op === 0 && c.frag) c.frag.parts.push(data);
    else continue;
    if (fin) {
      const whole = Buffer.concat(c.frag.parts); c.frag = null;
      if (whole.length <= MAX_BYTES * 2) onMessage(c, whole.toString("utf8"));
    }
  }
}

server.on("upgrade", (req, sock) => {
  if ((req.url || "").split("?")[0] !== "/ws" || (req.headers.upgrade || "").toLowerCase() !== "websocket" || !req.headers["sec-websocket-key"]) {
    sock.destroy(); return;
  }
  const accept = crypto.createHash("sha1").update(req.headers["sec-websocket-key"] + GUID).digest("base64");
  sock.write("HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: " + accept + "\r\n\r\n");
  sock.setNoDelay(true);
  const c = new Client(sock);
  c.sendJSON({ t: "hello", id: c.id });
  sock.on("data", (d) => { c.buf = Buffer.concat([c.buf, d]); parse(c); });
  sock.on("close", () => c.close());
  sock.on("error", () => c.close());
  c.hb = true;
  allClients.add(c);
});

const allClients = new Set();
setInterval(() => {
  for (const c of allClients) {
    if (c.closed) { allClients.delete(c); continue; }
    if (!c.alive) { c.close(); allClients.delete(c); continue; }
    c.alive = false;
    try { c.sock.write(frame(9, Buffer.alloc(0))); } catch (_) {}
  }
}, 25000);

server.listen(PORT, () => console.log("listening on " + PORT));
