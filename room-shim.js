/* جایگزین قابلیت room کلود: همان API، روی WebSocket سرور خودمان */
(function () {
  "use strict";
  if (window.claude && window.claude.use) return;
  if (!/^https?:$/.test(location.protocol)) { window.claude = { use: async function () { return null; } }; return; }

  var ws = null, open = false, myId = null, backoff = 500, rooms = {}, lobby = null;

  function send(o) { if (ws && open && ws.readyState === 1) { ws.send(JSON.stringify(o)); return true; } return false; }

  function connect() {
    ws = new WebSocket((location.protocol === "https:" ? "wss://" : "ws://") + location.host + "/ws");
    ws.onopen = function () {
      open = true; backoff = 500;
      for (var n in rooms) { send({ t: "join", room: n }); rooms[n]._resend(); }
    };
    ws.onmessage = function (e) {
      var m; try { m = JSON.parse(e.data); } catch (_) { return; }
      if (m.t === "hello") { myId = m.id; for (var n in rooms) rooms[n]._changed(); return; }
      var r = rooms[m.room]; if (!r) return;
      if (m.t === "peers") r._others(m.list); else if (m.t === "msg") r._msg(m);
    };
    ws.onclose = function () {
      open = false; ws = null;
      for (var n in rooms) rooms[n]._others([]);
      setTimeout(connect, backoff); backoff = Math.min(backoff * 2, 8000);
    };
    ws.onerror = function () { try { ws.close(); } catch (_) {} };
  }

  function bytes(x) { return new Blob([JSON.stringify(x === undefined ? null : x)]).size; }

  function makeRoom(key) {
    var mine = {}, others = [], cache = {}, prev = {}, snap = Object.freeze([]),
        handlers = {}, subs = [], timer = 0, patch = null, pTimer = 0, since = Date.now();

    function build() {
      var list = [{ peer: myId || "self", presence: mine, self: true }].concat(others.filter(function (p) { return p.peer !== myId; }));
      var next = {}, out = [];
      list.forEach(function (p) {
        var j = JSON.stringify(p.presence), c = cache[p.peer];
        if (!c || c.j !== j || c.self !== !!p.self) {
          c = cache[p.peer] = {
            j: j, self: !!p.self,
            o: Object.freeze({ peer: p.peer, by: null, isMe: !!p.self, sameTab: !!p.self, kind: "viewer", guest: false,
                               presence: Object.freeze(JSON.parse(j)), updatedAt: Date.now() })
          };
        }
        next[p.peer] = c.o; out.push(c.o);
      });
      for (var id in cache) if (!next[id]) delete cache[id];
      var joined = [], left = [], updated = [];
      out.forEach(function (o) { if (!prev[o.peer]) joined.push(o); else if (prev[o.peer] !== o) updated.push(o); });
      for (var k in prev) if (!next[k]) left.push(prev[k]);
      prev = next; snap = Object.freeze(out);
      return { peers: snap, joined: joined, left: left, updated: updated };
    }
    function changed() {
      if (timer) return;
      timer = setTimeout(function () {
        timer = 0; var ch = build();
        if (!ch.joined.length && !ch.left.length && !ch.updated.length) return;
        subs.slice().forEach(function (f) { try { f(ch); } catch (e) { console.error(e); } });
      }, 16);
    }
    build();

    var room = {
      emit: function (topic, data) {
        if (typeof topic !== "string" || !/^[a-z][a-z0-9_.-]{0,47}$/.test(topic) || bytes(data) > 4096) return Promise.reject({ code: "invalid_argument" });
        send({ t: "emit", room: key, topic: topic, data: data });
        return Promise.resolve();
      },
      on: function (topic, fn) {
        if (typeof fn !== "function") throw new TypeError("handler");
        (handlers[topic] = handlers[topic] || []).push(fn);
        return function () { var a = handlers[topic] || [], i = a.indexOf(fn); if (i >= 0) a.splice(i, 1); };
      },
      presence: function (p) {
        var next = Object.assign({}, mine);
        Object.keys(p || {}).forEach(function (k) { if (p[k] === null) delete next[k]; else next[k] = p[k]; });
        if (bytes(next) > 4096) return Promise.reject({ code: "invalid_argument" });
        mine = next; patch = Object.assign(patch || {}, p); changed();
        if (!pTimer) pTimer = setTimeout(function () { pTimer = 0; var x = patch; patch = null; if (x) send({ t: "pres", room: key, patch: x }); }, 40);
        return Promise.resolve();
      },
      peers: function () { return snap; },
      onPeers: function (fn) {
        if (typeof fn !== "function") throw new TypeError("handler");
        subs.push(fn);
        Promise.resolve().then(function () { if (subs.indexOf(fn) >= 0) { var c = build(); fn({ peers: snap, joined: snap, left: [], updated: [] }); } });
        return function () { var i = subs.indexOf(fn); if (i >= 0) subs.splice(i, 1); };
      },
      connected: function () { return open; },
      leave: function () { send({ t: "leave", room: key }); delete rooms[key]; subs = []; handlers = {}; return Promise.resolve(); }
    };
    room._others = function (list) { others = list || []; changed(); };
    room._changed = changed;
    room._resend = function () { if (Object.keys(mine).length) send({ t: "pres", room: key, patch: mine }); };
    room._msg = function (m) {
      (handlers[m.topic] || []).slice().forEach(function (f) {
        try {
          f({ topic: m.topic, data: m.data, peer: m.peer, by: null, isMe: m.peer === myId, sameTab: m.peer === myId, kind: "viewer", guest: false });
        } catch (e) { console.error(e); }
      });
    };
    rooms[key] = room;
    send({ t: "join", room: key });
    return room;
  }

  window.claude = {
    use: async function (name) {
      if (name !== "room") return null;
      if (!ws) connect();
      if (!lobby) {
        lobby = makeRoom("lobby");
        lobby.join = async function (code) {
          if (!/^[a-z0-9]{1,40}$/.test(String(code))) throw new Error("bad room");
          return makeRoom("p:" + code);
        };
      }
      return lobby;
    }
  };
})();
