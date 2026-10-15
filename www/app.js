/* ============================================================
   epoll-httpd · 运行状态面板
   ------------------------------------------------------------
   轮询 /api/status 并渲染首页顶部的实时数字。
   两点与服务器协作的设计：
     1. 每次请求带 cache: 'no-store'，确保拿到的是实时值
        （服务器侧对应的响应头是 Cache-Control: no-cache）
     2. 页面不可见时暂停轮询 —— 后台标签页不应该持续给
        服务器制造无效请求
   ============================================================ */

(function () {
  'use strict';

  var POLL_MS = 2000;
  var els = {
    uptime:   document.getElementById('s-uptime'),
    total:    document.getElementById('s-total'),
    hits:     document.getElementById('s-hits'),
    misses:   document.getElementById('s-misses'),
    rate:     document.getElementById('s-rate'),
    inflight: document.getElementById('s-inflight'),
    tag:      document.getElementById('live-tag'),
    text:     document.getElementById('live-text'),
    updated:  document.getElementById('live-updated'),
    refresh:  document.getElementById('live-refresh')
  };

  if (!els.uptime) return;

  var timer = null;
  var last = {};

  function fmt(n) {
    return Number(n).toLocaleString('en-US');
  }

  function fmtUptime(sec) {
    sec = Math.floor(Number(sec) || 0);
    if (sec < 60) return sec + 's';
    var d = Math.floor(sec / 86400);
    var h = Math.floor((sec % 86400) / 3600);
    var m = Math.floor((sec % 3600) / 60);
    var s = sec % 60;
    if (d > 0) return d + 'd ' + h + 'h';
    if (h > 0) return h + 'h ' + String(m).padStart(2, '0') + 'm';
    return m + 'm ' + String(s).padStart(2, '0') + 's';
  }

  function set(node, value, key) {
    if (!node) return;
    if (node.textContent !== value) {
      node.textContent = value;
      if (last[key] !== undefined) {
        node.classList.remove('flash');
        void node.offsetWidth;
        node.classList.add('flash');
      }
    }
    last[key] = value;
  }

  function online(ok, msg) {
    if (!els.tag) return;
    els.tag.classList.toggle('off', !ok);
    if (els.text) els.text.textContent = ok ? 'LIVE' : 'OFFLINE';
    if (els.updated && msg) els.updated.textContent = msg;
  }

  function render(d) {
    set(els.uptime, fmtUptime(d.uptime_seconds), 'uptime');
    set(els.total,  fmt(d.total_requests),       'total');
    set(els.hits,   fmt(d.cache_hits),           'hits');
    set(els.misses, fmt(d.cache_misses),         'misses');

    var lookups = Number(d.cache_hits || 0) + Number(d.cache_misses || 0);
    set(els.rate, lookups ? (d.cache_hits / lookups * 100).toFixed(1) + '%' : '—', 'rate');

    if (els.inflight) els.inflight.textContent = '处理中 ' + (d.in_flight || 0);

    online(true, '更新于 ' + new Date().toLocaleTimeString('zh-CN', { hour12: false })
                  + ' · 每 2 秒自动刷新');
  }

  function poll() {
    fetch('/api/status', { cache: 'no-store' })
      .then(function (res) {
        if (!res.ok) throw new Error('HTTP ' + res.status);
        return res.json();
      })
      .then(render)
      .catch(function (err) {
        online(false, '无法连接 /api/status：' + err.message
                      + ' · 点右侧按钮重试');
      });
  }

  function start() {
    stop();
    poll();
    timer = setInterval(poll, POLL_MS);
  }

  function stop() {
    if (timer !== null) {
      clearInterval(timer);
      timer = null;
    }
  }

  if (els.refresh) {
    els.refresh.addEventListener('click', poll);
  }

  document.addEventListener('visibilitychange', function () {
    if (document.hidden) {
      stop();
      online(true, '页面不可见，已暂停轮询');
    } else {
      start();
    }
  });

  start();
})();
