// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
//
// The headless stream check: the viewer cores of addons/selkies-web-core in
// Chrome against a Selkies started by serve.sh, over the WebSocket sink and
// over WebRTC, each once with a view-only grant and once with a full one. A
// local relay stands in for the edge and adds X-BP-Scopes to every request.
//
//   PORT=<published 3000> SERVER_LOG=<out>/server.log TRANSFER_KEY=<the container's
//   SELKIES_TRANSFER_KEY> node bp/stream-check/check.mjs
import { createHmac } from "node:crypto";
import { createServer } from "node:http";
import { readFile } from "node:fs/promises";
import { fileURLToPath } from "node:url";
import assert from "node:assert/strict";
import { chromium } from "playwright-core";
import { WebSocket, WebSocketServer } from "ws";

const port = process.env.PORT;
const serverLog = process.env.SERVER_LOG;
assert(/^\d{2,5}$/.test(port ?? ""), "PORT is the host port of the container's 3000");
assert(serverLog, "SERVER_LOG is the path of the container's server.log");
const transferKey = process.env.TRANSFER_KEY;
assert(transferKey, "TRANSFER_KEY is the container's SELKIES_TRANSFER_KEY");
const cores = fileURLToPath(new URL("../../addons/selkies-web-core", import.meta.url));
const ORIGIN = "http://localhost:53920";
const VIEW = "stream";
const FULL = "stream,input,clipboard,files";
const FRAMES = 20;

/** A read token of the file plane (session_files.py, sft1), valid for a minute. */
function transferToken() {
  const expires = String(Date.now() + 60000);
  const signature = createHmac("sha256", transferKey).update(`selkies-transfer\0v1\0r\0${expires}`).digest("base64url");
  return `sft1.r.${expires}.${signature}`;
}

const page = (mode) => `<html><body style="margin:0"><div id="app" style="width:100vw;height:100vh"></div>
<script>window.SELKIES_BASE_URL=location.origin;window.SELKIES_FORCE_TURN=false;
window.SELKIES_RTC_CONFIG={iceServers:[],iceTransportPolicy:'all'};</script>
<script type="module">import {updateRuntimeConfig} from './lib/runtime-config.js';
updateRuntimeConfig({enhancedResources:true});
const {default:core}=await import('./selkies-${mode}-core.js');const instance=core();
${mode === "wr" ? "instance.initialize();" : ""}</script></body></html>`;

/** A relay for one grant: serves the cores and forwards /api with the scopes header. */
async function relay(mode, scopes) {
  const headers = { "X-BP-Scopes": scopes, Origin: ORIGIN };
  const server = createServer(async (req, res) => {
    const path = new URL(req.url, "http://relay").pathname;
    if (path.startsWith("/api/")) {
      const token = req.headers["x-transfer-token"];
      const upstream = await fetch(`http://127.0.0.1:${port}${req.url}`, {
        headers: token ? { ...headers, "X-Transfer-Token": token } : headers,
      });
      res.writeHead(upstream.status, { "Content-Type": upstream.headers.get("content-type") || "application/json" });
      return res.end(Buffer.from(await upstream.arrayBuffer()));
    }
    if (path === "/") {
      res.setHeader("Content-Type", "text/html");
      return res.end(page(mode));
    }
    try {
      res.setHeader("Content-Type", path.endsWith(".js") ? "text/javascript" : "application/octet-stream");
      res.end(await readFile(cores + path));
    } catch {
      res.writeHead(404);
      res.end();
    }
  });
  const sockets = new WebSocketServer({ noServer: true });
  server.on("upgrade", (req, socket, head) => sockets.handleUpgrade(req, socket, head, (client) => {
    const upstream = new WebSocket(`ws://127.0.0.1:${port}${req.url}`, { headers });
    const queue = [];
    client.on("message", (data, binary) => upstream.readyState === WebSocket.OPEN ? upstream.send(data, { binary }) : queue.push([data, binary]));
    upstream.on("open", () => { for (const [data, binary] of queue) upstream.send(data, { binary }); });
    upstream.on("message", (data, binary) => client.readyState === WebSocket.OPEN && client.send(data, { binary }));
    upstream.on("close", () => client.close());
    upstream.on("error", () => client.close());
    client.on("close", () => upstream.close());
  }));
  await new Promise((resolve) => server.listen(0, "127.0.0.1", resolve));
  return { url: `http://127.0.0.1:${server.address().port}`, close: () => { for (const c of sockets.clients) c.terminate(); sockets.close(); server.close(); } };
}

const logText = async () => readFile(serverLog, "utf8");
const count = (text, pattern) => (text.match(pattern) || []).length;
const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

const browser = await chromium.launch({
  executablePath: process.env.CHROME_BIN || "/usr/bin/google-chrome",
  args: ["--no-sandbox", "--no-proxy-server", "--autoplay-policy=no-user-gesture-required"],
});
const results = [];
// Patch 0035: the congestion loop logs the encoder's own rate beside the goodput.
const ENCODER = /Congestion control\[primary\]: .*encoder [0-9.]+ kbps\)/;
let encoderLine;
try {
  for (const mode of ["ws", "wr"]) {
    for (const scopes of [VIEW, FULL]) {
      const proxy = await relay(mode, scopes);
      const context = await browser.newContext({ viewport: { width: 1280, height: 720 } });
      const tab = await context.newPage();
      const errors = [];
      tab.on("pageerror", (e) => errors.push(e.message));
      // The transport the core sends on, so the check can send raw protocol messages, as a
      // modified client would: the socket core's page handle (its socket lives in a worker), the
      // WebRTC core's data channel.
      await tab.addInitScript(() => {
        const send = RTCDataChannel.prototype.send;
        RTCDataChannel.prototype.send = function (data) {
          window.bpChannel = this;
          return send.call(this, data);
        };
      });
      try {
        await tab.goto(proxy.url);
        await tab.waitForFunction(() => document.querySelector("video")?.videoWidth > 0, null, { timeout: 30000 });
        const frames = await tab.evaluate((wanted) => new Promise((resolve) => {
          const video = document.querySelector("video");
          let seen = 0;
          const tick = () => { if (++seen >= wanted) resolve(seen); else video.requestVideoFrameCallback(tick); };
          video.requestVideoFrameCallback(tick);
          setTimeout(() => resolve(seen), 10000);
        }), FRAMES);
        const before = await logText();
        const transport = mode === "ws" ? "selkiesTransport" : "bpChannel";
        await tab.waitForFunction((name) => [1, "open"].includes(window[name]?.readyState), transport, { timeout: 10000 });
        await tab.evaluate((name) => { for (const msg of ["kd,65", "ku,65", "cw,YnA="]) window[name].send(msg); }, transport);
        const listing = await fetch(`${proxy.url}/api/files`, { headers: { "X-Transfer-Token": transferToken() } });
        const files = { status: listing.status, body: await listing.text() };
        await sleep(1500);
        if (mode === "wr" && scopes === FULL) {
          const deadline = Date.now() + 60000;
          while (!(encoderLine = (await logText()).match(ENCODER)?.[0]) && Date.now() < deadline) await sleep(1000);
        }
        const after = (await logText()).slice(before.length);
        const result = {
          mode, scopes, frames,
          inputDropped: count(after, /Dropping kd message without the input scope/g),
          clipboardDropped: count(after, /Dropping cw message without the clipboard scope/g),
          files: files.status,
          filesScopeRefusal: files.status === 403 && files.body.includes('"scope"'),
          errors,
        };
        results.push(result);
        console.log("RESULT", JSON.stringify(result));
      } finally {
        await context.close();
        proxy.close();
      }
    }
  }
  console.log("ENCODER", encoderLine ?? "missing");

  for (const r of results) {
    const label = `${r.mode} ${r.scopes}`;
    assert(r.frames >= FRAMES, `${label}: ${r.frames} frames decoded, want ${FRAMES}`);
    assert.deepEqual(r.errors, [], `${label}: page errors`);
    if (r.scopes === VIEW) {
      assert(r.inputDropped >= 1, `${label}: keyboard input was not dropped`);
      assert(r.clipboardDropped >= 1, `${label}: clipboard write was not dropped`);
      assert(r.filesScopeRefusal, `${label}: /api/files was not refused for the files scope (${r.files})`);
    } else {
      assert.equal(r.inputDropped, 0, `${label}: keyboard input was dropped`);
      assert.equal(r.clipboardDropped, 0, `${label}: clipboard write was dropped`);
      assert.equal(r.files, 200, `${label}: /api/files with a transfer token answered ${r.files}`);
    }
  }
  assert(encoderLine, "no congestion control line with the encoder rate (patch 0035)");
  console.log("PASS stream check: ws and wr decode, view-only input, clipboard and files refused, full grant passes");
} finally {
  await browser.close();
}
