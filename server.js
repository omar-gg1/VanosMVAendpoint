require('dotenv').config();
const { createClient, AgentEvents } = require("@deepgram/sdk");
const { WebSocketServer } = require("ws");
const http = require("http");

const PORT = process.env.PORT || 3001;
const MAX_CONNECTIONS_PER_IP = 3;
const RATE_LIMIT_WINDOW_MS = 60000;
const MAX_REQUESTS_PER_WINDOW = 10;

if (!process.env.DEEPGRAM_API_KEY) {
  console.error("FATAL: DEEPGRAM_API_KEY is not set. Exiting.");
  process.exit(1);
}

const deepgram = createClient(process.env.DEEPGRAM_API_KEY);
const connectionCounts = new Map();
const requestTimestamps = new Map();

// ── Rate limit cleanup ────────────────────────────────────────────────────────
setInterval(() => {
  const now = Date.now();
  for (const [ip, timestamps] of requestTimestamps.entries()) {
    const recent = timestamps.filter(t => now - t < RATE_LIMIT_WINDOW_MS);
    if (recent.length === 0) requestTimestamps.delete(ip);
    else requestTimestamps.set(ip, recent);
  }
}, 300000);

// ── HTTP server (health check only — Twilio connects over the WS upgrade) ─────
const server = http.createServer((req, res) => {
  res.writeHead(200, { "Content-Type": "text/plain" });
  res.end("ok");
});

// ── WebSocket ─────────────────────────────────────────────────────────────────
const wss = new WebSocketServer({ server, maxPayload: 1024 * 1024 });

wss.on("connection", (browserSocket, req) => {
  const clientIP = req.socket.remoteAddress;
  console.log(`[${new Date().toISOString()}] Browser connected from ${clientIP}`);

  const currentConnections = connectionCounts.get(clientIP) || 0;
  if (currentConnections >= MAX_CONNECTIONS_PER_IP) {
    browserSocket.send(JSON.stringify({ type: "error", message: "Too many connections from your IP." }));
    browserSocket.close();
    return;
  }

  const now = Date.now();
  const timestamps = requestTimestamps.get(clientIP) || [];
  const recentRequests = timestamps.filter(t => now - t < RATE_LIMIT_WINDOW_MS);
  if (recentRequests.length >= MAX_REQUESTS_PER_WINDOW) {
    browserSocket.send(JSON.stringify({ type: "error", message: "Rate limit exceeded." }));
    browserSocket.close();
    return;
  }

  recentRequests.push(now);
  requestTimestamps.set(clientIP, recentRequests);
  connectionCounts.set(clientIP, currentConnections + 1);

  let agentReady = false;
  const pendingAudio = [];
  let stripNextWavHeader = false;
  let deepgramConnection = null;
  let keepAliveInterval = null;
  let reconnectAttempts = 0;
  const MAX_RECONNECT_ATTEMPTS = 3;
  let isShuttingDown = false;
  let callEnding = false;
  let pendingFarewell = false;

  function connectToDeepgram() {
    if (isShuttingDown) return;
    try {
      deepgramConnection = deepgram.agent();

      deepgramConnection.on(AgentEvents.Open, () => {
        console.log(`[${clientIP}] Deepgram WS opened`);
        reconnectAttempts = 0;
        deepgramConnection.configure({
          audio: {
            input:  { encoding: "linear16", sample_rate: 48000 },
            output: { encoding: "linear16", sample_rate: 24000, container: "none" },
          },
          agent: {
            language: "en",
            listen: { provider: { type: "deepgram", version: "v2", model: "flux-general-en" } },
            think: {
              provider: { type: "google", model: "gemini-2.5-flash" },
              prompt: `AGENT_NAME = 'Ashley Miller'
COMPANY = 'Citizen Protection Plan'
LANGUAGE = 'English'

You are Ashley Miller, a warm and professional female intake specialist calling on behalf of Citizen Protection Plan.

This is a live outbound phone call.

You speak only English.
Your responses will be converted to speech, so use natural conversational language without formatting or special characters.
No emojis, no markdown, no symbols, no formatting.
Never say you are an AI or system.
Keep every response short, one or two sentences, and speak naturally like a real person on the phone.

The greeting has already been spoken by the system:
Hello, my name is Ashley Miller. How are you doing today?

Do not repeat the greeting.
After they respond to the greeting, introduce the purpose of the call.

Introduction:
Say that you are calling from Citizen Protection Plan to help make sure they did not miss any assistance or compensation after a car accident for their injuries, completely free of charge.

Qualifying questions:
Ask these one at a time, in order, and wait for each answer before moving on.

1. Have you had an accident in the last two years?
   If they say no, they are disqualified. End the call politely with this exact wording: I understand, thank you for your time and have a great day. Do not say anything after that.

2. Did you sustain any major or minor injury in the accident? It can be minor neck, back, or body pain.
   If they say no, they are disqualified. End the call politely with this exact wording: I understand, thank you for your time and have a great day. Do not say anything after that.

3. Do you have an attorney representing you on this matter?

Handling the attorney answer:

If they say yes:
Reassure them that is not a problem at all. Explain that you partner with a nationwide attorney network, and many clients compare to make sure they are getting the strongest support for their case. Tell them you are going to transfer them to your supervisor for a quick overview, and confirm that is okay.
Then say you are transferring them: Great, just bear with me for a moment please, here we go.

If they say no:
Say that is alright, and that you will now connect a specialist to sort this out for them.
Then say you are transferring them: Just a moment please, here we go.

Ending the call:
When the person says goodbye, asks to end the call, or says they need to go, acknowledge politely and close with have a great day, then stop. Do not continue after that.

Rules:
Stay warm, calm, and reassuring at all times.
Do not overwhelm them or stack multiple questions together.
Only follow this flow. Do not offer legal advice or make promises about outcomes or amounts.`
            },
            speak: { provider: { type: "deepgram", model: "aura-2-luna-en" } },
            greeting: "Hello, my name is Ashley Miller. How are you doing today?",
          },
        });
      });

      deepgramConnection.on(AgentEvents.Welcome, () => console.log(`[${clientIP}] Deepgram welcomed`));

      deepgramConnection.on(AgentEvents.SettingsApplied, () => {
        console.log(`[${clientIP}] Settings applied — agent live`);
        agentReady = true;
        if (browserSocket.readyState === browserSocket.OPEN) {
          browserSocket.send(JSON.stringify({ type: "ready" }));
        }
        for (const chunk of pendingAudio) deepgramConnection.send(chunk);
        pendingAudio.length = 0;
      });

      keepAliveInterval = setInterval(() => {
        if (deepgramConnection) { try { deepgramConnection.keepAlive(); } catch (_) {} }
      }, 5000);

      deepgramConnection.on(AgentEvents.AgentStartedSpeaking, () => { stripNextWavHeader = true; });

      deepgramConnection.on(AgentEvents.Audio, (data) => {
        if (browserSocket.readyState !== browserSocket.OPEN) return;
        let payload = Buffer.from(data);
        if (stripNextWavHeader && payload.length >= 44 && payload[0] === 0x52) {
          payload = payload.slice(44);
          stripNextWavHeader = false;
        }
        if (payload.length === 0) return;
        browserSocket.send(Buffer.concat([Buffer.from([0x01]), payload]));
      });

      deepgramConnection.on(AgentEvents.AgentAudioDone, () => {
        if (browserSocket.readyState === browserSocket.OPEN) {
          browserSocket.send(JSON.stringify({ type: "agent_done" }));
        }
        if (pendingFarewell && !isShuttingDown) {
          pendingFarewell = false;
          isShuttingDown = true;
          if (browserSocket.readyState === browserSocket.OPEN) {
            browserSocket.send(JSON.stringify({ type: "call_ended", message: "Call ended" }));
          }
          setTimeout(() => {
            if (deepgramConnection) { try { deepgramConnection.disconnect(); } catch (_) {} deepgramConnection = null; }
            if (browserSocket.readyState === browserSocket.OPEN) browserSocket.close();
            cleanup(true);
          }, 3000);
        }
      });

      deepgramConnection.on(AgentEvents.UserStartedSpeaking, () => {
        if (browserSocket.readyState === browserSocket.OPEN) {
          browserSocket.send(JSON.stringify({ type: "user_speaking" }));
        }
      });

      deepgramConnection.on(AgentEvents.ConversationText, (data) => {
        if (browserSocket.readyState === browserSocket.OPEN) {
          browserSocket.send(JSON.stringify({ type: "transcript", data }));
        }
        if (isShuttingDown || pendingFarewell) return;
        if (data?.role === "assistant" && data?.content) {
          const content = data.content.toLowerCase();
          const farewellPhrases = ["have a great day","goodbye","take care","talk to you soon","feel free to reach out","have a wonderful day","all the best"];
          if (farewellPhrases.find(p => content.includes(p))) { pendingFarewell = true; callEnding = true; }
        }
      });

      deepgramConnection.on("History", (data) => {
        if (browserSocket.readyState === browserSocket.OPEN) {
          browserSocket.send(JSON.stringify({ type: "transcript", data }));
        }
      });

      deepgramConnection.on(AgentEvents.Error, (err) => {
        console.error(`[${clientIP}] Deepgram error:`, err?.message);
        if (isShuttingDown || browserSocket.readyState !== browserSocket.OPEN) return;
        // Auth failures (bad/expired key) fail identically on every retry — don't burn
        // reconnect cycles on dead air, close cleanly instead.
        const msg = `${err?.message || ""} ${err?.code || ""}`.toLowerCase();
        if (msg.includes("401") || msg.includes("403") || msg.includes("unauthorized") || msg.includes("forbidden")) {
          isShuttingDown = true;
          browserSocket.send(JSON.stringify({ type: "error", message: "Voice service unavailable." }));
          if (browserSocket.readyState === browserSocket.OPEN) browserSocket.close();
          cleanup(true);
          return;
        }
        if (reconnectAttempts < MAX_RECONNECT_ATTEMPTS) {
          reconnectAttempts++;
          browserSocket.send(JSON.stringify({ type: "error", message: "Connection issue. Attempting to reconnect..." }));
          setTimeout(() => { cleanup(false); connectToDeepgram(); }, 2000 * reconnectAttempts);
        } else {
          browserSocket.send(JSON.stringify({ type: "error", message: "Unable to establish connection. Please refresh." }));
          cleanup(true);
        }
      });

      deepgramConnection.on(AgentEvents.Close, () => {
        if (isShuttingDown || browserSocket.readyState !== browserSocket.OPEN) return;
        if (reconnectAttempts < MAX_RECONNECT_ATTEMPTS) {
          reconnectAttempts++;
          setTimeout(() => { cleanup(false); connectToDeepgram(); }, 2000);
        }
      });

      deepgramConnection.on(AgentEvents.Unhandled, (data) => {
        if (data?.type === "History") {
          if (!isShuttingDown && !pendingFarewell && data?.role === "assistant" && data?.content) {
            const content = data.content.toLowerCase();
            const farewellPhrases = ["have a great day","goodbye","take care","talk to you soon","feel free to reach out","have a wonderful day","all the best"];
            if (farewellPhrases.find(p => content.includes(p))) { pendingFarewell = true; callEnding = true; }
          }
        }
      });

    } catch (err) {
      console.error(`[${clientIP}] Failed to connect to Deepgram:`, err.message);
      if (browserSocket.readyState === browserSocket.OPEN) {
        browserSocket.send(JSON.stringify({ type: "error", message: "Failed to initialize voice agent." }));
      }
      cleanup(true);
    }
  }

  browserSocket.on("message", (msg) => {
    if (isShuttingDown || callEnding) return;
    if (Buffer.isBuffer(msg) || msg instanceof ArrayBuffer) {
      const chunk = Buffer.isBuffer(msg) ? msg : Buffer.from(msg);
      if (chunk.length > 96000) return;
      if (agentReady && deepgramConnection) {
        try { deepgramConnection.send(chunk); } catch (err) { console.error(`Audio send failed:`, err.message); }
      } else {
        pendingAudio.push(chunk);
        if (pendingAudio.length > 100) pendingAudio.shift();
      }
      return;
    }
    try { const event = JSON.parse(msg.toString()); console.log(`[${clientIP}] Control:`, event); } catch (_) {}
  });

  function cleanup(decrement = true) {
    if (keepAliveInterval) { clearInterval(keepAliveInterval); keepAliveInterval = null; }
    if (deepgramConnection) { try { deepgramConnection.disconnect(); } catch (_) {} deepgramConnection = null; }
    agentReady = false;
    pendingAudio.length = 0;
    pendingFarewell = false;
    callEnding = false;
    if (decrement) {
      const count = connectionCounts.get(clientIP) || 0;
      if (count > 0) connectionCounts.set(clientIP, count - 1);
    }
  }

  browserSocket.on("close", () => { isShuttingDown = true; cleanup(true); });
  browserSocket.on("error", () => { isShuttingDown = true; cleanup(true); });

  connectToDeepgram();
});

// ── Graceful shutdown ─────────────────────────────────────────────────────────
function gracefulShutdown(signal) {
  console.log(`${signal} received, shutting down…`);
  wss.clients.forEach(client => client.close());
  server.close(() => { console.log("Server closed"); process.exit(0); });
}

process.on("SIGTERM", () => gracefulShutdown("SIGTERM"));
process.on("SIGINT",  () => gracefulShutdown("SIGINT"));

server.listen(PORT, () => {
  console.log(`Server running → http://localhost:${PORT}`);
});
