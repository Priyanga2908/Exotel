require("dotenv").config();

const fs = require("fs");
const WebSocket = require("ws");
const {
  TranscribeStreamingClient,
  StartStreamTranscriptionCommand,
} = require("@aws-sdk/client-transcribe-streaming");
const {
  BedrockRuntimeClient,
  InvokeModelCommand,
} = require("@aws-sdk/client-bedrock-runtime");

const PORT = 8080;
const CALLS_DIR = "calls";

// ── Shared AWS SDK clients (stateless — safe to reuse across connections) ──
const transcribeClient = new TranscribeStreamingClient({
  region: process.env.AWS_REGION,
});
const bedrock = new BedrockRuntimeClient({
  region: process.env.AWS_REGION,
});

if (!fs.existsSync(CALLS_DIR)) {
  fs.mkdirSync(CALLS_DIR, { recursive: true });
}

process.on("unhandledRejection", (reason) => {
  console.error("UNHANDLED REJECTION:");
  console.dir(reason, { depth: null });
});

// ─────────────────────────────────────────────────────────────────────────────
// Bedrock: Translate Kannada → English
// ─────────────────────────────────────────────────────────────────────────────
async function translateKannadaToEnglish(text) {
  try {
    const command = new InvokeModelCommand({
      modelId: "anthropic.claude-3-haiku-20240307-v1:0",
      contentType: "application/json",
      accept: "application/json",
      body: JSON.stringify({
        anthropic_version: "bedrock-2023-05-31",
        max_tokens: 200,
        messages: [
          {
            role: "user",
            content: `Translate the following Kannada text into natural English.\nTreat names as names and preserve context.\nReturn only the English translation.\n\nKannada:\n${text}`,
          },
        ],
      }),
    });
    const response = await bedrock.send(command);
    const body = JSON.parse(Buffer.from(response.body).toString());
    return body.content[0].text;
  } catch (err) {
    console.error("Translation Error:", err);
    return null;
  }
}

// ─────────────────────────────────────────────────────────────────────────────
// Bedrock: Check if a transcript segment is contextually meaningful
// and worth sending to the support agent LLM.
//
// Returns: { relevant: true/false, reason: string }
//
// Filters OUT:
//   - Greetings / sign-offs ("hello", "hi", "bye", "thank you")
//   - Filler sounds ("hmm", "ahh", "ok", "yeah", "ಹೂಂ", "ಆಯ್ತು")
//   - Small talk with no actionable content ("how are you", "which place are you from")
//   - Very short fragments under 4 words with no meaning
//
// Passes THROUGH:
//   - Issue reports ("my internet is down", "I have a problem with...")
//   - Ticket queries ("what is the status of my ticket", "when will it be resolved")
//   - Service requests ("I need help with...", "can you check...")
//   - Any sentence with clear intent or a question about a service/product
// ─────────────────────────────────────────────────────────────────────────────
async function isContextuallyRelevant(englishText) {
  try {
    const prompt = `You are a filter for a customer support call transcript.

Your job: decide if the customer's statement is RELEVANT to send to a support agent.

RELEVANT means:
- Reporting an issue or problem ("my internet is down", "the app is not working")
- Asking about a ticket status or resolution ("when will my ticket be resolved", "what is the update on case 123")
- Requesting a service or action ("can you reset my password", "I need to update my address")
- Any question or statement that requires a support agent to take action or provide information

NOT RELEVANT means:
- Greetings and sign-offs ("hello", "hi", "good morning", "bye", "thank you", "ok")
- Filler words and sounds ("hmm", "ahh", "yeah", "ok", "ಹೂಂ", "ಆಯ್ತು")
- Small talk with no service intent ("how are you", "which place are you from", "nice")
- Incomplete fragments that carry no meaning by themselves

Customer said: "${englishText}"

Reply with ONLY a valid JSON object on a single line, no explanation, no markdown:
{"relevant": true, "reason": "customer is reporting an issue"}
or
{"relevant": false, "reason": "greeting only"}`;

    const command = new InvokeModelCommand({
      modelId: "anthropic.claude-3-haiku-20240307-v1:0",
      contentType: "application/json",
      accept: "application/json",
      body: JSON.stringify({
        anthropic_version: "bedrock-2023-05-31",
        max_tokens: 100,
        messages: [{ role: "user", content: prompt }],
      }),
    });

    const response = await bedrock.send(command);
    const body = JSON.parse(Buffer.from(response.body).toString());
    const raw = body.content[0].text.trim();

    // Strip markdown fences if the model adds them despite instructions
    const clean = raw.replace(/```json|```/g, "").trim();
    const parsed = JSON.parse(clean);
    return parsed;
  } catch (err) {
    console.error("Relevance filter error:", err);
    // On filter failure: default to relevant so nothing is silently dropped
    return { relevant: true, reason: "filter error — defaulting to relevant" };
  }
}

// ─────────────────────────────────────────────────────────────────────────────
// Bedrock: Support agent LLM — responds live to a customer statement
//
// Role: a helpful support agent for a service company.
// It receives only contextually relevant customer statements.
// It replies as the agent would — acknowledging the issue, checking status,
// or asking for the ticket/account number if needed.
//
// conversationHistory: array of { role: "user"|"assistant", content: string }
//   — passed in full each call so the LLM has context of the ongoing call
// ─────────────────────────────────────────────────────────────────────────────
async function getAgentReply(customerStatement, conversationHistory) {
  try {
    const systemPrompt = `You are a professional and empathetic customer support agent.

Your responsibilities:
- Help customers who report issues, ask about ticket status, or need service assistance
- Acknowledge the customer's concern clearly and directly
- If the customer mentions a ticket number or case ID, refer to it by number
- If you need a ticket number or account detail to help, politely ask for it
- Give concise, helpful replies — 1 to 3 sentences maximum
- Do not use filler phrases like "Great!" or "Absolutely!" — be direct and warm
- Never make up ticket statuses or resolution times you don't know — say you will check
- Respond in English only`;

    // Build message history: previous exchanges + current customer message
    const messages = [
      ...conversationHistory,
      { role: "user", content: customerStatement },
    ];

    const command = new InvokeModelCommand({
      modelId: "anthropic.claude-3-haiku-20240307-v1:0",
      contentType: "application/json",
      accept: "application/json",
      body: JSON.stringify({
        anthropic_version: "bedrock-2023-05-31",
        max_tokens: 300,
        system: systemPrompt,
        messages,
      }),
    });

    const response = await bedrock.send(command);
    const body = JSON.parse(Buffer.from(response.body).toString());
    return body.content[0].text.trim();
  } catch (err) {
    console.error("Agent LLM error:", err);
    return null;
  }
}

// ─────────────────────────────────────────────────────────────────────────────
// Transcribe: stream audio → push final entries with English translation
// Also triggers the live agent pipeline for relevant segments.
//
// Per-connection isolation guaranteed:
//   - audioStreamGenerator  → own generator per connection
//   - sampleRateHertz       → locked number from this connection's start event
//   - transcriptEntries     → own array per connection (full transcript)
//   - agentExchanges        → own array per connection (filtered agent convo)
//   - sessionMeta           → frozen snapshot, not a live reference
// ─────────────────────────────────────────────────────────────────────────────
async function startTranscribe(
  audioStreamGenerator,
  sampleRateHertz,
  transcriptEntries,   // FILE 1: full transcript — every final segment
  agentExchanges,      // FILE 2: filtered agent conversation — relevant only
  sessionMeta,
  callId
) {
  console.log(
    `[${callId}] 🎛️  Transcribe starting | lang=${sessionMeta.language} | encoding=${sessionMeta.media_type} | sampleRate=${sampleRateHertz}Hz`
  );

  const command = new StartStreamTranscriptionCommand({
    LanguageCode: sessionMeta.language,
    MediaEncoding: sessionMeta.media_type,
    MediaSampleRateHertz: sampleRateHertz,
    AudioStream: audioStreamGenerator,
  });

  const response = await transcribeClient.send(command);
  console.log(`[${callId}] 🎙️  Transcribe stream open`);

  // Conversation history for the agent LLM — grows during the call
  // Keeps { role, content } pairs so the LLM understands the conversation so far
  const llmConversationHistory = [];

  for await (const event of response.TranscriptResultStream) {
    if (!event.TranscriptEvent) continue;

    const results = event.TranscriptEvent.Transcript.Results;
    for (const r of results) {
      const text = r.Alternatives[0].Transcript;

      if (r.IsPartial) {
        console.log(`[${callId}] PARTIAL: ${text}`);
        continue;
      }

      // ── Final transcript segment ──
      console.log(`[${callId}] FINAL (KN): ${text}`);

      // Step 1: Translate to English
      const english = await translateKannadaToEnglish(text);
      console.log(`[${callId}] FINAL (EN): ${english}`);

      // Step 2: Store in full transcript (FILE 1) — always, no filtering
      transcriptEntries.push({
        timestamp: new Date().toISOString(),
        language: sessionMeta.language,
        kannada: text,
        english: english,
        media_type: sessionMeta.media_type,
        sample_rate_hertz: sampleRateHertz,
      });

      // Step 3: Relevance filter — should this go to the agent LLM?
      if (!english) {
        console.log(`[${callId}] Skipping agent pipeline — translation failed`);
        continue;
      }

      const filter = await isContextuallyRelevant(english);
      console.log(
        `[${callId}] Relevance filter → relevant=${filter.relevant} | reason="${filter.reason}"`
      );

      if (!filter.relevant) {
        console.log(`[${callId}] Skipping agent LLM — not contextually relevant`);
        continue;
      }

      // Step 4: Send to agent LLM with full conversation history (FILE 2)
      console.log(`[${callId}] 🤖 Sending to agent LLM: "${english}"`);
      const agentReply = await getAgentReply(english, llmConversationHistory);
      console.log(`[${callId}] 🤖 Agent reply: "${agentReply}"`);

      // Step 5: Update LLM conversation history for next turn
      llmConversationHistory.push({ role: "user",      content: english });
      llmConversationHistory.push({ role: "assistant", content: agentReply || "" });

      // Step 6: Store exchange in agent conversation file (FILE 2)
      agentExchanges.push({
        timestamp: new Date().toISOString(),
        customer: english,
        agent: agentReply,
      });
    }
  }

  console.log(`[${callId}] ✅ Transcribe stream closed`);
}

// ─────────────────────────────────────────────────────────────────────────────
// Finalize: guaranteed teardown order
//   1. Flush audio file to disk
//   2. Await Transcribe + translations + agent replies
//   3. Write FILE 1: transcript-<callId>.json     (full transcript)
//   4. Write FILE 2: conversation-<callId>.json   (agent exchanges only)
//
// Guard: runs exactly ONCE per connection
// ─────────────────────────────────────────────────────────────────────────────
async function finalizeCall({
  callId,
  audioFile,
  transcribePromise,
  transcriptEntries,
  agentExchanges,
  transcriptPath,
  conversationPath,
  isPersisted,
  markPersisted,
}) {
  if (isPersisted()) return;
  markPersisted();

  console.log(`[${callId}] 🔄 Finalizing...`);

  // Step 1 — flush audio file
  await new Promise((resolve) => {
    if (audioFile.writableEnded) {
      resolve();
    } else {
      audioFile.end(resolve);
    }
  });
  console.log(`[${callId}] 🔇 Audio file flushed`);

  // Step 2 — wait for Transcribe + all LLM calls to finish
  if (transcribePromise) {
    try {
      await transcribePromise;
    } catch (err) {
      console.error(
        `[${callId}] ⚠️  Transcribe error — saving partial data. Error: ${err.message || err}`
      );
    }
  } else {
    console.warn(`[${callId}] ⚠️  No Transcribe session (start event never received?)`);
  }

  // Step 3 — write FILE 1: full transcript
  try {
    fs.writeFileSync(transcriptPath, JSON.stringify(transcriptEntries, null, 2));
    console.log(
      `[${callId}] 💾 FILE 1 saved | ${transcriptEntries.length} entries → ${transcriptPath}`
    );
  } catch (err) {
    console.error(`[${callId}] ❌ Failed to write transcript:`, err);
  }

  // Step 4 — write FILE 2: agent conversation exchanges
  try {
    fs.writeFileSync(conversationPath, JSON.stringify(agentExchanges, null, 2));
    console.log(
      `[${callId}] 💾 FILE 2 saved | ${agentExchanges.length} exchanges → ${conversationPath}`
    );
  } catch (err) {
    console.error(`[${callId}] ❌ Failed to write conversation:`, err);
  }
}

// ─────────────────────────────────────────────────────────────────────────────
// WebSocket Server
// Each connection is fully isolated — no shared mutable state between sessions.
// ─────────────────────────────────────────────────────────────────────────────
const wss = new WebSocket.Server({ port: PORT });

wss.on("connection", (ws) => {

  // ── Per-connection identity ──
  const callId           = `${Date.now()}-${Math.floor(Math.random() * 10000)}`;
  const audioPath        = `${CALLS_DIR}/audio-${callId}.raw`;
  const transcriptPath   = `${CALLS_DIR}/transcript-${callId}.json`;     // FILE 1
  const conversationPath = `${CALLS_DIR}/conversation-${callId}.json`;   // FILE 2

  console.log(`[${callId}] 📞 New connection`);

  // ── Per-connection I/O ──
  const audioFile        = fs.createWriteStream(audioPath);
  const transcriptEntries = []; // FILE 1 — every final transcript segment
  const agentExchanges    = []; // FILE 2 — filtered customer + agent reply pairs

  // ── Per-connection state ──
  const callMeta = {
    language:          "kn-IN",
    media_type:        "pcm",
    sample_rate_hertz: null, // set from start event
  };

  const audioQueue      = [];
  let streamEnded       = false;
  let transcribeStarted = false;
  let transcribePromise = null;

  let _persisted       = false;
  const isPersisted    = () => _persisted;
  const markPersisted  = () => { _persisted = true; };

  // ── Audio generator — yields only from THIS connection's audioQueue ──
  async function* audioStream() {
    while (true) {
      if (audioQueue.length > 0) {
        yield { AudioEvent: { AudioChunk: audioQueue.shift() } };
      } else if (streamEnded) {
        console.log(`[${callId}] Audio generator done`);
        return;
      } else {
        await new Promise((r) => setTimeout(r, 50));
      }
    }
  }

  // ── Shared teardown ──
  function triggerFinalize() {
    streamEnded = true;
    finalizeCall({
      callId,
      audioFile,
      transcribePromise,
      transcriptEntries,
      agentExchanges,
      transcriptPath,
      conversationPath,
      isPersisted,
      markPersisted,
    }).catch((err) => {
      console.error(`[${callId}] ❌ finalizeCall threw:`, err);
    });
  }

  // ── Message handler ──
  ws.on("message", (msg) => {
    try {
      const data = JSON.parse(msg.toString());

      switch (data.event) {

        case "connected":
          console.log(`[${callId}] connected`);
          break;

        case "start": {
          console.log(`[${callId}] start event:`);
          console.log(JSON.stringify(data, null, 2));

          const mediaFormat = data.start && data.start.media_format;
          const rawRate     = mediaFormat && (mediaFormat.sampleRate || mediaFormat.sample_rate);
          const sampleRateHertz = rawRate ? Number(rawRate) : null;

          if (!sampleRateHertz || isNaN(sampleRateHertz)) {
            console.warn(`[${callId}] ⚠️  sampleRate missing — defaulting to 8000Hz`);
            callMeta.sample_rate_hertz = 8000;
          } else {
            callMeta.sample_rate_hertz = sampleRateHertz;
          }

          console.log(`[${callId}] 🔍 sampleRateHertz locked in: ${callMeta.sample_rate_hertz}Hz`);

          if (!transcribeStarted) {
            transcribeStarted = true;

            const frozenMeta = Object.freeze({ ...callMeta });
            const lockedRate = callMeta.sample_rate_hertz;

            transcribePromise = startTranscribe(
              audioStream(),
              lockedRate,
              transcriptEntries,   // FILE 1 array
              agentExchanges,      // FILE 2 array
              frozenMeta,
              callId
            );

            transcribePromise.catch(() => {});
          }
          break;
        }

        case "media": {
          const audioBuffer = Buffer.from(data.media.payload, "base64");
          audioFile.write(audioBuffer);
          audioQueue.push(audioBuffer);
          break;
        }

        case "stop":
          console.log(`[${callId}] stop event`);
          triggerFinalize();
          break;

        default:
          console.log(`[${callId}] unknown event: ${data.event}`);
      }
    } catch (err) {
      console.error(`[${callId}] Error parsing message:`, err);
    }
  });

  ws.on("close", () => {
    console.log(`[${callId}] WebSocket closed`);
    triggerFinalize();
  });

  ws.on("error", (err) => {
    console.error(`[${callId}] WebSocket error:`, err);
    triggerFinalize();
  });
});

console.log(`🚀 WebSocket server running on ws://localhost:${PORT}`);