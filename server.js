require("dotenv").config();

const fs = require("fs");
const path = require("path");
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
// CallLogger — per-session file logger
// Used by startTranscribe and finalizeCall to write structured per-call logs.
// ─────────────────────────────────────────────────────────────────────────────
class CallLogger {
  constructor(sessionId) {
    this.sessionId = sessionId;
    this.logsDir = path.join(__dirname, "logs");
    this.filePath = path.join(this.logsDir, `call_${sessionId}.log`);

    if (!fs.existsSync(this.logsDir)) {
      fs.mkdirSync(this.logsDir, { recursive: true });
    }

    this.stream = fs.createWriteStream(this.filePath, { flags: "a" });
  }

  log(msg) {
    const timestamp = new Date().toISOString();
    this.stream.write(`[${timestamp}] ${msg}\n`);
  }

  error(msg, err) {
    const timestamp = new Date().toISOString();
    if (err) {
      this.stream.write(
        `[${timestamp}] ERROR: ${msg} - ${err.stack || err.message || err}\n`
      );
    } else {
      this.stream.write(`[${timestamp}] ERROR: ${msg}\n`);
    }
  }

  end() {
    this.stream.end();
  }
}

// ─────────────────────────────────────────────────────────────────────────────
// STEP 1 — code pre-filter 
// Returns true  → text has meaningful content, proceed to LLM
// Returns false → discard, store in transcript with answer=null
// ─────────────────────────────────────────────────────────────────────────────
function shouldProcessTranscription(text) {
  if (!text) return false;

  let cleanText = text.toLowerCase().trim();
  if (cleanText.length === 0) return false;

  // Remove punctuation
  cleanText = cleanText.replace(/[.,\/#!$%\^&\*;:{}=\-_`~()?]/g, "").trim();

  const unwantedPhrases = [
    "good morning", "good afternoon", "good evening",
    "thank you so much", "thank you very much", "thank you",
    "hello", "hi", "welcome", "thanks",
    "aaah", "uhhh", "ummm", "huh", "ohh", "ah", "uh", "um", "oh",
    "ನಮಸ್ಕಾರ", "ಹಲೋ", "ಶುಭೋದಯ", "ಧನ್ಯವಾದ", "ಸ್ವಾಗತ",
    "ಶುಭ ಮಧ್ಯಾಹ್ನ", "ಶುಭ ಸಂಜೆ",
    "ಆಹ್", "ಉಹ್", "ಓಹ್", "ಹೌದು", "ಹಾ", "ಹಂ", "ಹ್ಮ್",
    "ಥ್ಯಾಂಕ್ ಯೂ", "ಥ್ಯಾಂಕ್ಸ್",
  ];

  let remainingText = cleanText;
  for (const phrase of unwantedPhrases) {
    const regex = new RegExp(`\\b${phrase}\\b`, "g");
    remainingText = remainingText.replace(regex, "");
    // Simple split/join for Kannada (no word boundaries for non-ASCII)
    remainingText = remainingText.split(phrase).join("");
  }

  remainingText = remainingText.replace(/\s+/g, " ").trim();
  return remainingText.length > 0;
}

// ─────────────────────────────────────────────────────────────────────────────
// STEP 2 — Combined translate + topic-match LLM call 
//
// Returns: { translation: string, answer: string|null }
//   translation — English translation of the Kannada input
//   answer      — pre-defined topic reply if matched, or fallback string,
//                 or null if not a support query at all
// ─────────────────────────────────────────────────────────────────────────────
async function translateAndProcessQuery(text, callLogger) {
  try {
    const prompt = `
You are a customer support AI assistant. You will be provided with a Kannada transcript.
Your task is to:
1. Translate the Kannada text to English.
2. Analyze if the text contains a customer support query or question directed to the support team.
3. If it does contain a query, determine if it relates to any of these 10 predefined topics:
   - Topic 1: Ticket Status (e.g. status, ticket progress) -> Answer: "Your ticket status is currently 'In Progress'. Our engineering team is active on it."
   - Topic 2: Resolution Time (e.g. when will it be solved/fixed) -> Answer: "The estimated resolution time is within the next 2 hours."
   - Topic 3: Assigned Agent (e.g. who is handling this, who is my agent) -> Answer: "Your assigned support agent is Rithish."
   - Topic 4: Supervisor Escalation (e.g. speak to manager/supervisor) -> Answer: "Certainly. I can escalate this call to our supervisor, Priya, but please note she is currently on another call."
   - Topic 5: Check Updates (e.g. how do I check progress, where to view updates) -> Answer: "You can check updates by logging into your Exotel dashboard or via the SMS link sent to your registered mobile number."
   - Topic 6: Service Down (e.g. why is my internet/service down, outage) -> Answer: "We are experiencing a temporary fiber cut in your area, which has caused a localized outage."
   - Topic 7: Restoration Time (e.g. when will service be restored/back online) -> Answer: "The service is expected to be restored by 4:00 PM today."
   - Topic 8: Billing / Balance (e.g. bill amount, balance check, payment due) -> Answer: "Your outstanding bill amount is ₹450, due on the 15th of this month."
   - Topic 9: Refund Request (e.g. refund status, refund policy, money back) -> Answer: "Refund requests are processed within 5-7 working days after validation by our billing team."
   - Topic 10: New Ticket (e.g. raise a new ticket, file a new complaint) -> Answer: "I would be happy to raise a new ticket for you. The ticket number is #EX89230."

   Rule:
   - If it matches or is semantically close to one of the topics above, return the exact Answer specified.
   - If it is a customer support query/question but does NOT relate to any of the 10 topics above, return: "We will check into it later. The support team will contact you."
   - If it is NOT a support query/question (e.g., just conversational text, statement, greeting, filler), return null for the answer.

Respond ONLY with a JSON object containing two keys: "translation" and "answer". Do not include any formatting, markdown wrappers, backticks, or explanation.
Example Output:
{
  "translation": "Hello, when will my internet be restored?",
  "answer": "The service is expected to be restored by 4:00 PM today."
}

Kannada Text:
${text}
`;

    const command = new InvokeModelCommand({
      modelId: "google.gemma-3-4b-it",
      contentType: "application/json",
      accept: "application/json",
      body: JSON.stringify({
        max_tokens: 300,
        messages: [
          {
            role: "user",
            content: prompt, 
          },
        ],
      }),
    });

    const response = await bedrock.send(command);
    const body = JSON.parse(Buffer.from(response.body).toString());
    const raw = body.choices[0].message.content.trim();

    callLogger.log(`translateAndProcessQuery raw response: ${raw}`);

    // Strip markdown fences if model adds them
    let jsonText = raw.replace(/```json|```/g, "").trim();

    // Try direct parse first
    try {
      return JSON.parse(jsonText);
    } catch (_) {
      // Fallback: extract first { ... } block
      const match = jsonText.match(/\{[\s\S]*\}/);
      if (match) return JSON.parse(match[0]);
      throw new Error(`Cannot parse JSON from: ${raw}`);
    }
  } catch (err) {
    callLogger.error("translateAndProcessQuery failed", err);
    return null;
  }
}

// ─────────────────────────────────────────────────────────────────────────────
// startTranscribe — corrected pipeline flow
//
// FLOW PER FINAL SEGMENT:
//   1. shouldProcessTranscription(kannadaText)
//        false → store in FILE 1 with english=null, answer=null. Stop.
//        true  → proceed
//   2. translateAndProcessQuery(kannadaText)
//        returns { translation, answer }
//   3. Store in FILE 1 (transcript) always
//   4. If answer !== null → store { customer: translation, agent: answer } in FILE 2
// ─────────────────────────────────────────────────────────────────────────────
async function startTranscribe(
  audioStreamGenerator,
  sampleRateHertz,
  transcriptEntries,  // FILE 1
  agentExchanges,     // FILE 2
  sessionMeta,
  callId,
  callLogger          // per-session logger
) {
  callLogger.log(
    `Transcribe starting | lang=${sessionMeta.language} | encoding=${sessionMeta.media_type} | sampleRate=${sampleRateHertz}Hz`
  );

  const command = new StartStreamTranscriptionCommand({
    LanguageCode: sessionMeta.language,
    MediaEncoding: sessionMeta.media_type,
    MediaSampleRateHertz: sampleRateHertz,
    AudioStream: audioStreamGenerator,
  });

  const response = await transcribeClient.send(command);
  callLogger.log("Transcribe stream open");

  for await (const event of response.TranscriptResultStream) {
    if (!event.TranscriptEvent) continue;

    const results = event.TranscriptEvent.Transcript.Results;
    for (const r of results) {
      const kannadaText = r.Alternatives[0].Transcript;

      if (r.IsPartial) {
        callLogger.log(`partial transcript: ${kannadaText}`);
        continue;
      }

      console.log(`[${callId}] FINAL (KN): ${kannadaText}`);
      callLogger.log(`FINAL (KN): ${kannadaText}`);

      // ── STEP 1: Local pre-filter (free, fast) ──
      const passesLocalFilter = shouldProcessTranscription(kannadaText);

      if (!passesLocalFilter) {
        // Filler/greeting — store in transcript but skip LLM entirely
        callLogger.log(`Local filter blocked: "${kannadaText}"`);
        transcriptEntries.push({
          timestamp: new Date().toISOString(),
          language: sessionMeta.language,
          kannada: kannadaText,
          english: null,
          answer: null,
          filtered_by: "local",
          media_type: sessionMeta.media_type,
          sample_rate_hertz: sampleRateHertz,
        });
        continue;
      }

      // ── STEP 2: Combined translate + topic-match LLM call ──
      callLogger.log(`sending to translateAndProcessQuery: ${kannadaText}`);
      const result = await translateAndProcessQuery(kannadaText, callLogger);

      if (!result) {
        // LLM call failed — still store raw in transcript
        callLogger.log(`translateAndProcessQuery returned null for: "${kannadaText}"`);
        transcriptEntries.push({
          timestamp: new Date().toISOString(),
          language: sessionMeta.language,
          kannada: kannadaText,
          english: null,
          answer: null,
          filtered_by: "llm_error",
          media_type: sessionMeta.media_type,
          sample_rate_hertz: sampleRateHertz,
        });
        continue;
      }

      const { translation, answer } = result;
      console.log(`[${callId}] FINAL (EN): ${translation}`);
      console.log(`[${callId}] 🤖 Agent answer: ${answer}`);
      callLogger.log(`FINAL (EN): ${translation}`);
      callLogger.log(`Agent answer: ${answer}`);

      // ── STEP 3: Always store in FILE 1 (full transcript) ──
      transcriptEntries.push({
        timestamp: new Date().toISOString(),
        language: sessionMeta.language,
        kannada: kannadaText,
        english: translation,
        answer: answer,
        media_type: sessionMeta.media_type,
        sample_rate_hertz: sampleRateHertz,
      });

      // ── STEP 4: If answer exists → store in FILE 2 (agent exchanges) ──
      if (answer !== null && answer !== undefined) {
        agentExchanges.push({
          timestamp: new Date().toISOString(),
          customer: translation,
          agent: answer,
        });
        callLogger.log(`Exchange stored — customer: "${translation}" | agent: "${answer}"`);
      } else {
        callLogger.log(`No agent answer (not a support query) — not stored in FILE 2`);
      }
    }
  }

  callLogger.log("Transcribe stream closed");
}

// ─────────────────────────────────────────────────────────────────────────────
// finalizeCall — guaranteed teardown order
//   1. Flush audio file
//   2. Await Transcribe + all LLM calls
//   3. Write FILE 1: transcript-<callId>.json
//   4. Write FILE 2: conversation-<callId>.json
//   5. Close call logger
// ─────────────────────────────────────────────────────────────────────────────
async function finalizeCall({
  callId,
  audioFile,
  transcribePromise,
  transcriptEntries,
  agentExchanges,
  transcriptPath,
  conversationPath,
  callLogger,
  isPersisted,
  markPersisted,
}) {
  if (isPersisted()) return;
  markPersisted();

  callLogger.log("Finalizing call...");

  // Step 1 — flush audio file
  await new Promise((resolve) => {
    if (audioFile.writableEnded) resolve();
    else audioFile.end(resolve);
  });
  callLogger.log("Audio file flushed");

  // Step 2 — wait for Transcribe + all LLM calls
  if (transcribePromise) {
    try {
      await transcribePromise;
    } catch (err) {
      callLogger.error("Transcribe error during finalize", err);
      console.error(`[${callId}] ⚠️  Transcribe error: ${err.message || err}`);
    }
  } else {
    callLogger.log("WARNING: No Transcribe session was started");
    console.warn(`[${callId}] ⚠️  No Transcribe session started`);
  }

  // Step 3 — FILE 1: full transcript
  try {
    fs.writeFileSync(transcriptPath, JSON.stringify(transcriptEntries, null, 2));
    callLogger.log(`FILE 1 saved | ${transcriptEntries.length} entries → ${transcriptPath}`);
  } catch (err) {
    callLogger.error("FILE 1 write failed", err);
    console.error(`[${callId}] ❌ FILE 1 write failed:`, err);
  }

  // Step 4 — FILE 2: agent exchanges
  try {
    fs.writeFileSync(conversationPath, JSON.stringify(agentExchanges, null, 2));
    callLogger.log(`FILE 2 saved | ${agentExchanges.length} exchanges → ${conversationPath}`);
  } catch (err) {
    callLogger.error("FILE 2 write failed", err);
    console.error(`[${callId}] ❌ FILE 2 write failed:`, err);
  }

  // Step 5 — close logger
  callLogger.log("Call finalized.");
  callLogger.end();
}

// ─────────────────────────────────────────────────────────────────────────────
// WebSocket Server — each connection fully isolated
// ─────────────────────────────────────────────────────────────────────────────
const wss = new WebSocket.Server({ port: PORT });

wss.on("connection", (ws) => {

  const callId           = `${Date.now()}-${Math.floor(Math.random() * 10000)}`;
  const audioPath        = `${CALLS_DIR}/audio-${callId}.raw`;
  const transcriptPath   = `${CALLS_DIR}/transcript-${callId}.json`;
  const conversationPath = `${CALLS_DIR}/conversation-${callId}.json`;

  // Per-session logger — writes to logs/call_<callId>.log
  const callLogger = new CallLogger(callId);

  callLogger.log("New connection");

  const audioFile         = fs.createWriteStream(audioPath);
  const transcriptEntries = [];
  const agentExchanges    = [];

  const callMeta = {
    language:          "kn-IN",
    media_type:        "pcm",
    sample_rate_hertz: null,
  };

  const audioQueue      = [];
  let streamEnded       = false;
  let transcribeStarted = false;
  let transcribePromise = null;

  let _persisted      = false;
  const isPersisted   = () => _persisted;
  const markPersisted = () => { _persisted = true; };

  async function* audioStream() {
    while (true) {
      if (audioQueue.length > 0) {
        yield { AudioEvent: { AudioChunk: audioQueue.shift() } };
      } else if (streamEnded) {
        callLogger.log("Audio generator exhausted");
        return;
      } else {
        await new Promise((r) => setTimeout(r, 50));
      }
    }
  }

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
      callLogger,
      isPersisted,
      markPersisted,
    }).catch((err) => {
      callLogger.error("finalizeCall threw", err);
      console.error(`[${callId}] ❌ finalizeCall threw:`, err);
    });
  }

  ws.on("message", (msg) => {
    try {
      const rawText = msg.toString();
      const data = JSON.parse(rawText);
      if (data.event === "media") {
        callLogger.log('incoming message: {"event":"media"}');
      } else {
        callLogger.log(`incoming message: ${rawText}`);
      }

      switch (data.event) {
        case "connected":
          callLogger.log("received connected event");
          break;

        case "start": {
          callLogger.log(`received start event: ${JSON.stringify(data)}`);
          const mediaFormat     = data.start && data.start.media_format;
          const rawRate         = mediaFormat && (mediaFormat.sampleRate || mediaFormat.sample_rate);
          const sampleRateHertz = rawRate ? Number(rawRate) : null;

          if (!sampleRateHertz || isNaN(sampleRateHertz)) {
            callLogger.log("sampleRate missing — defaulting to 8000Hz");
            callMeta.sample_rate_hertz = 8000;
          } else {
            callMeta.sample_rate_hertz = sampleRateHertz;
          }

          callLogger.log(`sampleRateHertz locked: ${callMeta.sample_rate_hertz}Hz`);

          if (!transcribeStarted) {
            transcribeStarted = true;
            const frozenMeta = Object.freeze({ ...callMeta });
            const lockedRate = callMeta.sample_rate_hertz;

            transcribePromise = startTranscribe(
              audioStream(),
              lockedRate,
              transcriptEntries,
              agentExchanges,
              frozenMeta,
              callId,
              callLogger
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
          callLogger.log("received stop event");
          triggerFinalize();
          break;

        default:
          callLogger.log(`received unknown event: ${data.event}`);
      }
    } catch (err) {
      callLogger.error("Error parsing message", err);
      console.error(`[${callId}] Error parsing message:`, err);
    }
  });

  ws.on("close", () => {
    callLogger.log("WebSocket closed");
    triggerFinalize();
  });

  ws.on("error", (err) => {
    callLogger.error("WebSocket error", err);
    console.error(`[${callId}] WebSocket error:`, err);
    triggerFinalize();
  });
});

console.log(`🚀 WebSocket server running on ws://localhost:${PORT}`);