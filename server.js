// require("dotenv").config();
// console.log("__dirname =", __dirname);
// console.log("cwd =", process.cwd());
// const fs = require("fs");
// const WebSocket = require("ws");
// const {
//   TranscribeStreamingClient,
//   StartStreamTranscriptionCommand,
// } = require("@aws-sdk/client-transcribe-streaming");

// //bedrcok
// const {
//   BedrockRuntimeClient,
//   InvokeModelCommand,
// } = require("@aws-sdk/client-bedrock-runtime");

// const PORT = 8080;

// const client = new TranscribeStreamingClient({
//   region: process.env.AWS_REGION,
// });

// //bedrock
// const bedrock = new BedrockRuntimeClient({
//   region: process.env.AWS_REGION,
// });

// process.on("unhandledRejection", (reason) => {
//   console.error("UNHANDLED REJECTION:");
//   console.dir(reason, { depth: null });
// });

// async function translateKannadaToEnglish(text) {

//   try {

//     const prompt = `
// Translate the following Kannada text into natural English.
// Treat names as names and preserve context.
// Return only the English translation.

// Kannada:
// ${text}
// `;

//     const command = new InvokeModelCommand({
//       modelId: "anthropic.claude-3-haiku-20240307-v1:0",
//       contentType: "application/json",
//       accept: "application/json",
//       body: JSON.stringify({
//         anthropic_version: "bedrock-2023-05-31",
//         max_tokens: 200,
//         messages: [
//           {
//             role: "user",
//             content: prompt,
//           },
//         ],
//       }),
//     });

//     const response = await bedrock.send(command);

//     const responseBody = JSON.parse(
//       Buffer.from(response.body).toString()
//     );

//     return responseBody.content[0].text;

//   } catch (err) {

//     console.error("Translation Error");
//     console.error(err);

//     return null;
//   }
// }

// async function startTranscribe(audioStreamGenerator) {
//   try {
//     const command = new StartStreamTranscriptionCommand({
//     LanguageCode: "kn-IN",
//       MediaEncoding: "pcm",
//       MediaSampleRateHertz: 16000,
//       AudioStream: audioStreamGenerator,
//     });

//     const response = await client.send(command);


//     console.log("🎙️ Transcribe started...");

//     for await (const event of response.TranscriptResultStream) {
//       if (event.TranscriptEvent) {
//         const results = event.TranscriptEvent.Transcript.Results;

//         for (const r of results) {
//           const text = r.Alternatives[0].Transcript;

//           if (r.IsPartial)
//             console.log("PARTIAL:", text);
            
//           else
//           {
          
//              console.log("FINAL (KN):", text);

//              const english = await translateKannadaToEnglish(text);

//              console.log("ENGLISH:", english);} }
        
//       }
//     }

//     console.log("Transcribe stream closed");
//   } catch (err) {
//     console.error("TRANSCRIBE ERROR:");
//     console.dir(err, { depth: null });
//   }
// }

// const wss = new WebSocket.Server({ port: PORT });
// const audioFile = fs.createWriteStream("audio.raw");
// wss.on("connection", (ws) => {

//   console.log("📞 Call connected");

//   let audioQueue = [];
//   let streamEnded = false;

//   async function* audioStream() {
//     while (true) {

//       if (audioQueue.length > 0) {
//         const chunk = audioQueue.shift();
//         yield {
//           AudioEvent: {
//             AudioChunk: chunk,
//           },
//         };

//       } else if (streamEnded) {
//         console.log("Audio stream ended");
//         return;
//       } else {
//         await new Promise((r) => setTimeout(r, 50));

//       }
//     }
//   }

//   startTranscribe(audioStream());
//   ws.on("message", (msg) => {

//     try {

//       const data = JSON.parse(msg.toString());
//       console.log("Received event:", data.event);//fr testing
//       //console.log(JSON.stringify(data, null, 2));
//        if (data.event === "start") {
//          console.log(JSON.stringify(data, null, 2));
//        }

//       switch (data.event) {
//         case "connected":
//           console.log("Connected event received");
//           break;
//         case "start":
//           console.log("Start event received");
//           break;
//         case "media":

//           const audioBuffer = Buffer.from(
//             data.media.payload,
//             "base64"
//           );
//            audioFile.write(audioBuffer);
//           audioQueue.push(audioBuffer);

//           break;

//         case "stop":
//           console.log("Stop event received");
//           streamEnded = true;
//            audioFile.end();
//           break;
//         default:

//           console.log("Unknown event:", data.event);
//       }

//     } catch (err) {

//       console.error("Error parsing message:");
//       console.error(err);

//     }
//   });

//   ws.on("close", () => {

//     console.log(" WebSocket closed");
//     streamEnded = true;

//   });

//   ws.on("error", (err) => {

//     console.error("WebSocket error:");
//     console.error(err);
//     streamEnded = true;

//   });

// });

// console.log(` Server running on ws://localhost:${PORT}`);

require("dotenv").config();

const fs = require("fs");
const path = require("path");
const WebSocket = require("ws");

class CallLogger {
  constructor(sessionId) {
    this.sessionId = sessionId;
    this.logsDir = path.join(__dirname, "logs");
    this.filePath = path.join(this.logsDir, `call_${sessionId}.log`);
    
    // Ensure logs directory exists
    if (!fs.existsSync(this.logsDir)) {
      fs.mkdirSync(this.logsDir, { recursive: true });
    }
    
    this.stream = fs.createWriteStream(this.filePath, { flags: 'a' });
  }

  log(msg) {
    const timestamp = new Date().toISOString();
    this.stream.write(`[${timestamp}] ${msg}\n`);
  }

  error(msg, err) {
    const timestamp = new Date().toISOString();
    if (err) {
      this.stream.write(`[${timestamp}] ERROR: ${msg} - ${err.stack || err.message || err}\n`);
    } else {
      this.stream.write(`[${timestamp}] ERROR: ${msg}\n`);
    }
  }

  end() {
    this.stream.end();
  }
}
const {
  TranscribeStreamingClient,
  StartStreamTranscriptionCommand,
} = require("@aws-sdk/client-transcribe-streaming");

//bedrcok
const {
  BedrockRuntimeClient,
  InvokeModelCommand,
} = require("@aws-sdk/client-bedrock-runtime");

const PORT = 8080;

const client = new TranscribeStreamingClient({
  region: process.env.AWS_REGION,
});

//bedrock
const bedrock = new BedrockRuntimeClient({
  region: process.env.AWS_REGION,
});

process.on("unhandledRejection", (reason) => {
  console.error("UNHANDLED REJECTION:");
  console.dir(reason, { depth: null });
});

function shouldProcessTranscription(text) {
  if (!text) return false;
  
  // Normalize: lower case and trim
  let cleanText = text.toLowerCase().trim();
  if (cleanText.length === 0) return false;

  // Remove punctuation
  cleanText = cleanText.replace(/[.,\/#!$%\^&\*;:{}=\-_`~()?]/g, "").trim();

  // List of filler words/noises/greetings to filter out
  const unwantedPhrases = [
    "good morning", "good afternoon", "good evening", "thank you so much", "thank you very much", "thank you",
    "hello", "hi", "welcome", "thanks", "aaah", "uhhh", "ummm", "huh", "ohh", "ah", "uh", "um", "oh",
    "ನಮಸ್ಕಾರ", "ಹಲೋ", "ಶುಭೋದಯ", "ಧನ್ಯವಾದ", "ಸ್ವಾಗತ", "ಶುಭ ಮಧ್ಯಾಹ್ನ", "ಶುಭ ಸಂಜೆ",
    "ಆಹ್", "ಉಹ್", "ಓಹ್", "ಹೌದು", "ಹಾ", "ಹಂ", "ಹ್ಮ್", "ಥ್ಯಾಂಕ್ ಯೂ", "ಥ್ಯಾಂಕ್ಸ್"
  ];

  // Remove all unwanted phrases from the clean text
  let remainingText = cleanText;
  for (const phrase of unwantedPhrases) {
    // Replace with word boundary for English words to avoid partial matching
    const regex = new RegExp(`\\b${phrase}\\b`, 'g');
    remainingText = remainingText.replace(regex, "");
    
    // Also do a simple string split/join for non-ASCII Kannada characters since \b doesn't apply to them
    remainingText = remainingText.split(phrase).join("");
  }

  // Clean remaining text (replace multiple spaces with a single space)
  remainingText = remainingText.replace(/\s+/g, " ").trim();

  // If there is no meaningful content left, ignore it
  if (remainingText.length === 0) {
    return false;
  }

  return true;
}

async function translateAndProcessQuery(text) {
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
      modelId: process.env.MODEL_ID || "us.anthropic.claude-haiku-4-5-20251001-v1:0",
      contentType: "application/json",
      accept: "application/json",
      body: JSON.stringify({
        anthropic_version: "bedrock-2023-05-31",
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
    const responseBody = JSON.parse(
      Buffer.from(response.body).toString()
    );

    const contentText = responseBody.content[0].text.trim();
    // Strip markdown code block formatting if present
    let jsonText = contentText;
    if (jsonText.startsWith("```")) {
      jsonText = jsonText.replace(/^```(?:json)?\n?/, "").replace(/\n?```$/, "").trim();
    }

    try {
      return JSON.parse(jsonText);
    } catch (parseErr) {
      const jsonMatch = jsonText.match(/\{[\s\S]*\}/);
      if (jsonMatch) {
        try {
          return JSON.parse(jsonMatch[0]);
        } catch (regexErr) {
          console.error("Failed to parse JSON from Bedrock response:", contentText);
          throw regexErr;
        }
      }
      console.error("Failed to parse JSON from Bedrock response:", contentText);
      throw parseErr;
    }

  } catch (err) {
    console.error("Translation and Query Processing Error");
    console.error(err);
    return null;
  }
}

async function startTranscribe(audioStreamGenerator, sampleRateHertz, logger) {
  try {
    if (logger) logger.log(`🎛️  Starting Transcribe with MediaSampleRateHertz=${sampleRateHertz}`);

    const command = new StartStreamTranscriptionCommand({
      LanguageCode: "kn-IN",
      MediaEncoding: "pcm",
      MediaSampleRateHertz: sampleRateHertz,
      AudioStream: audioStreamGenerator,
    });

    const response = await client.send(command);

    if (logger) logger.log("🎙️ Transcribe started...");

    for await (const event of response.TranscriptResultStream) {
      if (event.TranscriptEvent) {
        const results = event.TranscriptEvent.Transcript.Results;

        for (const r of results) {
          const text = r.Alternatives[0].Transcript;

          if (r.IsPartial) {
            if (logger) logger.log(`PARTIAL: ${text}`);
          } else {
            if (logger) logger.log(`FINAL (KN): ${text}`);

            if (!shouldProcessTranscription(text)) {
              if (logger) logger.log("FILTERED (IGNORED GREETING/FILLER/SILENCE)");
            } else {
              if (logger) logger.log(`PROCESSING THROUGH CLAUDE: ${text}`);
              const result = await translateAndProcessQuery(text);
              if (result) {
                if (logger) logger.log(`ENGLISH TRANSLATION: ${result.translation}`);
                if (result.answer) {
                  if (logger) logger.log(`SUPPORT ANSWER: ${result.answer}`);
                } else {
                  if (logger) logger.log("SUPPORT ANSWER: (Not a support query)");
                }

                // Clean console output for the support team
                console.log("\n==================================================");
                console.log(`FINAL (KN):          ${text}`);
                console.log(`ENGLISH TRANSLATION: ${result.translation}`);
                if (result.answer) {
                  console.log(`SUPPORT ANSWER:      ${result.answer}`);
                } else {
                  console.log(`SUPPORT ANSWER:      (Not a support query)`);
                }
                console.log("==================================================\n");
              }
            }
          }
        }
        
      }
    }

    if (logger) logger.log("Transcribe stream closed");
  } catch (err) {
    if (logger) {
      logger.error("TRANSCRIBE ERROR", err);
    } else {
      console.error("TRANSCRIBE ERROR:");
      console.dir(err, { depth: null });
    }
  } finally {
    if (logger) logger.end();
  }
}

const wss = new WebSocket.Server({ port: PORT });
const audioFile = fs.createWriteStream("audio.raw");
wss.on("connection", (ws) => {

  let audioQueue = [];
  let streamEnded = false;
  let transcribeStarted = false; // guards against starting Transcribe more than once
  let logger = null;

  async function* audioStream() {
    while (true) {

      if (audioQueue.length > 0) {
        const chunk = audioQueue.shift();
        yield {
          AudioEvent: {
            AudioChunk: chunk,
          },
        };

      } else if (streamEnded) {
        if (logger) logger.log("Audio stream ended");
        return;
      } else {
        await new Promise((r) => setTimeout(r, 50));

      }
    }
  }

  ws.on("message", (msg) => {

    try {

      const data = JSON.parse(msg.toString());
      const streamSid = data.streamSid || data.stream_sid || `session_${Date.now()}`;

      if (!logger) {
        logger = new CallLogger(streamSid);
        logger.log(`📞 WebSocket connection established for stream: ${streamSid}`);
        console.log(`📞 Call connected (Stream SID: ${streamSid}). Logs saved to logs/call_${streamSid}.log`);
      }

      logger.log(`Received event: ${data.event}`);

      switch (data.event) {
        case "connected":
          logger.log("Connected event received");
          break;
        case "start": {
          logger.log("Start event received");
          logger.log(JSON.stringify(data, null, 2));

          // Pull sample rate from Exotel's mediaFormat block (handling both camelCase and snake_case).
          const mediaFormat = data.start && (data.start.mediaFormat || data.start.media_format);
          let sampleRateHertz = mediaFormat && (mediaFormat.sampleRate || mediaFormat.sample_rate);

          if (!sampleRateHertz) {
            logger.log("⚠️  No sampleRate found in start event mediaFormat — defaulting to 8000Hz");
            sampleRateHertz = 8000;
          }

          logger.log(`🔍 Using MediaSampleRateHertz: ${sampleRateHertz}`);

          if (!transcribeStarted) {
            transcribeStarted = true;
            startTranscribe(audioStream(), sampleRateHertz, logger);
          }

          break;
        }
        case "media":

          const audioBuffer = Buffer.from(
            data.media.payload,
            "base64"
          );
          audioFile.write(audioBuffer);
          audioQueue.push(audioBuffer);

          break;

        case "stop":
          logger.log("Stop event received");
          streamEnded = true;
          audioFile.end();
          break;
        default:

          logger.log(`Unknown event: ${data.event}`);
      }

    } catch (err) {

      if (logger) {
        logger.error("Error parsing message", err);
      } else {
        console.error("Error parsing message:");
        console.error(err);
      }

    }
  });

  ws.on("close", () => {

    if (logger) {
      logger.log("WebSocket closed");
    }
    streamEnded = true;

  });

  ws.on("error", (err) => {

    if (logger) {
      logger.error("WebSocket error", err);
    } else {
      console.error("WebSocket error:");
      console.error(err);
    }
    streamEnded = true;

  });

});

console.log(` Server running on ws://localhost:${PORT}`);