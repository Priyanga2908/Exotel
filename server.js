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
const WebSocket = require("ws");
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

async function translateKannadaToEnglish(text) {

  try {

    const prompt = `
Translate the following Kannada text into natural English.
Treat names as names and preserve context.
Return only the English translation.

Kannada:
${text}
`;

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
            content: prompt,
          },
        ],
      }),
    });

    const response = await bedrock.send(command);

    const responseBody = JSON.parse(
      Buffer.from(response.body).toString()
    );

    return responseBody.content[0].text;

  } catch (err) {

    console.error("Translation Error");
    console.error(err);

    return null;
  }
}

async function startTranscribe(audioStreamGenerator, sampleRateHertz) {
  try {
    console.log(`🎛️  Starting Transcribe with MediaSampleRateHertz=${sampleRateHertz}`);

    const command = new StartStreamTranscriptionCommand({
    LanguageCode: "kn-IN",
      MediaEncoding: "pcm",
      MediaSampleRateHertz: sampleRateHertz,
      AudioStream: audioStreamGenerator,
    });

    const response = await client.send(command);


    console.log("🎙️ Transcribe started...");

    for await (const event of response.TranscriptResultStream) {
      if (event.TranscriptEvent) {
        const results = event.TranscriptEvent.Transcript.Results;

        for (const r of results) {
          const text = r.Alternatives[0].Transcript;

          if (r.IsPartial)
            console.log("PARTIAL:", text);
            
          else
          {
          
             console.log("FINAL (KN):", text);

             const english = await translateKannadaToEnglish(text);

             console.log("ENGLISH:", english);} }
        
      }
    }

    console.log("Transcribe stream closed");
  } catch (err) {
    console.error("TRANSCRIBE ERROR:");
    console.dir(err, { depth: null });
  }
}

const wss = new WebSocket.Server({ port: PORT });
const audioFile = fs.createWriteStream("audio.raw");
wss.on("connection", (ws) => {

  console.log("📞 Call connected");

  let audioQueue = [];
  let streamEnded = false;
  let transcribeStarted = false; // guards against starting Transcribe more than once

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
        console.log("Audio stream ended");
        return;
      } else {
        await new Promise((r) => setTimeout(r, 50));

      }
    }
  }

  // NOTE: startTranscribe() is no longer called immediately on connection.
  // It now waits for the "start" event below, since that's where we learn
  // the real sample rate to pass to Transcribe.

  ws.on("message", (msg) => {

    try {

      const data = JSON.parse(msg.toString());
      console.log("Received event:", data.event);//fr testing

      switch (data.event) {
        case "connected":
          console.log("Connected event received");
          break;
        case "start": {
          console.log("Start event received");
          console.log(JSON.stringify(data, null, 2));

          // Pull sample rate from Exotel's mediaFormat block. Fall back to
          // 8000 (Exotel's typical default) only if it's missing, and log
          // loudly so a missing field doesn't go unnoticed.
          const mediaFormat = data.start && data.start.media_format;
          let sampleRateHertz = mediaFormat && (mediaFormat.sampleRate || mediaFormat.sample_rate);

          if (!sampleRateHertz) {
            console.warn("⚠️  No sampleRate found in start event mediaFormat — defaulting to 8000Hz");
            sampleRateHertz = 8000;
          }

          console.log("🔍 Using MediaSampleRateHertz:", sampleRateHertz);

          if (!transcribeStarted) {
            transcribeStarted = true;
            startTranscribe(audioStream(), sampleRateHertz);
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
          console.log("Stop event received");
          streamEnded = true;
           audioFile.end();
          break;
        default:

          console.log("Unknown event:", data.event);
      }

    } catch (err) {

      console.error("Error parsing message:");
      console.error(err);

    }
  });

  ws.on("close", () => {

    console.log(" WebSocket closed");
    streamEnded = true;

  });

  ws.on("error", (err) => {

    console.error("WebSocket error:");
    console.error(err);
    streamEnded = true;

  });

});

console.log(` Server running on ws://localhost:${PORT}`);