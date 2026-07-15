const WebSocket = require("ws");
const fs = require("fs");

const ws = new WebSocket("ws://localhost:8080?channels=mono");

ws.on("open", () => {
  console.log("Connected to local streaming server");

  // Send connected event
  ws.send(JSON.stringify({
    event: "connected"
  }));

  // Send start event (Option B payload format with customer phone number)
  ws.send(JSON.stringify({
    event: "start",
    stream_sid: "test-stream-sid-999",
    sequence_number: "1",
    start: {
      stream_sid: "test-stream-sid-999",
      call_sid: "test-call-sid-999",
      media_format: {
        encoding: "pcm",
        sample_rate: "8000"
      },
      custom_parameters: {
        From: "+919876543210"
      }
    }
  }));

  // Stream a dummy speech pcm file to simulate inbound customer audio
  const customerStream = fs.createReadStream("speech01.pcm", {
    highWaterMark: 3200
  });

  customerStream.on("data", (chunk) => {
    ws.send(JSON.stringify({
      event: "media",
      media: {
        track: "inbound",
        payload: chunk.toString("base64")
      }
    }));
  });

  customerStream.on("end", () => {
    console.log("Finished customer audio stream, sending stop event...");
    
    ws.send(JSON.stringify({
      event: "stop"
    }));

    setTimeout(() => {
      ws.close();
    }, 5000); // Wait for translation and processing to finish before closing connection
  });
});

ws.on("close", () => {
  console.log("Connection closed");
  process.exit(0);
});

ws.on("error", (err) => {
  console.error("WebSocket Client Error:", err);
});
