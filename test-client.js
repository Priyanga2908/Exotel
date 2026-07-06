// const WebSocket = require("ws");
// const fs = require("fs");
// const ws = new WebSocket("ws://localhost:8080");

// ws.on("open", () => {
//   console.log("connected");

//   const stream = fs.createReadStream("speech02.pcm", {
//     highWaterMark: 3200
//   });

//   stream.on("data", (chunk) => {
//     ws.send(JSON.stringify({
//       event: "media",
//       media: {
//         payload: chunk.toString("base64")
//       }
//     }));
//   });

//   stream.on("end", () => {
//   ws.send(JSON.stringify({
//     event: "stop"
//   }));
//   console.log("Finished sending audio");
//   ws.close();
// });

// ws.on("close", () => {
//   console.log("Connection closed");
//   process.exit(0);
// });

// });

const WebSocket = require("ws");
const fs = require("fs");

const ws = new WebSocket("ws://localhost:8080");

ws.on("open", () => {

  console.log("connected");

  // Send connected event
  ws.send(JSON.stringify({
    event: "connected"
  }));

  // Send start event (similar to Exotel)
  ws.send(JSON.stringify({
    event: "start",
    stream_sid: "test-stream",
    sequence_number: "1",
    start: {
      stream_sid: "test-stream",
      call_sid: "test-call",
      media_format: {
        encoding: "pcm",
        sample_rate: "16000",
        bit_rate: "256kbps"
      }
    }
  }));

  const stream = fs.createReadStream("speech01.pcm", {
    highWaterMark: 3200
  });

  stream.on("data", (chunk) => {

    ws.send(JSON.stringify({
      event: "media",
      media: {
        payload: chunk.toString("base64")
      }
    }));

  });

  stream.on("end", () => {

    ws.send(JSON.stringify({
      event: "stop"
    }));

    console.log("Finished sending audio");

    setTimeout(() => {
      ws.close();
    }, 1000);

  });

});

ws.on("close", () => {

  console.log("Connection closed");
  process.exit(0);

});

ws.on("error", (err) => {

  console.error(err);

});



//speech 1 16khz or 24
//speech 2  8khz   128kbs
//speech 3  16khz  256kbs