const http = require("http");
const amqp = require("amqplib");

const queue = "webhookml";
const rabbitMQUrl = "amqp://lightdata:QQyfVBKRbw6fBb@158.69.131.226:5672";

let rabbitConnection;
let rabbitChannel;

async function connectRabbit() {
  if (rabbitChannel) return;
  rabbitConnection = await amqp.connect(rabbitMQUrl);
  rabbitChannel = await rabbitConnection.createChannel();
  await rabbitChannel.assertQueue(queue, { durable: true });
}

const server = http.createServer(async (req, res) => {
  if (req.method === "POST") {
    let body = "";
    req.on("data", (chunk) => (body += chunk));
    req.on("end", async () => {
      try {
        await connectRabbit();
        const data = JSON.parse(body);
        rabbitChannel.sendToQueue(queue, Buffer.from(JSON.stringify(data)), {
          persistent: true,
        });
        res.writeHead(200);
        res.end("Webhook recibido");
      } catch (err) {
        console.error("❌ Error procesando webhook:", err.message);
        res.writeHead(500);
        res.end("Error interno");
      }
    });
  } else {
    res.writeHead(404);
    res.end();
  }
});

server.listen(3000, "localhost", () => {
  console.log("🚀 Webhook listener activo en http://localhost:3000");
});
