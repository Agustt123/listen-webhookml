const http = require("http");
const amqp = require("amqplib");
const redis = require("redis");

const client = redis.createClient({
  socket: {
    path: "/home/callback/ea-podman.d/ea-redis62.callback.01/redis.sock",
    family: 0,
  },
});
client
  .connect()
  .then(() => {
    console.log("✅ Redis local conectado.");
  })
  .catch((err) => {
    console.error("❌ Error al conectar a Redis local:", err.message);
  });

const clientFF = redis.createClient({
  socket: {
    host: "192.99.190.137",
    port: 50301,
  },
  password: "sdJmdxXC8luknTrqmHceJS48NTyzExQg",
});
clientFF
  .connect()
  .then(() => {
    console.log("✅ Redis remoto conectado.");
  })
  .catch((err) => {
    console.error("❌ Error al conectar a Redis remoto:", err.message);
  });

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
        const sellerId = data.user_id; // Obtener el seller_id del cuerpo

        // Verificar si el seller_id existe en Redis
        const exists = await client.sIsMember("sellersactivos", sellerId);

        if (exists) {
          // Solo enviar el mensaje si el seller_id existe en Redis
          rabbitChannel.sendToQueue(queue, Buffer.from(JSON.stringify(data)), {
            persistent: true,
          });
          res.writeHead(200);
          res.end("Webhook recibido");
        } else {
          console.log(`❌ Seller ID ${sellerId} no encontrado en Redis.`);
          res.writeHead(404);
          res.end("Seller ID no encontrado");
        }
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
