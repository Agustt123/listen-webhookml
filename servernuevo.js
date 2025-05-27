const http = require("http");
const amqp = require("amqplib");
const https = require("https");

const queue = "webhookml";
const rabbitMQUrl = "amqp://lightdata:QQyfVBKRbw6fBb@158.69.131.226:5672";
const SELLERS_URL = "https://callbackml.lightdata.app/MLProcesar/get/";

let rabbitConnection;
let rabbitChannel;
let sellersActivos = new Set();

async function connectRabbit() {
  if (rabbitChannel) return;
  rabbitConnection = await amqp.connect(rabbitMQUrl);
  rabbitChannel = await rabbitConnection.createChannel();
  await rabbitChannel.assertQueue(queue, { durable: true });
}

function fetchSellersActivos() {
  return new Promise((resolve, reject) => {
    https
      .get(SELLERS_URL, (res) => {
        let data = "";
        res.on("data", (chunk) => (data += chunk));
        res.on("end", () => {
          try {
            const json = JSON.parse(data);
            if (json.success && Array.isArray(json.sellers)) {
              sellersActivos = new Set(json.sellers.filter(Boolean));
              console.log(
                `✅ Sellers activos cargados (${sellersActivos.size})`
              );
              resolve();
            } else {
              console.error("❌ Formato de respuesta inesperado");
              reject();
            }
          } catch (err) {
            console.error("❌ Error parseando JSON:", err.message);
            reject(err);
          }
        });
      })
      .on("error", (err) => {
        console.error("❌ Error solicitando sellers activos:", err.message);
        reject(err);
      });
  });
}

const server = http.createServer(async (req, res) => {
  if (req.method === "POST") {
    let body = "";
    req.on("data", (chunk) => (body += chunk));
    req.on("end", async () => {
      try {
        const data = JSON.parse(body);
        const userId = String(data.user_id);
        if (!sellersActivos.has(userId)) {
          console.log(`🔕 Webhook ignorado (user_id no activo): ${userId}`);
          res.writeHead(200);
          return res.end("Seller inactivo");
        }

        await connectRabbit();
        rabbitChannel.sendToQueue(queue, Buffer.from(JSON.stringify(data)), {
          persistent: true,
        });

        console.log(`✅ Webhook encolado para user_id: ${userId}`);
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

// Cargar sellers al arrancar el servidor
fetchSellersActivos().then(() => {
  server.listen(3000, "localhost", () => {
    console.log("🚀 Webhook listener activo en http://localhost:3000");
  });
});
