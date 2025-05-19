const http = require("http");
const mysql = require("mysql");
const amqp = require("amqplib");
const redis = require("redis");

// Configuración de Redis
const client = redis.createClient({
  socket: {
    path: "/home/callback/ea-podman.d/ea-redis62.callback.01/redis.sock",
    family: 0,
  },
});

// Conexión a Redis
client.connect().catch((err) => {
  console.log(err.message);
});

const clientFF = redis.createClient({
  socket: {
    host: "192.99.190.137",
    port: 50301,
  },
  password: "sdJmdxXC8luknTrqmHceJS48NTyzExQg",
});

// Conexión a Redis
clientFF.connect().catch((err) => {
  console.log(err.message);
});

// Configuración de RabbitMQ
let rabbitConnection;
let rabbitChannel;
let isConnecting = false;
const rabbitMQUrl = "amqp://lightdata:QQyfVBKRbw6fBb@158.69.131.226:5672";
const queue = "shipments_states_callback_ml";

// Reconexión automática de RabbitMQ
async function initRabbitMQ() {
  if (isConnecting) return;
  isConnecting = true;

  try {
    if (rabbitConnection) await rabbitConnection.close();

    rabbitConnection = await amqp.connect(rabbitMQUrl);
    rabbitConnection.on("error", handleRabbitError);
    rabbitConnection.on("close", handleRabbitClose);

    rabbitChannel = await rabbitConnection.createChannel();
    await rabbitChannel.assertQueue(queue, { durable: true });

    console.log("✅ Conectado a RabbitMQ y canal creado");
  } catch (error) {
    console.error("❌ Error al conectar a RabbitMQ:", error.message);
    setTimeout(() => initRabbitMQ(), 5000);
  } finally {
    isConnecting = false;
  }
}

function handleRabbitError(err) {
  console.error("❌ Error en RabbitMQ:", err.message);
}

function handleRabbitClose() {
  console.warn("⚠️ Conexión a RabbitMQ cerrada. Reintentando...");
  setTimeout(() => initRabbitMQ(), 5000);
}

// Función para enviar mensajes
async function enviarMensajeEstadoML(data, cola) {
  try {
    await initRabbitMQ();
    if (rabbitChannel) {
      rabbitChannel.sendToQueue(cola, Buffer.from(JSON.stringify(data)), {
        persistent: true,
      });
    } else {
      console.warn("❗ Canal no disponible, reintentando en 3s...");
      setTimeout(() => enviarMensajeEstadoML(data, cola), 3000);
    }
  } catch (error) {
    console.error("❌ Error al enviar mensaje a RabbitMQ:", error.message);
    setTimeout(() => enviarMensajeEstadoML(data, cola), 3000);
  }
}

// Configuración de MySQL
const dbuser = "callback_uincomes";
const dbpass = "[dH*XT[4XkFB";
const db = "callback_incomesML";

const con = mysql.createConnection({
  host: "localhost",
  user: dbuser,
  password: dbpass,
  database: db,
});

con.connect((err) => {
  if (err) throw err;
  console.log("✅ Conectado a MySQL!");
});

// HTTP server
const hostname = "localhost";
const port = 3000;

const server = http.createServer(async (req, res) => {
  if (req.method === "POST") {
    let body = "";
    req.on("data", (chunk) => {
      body += chunk;
    });

    req.on("end", async () => {
      try {
        const data2 = JSON.parse(body);
        const incomeuserid = data2.user_id ? data2.user_id.toString() : "";
        const resource = data2.resource;
        const topic = data2.topic;

        let buffer = JSON.stringify(data2) + "-" + incomeuserid + "<br>";
        buffer += "incomeuserid => " + incomeuserid + "<br>";

        let exists = await client.sIsMember("sellersactivos", incomeuserid);
        const now = new Date();
        now.setHours(now.getHours() - 3); // Zona horaria

        if (topic === "flex-handshakes") {
          exists = true;
        }

        //ff

        if (1 === 1) {
          let tablename = "";

          switch (topic) {
            case "orders_v2":
              if (exists) {
                tablename = "db_orders";
                const mensajeRA2 = {
                  resource: resource,
                  sellerid: incomeuserid,
                  fecha: now.toISOString().slice(0, 19).replace("T", " "),
                };
                enviarMensajeEstadoML(mensajeRA2, "enviosml_ia");
              }

              let existsFF = await clientFF.sIsMember(
                "seller_ff",
                incomeuserid
              );
              if (existsFF) {
                let mensajeRA2 = {
                  resource: resource,
                  sellerid: incomeuserid,
                  fecha: now.toISOString().slice(0, 19).replace("T", " "),
                };
                enviarMensajeEstadoML(mensajeRA2, "ordenesFF");
              }

              break;

            case "shipments":
              tablename = "db_shipments";
              const mensajeRA = {
                resource: resource,
                sellerid: incomeuserid,
                fecha: now.toISOString().slice(0, 19).replace("T", " "),
              };
              enviarMensajeEstadoML(mensajeRA, "shipments_states_callback_ml");
              break;

            case "flex-handshakes":
              tablename = "db_flex_handshakes";
              break;
          }

          if (tablename !== "") {
            const sql = `SELECT id FROM ${tablename} WHERE seller_id = ${mysql.escape(
              incomeuserid
            )} and resource= ${mysql.escape(resource)} LIMIT 1`;
            con.query(sql, (err, result) => {
              if (err) throw err;

              const existe = result.length > 0 ? 1 : 0;
              if (existe === 0) {
                const insertSql = `INSERT INTO ${tablename} (seller_id, resource) VALUES (${mysql.escape(
                  incomeuserid
                )}, ${mysql.escape(resource)})`;
                con.query(insertSql, (err) => {
                  if (err) throw err;
                });
              }
            });
          }

          buffer += "OK<br>";
        } else {
          buffer += "NO SELLER ID<br>";
        }

        buffer += "FIN<br>";
        res.writeHead(200, { "Content-Type": "text/html" });
        res.end(buffer);
      } catch (e) {
        console.error("Error processing request: ", e);
        res.writeHead(500);
        res.end("Internal Server Error");
      }
    });
  } else {
    res.writeHead(404);
    res.end();
  }
});

server.listen(port, hostname, () => {
  console.log(`Server running at http://${hostname}:${port}/`);
});

async function shutdown() {
  try {
    if (rabbitChannel) await rabbitChannel.close();
    if (rabbitConnection) await rabbitConnection.close();
    console.log("🔌 Conexión a RabbitMQ cerrada");
  } catch (error) {
    console.error("Error cerrando conexiones:", error);
  } finally {
    process.exit(1);
  }
}

// Manejo de errores globales
process.on("exit", shutdown);
process.on("uncaughtException", async (error) => {
  console.error("Error no capturado:", error);
  await shutdown();
});
process.on("unhandledRejection", async (reason) => {
  console.error("Rechazo no manejado:", reason);
  await shutdown();
});

// Utilidad de fecha/hora actual
const getCurrentDateTime = () => {
  const now = new Date();
  return now.toISOString().slice(0, 19).replace("T", " ");
};
