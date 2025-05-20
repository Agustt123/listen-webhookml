const amqp = require("amqplib");
const mysql = require("mysql2");
const redis = require("redis");
const axios = require("axios");
let pLimit;

async function initializePLimit() {
  const module = await import("p-limit");
  pLimit = module.default;
}

initializePLimit()
  .then(() => {
    // El resto de tu código que depende de 'pLimit' va aquí
    consumeQueue(); // Comenzar a consumir la cola después de inicializar pLimit
  })
  .catch((err) => {
    console.error("Error al importar p-limit:", err);
  });

// Configuración de Redis
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

// RabbitMQ
const rabbitMQUrl = "amqp://lightdata:QQyfVBKRbw6fBb@158.69.131.226:5672";
const queue = "webhookml";
let rabbitConnection;
let rabbitChannel;
let isConnecting = false;
let rabbitConnectionActive = false; // Nueva bandera

// Configuración de MySQL
const dbuser = "callback_u2u3";
const dbpass = "7L35HWuw,8,i";
const db = "callback_incomesML";

const con = mysql.createConnection({
  host: "bhsws10.ticdns.com",
  user: dbuser,
  password: dbpass,
  database: db,
});

con.connect((err) => {
  if (err) {
    console.error("❌ Error al conectar a MySQL:", err.message);
  } else {
    console.log("✅ Conectado a MySQL!");
  }
});

async function initRabbitMQ() {
  if (isConnecting || rabbitConnectionActive) return;
  isConnecting = true;
  try {
    rabbitConnection = await amqp.connect(rabbitMQUrl);
    rabbitConnection.on("error", handleRabbitError);
    rabbitConnection.on("close", handleRabbitClose);
    rabbitChannel = await rabbitConnection.createChannel();
    await rabbitChannel.assertQueue(queue, { durable: true });
    rabbitConnectionActive = true;
  } catch (error) {
    console.error("❌ Error al conectar a RabbitMQ:", error.message);
    setTimeout(initRabbitMQ, 5000);
  } finally {
    isConnecting = false;
  }
}

function handleRabbitError(err) {
  console.error("❌ Error en RabbitMQ:", err.message);
  rabbitConnectionActive = false;
}

function handleRabbitClose() {
  console.warn("⚠️ Conexión a RabbitMQ cerrada.");
  rabbitConnectionActive = false;
}

async function ensureRabbitMQConnection() {
  if (!rabbitConnectionActive) {
    await initRabbitMQ();
  }
}

async function enviarMensajeEstadoML(data, cola) {
  try {
    await ensureRabbitMQConnection();
    if (rabbitChannel && rabbitConnectionActive) {
      rabbitChannel.sendToQueue(cola, Buffer.from(JSON.stringify(data)), {
        persistent: true,
      });
    } else {
      console.warn(
        "❗ Conexión a RabbitMQ no activa, no se pudo enviar a:",
        cola
      );
    }
  } catch (error) {
    console.error("❌ Error al enviar mensaje a RabbitMQ:", error.message);
  }
}

// Función para verificar si el seller está permitido
function isSellerAllowed(sellerId) {
  const allowedSellers = [
    /* lista de sellers permitidos */
  ];
  return allowedSellers.includes(sellerId);
}

let cachedSellers = [];

async function processWebhook(data2) {
  const limit = pLimit(5); // Mantener el límite de concurrencia
  try {
    const incomeuserid = data2.user_id ? data2.user_id.toString() : "";
    const resource = data2.resource;
    const topic = data2.topic;
    let now = new Date();
    now.setHours(now.getHours() - 3); // Ajuste horario

    let exists = false;

    if (topic === "flex-handshakes") {
      exists = true;
    } else {
      if (cachedSellers.length === 0 || !cachedSellers.includes(incomeuserid)) {
        try {
          const response = await axios.get(
            "https://callbackml.lightdata.app/MLProcesar/get/"
          );
          if (
            response.data &&
            response.data.success &&
            Array.isArray(response.data.sellers)
          ) {
            cachedSellers = response.data.sellers;
            exists = cachedSellers.includes(incomeuserid);
          } else {
            console.warn("⚠️ Respuesta inesperada del endpoint de sellers");
          }
        } catch (error) {
          console.error(
            "❌ Error consultando endpoint externo:",
            error.message
          );
        }
      } else {
        exists = true;
      }
    }

    if (exists) {
      let tablename = "";
      switch (topic) {
        case "orders_v2":
          tablename = "db_orders";
          const mensajeRA2 = {
            resource,
            sellerid: incomeuserid,
            fecha: now.toISOString().slice(0, 19).replace("T", " "),
          };
          await enviarMensajeEstadoML(mensajeRA2, "enviosml_ia");
          break;

        case "shipments":
          tablename = "db_shipments";
          const mensajeRA = {
            resource,
            sellerid: incomeuserid,
            fecha: now.toISOString().slice(0, 19).replace("T", " "),
          };
          await enviarMensajeEstadoML(
            mensajeRA,
            "shipments_states_callback_ml"
          );
          break;

        case "flex-handshakes":
          tablename = "db_flex_handshakes";
          break;
      }

      if (tablename !== "") {
        const sql = `SELECT id FROM ${tablename} WHERE seller_id = ${mysql.escape(
          incomeuserid
        )} AND resource= ${mysql.escape(resource)} LIMIT 1`;
        con.query(sql, (err, result) => {
          if (err) {
            console.error("❌ Error en SELECT:", err.message);
            return;
          }
          if (result.length === 0) {
            const insertSql = `INSERT INTO ${tablename} (seller_id, resource) VALUES (${mysql.escape(
              incomeuserid
            )}, ${mysql.escape(resource)})`;
            con.query(insertSql, (err) => {
              if (err) {
                console.error(
                  `❌ Error insertando en ${tablename}:`,
                  err.message
                );
              } else {
                console.log(`✅ Registro insertado en ${tablename}`);
              }
            });
          } else {
            console.log(`ℹ️ Registro ya existe en ${tablename}`);
          }
        });
      }
    } else {
      //   console.warn(
      //   `⚠️ Usuario ${incomeuserid} no está en la lista de sellers permitidos`
      //);
    }
  } catch (e) {
    console.error("❌ Error procesando webhook:", e.message);
  }
}

async function consumeQueue() {
  const limit = pLimit(100); // Aumentar el límite a 100
  try {
    await ensureRabbitMQConnection();
    if (rabbitChannel && rabbitConnectionActive) {
      rabbitChannel.consume(queue, async (msg) => {
        if (!msg) return;
        await limit(async () => {
          try {
            const data = JSON.parse(msg.content.toString());
            await processWebhook(data);
            rabbitChannel.ack(msg);
            if (rabbitChannel && rabbitConnectionActive) {
            } else {
              console.warn("⚠️ Conexión a RabbitMQ inactiva, no se pudo ack.");
            }
          } catch (e) {
            console.error("❌ Error procesando mensaje:", e.message);
            await ensureRabbitMQConnection();
            if (rabbitChannel && rabbitConnectionActive) {
              rabbitChannel.nack(msg, false, false);
            } else {
              console.warn("⚠️ Conexión a RabbitMQ inactiva, no se pudo nack.");
            }
          }
        });
      });
      console.log(
        "✅ Conectado a RabbitMQ y canal creado. Esperando mensajes..."
      );
    } else {
      console.warn("❗ No se pudo iniciar el consumo de la cola.");
    }
  } catch (error) {
    console.error("❌ Error al consumir la cola:", error.message);
    setTimeout(consumeQueue, 5000);
  }
}

// Iniciar el sistema
initRabbitMQ().then(consumeQueue);
