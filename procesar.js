const amqp = require("amqplib");
const mysql = require("mysql2");
const redis = require("redis");

const axios = require("axios");

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
// Configuración de MySQL
//const dbuser = "logisticaA";
//const dbpass = "logisticaa";
//const db = "logisticaa";
const dbuser = "callback_uincomes";
const dbpass = "[dH*XT[4XkFB";
const db = "callback_incomesML";

const con = mysql.createConnection({
  host: "bhsmysql1.lightdata.com.ar",
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
  if (isConnecting) return;
  isConnecting = true;

  try {
    if (rabbitConnection) await rabbitConnection.close();

    rabbitConnection = await amqp.connect(rabbitMQUrl);
    rabbitConnection.on("error", handleRabbitError);
    rabbitConnection.on("close", handleRabbitClose);

    rabbitChannel = await rabbitConnection.createChannel();
    await rabbitChannel.assertQueue(queue, { durable: true });

    console.log(
      "✅ Conectado a RabbitMQ y canal creado. Esperando mensajes..."
    );

    const pLimit = (await import("p-limit")).default; // Importar dinámicamente
    const limit = pLimit(1); // Limitar a 1 para procesar un mensaje a la vez

    rabbitChannel.consume(queue, async (msg) => {
      if (!msg) return;

      // Utilizar el límite aquí
      await limit(async () => {
        console.log(
          "Recibido mensaje con deliveryTag:",
          msg.fields.deliveryTag
        );

        try {
          console.log("AHORA ENTRO ACA PARA PROCESAR");

          const data = JSON.parse(msg.content.toString());
          console.log(data);

          // Verificar si el usuario está permitido

          await processWebhook(data);
          console.log("AHORA SALGO ACA PARA PROCESAR");
          rabbitChannel.ack(msg); // Acknowledge solo si se procesa correctamente
        } catch (e) {
          console.error("❌ Error procesando mensaje:", e.message);
          rabbitChannel.nack(msg, false, false); // Nack si hay un error
        }
      });
    });
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

// Función para verificar si el seller está permitido
function isSellerAllowed(sellerId) {
  // Implementa tu lógica para verificar si el sellerId está permitido
  const allowedSellers = [
    /* lista de sellers permitidos */
  ];
  return allowedSellers.includes(sellerId);
}

let cachedSellers = [];

async function processWebhook(data2) {
  const pLimit = (await import("p-limit")).default; // Importar dinámicamente
  const limit = pLimit(5);
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
          // Limitar la llamada al endpoint externo
          const response = await axios.get(
            "https://callbackml.lightdata.app/MLProcesar/get/"
          );
          console.log("llegamoss");

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
        console.log("fsdsfsdfsdf");

        exists = true;
      }
    }

    if (exists) {
      console.log("mepa quie si ");

      let tablename = "";
      console.log("llegamoss222");
      switch (topic) {
        case "orders_v2":
          tablename = "db_orders";
          const mensajeRA2 = {
            resource,
            sellerid: incomeuserid,
            fecha: now.toISOString().slice(0, 19).replace("T", " "),
          };
          console.log("llegamoss333");

          //await enviarMensajeEstadoML(mensajeRA2, "enviosml_ia");
          console.log("📤 Enviado a cola enviosml_ia:", mensajeRA2);
          break;

        case "shipments":
          tablename = "db_shipments";
          const mensajeRA = {
            resource,
            sellerid: incomeuserid,
            fecha: now.toISOString().slice(0, 19).replace("T", " "),
          };
          /*await enviarMensajeEstadoML(
            mensajeRA,
            "shipments_states_callback_ml"
          );
          console.log(
            "📤 Enviado a cola shipments_states_callback_ml:",
            mensajeRA
          );*/
          break;

        case "flex-handshakes":
          tablename = "db_flex_handshakes";
          break;
      }
      console.log("llegamoss444444");
      console.log(tablename, incomeuserid, resource);

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
      console.warn(
        `⚠️ Usuario ${incomeuserid} no está en la lista de sellers permitidos`
      );
    }
  } catch (e) {
    console.error("❌ Error procesando webhook:", e.message);
  }
}

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

// Iniciar el sistema
