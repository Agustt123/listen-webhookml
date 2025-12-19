const amqp = require("amqplib");
const mysql = require("mysql2");
const { executeQuery } = require("./db.js"); // ajustá path
const { logYellow, logRed } = require("./fuctions/logsCustom");

// =======================
// CONFIG RABBIT
// =======================
const rabbitMQUrl = "amqp://lightdata:QQyfVBKRbw6fBb@158.69.131.226:5672";
const migrationQueue = "callbackMS";

// =======================
// NUEVA BASE DE DATOS
// =======================
const con = mysql.createPool({
    host: "149.56.182.49",
    port: 44353,
    user: "root",
    password: "4AVtLery67GFEd",
    database: "callback_incomesML",
    waitForConnections: true,
    connectionLimit: 10,
    queueLimit: 0,
});

// Test conexión DB
con.getConnection((err, connection) => {
    if (err) {
        logRed(`❌ Error conectando a BD: ${err.message}`);
    } else {
        logYellow("✅ Conectado a BD de migración");
        connection.release();
    }
});

// =======================
// RABBIT
// =======================
let connection;
let channel;

async function initRabbit() {
    try {
        connection = await amqp.connect(rabbitMQUrl);
        channel = await connection.createChannel();

        await channel.assertQueue(migrationQueue, { durable: true });

        logYellow(`🐇 Escuchando cola ${migrationQueue}`);

        channel.consume(migrationQueue, async (msg) => {
            if (!msg) return;

            try {
                const evento = JSON.parse(msg.content.toString());
                await procesarEvento(evento);
                channel.ack(msg);
            } catch (err) {
                logRed(`❌ Error procesando evento: ${err.message}`);
                channel.nack(msg, false, false); // se descarta
            }
        });

    } catch (err) {
        logRed(`❌ Error iniciando RabbitMQ: ${err.message}`);
        process.exit(1);
    }
}

// =======================
// PROCESADOR
// =======================
async function procesarEvento(evento) {
    const { topic, data } = evento;

    switch (topic) {
        case "orders_v2":
            await insertarOrder(data);
            break;

        case "shipments":
            await insertarShipment(data);
            break;

        case "flex-handshakes":
            await insertarFlexHandshake(data);
            break;

        default:
            logYellow(`⚠️ Topic no manejado: ${topic}`);
    }
}

// =======================
// INSERTS (TABLAS REALES)
// =======================
async function insertarOrder(data) {
    const sql = `
        INSERT INTO db_orders (seller_id, resource)
        VALUES (?, ?)
    `;

    await executeQuery(
        con,
        sql,
        [data.seller_id, data.resource],
        true
    );

    logYellow("✅ Order migrada");
}

async function insertarShipment(data) {
    const sql = `
        INSERT INTO db_shipments (seller_id, resource, fecha)
        VALUES (?, ?, ?)
    `;

    await executeQuery(
        con,
        sql,
        [data.seller_id, data.resource, data.fecha],
        true
    );

    logYellow("✅ Shipment migrado");
}

async function insertarFlexHandshake(data) {
    const sql = `
        INSERT INTO db_flex_handshakes (seller_id, resource)
        VALUES (?, ?)
    `;

    await executeQuery(
        con,
        sql,
        [data.seller_id, data.resource],
        true
    );

    logYellow("✅ Flex-handshake migrado");
}

// =======================
// START
// =======================
initRabbit();
