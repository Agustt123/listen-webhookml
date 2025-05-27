const axios = require("axios");
const http = require("http");
const https = require("https");

const RABBITMQ_URL = "http://158.69.131.226:15672/api";
const RABBITMQ_USER = "lightdata";
const RABBITMQ_PASS = "QQyfVBKRbw6fBb";

const auth = {
  username: RABBITMQ_USER,
  password: RABBITMQ_PASS,
};

const axiosInstance = axios.create({
  baseURL: RABBITMQ_URL,
  auth,
  timeout: 30000,
  httpAgent: new http.Agent({ keepAlive: true }),
  httpsAgent: new https.Agent({ keepAlive: true }),
});

async function retryRequest(fn, retries = 2, delayMs = 1000) {
  for (let i = 0; i < retries; i++) {
    try {
      return await fn();
    } catch (err) {
      console.warn(`⚠️ Intento ${i + 1} falló: ${err.message}`);
      if (i < retries - 1)
        await new Promise((res) => setTimeout(res, delayMs * (i + 1)));
    }
  }
  throw new Error("Fallaron todos los reintentos");
}

async function getConnections() {
  const response = await retryRequest(() => axiosInstance.get("/connections"));
  return response.data;
}

async function closeConnection(name) {
  await retryRequest(() =>
    axiosInstance.delete(`/connections/${encodeURIComponent(name)}`)
  );
}

async function main() {
  try {
    const connections = await getConnections();

    const filteredConnections = connections.filter(
      (conn) => conn.user === "lightdata" && conn.channels > 5
    );

    console.log(
      `🔍 Conexiones a cerrar (usuario 'lightdata' con más de 5 canales): ${filteredConnections.length}`
    );

    for (const conn of filteredConnections) {
      try {
        console.log(
          `🛑 Cerrando conexión: ${conn.name} (IP: ${conn.peer_host}, Channels: ${conn.channels})`
        );
        await closeConnection(conn.name);
        console.log(`✅ Cerrada: ${conn.name}`);
      } catch (error) {
        console.error(`❌ Error cerrando ${conn.name}: ${error.message}`);
      }
    }

    console.log("✔ Finalizado.");
  } catch (err) {
    console.error("❌ Error general:", err.message);
  }
}

// Ejecutar una vez al inicio
main();

// Ejecutar cada 10 minutos (600,000 ms)
setInterval(main, 2 * 60 * 1000);
