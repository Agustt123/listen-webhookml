const axios = require("axios");
const express = require("express");
const app = express();
const redis = require("redis");
const PORT = 13000; // O el puerto que necesites
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

// Endpoint para obtener los sellers activos
app.get("/sellersactivos", async (req, res) => {
  try {
    const sellers = await client.sMembers("sellersactivos");
    res.json({ success: true, sellers });
  } catch (err) {
    console.error("❌ Error obteniendo sellersactivos:", err.message);
    res.status(500).json({ success: false, error: err.message });
  }
});
async function name() {
  const response = await axios.get(
    "https://callbackml.lightdata.app/MLProcesar/get/"
  );
  console.log(response.data.sellers, "adsadasda");
}
name();
// Iniciar servidor
app.listen(PORT, () => {
  console.log(`🚀 API escuchando en http://localhost:${PORT}`);
});
