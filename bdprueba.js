const mysql = require("mysql2/promise");

const config = {
  host: "149.56.182.49",
  user: "ulog",
  password: "yusito23",
  database: "data",
  port: 44339,
};

async function checkConnection() {
  let connection;
  try {
    connection = await mysql.createConnection(config);
    console.log("Conexión exitosa.");
    return true;
  } catch (error) {
    console.error("Error al conectar:", error.message);
    return false;
  } finally {
    if (connection) {
      await connection.end();
      console.log("Conexión cerrada.");
    }
  }
}

// Ejecutar el chequeo
checkConnection().then((result) => {
  console.log("Resultado de la conexión:", result);
});
