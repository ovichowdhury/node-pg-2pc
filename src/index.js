const express = require("express");
const { Pool } = require("pg");
const crypto = require("crypto");

const app = express();

app.use(express.json());

// PostgreSQL connection details
const pool = new Pool({
  user: "postgres",
  host: "localhost",
  database: "testdb",
  password: "1234",
  port: 5432, // Default PostgreSQL port
  idleTimeoutMillis: 30000,
  connectionTimeoutMillis: 2000,
});

/**
 * Health Check
 */
app.get("/health", (req, res) => {
  res.status(200).json({
    message: "Ok",
  });
});

/**
 * Prepare a Transaction for Commit
 */
app.post("/prepare", async (req, res) => {
  let result = null;
  let client = null;
  const { name, cgpa } = req.body;
  const transactionName = crypto.randomBytes(5).toString("hex");

  try {
    // Connect to the database
    client = await pool.connect();

    // Start the transaction
    await client.query("BEGIN");

    // Perform some operations
    result = await client.query(
      "INSERT INTO students (name, cgpa) VALUES ($1, $2) RETURNING id",
      [name, cgpa]
    );

    // Prepare the transaction
    await client.query(`PREPARE TRANSACTION '${transactionName}'`);

    console.log(`[INFO] Transaction '${transactionName}' prepared.`);
  } catch (error) {
    console.error("[ERROR] Error during transaction:", error.message);
    return res.status(500).json({
      message: error.message,
    });
  } finally {
    // Clean up and close the connection
    await client.release();
  }

  return res.status(201).json({
    id: result ? result.rows[0].id : null,
    txId: transactionName,
  });
});

/**
 * Commit the Transaction
 */
app.post("/commit", async (req, res) => {
  let client = null;
  const { txId } = req.body;

  try {
    // Connect to the database
    client = await pool.connect();
    // Commit the prepared transaction
    await client.query(`COMMIT PREPARED '${txId}'`);

    console.log(`[INFO] Transaction '${txId}' committed.`);
  } catch (error) {
    console.error("[ERROR] Error during transaction:", error.message);
    return res.status(500).json({
      message: error.message,
    });
  } finally {
    // Clean up and close the connection
    await client.release();
  }

  return res.status(200).json({
    txId: txId,
  });
});

/**
 * Commit the Transaction
 */
app.post("/rollback", async (req, res) => {
  let client = null;
  const { txId } = req.body;

  try {
    // Connect to the database
    client = await pool.connect();
    // Commit the prepared transaction
    await client.query(`ROLLBACK PREPARED '${txId}'`);

    console.log(`[INFO] Transaction '${txId}' rollbacked.`);
  } catch (error) {
    console.error("[ERROR] Error during transaction:", error.message);
    return res.status(500).json({
      message: error.message,
    });
  } finally {
    // Clean up and close the connection
    await client.release();
  }

  return res.status(200).json({
    txId: txId,
  });
});

// Start the application
app.listen(3030, () => {
  console.log("[INFO] Server listening on port 3030");
});
