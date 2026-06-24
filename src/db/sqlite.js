const fs = require("fs");
const path = require("path");
const { DatabaseSync } = require("node:sqlite");

const DATA_DIR = path.join(__dirname, "..", "..", "data");
const DB_PATH = process.env.DISUMQ_DB_PATH || path.join(DATA_DIR, "disumq.sqlite");

fs.mkdirSync(path.dirname(DB_PATH), {
  recursive: true,
});

const db = new DatabaseSync(DB_PATH);

db.exec(`
  PRAGMA journal_mode = WAL;
  PRAGMA foreign_keys = ON;

  CREATE TABLE IF NOT EXISTS topics (
    name TEXT PRIMARY KEY,
    created_at TEXT NOT NULL
  );

  CREATE TABLE IF NOT EXISTS queues (
    name TEXT PRIMARY KEY,
    created_at TEXT NOT NULL
  );

  CREATE TABLE IF NOT EXISTS topic_bindings (
    topic TEXT NOT NULL,
    queue TEXT NOT NULL,
    created_at TEXT NOT NULL,
    PRIMARY KEY (topic, queue),
    FOREIGN KEY (topic) REFERENCES topics(name),
    FOREIGN KEY (queue) REFERENCES queues(name)
  );

  CREATE TABLE IF NOT EXISTS messages (
    id TEXT PRIMARY KEY,
    topic TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    producer_id TEXT NOT NULL,
    published_at TEXT NOT NULL,
    FOREIGN KEY (topic) REFERENCES topics(name)
  );

  CREATE TABLE IF NOT EXISTS deliveries (
    id TEXT PRIMARY KEY,
    message_id TEXT NOT NULL,
    topic TEXT NOT NULL,
    queue TEXT NOT NULL,
    status TEXT NOT NULL,
    attempts INTEGER NOT NULL,
    created_at TEXT NOT NULL,
    delivered_at TEXT,
    delivered_connection_id TEXT,
    delivered_to TEXT,
    acked_at TEXT,
    nacked_at TEXT,
    failed_at TEXT,
    next_retry_at TEXT,
    last_error TEXT,
    FOREIGN KEY (message_id) REFERENCES messages(id),
    FOREIGN KEY (topic) REFERENCES topics(name),
    FOREIGN KEY (queue) REFERENCES queues(name)
  );

  CREATE TABLE IF NOT EXISTS dead_letter_deliveries (
    delivery_id TEXT PRIMARY KEY,
    message_id TEXT NOT NULL,
    topic TEXT NOT NULL,
    queue TEXT NOT NULL,
    payload_json TEXT,
    attempts INTEGER NOT NULL,
    last_error TEXT,
    failed_at TEXT NOT NULL,
    FOREIGN KEY (delivery_id) REFERENCES deliveries(id),
    FOREIGN KEY (message_id) REFERENCES messages(id),
    FOREIGN KEY (topic) REFERENCES topics(name),
    FOREIGN KEY (queue) REFERENCES queues(name)
  );

  CREATE INDEX IF NOT EXISTS idx_deliveries_queue_status
    ON deliveries(queue, status);

  CREATE INDEX IF NOT EXISTS idx_deliveries_status_retry
    ON deliveries(status, next_retry_at);
`);

function nowIso() {
  return new Date().toISOString();
}

function run(sql, params = {}) {
  return db.prepare(sql).run(params);
}

function get(sql, params = {}) {
  return db.prepare(sql).get(params);
}

function all(sql, params = {}) {
  return db.prepare(sql).all(params);
}

function ensureTopic(topic) {
  run(
    `
      INSERT OR IGNORE INTO topics (name, created_at)
      VALUES (:name, :created_at)
    `,
    {
      name: topic,
      created_at: nowIso(),
    }
  );
}

function ensureQueue(queue) {
  run(
    `
      INSERT OR IGNORE INTO queues (name, created_at)
      VALUES (:name, :created_at)
    `,
    {
      name: queue,
      created_at: nowIso(),
    }
  );
}

module.exports = {
  db,
  DB_PATH,
  run,
  get,
  all,
  ensureTopic,
  ensureQueue,
};
