const crypto = require("crypto");

const clients = new Map();

function generateConnectionId() {
  return `conn_${crypto.randomUUID()}`;
}

function addClient(socket) {
  const now = new Date();
  const connectionId = generateConnectionId();

  clients.set(connectionId, {
    connectionId,
    clientId: null,
    role: "unknown",
    socket,
    remoteAddress: socket.remoteAddress,
    remotePort: socket.remotePort,
    localAddress: socket.localAddress,
    localPort: socket.localPort,
    connectedAt: now,
    lastSeenAt: now,
    subscriptions: [],
  });

  return connectionId;
}

function getClient(connectionId) {
  return clients.get(connectionId);
}

function updateLastSeen(connectionId) {
  const client = clients.get(connectionId);

  if (!client) return;

  client.lastSeenAt = new Date();
}

function markClientAsConnected(connectionId, clientId, role) {
  const client = clients.get(connectionId);

  if (!client) return null;

  client.clientId = clientId;
  client.role = role;
  client.lastSeenAt = new Date();

  return client;
}

function isClientConnected(connectionId) {
  const client = clients.get(connectionId);

  if (!client) return false;

  return Boolean(client.clientId);
}

function removeClient(connectionId) {
  clients.delete(connectionId);
}

function getClientCount() {
  return clients.size;
}

module.exports = {
  addClient,
  getClient,
  updateLastSeen,
  markClientAsConnected,
  isClientConnected,
  removeClient,
  getClientCount,
};