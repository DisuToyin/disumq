const {
  getClient,
  updateLastSeen,
  markClientAsConnected,
  isClientConnected,
} = require("../clients/clients.store");

const { send } = require("../utils/response");

const VALID_ROLES = ["producer", "consumer", "unknown"];

function handleConnect(socket, connectionId, command) {
  const clientId = command.client_id;
  const role = command.role || "unknown";

  if (!clientId) {
    return send(socket, {
      type: "ERROR",
      message: "CONNECT requires client_id",
    });
  }

  if (!VALID_ROLES.includes(role)) {
    return send(socket, {
      type: "ERROR",
      message: "Invalid role. Role must be producer, consumer, or unknown",
    });
  }

  const client = markClientAsConnected(connectionId, clientId, role);

  return send(socket, {
    type: "CONNECTED",
    message: "Client connected successfully",
    connectionId: client.connectionId,
    clientId: client.clientId,
    role: client.role,
  });
}

function handlePing(socket) {
  return send(socket, {
    type: "PONG",
    message: "Server is alive",
    timestamp: new Date().toISOString(),
  });
}

function handleDisconnect(socket) {
  send(socket, {
    type: "DISCONNECTED",
    message: "Client disconnected successfully",
  });

  socket.end();
}

function handleCommand(socket, connectionId, command) {
  updateLastSeen(connectionId);

  const client = getClient(connectionId);

  if (!client) {
    return send(socket, {
      type: "ERROR",
      message: "Unknown connection",
    });
  }

  if (!command.type) {
    return send(socket, {
      type: "ERROR",
      message: "Command type is required",
    });
  }

  /**
   * Important broker rule:
   * CONNECT is the only command allowed before authentication/registration.
   */
  if (command.type !== "CONNECT" && !isClientConnected(connectionId)) {
    return send(socket, {
      type: "ERROR",
      message: "Client must CONNECT before sending commands",
    });
  }

  switch (command.type) {
    case "CONNECT": {
      return handleConnect(socket, connectionId, command);
    }

    case "PING": {
      return handlePing(socket);
    }

    case "DISCONNECT": {
      return handleDisconnect(socket);
    }

    default: {
      return send(socket, {
        type: "ERROR",
        message: `Unknown command type: ${command.type}`,
      });
    }
  }
}

module.exports = {
  handleCommand,
};