const {
  getClient,
  updateLastSeen,
  markClientAsConnected,
  isClientConnected,
} = require("../clients/clients.store");

const { send } = require("../utils/response");
const {
  publishMessage,
  ackDelivery,
  nackDelivery,
} = require("../messages/messages.store");
const {
  subscribeClient,
  dispatchReadyDeliveries,
  routeMessageToQueues,
} = require("../subscriptions/subscriptions.store");

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

function getCommandTopic(command) {
  return command.topic || command.queue;
}

function handlePublish(socket, client, command) {
  const topic = getCommandTopic(command);
  const payload =
    command.payload !== undefined ? command.payload : command.message;

  if (client.role !== "producer") {
    return send(socket, {
      type: "ERROR",
      message: "Only producer clients can publish messages",
    });
  }

  if (!topic) {
    return send(socket, {
      type: "ERROR",
      message: "PUBLISH requires topic or queue",
    });
  }

  if (payload === undefined) {
    return send(socket, {
      type: "ERROR",
      message: "PUBLISH requires payload or message",
    });
  }

  const { message, topicDepth } = publishMessage(topic, payload, client.clientId);
  const deliveries = routeMessageToQueues(topic, message);

  return send(socket, {
    type: "PUBLISHED",
    message: "Message stored successfully",
    messageId: message.id,
    topic,
    topicDepth,
    deliveryCount: deliveries.length,
    deliveries: deliveries.map(({ delivery, deliveredTo }) => ({
      deliveryId: delivery.id,
      queue: delivery.queue,
      deliveredTo: deliveredTo ? deliveredTo.clientId : null,
      status: delivery.status,
    })),
    publishedAt: message.publishedAt,
  });
}

function handleSubscribe(socket, connectionId, client, command) {
  const topic = command.topic;
  const queue = command.queue;

  if (client.role !== "consumer") {
    return send(socket, {
      type: "ERROR",
      message: "Only consumer clients can subscribe to topics",
    });
  }

  if (!topic) {
    return send(socket, {
      type: "ERROR",
      message: "SUBSCRIBE requires topic",
    });
  }

  if (!queue) {
    return send(socket, {
      type: "ERROR",
      message: "SUBSCRIBE requires queue",
    });
  }

  const { queueCount, subscriberCount } = subscribeClient(
    topic,
    queue,
    connectionId
  );

  send(socket, {
    type: "SUBSCRIBED",
    message: "Client subscribed successfully",
    topic,
    queue,
    queueCount,
    subscriberCount,
  });

  dispatchReadyDeliveries(queue);
}

function getDeliveryId(command) {
  return command.delivery_id || command.deliveryId;
}

function handleAck(socket, connectionId, client, command) {
  const deliveryId = getDeliveryId(command);

  if (client.role !== "consumer") {
    return send(socket, {
      type: "ERROR",
      message: "Only consumer clients can ACK deliveries",
    });
  }

  if (!deliveryId) {
    return send(socket, {
      type: "ERROR",
      message: "ACK requires delivery_id",
    });
  }

  const result = ackDelivery(deliveryId, connectionId);

  if (!result.ok) {
    return send(socket, {
      type: "ERROR",
      message: result.error,
    });
  }

  send(socket, {
    type: "ACKED",
    delivery_id: result.delivery.id,
    message_id: result.delivery.messageId,
    queue: result.delivery.queue,
    status: result.delivery.status,
  });

  dispatchReadyDeliveries(result.delivery.queue);
}

function scheduleRetry(queue, retryDelayMs) {
  const retryTimer = setTimeout(() => {
    dispatchReadyDeliveries(queue);
  }, retryDelayMs);

  if (retryTimer.unref) {
    retryTimer.unref();
  }
}

function handleNack(socket, connectionId, client, command) {
  const deliveryId = getDeliveryId(command);

  if (client.role !== "consumer") {
    return send(socket, {
      type: "ERROR",
      message: "Only consumer clients can NACK deliveries",
    });
  }

  if (!deliveryId) {
    return send(socket, {
      type: "ERROR",
      message: "NACK requires delivery_id",
    });
  }

  const result = nackDelivery(deliveryId, connectionId);

  if (!result.ok) {
    return send(socket, {
      type: "ERROR",
      message: result.error,
    });
  }

  if (result.retry) {
    scheduleRetry(result.delivery.queue, result.retryDelayMs);
  }

  return send(socket, {
    type: "NACKED",
    delivery_id: result.delivery.id,
    message_id: result.delivery.messageId,
    queue: result.delivery.queue,
    status: result.delivery.status,
    attempts: result.delivery.attempts,
    retry: result.retry,
    next_retry_at: result.delivery.nextRetryAt,
  });
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

    case "PUBLISH": {
      return handlePublish(socket, client, command);
    }

    case "SUBSCRIBE": {
      return handleSubscribe(socket, connectionId, client, command);
    }

    case "ACK": {
      return handleAck(socket, connectionId, client, command);
    }

    case "NACK": {
      return handleNack(socket, connectionId, client, command);
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
