const crypto = require("crypto");

const messages = new Map();
const messagesByTopic = new Map();
const deliveries = new Map();
const deliveriesByQueue = new Map();

const DELIVERY_STATUS = {
  READY: "READY",
  IN_FLIGHT: "IN_FLIGHT",
  ACKED: "ACKED",
  FAILED: "FAILED",
};

const MAX_DELIVERY_ATTEMPTS = 5;
const RETRY_DELAY_MS = 10_000;

function getTopicMessages(topic) {
  if (!messagesByTopic.has(topic)) {
    messagesByTopic.set(topic, []);
  }

  return messagesByTopic.get(topic);
}

function getQueueDeliveries(queueName) {
  if (!deliveriesByQueue.has(queueName)) {
    deliveriesByQueue.set(queueName, []);
  }

  return deliveriesByQueue.get(queueName);
}

function publishMessage(topic, payload, producerId) {
  const now = new Date().toISOString();

  const message = {
    id: `msg_${crypto.randomUUID()}`,
    topic,
    payload,
    producerId,
    publishedAt: now,
  };

  messages.set(message.id, message);
  getTopicMessages(topic).push(message.id);

  return {
    message,
    topicDepth: getTopicMessages(topic).length,
  };
}

function createDelivery(message, queueName) {
  const now = new Date().toISOString();

  const delivery = {
    id: `delivery_${crypto.randomUUID()}`,
    messageId: message.id,
    topic: message.topic,
    queue: queueName,
    status: DELIVERY_STATUS.READY,
    attempts: 0,
    createdAt: now,
    deliveredAt: null,
    deliveredConnectionId: null,
    deliveredTo: null,
    ackedAt: null,
    nackedAt: null,
    failedAt: null,
    nextRetryAt: null,
  };

  deliveries.set(delivery.id, delivery);
  getQueueDeliveries(queueName).push(delivery.id);

  return delivery;
}

function getMessage(messageId) {
  return messages.get(messageId);
}

function getDelivery(deliveryId) {
  return deliveries.get(deliveryId);
}

function markDeliveryInFlight(deliveryId, client) {
  const delivery = deliveries.get(deliveryId);

  if (!delivery) return null;

  delivery.status = DELIVERY_STATUS.IN_FLIGHT;
  delivery.deliveredAt = new Date().toISOString();
  delivery.deliveredConnectionId = client.connectionId;
  delivery.deliveredTo = client.clientId;

  return delivery;
}

function ackDelivery(deliveryId, connectionId) {
  const delivery = deliveries.get(deliveryId);

  if (!delivery) {
    return {
      ok: false,
      error: "Delivery not found",
    };
  }

  if (delivery.status !== DELIVERY_STATUS.IN_FLIGHT) {
    return {
      ok: false,
      error: `Delivery is not in flight. Current status: ${delivery.status}`,
    };
  }

  if (delivery.deliveredConnectionId !== connectionId) {
    return {
      ok: false,
      error: "Delivery is owned by another consumer",
    };
  }

  delivery.status = DELIVERY_STATUS.ACKED;
  delivery.ackedAt = new Date().toISOString();
  delivery.nextRetryAt = null;

  return {
    ok: true,
    delivery,
  };
}

function nackDelivery(deliveryId, connectionId) {
  const delivery = deliveries.get(deliveryId);

  if (!delivery) {
    return {
      ok: false,
      error: "Delivery not found",
    };
  }

  if (delivery.status !== DELIVERY_STATUS.IN_FLIGHT) {
    return {
      ok: false,
      error: `Delivery is not in flight. Current status: ${delivery.status}`,
    };
  }

  if (delivery.deliveredConnectionId !== connectionId) {
    return {
      ok: false,
      error: "Delivery is owned by another consumer",
    };
  }

  delivery.attempts += 1;
  delivery.nackedAt = new Date().toISOString();
  delivery.deliveredAt = null;
  delivery.deliveredConnectionId = null;
  delivery.deliveredTo = null;

  if (delivery.attempts >= MAX_DELIVERY_ATTEMPTS) {
    delivery.status = DELIVERY_STATUS.FAILED;
    delivery.failedAt = new Date().toISOString();
    delivery.nextRetryAt = null;

    return {
      ok: true,
      delivery,
      retry: false,
    };
  }

  delivery.status = DELIVERY_STATUS.READY;
  delivery.nextRetryAt = new Date(Date.now() + RETRY_DELAY_MS).toISOString();

  return {
    ok: true,
    delivery,
    retry: true,
    retryDelayMs: RETRY_DELAY_MS,
  };
}

function getReadyDeliveries(queueName, now = new Date()) {
  return getQueueDeliveries(queueName)
    .map((deliveryId) => deliveries.get(deliveryId))
    .filter((delivery) => {
      if (!delivery || delivery.status !== DELIVERY_STATUS.READY) {
        return false;
      }

      if (!delivery.nextRetryAt) {
        return true;
      }

      return new Date(delivery.nextRetryAt) <= now;
    });
}

function requeueInFlightDeliveriesForConnection(connectionId) {
  const requeuedDeliveries = [];

  for (const delivery of deliveries.values()) {
    if (
      delivery.status === DELIVERY_STATUS.IN_FLIGHT &&
      delivery.deliveredConnectionId === connectionId
    ) {
      delivery.status = DELIVERY_STATUS.READY;
      delivery.deliveredAt = null;
      delivery.deliveredConnectionId = null;
      delivery.deliveredTo = null;
      delivery.nextRetryAt = null;
      requeuedDeliveries.push(delivery);
    }
  }

  return requeuedDeliveries;
}

function getTopicDepth(topic) {
  return getTopicMessages(topic).length;
}

function getQueueDeliveryDepth(queueName) {
  return getQueueDeliveries(queueName).length;
}

module.exports = {
  DELIVERY_STATUS,
  MAX_DELIVERY_ATTEMPTS,
  RETRY_DELAY_MS,
  publishMessage,
  createDelivery,
  getMessage,
  getDelivery,
  markDeliveryInFlight,
  ackDelivery,
  nackDelivery,
  getReadyDeliveries,
  requeueInFlightDeliveriesForConnection,
  getTopicDepth,
  getQueueDeliveryDepth,
};
