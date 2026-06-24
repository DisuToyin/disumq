const crypto = require("crypto");

const messages = new Map();
const messagesByTopic = new Map();
const deliveries = new Map();
const deliveriesByQueue = new Map();
const deadLetterDeliveries = new Map();

const DELIVERY_STATUS = {
  READY: "READY",
  IN_FLIGHT: "IN_FLIGHT",
  ACKED: "ACKED",
  FAILED: "FAILED",
};

const MAX_DELIVERY_ATTEMPTS = 5;
const RETRY_DELAY_MS = 10_000;
const ACK_TIMEOUT_MS = 30_000;

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
    lastError: null,
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
  delivery.nextRetryAt = null;

  return delivery;
}

function setDeliveryDeliveredAt(deliveryId, deliveredAt) {
  const delivery = deliveries.get(deliveryId);

  if (!delivery) return null;

  delivery.deliveredAt = deliveredAt;

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
  delivery.lastError = null;

  return {
    ok: true,
    delivery,
  };
}

function nackDelivery(deliveryId, connectionId, options = {}) {
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

  delivery.nackedAt = new Date().toISOString();
  const result = retryDelivery(delivery, {
    lastError: options.lastError || "Consumer NACKed delivery",
  });

  return {
    ok: true,
    delivery,
    retry: result.retry,
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

function getQueueNamesWithDeliveries() {
  return Array.from(deliveriesByQueue.keys());
}

function createDeadLetterDelivery(delivery) {
  const message = messages.get(delivery.messageId);

  const deadLetterDelivery = {
    delivery_id: delivery.id,
    message_id: delivery.messageId,
    topic: delivery.topic,
    queue: delivery.queue,
    payload: message ? message.payload : null,
    attempts: delivery.attempts,
    last_error: delivery.lastError,
    failed_at: delivery.failedAt,
  };

  deadLetterDeliveries.set(delivery.id, deadLetterDelivery);

  return deadLetterDelivery;
}

function retryDelivery(delivery, options = {}) {
  const retryDelayMs =
    options.retryDelayMs === undefined ? RETRY_DELAY_MS : options.retryDelayMs;
  const lastError = options.lastError || "Delivery failed";

  delivery.attempts += 1;
  delivery.deliveredAt = null;
  delivery.deliveredConnectionId = null;
  delivery.deliveredTo = null;
  delivery.lastError = lastError;

  if (delivery.attempts >= MAX_DELIVERY_ATTEMPTS) {
    delivery.status = DELIVERY_STATUS.FAILED;
    delivery.failedAt = new Date().toISOString();
    delivery.nextRetryAt = null;
    createDeadLetterDelivery(delivery);

    return {
      retry: false,
      delivery,
    };
  }

  delivery.status = DELIVERY_STATUS.READY;
  delivery.nextRetryAt =
    retryDelayMs > 0
      ? new Date(Date.now() + retryDelayMs).toISOString()
      : null;

  return {
    retry: true,
    delivery,
  };
}

function requeueTimedOutDeliveries(now = new Date()) {
  const requeuedDeliveries = [];
  const failedDeliveries = [];

  for (const delivery of deliveries.values()) {
    if (
      delivery.status !== DELIVERY_STATUS.IN_FLIGHT ||
      !delivery.deliveredAt
    ) {
      continue;
    }

    const deliveredAt = new Date(delivery.deliveredAt);
    const timedOutAt = new Date(deliveredAt.getTime() + ACK_TIMEOUT_MS);

    if (timedOutAt > now) {
      continue;
    }

    const result = retryDelivery(delivery, {
      retryDelayMs: 0,
      lastError: "ACK timeout",
    });

    if (result.retry) {
      requeuedDeliveries.push(delivery);
    } else {
      failedDeliveries.push(delivery);
    }
  }

  return {
    requeuedDeliveries,
    failedDeliveries,
  };
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

function getDeadLetterDelivery(deliveryId) {
  return deadLetterDeliveries.get(deliveryId);
}

function replayDeadLetterDelivery(deliveryId) {
  const deadLetterDelivery = deadLetterDeliveries.get(deliveryId);
  const delivery = deliveries.get(deliveryId);

  if (!deadLetterDelivery || !delivery) {
    return {
      ok: false,
      error: "Dead letter delivery not found",
    };
  }

  delivery.status = DELIVERY_STATUS.READY;
  delivery.attempts = 0;
  delivery.deliveredAt = null;
  delivery.deliveredConnectionId = null;
  delivery.deliveredTo = null;
  delivery.ackedAt = null;
  delivery.nackedAt = null;
  delivery.failedAt = null;
  delivery.nextRetryAt = null;
  delivery.lastError = null;

  deadLetterDeliveries.delete(deliveryId);

  return {
    ok: true,
    delivery,
    deadLetterDelivery,
  };
}

module.exports = {
  DELIVERY_STATUS,
  MAX_DELIVERY_ATTEMPTS,
  RETRY_DELAY_MS,
  ACK_TIMEOUT_MS,
  publishMessage,
  createDelivery,
  getMessage,
  getDelivery,
  markDeliveryInFlight,
  setDeliveryDeliveredAt,
  ackDelivery,
  nackDelivery,
  getReadyDeliveries,
  getQueueNamesWithDeliveries,
  requeueTimedOutDeliveries,
  requeueInFlightDeliveriesForConnection,
  getDeadLetterDelivery,
  replayDeadLetterDelivery,
  getTopicDepth,
  getQueueDeliveryDepth,
};
