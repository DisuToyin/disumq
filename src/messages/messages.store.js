const crypto = require("crypto");
const {
  all,
  ensureQueue,
  ensureTopic,
  run,
} = require("../db/sqlite");

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

function serializePayload(payload) {
  return JSON.stringify(payload);
}

function deserializePayload(payloadJson) {
  return JSON.parse(payloadJson);
}

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
  ensureTopic(topic);

  const message = {
    id: `msg_${crypto.randomUUID()}`,
    topic,
    payload,
    producerId,
    publishedAt: now,
  };

  messages.set(message.id, message);
  getTopicMessages(topic).push(message.id);
  persistMessage(message);

  return {
    message,
    topicDepth: getTopicMessages(topic).length,
  };
}

function createDelivery(message, queueName) {
  const now = new Date().toISOString();
  ensureTopic(message.topic);
  ensureQueue(queueName);

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
  persistDelivery(delivery);

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
  persistDelivery(delivery);

  return delivery;
}

function setDeliveryDeliveredAt(deliveryId, deliveredAt) {
  const delivery = deliveries.get(deliveryId);

  if (!delivery) return null;

  delivery.deliveredAt = deliveredAt;
  persistDelivery(delivery);

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
  persistDelivery(delivery);

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
  persistDeadLetterDelivery(deadLetterDelivery);

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
    persistDelivery(delivery);

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
  persistDelivery(delivery);

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
      persistDelivery(delivery);
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
  deleteDeadLetterDelivery(deliveryId);
  persistDelivery(delivery);

  return {
    ok: true,
    delivery,
    deadLetterDelivery,
  };
}

function persistMessage(message) {
  ensureTopic(message.topic);

  run(
    `
      INSERT INTO messages (
        id,
        topic,
        payload_json,
        producer_id,
        published_at
      )
      VALUES (
        :id,
        :topic,
        :payload_json,
        :producer_id,
        :published_at
      )
      ON CONFLICT(id) DO UPDATE SET
        topic = excluded.topic,
        payload_json = excluded.payload_json,
        producer_id = excluded.producer_id,
        published_at = excluded.published_at
    `,
    {
      id: message.id,
      topic: message.topic,
      payload_json: serializePayload(message.payload),
      producer_id: message.producerId,
      published_at: message.publishedAt,
    }
  );
}

function persistDelivery(delivery) {
  ensureTopic(delivery.topic);
  ensureQueue(delivery.queue);

  run(
    `
      INSERT INTO deliveries (
        id,
        message_id,
        topic,
        queue,
        status,
        attempts,
        created_at,
        delivered_at,
        delivered_connection_id,
        delivered_to,
        acked_at,
        nacked_at,
        failed_at,
        next_retry_at,
        last_error
      )
      VALUES (
        :id,
        :message_id,
        :topic,
        :queue,
        :status,
        :attempts,
        :created_at,
        :delivered_at,
        :delivered_connection_id,
        :delivered_to,
        :acked_at,
        :nacked_at,
        :failed_at,
        :next_retry_at,
        :last_error
      )
      ON CONFLICT(id) DO UPDATE SET
        message_id = excluded.message_id,
        topic = excluded.topic,
        queue = excluded.queue,
        status = excluded.status,
        attempts = excluded.attempts,
        created_at = excluded.created_at,
        delivered_at = excluded.delivered_at,
        delivered_connection_id = excluded.delivered_connection_id,
        delivered_to = excluded.delivered_to,
        acked_at = excluded.acked_at,
        nacked_at = excluded.nacked_at,
        failed_at = excluded.failed_at,
        next_retry_at = excluded.next_retry_at,
        last_error = excluded.last_error
    `,
    {
      id: delivery.id,
      message_id: delivery.messageId,
      topic: delivery.topic,
      queue: delivery.queue,
      status: delivery.status,
      attempts: delivery.attempts,
      created_at: delivery.createdAt,
      delivered_at: delivery.deliveredAt,
      delivered_connection_id: delivery.deliveredConnectionId,
      delivered_to: delivery.deliveredTo,
      acked_at: delivery.ackedAt,
      nacked_at: delivery.nackedAt,
      failed_at: delivery.failedAt,
      next_retry_at: delivery.nextRetryAt,
      last_error: delivery.lastError,
    }
  );
}

function persistDeadLetterDelivery(deadLetterDelivery) {
  ensureTopic(deadLetterDelivery.topic);
  ensureQueue(deadLetterDelivery.queue);

  run(
    `
      INSERT INTO dead_letter_deliveries (
        delivery_id,
        message_id,
        topic,
        queue,
        payload_json,
        attempts,
        last_error,
        failed_at
      )
      VALUES (
        :delivery_id,
        :message_id,
        :topic,
        :queue,
        :payload_json,
        :attempts,
        :last_error,
        :failed_at
      )
      ON CONFLICT(delivery_id) DO UPDATE SET
        message_id = excluded.message_id,
        topic = excluded.topic,
        queue = excluded.queue,
        payload_json = excluded.payload_json,
        attempts = excluded.attempts,
        last_error = excluded.last_error,
        failed_at = excluded.failed_at
    `,
    {
      delivery_id: deadLetterDelivery.delivery_id,
      message_id: deadLetterDelivery.message_id,
      topic: deadLetterDelivery.topic,
      queue: deadLetterDelivery.queue,
      payload_json: serializePayload(deadLetterDelivery.payload),
      attempts: deadLetterDelivery.attempts,
      last_error: deadLetterDelivery.last_error,
      failed_at: deadLetterDelivery.failed_at,
    }
  );
}

function deleteDeadLetterDelivery(deliveryId) {
  run(
    `
      DELETE FROM dead_letter_deliveries
      WHERE delivery_id = :delivery_id
    `,
    {
      delivery_id: deliveryId,
    }
  );
}

function hydrateMessages() {
  const rows = all(`
    SELECT id, topic, payload_json, producer_id, published_at
    FROM messages
    ORDER BY published_at ASC
  `);

  for (const row of rows) {
    const message = {
      id: row.id,
      topic: row.topic,
      payload: deserializePayload(row.payload_json),
      producerId: row.producer_id,
      publishedAt: row.published_at,
    };

    messages.set(message.id, message);
    getTopicMessages(message.topic).push(message.id);
  }
}

function hydrateDeliveries() {
  const rows = all(`
    SELECT
      id,
      message_id,
      topic,
      queue,
      status,
      attempts,
      created_at,
      delivered_at,
      delivered_connection_id,
      delivered_to,
      acked_at,
      nacked_at,
      failed_at,
      next_retry_at,
      last_error
    FROM deliveries
    ORDER BY created_at ASC
  `);

  for (const row of rows) {
    const delivery = {
      id: row.id,
      messageId: row.message_id,
      topic: row.topic,
      queue: row.queue,
      status: row.status,
      attempts: row.attempts,
      createdAt: row.created_at,
      deliveredAt: row.delivered_at,
      deliveredConnectionId: row.delivered_connection_id,
      deliveredTo: row.delivered_to,
      ackedAt: row.acked_at,
      nackedAt: row.nacked_at,
      failedAt: row.failed_at,
      nextRetryAt: row.next_retry_at,
      lastError: row.last_error,
    };

    if (delivery.status === DELIVERY_STATUS.IN_FLIGHT) {
      delivery.status = DELIVERY_STATUS.READY;
      delivery.deliveredAt = null;
      delivery.deliveredConnectionId = null;
      delivery.deliveredTo = null;
      delivery.nextRetryAt = null;
      persistDelivery(delivery);
    }

    deliveries.set(delivery.id, delivery);
    getQueueDeliveries(delivery.queue).push(delivery.id);
  }
}

function hydrateDeadLetterDeliveries() {
  const rows = all(`
    SELECT
      delivery_id,
      message_id,
      topic,
      queue,
      payload_json,
      attempts,
      last_error,
      failed_at
    FROM dead_letter_deliveries
    ORDER BY failed_at ASC
  `);

  for (const row of rows) {
    deadLetterDeliveries.set(row.delivery_id, {
      delivery_id: row.delivery_id,
      message_id: row.message_id,
      topic: row.topic,
      queue: row.queue,
      payload: row.payload_json ? deserializePayload(row.payload_json) : null,
      attempts: row.attempts,
      last_error: row.last_error,
      failed_at: row.failed_at,
    });
  }
}

function hydrateFromDatabase() {
  hydrateMessages();
  hydrateDeliveries();
  hydrateDeadLetterDeliveries();
}

hydrateFromDatabase();

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
