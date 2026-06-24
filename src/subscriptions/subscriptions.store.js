const { getClient } = require("../clients/clients.store");
const { send } = require("../utils/response");
const {
  createDelivery,
  getMessage,
  getReadyDeliveries,
  markDeliveryInFlight,
  requeueInFlightDeliveriesForConnection,
} = require("../messages/messages.store");

const topicBindings = new Map();
const queues = new Map();

function getTopicQueues(topic) {
  if (!topicBindings.has(topic)) {
    topicBindings.set(topic, new Set());
  }

  return topicBindings.get(topic);
}

function getQueue(queueName) {
  if (!queues.has(queueName)) {
    queues.set(queueName, {
      name: queueName,
      subscribers: [],
      nextSubscriberIndex: 0,
    });
  }

  return queues.get(queueName);
}

function subscribeClient(topic, queueName, connectionId) {
  const topicQueues = getTopicQueues(topic);
  const queue = getQueue(queueName);

  topicQueues.add(queueName);

  if (!queue.subscribers.includes(connectionId)) {
    queue.subscribers.push(connectionId);
  }

  return {
    queueCount: topicQueues.size,
    subscriberCount: queue.subscribers.length,
  };
}

function removeClientFromSubscriptions(connectionId) {
  const affectedQueues = new Set();

  for (const queue of queues.values()) {
    if (queue.subscribers.includes(connectionId)) {
      affectedQueues.add(queue.name);
    }

    queue.subscribers = queue.subscribers.filter(
      (subscriberId) => subscriberId !== connectionId
    );

    if (queue.nextSubscriberIndex >= queue.subscribers.length) {
      queue.nextSubscriberIndex = 0;
    }
  }

  const requeuedDeliveries = requeueInFlightDeliveriesForConnection(connectionId);

  for (const delivery of requeuedDeliveries) {
    affectedQueues.add(delivery.queue);
  }

  for (const queueName of affectedQueues) {
    dispatchReadyDeliveries(queueName);
  }
}

function getNextSubscriber(queue) {
  while (queue.subscribers.length > 0) {
    if (queue.nextSubscriberIndex >= queue.subscribers.length) {
      queue.nextSubscriberIndex = 0;
    }

    const connectionId = queue.subscribers[queue.nextSubscriberIndex];
    const client = getClient(connectionId);

    if (client && client.socket && !client.socket.destroyed) {
      queue.nextSubscriberIndex =
        (queue.nextSubscriberIndex + 1) % queue.subscribers.length;

      return client;
    }

    queue.subscribers.splice(queue.nextSubscriberIndex, 1);
  }

  return null;
}

function sendDeliveryToSubscriber(delivery, subscriber) {
  const message = getMessage(delivery.messageId);

  if (!message) {
    return null;
  }

  send(subscriber.socket, {
    type: "MESSAGE",
    delivery_id: delivery.id,
    topic: message.topic,
    queue: delivery.queue,
    message_id: message.id,
    payload: message.payload,
    producer_id: message.producerId,
    published_at: message.publishedAt,
  });

  markDeliveryInFlight(delivery.id, subscriber);

  return {
    delivery,
    deliveredTo: subscriber,
  };
}

function dispatchReadyDeliveries(queueName) {
  const queue = queues.get(queueName);

  if (!queue || queue.subscribers.length === 0) {
    return [];
  }

  const results = [];
  const readyDeliveries = getReadyDeliveries(queueName);

  for (const delivery of readyDeliveries) {
    const subscriber = getNextSubscriber(queue);

    if (!subscriber) {
      break;
    }

    const result = sendDeliveryToSubscriber(delivery, subscriber);

    if (result) {
      results.push(result);
    }
  }

  return results;
}

function routeMessageToQueues(topic, message) {
  const boundQueues = topicBindings.get(topic);

  if (!boundQueues || boundQueues.size === 0) {
    return [];
  }

  return Array.from(boundQueues).map((queueName) => {
    const delivery = createDelivery(message, queueName);
    dispatchReadyDeliveries(queueName);

    return {
      delivery,
      deliveredTo: delivery.deliveredTo
        ? {
            clientId: delivery.deliveredTo,
          }
        : null,
    };
  });
}

module.exports = {
  subscribeClient,
  removeClientFromSubscriptions,
  dispatchReadyDeliveries,
  routeMessageToQueues,
};
