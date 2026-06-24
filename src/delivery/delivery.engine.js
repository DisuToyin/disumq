const {
  getQueueNamesWithDeliveries,
  requeueTimedOutDeliveries,
  ACK_TIMEOUT_MS,
} = require("../messages/messages.store");
const { dispatchReadyDeliveries } = require("../subscriptions/subscriptions.store");

const DELIVERY_ENGINE_INTERVAL_MS = 1_000;

let intervalId = null;

function runDeliveryEngineTick() {
  const timedOut = requeueTimedOutDeliveries();
  const queueNames = new Set(getQueueNamesWithDeliveries());

  for (const delivery of timedOut.requeuedDeliveries) {
    queueNames.add(delivery.queue);
  }

  for (const queueName of queueNames) {
    dispatchReadyDeliveries(queueName);
  }

  return {
    checkedQueueCount: queueNames.size,
    timedOutCount: timedOut.requeuedDeliveries.length,
    failedTimeoutCount: timedOut.failedDeliveries.length,
  };
}

function startDeliveryEngine() {
  if (intervalId) {
    return intervalId;
  }

  intervalId = setInterval(runDeliveryEngineTick, DELIVERY_ENGINE_INTERVAL_MS);

  console.log(
    `Delivery engine running every ${DELIVERY_ENGINE_INTERVAL_MS}ms with ${ACK_TIMEOUT_MS}ms ACK timeout`
  );

  return intervalId;
}

function stopDeliveryEngine() {
  if (!intervalId) {
    return;
  }

  clearInterval(intervalId);
  intervalId = null;
}

module.exports = {
  DELIVERY_ENGINE_INTERVAL_MS,
  runDeliveryEngineTick,
  startDeliveryEngine,
  stopDeliveryEngine,
};
