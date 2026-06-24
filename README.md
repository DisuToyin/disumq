# disumq

Mini TCP message broker.

Commands are newline-delimited JSON.

## Persistence

Broker state is persisted to SQLite at:

```text
data/disumq.sqlite
```

You can override the database path with:

```bash
DISUMQ_DB_PATH=/path/to/disumq.sqlite node src/server.js
```

The broker persists:

- topics
- queues
- topic-to-queue bindings
- messages
- deliveries
- dead letter deliveries

Connected TCP consumers are not persisted. They are runtime state and must
reconnect after a restart.

On startup, stored `IN_FLIGHT` deliveries are recovered as `READY` because the
old consumer connection no longer exists.

## Publish

Connect as a producer first:

```json
{"type":"CONNECT","client_id":"producer-1","role":"producer"}
```

Then publish an event to a topic:

```json
{"type":"PUBLISH","topic":"order.created","payload":{"orderId":"ord_001"}}
```

The broker stores the original message, creates one delivery for each queue
subscribed to that topic, and responds with:

```json
{"type":"PUBLISHED","message":"Message stored successfully","messageId":"msg_...","topic":"order.created","topicDepth":1,"deliveryCount":3,"deliveries":[{"deliveryId":"delivery_...","queue":"notification-service","deliveredTo":"notification-worker-1","status":"IN_FLIGHT"}],"publishedAt":"..."}
```

## Subscribe

Connect as a consumer first:

```json
{"type":"CONNECT","client_id":"worker-1","role":"consumer"}
```

Then subscribe a service queue to a topic:

```json
{"type":"SUBSCRIBE","topic":"order.created","queue":"notification-service"}
```

The broker responds with:

```json
{"type":"SUBSCRIBED","message":"Client subscribed successfully","topic":"order.created","queue":"notification-service","queueCount":1,"subscriberCount":1}
```

When a producer publishes to that topic, each subscribed queue gets one delivery:

```text
order.created
  -> notification-service queue
  -> inventory-service queue
  -> analytics-service queue
```

One worker from each queue receives its queue's delivery:

```json
{"type":"MESSAGE","delivery_id":"delivery_...","topic":"order.created","queue":"notification-service","message_id":"msg_...","payload":{"orderId":"ord_001"},"producer_id":"producer-1","published_at":"..."}
```

If three workers subscribe to the same queue, each queue delivery is handled by
only one of those workers using round-robin delivery.

## ACK

After a consumer successfully processes a message, it should acknowledge the
delivery:

```json
{"type":"ACK","delivery_id":"delivery_..."}
```

The broker confirms that the same consumer owns the in-flight delivery, marks it
as `ACKED`, and does not redeliver it:

```json
{"type":"ACKED","delivery_id":"delivery_...","message_id":"msg_...","queue":"notification-service","status":"ACKED"}
```

## NACK

If a consumer fails to process a message, it should negatively acknowledge the
delivery:

```json
{"type":"NACK","delivery_id":"delivery_..."}
```

The broker confirms ownership, increases the attempt count, marks the delivery
as `READY`, and retries it after 10 seconds:

```json
{"type":"NACKED","delivery_id":"delivery_...","message_id":"msg_...","queue":"notification-service","status":"READY","attempts":1,"retry":true,"next_retry_at":"..."}
```

For the MVP, deliveries retry after 10 seconds and fail after 5 NACK attempts.

## Dead Letter Queue

After 5 failed attempts, the broker stops retrying a delivery and moves it to
the dead letter queue.

A DLQ record stores:

```json
{"delivery_id":"delivery_...","message_id":"msg_...","topic":"order.created","queue":"notification-service","payload":{"orderId":"ord_001"},"attempts":5,"last_error":"email provider unavailable","failed_at":"..."}
```

Replay a dead letter delivery later:

```json
{"type":"REPLAY_DLQ","delivery_id":"delivery_..."}
```

The broker removes it from the DLQ, resets it to `READY`, and tries to deliver
it again:

```json
{"type":"DLQ_REPLAYED","delivery_id":"delivery_...","message_id":"msg_...","topic":"order.created","queue":"notification-service","status":"READY"}
```

## Delivery engine

The broker runs a background delivery engine every 1 second.

The engine:

- scans queues for `READY` deliveries
- checks whether delayed retries are now available
- sends each delivery to an active consumer in the delivery's queue
- marks sent deliveries as `IN_FLIGHT`
- requeues stuck `IN_FLIGHT` deliveries after a 30 second ACK timeout

## Consumer disconnects

If a consumer disconnects while it owns an `IN_FLIGHT` delivery, the broker
marks that delivery as `READY` again and tries to send it to another active
consumer in the same queue.

That means a worker can crash before sending `ACK`, and the message is not lost.

The same protection applies when a consumer stays connected but never sends
`ACK` or `NACK`: after 30 seconds, the delivery becomes retryable again.
