# disumq

DisuMQ is a small TCP message broker built to understand the core ideas behind
systems like RabbitMQ.

It speaks newline-delimited JSON over TCP. Producers publish events to topics.
Consumers subscribe service queues to topics. The broker stores messages,
creates deliveries, sends them to workers, waits for `ACK` or `NACK`, retries
failed work, and moves exhausted deliveries to a dead letter queue.

This is a learning project, but it already models the important broker pieces:

- topic-based routing
- queue fan-out
- competing consumers per queue
- message delivery state
- `ACK` and `NACK`
- retry delay
- ACK timeout recovery
- consumer disconnect recovery
- dead letter queue replay
- SQLite persistence

## Requirements

- Node.js with `node:sqlite` support

This project currently uses Node's built-in `node:sqlite` module, so Node may
print an experimental SQLite warning when the broker starts.

## Setup

Clone or open the project, then start the broker:

```bash
cd /home/disu/Documents/Dev/disumq
node src/server.js
```

The broker listens on:

```text
127.0.0.1:7070
```

Connect with `telnet`:

```bash
telnet 127.0.0.1 7070
```

Or with `nc`:

```bash
nc 127.0.0.1 7070
```

Every command must be one JSON object followed by a newline.

## Mental Model

There are five main concepts.

**Topic**

A topic is the name of something that happened.

```text
order.created
payment.failed
user.registered
```

Producers publish to topics.

**Queue**

A queue is a service inbox.

```text
notification-service
inventory-service
analytics-service
```

Queues subscribe to topics. If three queues subscribe to `order.created`, one
published message creates one delivery for each queue.

**Message**

A message is the original event published by a producer.

```text
message_id: msg_001
topic: order.created
payload: { "order_id": "123" }
```

**Delivery**

A delivery is one queue's attempt to process a message.

```text
message msg_001
  -> delivery_001 for notification-service
  -> delivery_002 for inventory-service
  -> delivery_003 for analytics-service
```

Each delivery has its own state. One service may ACK its delivery while another
service retries or fails its own delivery.

**Consumer**

A consumer is a connected worker process. Multiple consumers can subscribe to
the same queue. They become competing consumers, so each delivery for that queue
goes to only one worker.

```text
inventory-service queue
  -> inventory-worker-1
  -> inventory-worker-2
  -> inventory-worker-3
```

## Delivery States

Deliveries move through these states:

```text
READY -> IN_FLIGHT -> ACKED
READY -> IN_FLIGHT -> NACK -> READY -> retry later
READY -> IN_FLIGHT -> too many failures -> FAILED -> DLQ
```

`READY` means the delivery is waiting in a queue.

`IN_FLIGHT` means the broker sent the delivery to a consumer and is waiting for
`ACK` or `NACK`.

`ACKED` means the consumer processed it successfully. The broker will not
redeliver it.

`FAILED` means it exceeded the maximum attempts and was moved to the dead letter
queue.

## Persistence

Broker state is persisted to SQLite at:

```text
data/disumq.sqlite
```

You can override the database path:

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

Connected TCP clients are not persisted. They are runtime state only. After a
restart, producers and consumers must reconnect.

On startup, stored `IN_FLIGHT` deliveries are recovered as `READY` because the
old consumer connection no longer exists.

## Quick Manual Test

Use three terminal windows.

Terminal 1 starts the broker:

```bash
cd /home/disu/Documents/Dev/disumq
node src/server.js
```

Terminal 2 is a consumer:

```bash
telnet 127.0.0.1 7070
```

Send:

```json
{"type":"CONNECT","client_id":"inventory-worker-1","role":"consumer"}
```

Then:

```json
{"type":"SUBSCRIBE","topic":"order.created","queue":"inventory-service"}
```

Terminal 3 is a producer:

```bash
telnet 127.0.0.1 7070
```

Send:

```json
{"type":"CONNECT","client_id":"producer-1","role":"producer"}
```

Then publish:

```json
{"type":"PUBLISH","topic":"order.created","payload":{"order_id":"123"}}
```

The consumer terminal should receive:

```json
{"type":"MESSAGE","delivery_id":"delivery_...","topic":"order.created","queue":"inventory-service","message_id":"msg_...","payload":{"order_id":"123"},"producer_id":"producer-1","published_at":"..."}
```

Copy the `delivery_id` from the consumer's `MESSAGE`, then ACK it from the same
consumer terminal:

```json
{"type":"ACK","delivery_id":"delivery_..."}
```

The broker responds:

```json
{"type":"ACKED","delivery_id":"delivery_...","message_id":"msg_...","queue":"inventory-service","status":"ACKED"}
```

## Commands

### CONNECT

Every TCP client must connect before sending other commands.

Producer:

```json
{"type":"CONNECT","client_id":"producer-1","role":"producer"}
```

Consumer:

```json
{"type":"CONNECT","client_id":"inventory-worker-1","role":"consumer"}
```

Response:

```json
{"type":"CONNECTED","message":"Client connected successfully","connectionId":"conn_...","clientId":"inventory-worker-1","role":"consumer"}
```

### PUBLISH

Only producers can publish.

```json
{"type":"PUBLISH","topic":"order.created","payload":{"order_id":"123"}}
```

The broker stores the original message, finds all queues subscribed to that
topic, creates one delivery per queue, and tries to dispatch each delivery.

Response:

```json
{"type":"PUBLISHED","message":"Message stored successfully","messageId":"msg_...","topic":"order.created","topicDepth":1,"deliveryCount":1,"deliveries":[{"deliveryId":"delivery_...","queue":"inventory-service","deliveredTo":"inventory-worker-1","status":"IN_FLIGHT"}],"publishedAt":"..."}
```

If `deliveredTo` is `null`, the delivery was created but no active consumer was
available for that queue. It stays `READY` until a consumer subscribes or the
delivery engine finds an available worker.

### SUBSCRIBE

Only consumers can subscribe.

```json
{"type":"SUBSCRIBE","topic":"order.created","queue":"inventory-service"}
```

This does two things:

- binds `inventory-service` queue to `order.created`
- registers the current TCP consumer as a worker for that queue

Response:

```json
{"type":"SUBSCRIBED","message":"Client subscribed successfully","topic":"order.created","queue":"inventory-service","queueCount":1,"subscriberCount":1}
```

If multiple workers subscribe to the same queue, the broker uses round-robin
delivery across those workers.

### MESSAGE

`MESSAGE` is sent by the broker to a consumer.

```json
{"type":"MESSAGE","delivery_id":"delivery_...","topic":"order.created","queue":"inventory-service","message_id":"msg_...","payload":{"order_id":"123"},"producer_id":"producer-1","published_at":"..."}
```

At this point the delivery is `IN_FLIGHT`. The consumer must send `ACK` or
`NACK`.

### ACK

`ACK` means the consumer processed the delivery successfully.

```json
{"type":"ACK","delivery_id":"delivery_..."}
```

Rules:

- the delivery must be `IN_FLIGHT`
- the same consumer that received the `MESSAGE` must send the `ACK`
- after ACK, the delivery becomes `ACKED`
- ACKed deliveries are not redelivered

Response:

```json
{"type":"ACKED","delivery_id":"delivery_...","message_id":"msg_...","queue":"inventory-service","status":"ACKED"}
```

### NACK

`NACK` means the consumer failed to process the delivery.

```json
{"type":"NACK","delivery_id":"delivery_...","error":"inventory service unavailable"}
```

Rules:

- the delivery must be `IN_FLIGHT`
- the same consumer that received the `MESSAGE` must send the `NACK`
- attempts increase by 1
- the delivery becomes `READY`
- the broker retries after 10 seconds
- after 5 failed attempts, the delivery becomes `FAILED` and moves to DLQ

Response:

```json
{"type":"NACKED","delivery_id":"delivery_...","message_id":"msg_...","queue":"inventory-service","status":"READY","attempts":1,"retry":true,"next_retry_at":"..."}
```

On the fifth failed attempt:

```json
{"type":"NACKED","delivery_id":"delivery_...","message_id":"msg_...","queue":"inventory-service","status":"FAILED","attempts":5,"retry":false,"next_retry_at":null}
```

### REPLAY_DLQ

After a delivery fails too many times, it goes to the dead letter queue.

Replay it later:

```json
{"type":"REPLAY_DLQ","delivery_id":"delivery_..."}
```

The broker removes the record from DLQ, resets the delivery attempts to `0`,
marks it `READY`, and tries to deliver it again.

Response:

```json
{"type":"DLQ_REPLAYED","delivery_id":"delivery_...","message_id":"msg_...","topic":"order.created","queue":"inventory-service","status":"READY"}
```

## Fan-Out Example

If three service queues subscribe to the same topic:

```json
{"type":"SUBSCRIBE","topic":"order.created","queue":"notification-service"}
{"type":"SUBSCRIBE","topic":"order.created","queue":"inventory-service"}
{"type":"SUBSCRIBE","topic":"order.created","queue":"analytics-service"}
```

Then one publish:

```json
{"type":"PUBLISH","topic":"order.created","payload":{"order_id":"123"}}
```

creates:

```text
msg_001
  -> delivery_001 for notification-service
  -> delivery_002 for inventory-service
  -> delivery_003 for analytics-service
```

That is pub-sub fan-out.

## Competing Consumers Example

If three workers subscribe to the same queue:

```json
{"type":"SUBSCRIBE","topic":"order.created","queue":"inventory-service"}
```

then each delivery for `inventory-service` goes to only one worker.

```text
delivery_001 -> inventory-worker-1
delivery_002 -> inventory-worker-2
delivery_003 -> inventory-worker-3
delivery_004 -> inventory-worker-1
```

That is competing consumer behavior.

## Delivery Engine

The broker starts a background delivery engine from `src/server.js`.

The engine runs every 1 second.

It:

- scans all queues that have deliveries
- finds `READY` deliveries whose `nextRetryAt` is due
- sends deliveries to active consumers
- marks sent deliveries as `IN_FLIGHT`
- checks stuck `IN_FLIGHT` deliveries
- requeues stuck deliveries after a 30 second ACK timeout

This protects the broker from consumers that hang without disconnecting.

Example:

```text
10:00:00 broker sends delivery_001
10:00:30 no ACK or NACK received
10:00:30 broker marks delivery_001 READY
10:00:31 delivery engine redelivers delivery_001
```

## Consumer Disconnects

If a consumer disconnects while it owns an `IN_FLIGHT` delivery, the broker:

1. detects the socket close
2. removes that consumer from queue subscriptions
3. finds its `IN_FLIGHT` deliveries
4. marks them `READY`
5. tries to redeliver them to another worker in the same queue

This means a worker can crash before sending `ACK`, and the message is not lost.

## Dead Letter Queue

The dead letter queue stores deliveries that could not be processed after 5
attempts.

DLQ records contain:

```json
{"delivery_id":"delivery_...","message_id":"msg_...","topic":"order.created","queue":"inventory-service","payload":{"order_id":"123"},"attempts":5,"last_error":"inventory service unavailable","failed_at":"..."}
```

The project currently stores DLQ records in SQLite but does not yet expose a
list command to inspect them over TCP. You can replay a known `delivery_id` with
`REPLAY_DLQ`.

## Project Structure

```text
src/server.js
  TCP server entrypoint. Accepts sockets, creates client records, parses
  newline-delimited JSON, and starts the delivery engine.

src/parser.js
  Buffers TCP chunks and emits full JSON commands line by line.

src/commands/handle.command.js
  Handles CONNECT, PUBLISH, SUBSCRIBE, ACK, NACK, REPLAY_DLQ, PING, DISCONNECT.

src/clients/clients.store.js
  Tracks connected TCP clients and their sockets.

src/subscriptions/subscriptions.store.js
  Tracks topic-to-queue bindings and active consumers per queue.

src/messages/messages.store.js
  Stores and persists messages, deliveries, delivery state transitions, retry
  attempts, ACK/NACK handling, DLQ, and startup recovery.

src/delivery/delivery.engine.js
  Background loop that dispatches READY deliveries and handles ACK timeouts.

src/db/sqlite.js
  SQLite schema and helper functions.

src/utils/response.js
  Writes JSON responses back to sockets.
```

## Recovery Rules

When the broker restarts:

- messages are loaded from SQLite
- deliveries are loaded from SQLite
- dead letter deliveries are loaded from SQLite
- topic-to-queue bindings are loaded from SQLite
- old `IN_FLIGHT` deliveries become `READY`
- TCP clients are not restored

Consumers must reconnect and subscribe again. Once they do, READY deliveries are
sent to them.

## Current Limitations

This is intentionally still small.

Current limitations:

- no authentication
- no TLS
- no SDK yet
- no admin API
- no list command for queues, messages, deliveries, or DLQ
- no deletion or purge command
- no horizontal broker clustering
- no publisher confirms beyond the `PUBLISHED` response
- no prefetch limit per consumer
- no durable consumer identity; connected workers are runtime-only

Good next improvements:

- `LIST_QUEUES`
- `LIST_DELIVERIES`
- `LIST_DLQ`
- queue purge command
- per-consumer prefetch
- Node SDK
- Dockerfile
- tests
