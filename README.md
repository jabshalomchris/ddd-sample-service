# Sample service: outbox, Kafka and idempotent consumers

This is a reference for every bounded context team. It shows, in a small running service, how to publish domain events reliably with the outbox library and how to consume them safely with the consumer library, on our stack (Kafka and PostgreSQL from the `ddd-demo` docker-compose).

`Reservation` is **only an example**. It is not one of our bounded contexts and makes no modelling decision for anyone. Look at the mechanics, not the model.

**Overview:** [how an event travels from one service to another](docs/outbox-flow.svg) (also as [PNG](docs/outbox-flow.png) for slides and Miro).

## 1. Run it

You need Java 21 and Docker. The libraries' Maven packages and the Outbox-Service image are **public** on gitlab.com; Maven finds them through the registries declared in `pom.xml`, no token needed.

Only if a download fails anyway (network, proxy), build the libraries from source once; Maven then finds them locally:

```bash
git clone https://gitlab.com/microservice-dungeon/services/outbox-lib.git
git clone https://gitlab.com/microservice-dungeon/services/consumer-lib.git
cd outbox-lib   && ./mvnw install -DskipTests && cd ..   # their tests need Docker; skipping them is fine here
cd consumer-lib && ./mvnw install -DskipTests && cd ..
```

First start the shared infrastructure, then the service on top of it:

```bash
# 1. in the ddd-demo repository: Kafka on localhost:9092, Kafka UI on localhost:8090, PostgreSQL on localhost:5432
docker compose up -d
```

The service only *writes* events into its outbox table. Something has to *send* them to Kafka. Pick one:

**A. Service and Outbox-Service in Docker (the real setup, also for the demo)**

```bash
# 2. in this repository: builds the service and starts it with its own Outbox-Service
docker compose up -d --build
```

Both containers join the ddd-demo network and reach `kafka:29092` and `postgres:5432` by name. Docker names that network after the folder the ddd-demo stack runs in; if it is not `ddd-demo`, set `DDD_NETWORK` (for example `myfolder_default`). Stop with `docker compose down`.

**A2. Service in your IDE, Outbox-Service in Docker (while working on the code)**

```bash
docker compose up -d --no-deps sample-outbox-service
./mvnw spring-boot:run                     # Windows: .\mvnw.cmd spring-boot:run
```

**B. Without the Outbox-Service (local development, demo fallback)**

```bash
./mvnw spring-boot:run -Dspring-boot.run.profiles=local-relay
# Windows PowerShell needs quotes: .\mvnw.cmd spring-boot:run "-Dspring-boot.run.profiles=local-relay"
```

The `local-relay` profile switches on `LocalOutboxRelay`, which does inside the service what the Outbox-Service does, with the same library calls and the same Kafka record format. Never use A and B at the same time: every partition must have exactly one reader.

### The demo page

Open **http://localhost:8080**. One page shows the whole path of an event:

1. **Reservations** (the aggregate): create, change and cancel with buttons.
2. **Outbox**: events written but not yet sent. The Outbox-Service deletes a row once Kafka has confirmed it, so this table is usually empty.
3. **Received events** and **Consumer's view**: what a neighbouring context processed (with each event's `sequence_id`) and the copy it built from the events alone.

The short ID next to each name (for example `#7b09`) lets you follow one reservation across the panels. Cancelling twice or entering a party of 13 shows the API's error (409 and 400).

To see the outbox at work, stop the `sample-outbox-service` container (or `kafka`) in Docker Desktop, change a reservation, and watch the event stay *waiting*. Start it again and the event goes out by itself.

The page (`static/index.html`) and its read-only `/demo/*` endpoints (`demo` package) exist only for showing the service. Do not copy them.

**Without Docker:** `./mvnw test -Dtest=DemoPageRunner -Ddemo.run=true` starts the service with an embedded PostgreSQL, an in-process Kafka and the local relay (every 4 s, so you can see events wait) on port 8080 for 30 minutes.

### Or from the command line

```bash
ID=$(curl -s -X POST localhost:8080/reservations -H 'Content-Type: application/json' \
  -d '{"guestName":"Ada","partySize":4,"reservedFor":"2026-11-06T19:00:00Z"}' | sed -E 's/.*"id":"([^"]+)".*/\1/')
curl -X PUT  localhost:8080/reservations/$ID/party-size   -H 'Content-Type: application/json' -d '{"partySize":6}'
curl -X POST localhost:8080/reservations/$ID/cancellation -H 'Content-Type: application/json' -d '{"reason":"ill"}'
```

Kafka UI, topic `bl.reservation.events.v1`: three messages, key = your reservation ID, all on one partition, with the standard headers (`event_id`, `sequence_id`, `event_type`, …). Topic `db.reservation.ecst.v1`: three `reservation.state-transfer` messages with the full state after each change.

The connection uses the same environment variables as the MSD services and the Outbox-Service (`POSTGRES_HOST`, `POSTGRES_PORT`, `POSTGRES_TABLE` (the database name), `POSTGRES_USER`, `POSTGRES_PASS`, `KAFKA_BOOTSTRAP_SERVERS`). The defaults fit the ddd-demo stack, so locally you set none of them.

Run the tests with `./mvnw test`. They need no Docker: real PostgreSQL 18 binaries and an in-process Kafka broker. (PostgreSQL refuses to start as root, so run them as a normal user.)

## 2. How an event travels

```
 Your service (one DB transaction)              Outbox-Service (own container, polls every 500 ms)
 ┌───────────────────────────────────────┐      ┌───────────────────────────────────────────┐
 │ reservation.cancel(...)               │      │ read new rows of event_outbox             │
 │ save reservation          ──┐         │      │ send to Kafka (key = aggregate ID)        │
 │ writeOutbox.write(event)  ──┴─ commit │─────▶│ wait until Kafka confirmed all of them    │
 └───────────────────────────────────────┘      │ delete the rows                           │
   outbox-lib-jpa                               └──────────────────────┬────────────────────┘
                                                                       ▼
 Neighbouring service (one DB transaction)      Kafka topic bl.reservation.events.v1 (3 partitions)
 ┌───────────────────────────────────────┐                             │
 │ consumer-lib: read headers, pick the  │◀────────────────────────────┘
 │   handler by event_type               │
 │ seen this event_id before? → skip     │
 │ revision older than view? → skip      │
 │ update own data          ── commit    │  offset committed after the DB commit
 └───────────────────────────────────────┘
```

Each change writes two rows: the business event for `bl.reservation.events.v1` and the reservation's full state for `db.reservation.ecst.v1` (section 3, rule 3). Both go through the same outbox.

The point of the outbox: the state change and the event are written **in the same transaction**. Either both happen or neither does. Sending to Kafka directly from the service would allow "saved but never published" and "published but rolled back".

## 3. Publishing events with the outbox library

### Setup (once per service)

1. **Registry and dependency.** Add the GitLab registry and `de.microservice-dungeon:outbox-lib-jpa` to `pom.xml` (see this repository's `pom.xml`). Use `outbox-lib-jpa`, never `outbox-lib-direct` for anything but quick experiments: direct mode **loses events** on any failure.
2. **Let Spring find the library's entity.** On the application class, list both your package and the library's: `@EnableJpaRepositories({"<your.package>", "de.microservicedungeon"})` and the same for `@EntityScan` (`SampleServiceApplication`).
3. **Create the outbox table yourself.** The library does not. Copy its script into a Flyway migration (`V2__use_outbox_lib.sql`). It lands in your service's own schema.
4. **Configure your topics.** List every topic you write to under `outbox.topics` (`application.yaml`). The library checks at startup that each one **exists in Kafka** and reads its partition count, so the topic must exist before the service starts. It needs the count because **the library picks the partition itself, when it writes to the outbox** (from the key), not Kafka when the event is sent. Here the service creates both topics itself (`MessagingConfig`, the ECST topic log-compacted); MSD creates all topics centrally instead (a one-time `kafka-init` container).
5. **Set `spring.jpa.hibernate.ddl-auto: none`.** With `validate`, the service does not start: the library's table uses `SMALLINT` columns that its entity maps as `Integer`. The library, the Outbox-Service and the MSD services all use `none`.
   The MSD services also set `spring.datasource.hikari.auto-commit: false`, `spring.jpa.open-in-view: false` and `reWriteBatchedInserts=true` in the JDBC URL. The sample does the same.
6. **Run one Outbox-Service per database**, pointed at your schema and your topics (`docker-compose.yaml`, service `sample-outbox-service`). Its topic and partition list must match your `outbox.topics` and the partition counts in Kafka: it reads only the partitions it is told to. Image: `registry.gitlab.com/microservice-dungeon/services/outbox-service:4.1` (or `latest`). Partitions are given as a list, e.g. `APP_OUTBOX_TOPICS_0_PARTITIONS: "0,1,2"`, as in MSD's [local-dev-env](https://gitlab.com/microservice-dungeon/infrastructure/deployments/local-dev-env/-/tree/master/services).

### Rules

1. **Never call `KafkaTemplate` from your service.** Record events in the aggregate, then call `WriteOutbox.write(...)` in the same transaction (`ReservationEventPublisher`). The library refuses to run outside a transaction (`Propagation.MANDATORY`).
2. **Implement `OutboxWritableEvent` once per service** (`ReservationOutboxEvent`):
   - `getKey()` = **the aggregate ID**. Same key, same partition, events of one aggregate stay in order. Without a key, events are spread randomly and arrive out of order.
   - `getEventType()` = `<entity>.<event-name>` in lowercase with dots and dashes, plus a version, e.g. `reservation.cancelled` v1. Event names are past-tense business facts (the exploration team's rule from the EventStorming). Raise the version when a payload changes incompatibly; never change a published schema.
   - `getEntityIdentifier()` = `reservation` + ID. Used for debugging.
   - `getPayload()` = the event as UTF-8 JSON bytes.
   - `getDelayInterval()` = empty. Delays break ordering and duplicate detection.
3. **Publish the current state as well (ECST), as the MSD services do.** Next to the business events on `bl.<aggregate>.events.v1`, write the aggregate's full state after every change to `db.<aggregate>.ecst.v1`, event type `<aggregate>.state-transfer` (`ReservationStateTransfer`). The topic is log-compacted and keyed by aggregate ID, so Kafka keeps the newest state per aggregate. A context that only needs "what does X look like now" reads this topic and needs no logic for every single event. Include the `revision`, so it can ignore an older state that arrives late. Write it once per transaction, after the events.
4. **Lock the aggregate.** The library numbers events when they are written, not when they commit, and asks that concurrent changes to one entity are serialised. An `@Version` field on the aggregate does that (`Reservation`).
5. **Give every event a `revision`** in its payload: 1 for the first event of an aggregate, +1 for each later one. Consumers use it to recognise late events.
6. **Document every event in AsyncAPI** (`docs/asyncapi.yaml`). The events you publish are your service's API. As in game-service, name each message after its `event_type` (`reservation.cancelled`) and put the version in the title (`Reservation Cancelled V1`).
7. **Keep your tables in your own schema**, including `event_outbox`. Reading another service's tables crosses a context boundary.

## 4. What the outbox guarantees, and what it does not

| Guaranteed | Not guaranteed |
|---|---|
| No event without the state change, and no state change without the event | **Exactly once.** If the Outbox-Service fails after sending but before deleting the rows, it sends them again. Delivery is *at least once*. |
| Events with the same key are published in the order they were written | Order across keys, partitions or topics |
| An event that cannot be sent stays in the outbox and is retried (backoff up to 30 s) | How fast: polling interval plus Kafka latency. One event that always fails blocks its whole topic until removed by hand. |
| | That anybody consumed it |

So every consumer must cope with **duplicates**, and should cope with **late or missing** events. That is exactly what the 3-point level of the implementation criterion asks for: *messages that arrive twice, out of order, or not at all are handled as a design concern.*

## 5. Consuming events with the consumer library

### Setup

1. Add the GitLab registry and `de.microservice-dungeon:consumer-lib` to `pom.xml`.
2. Records arrive as bytes: `spring.kafka.consumer.value-deserializer: org.apache.kafka.common.serialization.ByteArrayDeserializer`.
3. Write **one `EventSpecification` per event type** you need (`ReservationEventHandlers`): it names the `event_type` and version and processes the payload.
4. Register them in a `ProcessorChain` from the injected `KafkaProcessorChainFactory` and call `chain.process(record)` from a `@KafkaListener` method marked `@Transactional` (`ReservationEventListener`).
5. Put the topics you consume under `consumer.listener.<context>.topic` (pattern from robot-service), separate from the `outbox.topics` you publish to. Write the producer's topic name out, as its AsyncAPI states it.

Note that the library's README example uses `KafkaDefaultHeaders`; the class is `KafkaDefaultHeader`.

### Rules

1. **Skip duplicates by `event_id`, yourself.** Consumer-lib 1.0.0 reads the headers and picks the handler, but it does **not** detect duplicates or handle errors yet. Daniel Köllgen confirmed (03.11.2026) that the library is minimal and both are still work in progress, so plan without them. The sample does it in a base class (`IdempotentEventSpecification`): the `event_id` goes into a `processed_event` table in the same transaction as your own writes. If your handler fails, the mark rolls back too, and the redelivered event is handled again.
2. **Do not filter duplicates by a `sequence_id` high-watermark alone.** The library's docs and game-service's AsyncAPI suggest it, but the id is assigned when an event is *written*, not when it *commits*. Two transactions on different entities in the same partition can commit in the opposite order, and the Outbox-Service then publishes the lower id after the higher one. A watermark would silently drop that event; the `event_id` check never does.
3. **Skip late events by revision.** Keep the last revision you applied per aggregate and ignore anything not newer (`ReservationView`).
4. **Write your own DTOs from the producer's AsyncAPI file.** Never import another context's classes. Ignore fields you do not need (`@JsonIgnoreProperties(ignoreUnknown = true)`). Event types without a handler are skipped by the library, so producers can add new events without breaking you.
5. **Let exceptions propagate.** The library rethrows (leave `onError()` empty), the transaction rolls back, Spring Kafka retries twice and then moves the record to `<topic>.DLT` (`MessagingConfig`). That also catches records with missing or broken headers.
6. **The offset is committed after your database commit.** A crash in between causes a redelivery, which rule 1 handles.

## 6. Copying this into your service

| File | What to do |
|---|---|
| `pom.xml` (repositories, two library dependencies) | Copy |
| `V2__use_outbox_lib.sql` (`event_outbox` part) and `processed_event` from `V1__init.sql` | Copy into your migrations |
| `SampleServiceApplication` (`@EnableJpaRepositories`, `@EntityScan`) | Copy the two annotations |
| `application.yaml` (env variables, `outbox.topics`, `consumer.listener`, byte-array (de)serializers, `ddl-auto: none`, `auto-commit: false`) | Copy; change schema, topics and consumer group |
| `ReservationOutboxEvent`, `ReservationEventPublisher`, `ReservationStateTransfer` | Pattern: one per aggregate that publishes |
| `inbox/*` (`IdempotencyGuard`, `IdempotentEventSpecification`) | Copy as they are |
| `ReservationEventHandlers`, `ReservationEventListener` | Pattern for every event you consume |
| `shared/application/MessagingConfig` | Copy; rename the topics (keep `.compact()` on the ECST topic) |
| `docker-compose.yaml`, `Dockerfile` | Template: your service and its Outbox-Service as containers on the shared network |
| `docs/asyncapi.yaml` | Template for your own event API |
| `localrelay/*` | Only if you have no Outbox-Service locally. Remove it once you do. |
| `demo/*`, `static/index.html`, `DemoPageRunner` | Do not copy. For your own dummy client, build on your service's real API |

The packages follow the course's DDD package convention (one top-level package per aggregate, with `domain` and `application`). Check this against the starter platform the tactical team hands out on 06.11.; where they differ, theirs wins.

## 7. Tests that show the guarantees

`MessagingIntegrationTest` (real PostgreSQL 18, in-process Kafka, the real libraries):

| Test | Shows |
|---|---|
| `savingTheAggregateAlsoWritesTheEventToTheOutbox` | the library writes the event with key, type, version and payload |
| `rollbackRemovesTheAggregateAndItsEventTogether` | a failure after the save leaves neither behind |
| `outboxCannotBeWrittenOutsideATransaction` | the library refuses to run without a transaction |
| `eventsOfOneReservationShareAPartitionAndKeepTheirOrder` | one aggregate, one partition, written in order |
| `everyChangeAlsoWritesTheFullStateForTheCompactedTopic` | one `reservation.state-transfer` per change (none when nothing changed); the ECST topic is compacted |
| `relayPublishesAndTheConsumerFollows` | full path: confirm, change, cancel; view ends at revision 3; outbox rows deleted |
| `publishedRecordsCarryTheStandardHeaders` | records carry `event_id`, `sequence_id`, `event_type`, … |
| `eventsSentTwiceAfterARelayCrashAreHandledOnce` | the relay sends, "crashes" before deleting, sends again; each event is handled once |
| `lateEventDoesNotOverwriteNewerState` | revision 1 after revision 2 is ignored |
| `brokenMessagesGoToTheDeadLetterTopicAndDoNotBlockThePartition` | bad JSON and missing headers end in `.DLT`; the next event is processed |
| `unknownEventTypesAreSkipped` | a new event type does not break the consumer |

`ScheduledRelayTest` runs the walkthrough from section 1 over real HTTP with the relay on its schedule. `ReservationTest` covers the aggregate's own invariants.

The setup was also run end to end with the **real Outbox-Service 4.1.0** (from source, against the same embedded database and broker, configured exactly like the `sample-outbox-service` in `docker-compose.yaml`, with both topics): events were published and consumed; stopped, it left new events waiting in the outbox; restarted, it sent them by itself.
