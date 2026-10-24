# Sample service: outbox, Kafka and idempotent consumers

> **Draft 1, 22.10.2026.** Architecture team, workstream 2 (helper libraries: usage guide, sample service, outbox).
> Lead: Judah. Newcomer read: Dongxin. ADR on outbox and library use: Eldar.

This is a reference for every bounded context team. It shows, in a small running service, how to publish domain events reliably and how to consume them safely on our stack (Kafka and PostgreSQL from the `ddd-demo` docker-compose).

Two things to know before you copy anything:

- **`Reservation` is only an example.** It is not one of our bounded contexts and makes no modelling decision for anyone. Look at the mechanics, not the model.
- **`outbox` and `inbox` are stand-ins.** The supervisors provide helper libraries for the outbox and for Kafka producers and consumers. When we have them, those packages are replaced. The rules in this guide stay the same.

## 1. Run it

You need Java 21 and Docker.

```bash
# in the ddd-demo repository
docker compose up -d          # Kafka on localhost:9092, Kafka UI on localhost:8090, PostgreSQL on localhost:5432

# in this repository
./mvnw spring-boot:run        # service on localhost:8080
```

Create, change and cancel a reservation:

```bash
ID=$(curl -s -X POST localhost:8080/reservations -H 'Content-Type: application/json' \
  -d '{"guestName":"Ada","partySize":4,"reservedFor":"2026-11-06T19:00:00Z"}' | sed -E 's/.*"id":"([^"]+)".*/\1/')

curl -X PUT  localhost:8080/reservations/$ID/party-size   -H 'Content-Type: application/json' -d '{"partySize":6}'
curl -X POST localhost:8080/reservations/$ID/cancellation -H 'Content-Type: application/json' -d '{"reason":"ill"}'
```

Then look at:

- **Kafka UI** (http://localhost:8090), topic `sample.reservation-events`: three messages, all with your reservation ID as key, on the same partition, with `eventId` and `eventType` headers.
- **PostgreSQL**, schema `sample_service`:
  - `outbox_message`: three rows, each with `published_at` set.
  - `reservation_view`: one row, status `CANCELLED`, revision 3. The consumer built it from the events alone.

Run the tests with `./mvnw test`. They need no Docker: they use real PostgreSQL 18 binaries and an in-process Kafka broker. (PostgreSQL refuses to start as root, so run them as a normal user.)

## 2. How an event travels

```
 ReservationService (one DB transaction)                 OutboxRelay (every 500 ms)
 ┌──────────────────────────────────────┐               ┌──────────────────────────┐
 │ reservation.cancel(...)               │               │ lock oldest unpublished  │
 │ save reservation          ──┐         │               │ send to Kafka, key =     │
 │ append event to outbox    ──┴─ commit │──────────────▶│   aggregate ID, wait ack │
 └──────────────────────────────────────┘               │ set published_at, commit │
                                                         └────────────┬─────────────┘
                                                                      ▼
 ReservationEventListener (one DB transaction)            Kafka topic, 3 partitions
 ┌──────────────────────────────────────┐               ┌──────────────────────────┐
 │ seen this eventId before? → skip      │◀──────────────│ sample.reservation-events│
 │ revision older than view? → skip      │               └──────────────────────────┘
 │ update own view           ── commit   │
 └──────────────────────────────────────┘
   offset committed after the DB commit
```

The point of the outbox: the state change and the event are written **in the same transaction**. Either both happen or neither does. Sending to Kafka directly from the service would allow "saved but never published" and "published but rolled back".

## 3. Publishing events: rules

1. **Never call `KafkaTemplate` from your domain or application service.** Record events in the aggregate, then write them to the outbox in the same transaction (`ReservationEventPublisher`). `OutboxWriter` uses `Propagation.MANDATORY`, so calling it outside a transaction fails immediately instead of silently breaking the guarantee.
2. **Use the aggregate ID as the message key.** Kafka keeps order only within a partition, and the key decides the partition. Same key, same partition, events arrive in the order they happened. Without a key, two events of one aggregate can overtake each other. Our topics have 3 partitions on purpose, so this shows up locally and not only later.
3. **Give every event a `revision`.** 1 for the first event of an aggregate, +1 for each later one. Consumers use it to recognise late events. (The JPA `@Version` field is for optimistic locking and is not used for this.)
4. **Name events as past-tense business facts** (`ReservationCancelled`, not `CancelReservation` or `ButtonClicked`). That is the exploration team's rule from the EventStorming, and the event type is part of your API.
5. **Document every event in AsyncAPI** (`docs/asyncapi.yaml`). The events you publish are your service's API.
6. **Keep your tables in your own schema.** We share one PostgreSQL instance, but each service sets `currentSchema` in its JDBC URL and only touches its own schema. Reading another service's tables crosses a context boundary.

## 4. What the outbox guarantees, and what it does not

| Guaranteed | Not guaranteed |
|---|---|
| No event without the state change, and no state change without the event | **Exactly once.** Delivery is *at least once*: if the service crashes after Kafka acknowledged a message but before `published_at` is committed, the message is sent again. |
| Events of one aggregate are published in the order they were written | Order *across* aggregates |
| An event that Kafka could not take is retried in the next run, and later events wait behind it | How fast: up to one relay interval (500 ms) plus Kafka latency |
| | That anybody consumed it |

So every consumer must cope with **duplicates**, and should cope with **late or missing** events. That is exactly what the 3-point level of the implementation criterion asks for: *messages that arrive twice, out of order, or not at all are handled as a design concern.*

## 5. Consuming events: rules

1. **Skip duplicates by event ID**, in the same transaction as your own writes (`IdempotencyGuard`). If your handler fails, the "processed" mark rolls back too, and the redelivered event is handled again. If you mark it in a separate transaction, a failure loses the event.
2. **Skip late events by revision.** Keep the last revision you applied per aggregate and ignore anything not newer (`ReservationView`). A gap (revision 3 arriving when you have 1) means something is late or missing; the sample logs a warning and applies it.
3. **Write your own DTOs from the producer's AsyncAPI file.** Never import another context's classes. Ignore fields you do not need (`@JsonIgnoreProperties(ignoreUnknown = true)`) and event types you do not know. Then the producer can add fields and events without breaking you.
4. **Let exceptions propagate.** A failing record is retried twice, one second apart, then moved to `<topic>.DLT` so it does not block the partition (`MessagingConfig`). Swallowing an exception means the offset is committed and the event is lost without a trace.
5. **The offset is committed after your database commit.** A crash between the two causes a redelivery, which rule 1 handles.

## 6. Copying this into your service

| File | What to do |
|---|---|
| `outbox/*`, `inbox/*`, `V1__init.sql` (outbox and processed_event tables) | Copy as they are, until the supervisors' library replaces them |
| `shared/application/MessagingConfig.java` | Copy; rename the topic bean and property |
| `reservation/application/ReservationEventPublisher.java` | One per aggregate that publishes events |
| `reservation/domain/Reservation.java` | Pattern only: `pendingEvents`, `pullEvents()`, `revision` |
| `reservationview/application/ReservationEventListener.java` | Pattern for every listener: guard, revision check, own DTOs |
| `application.yaml` | Change schema, topic names and consumer group |
| `docs/asyncapi.yaml` | Template for your own event API |

The packages follow the course's DDD package convention (one top-level package per aggregate, with `domain` and `application`). Before copying, check this against the starter platform the tactical team hands out on 06.11.; where they differ, theirs wins.

## 7. Tests that show the guarantees

`MessagingIntegrationTest` (real PostgreSQL 18, in-process Kafka):

| Test | Shows |
|---|---|
| `savingTheAggregateAlsoWritesTheEventToTheOutbox` | aggregate row and outbox row are written together |
| `rollbackRemovesTheAggregateAndItsEventTogether` | a failure after the save leaves neither behind |
| `outboxCannotBeWrittenOutsideATransaction` | `MANDATORY` stops misuse |
| `relayPublishesInOrderAndTheConsumerFollows` | full path: confirm, change, cancel, view ends at revision 3 |
| `duplicateDeliveryIsHandledOnce` | the same event ID twice is processed once |
| `lateEventDoesNotOverwriteNewerState` | revision 1 after revision 2 is ignored |
| `poisonMessageGoesToTheDeadLetterTopicAndDoesNotBlockThePartition` | broken message ends in `.DLT`, the next one is processed |

`ScheduledRelayTest` runs the walkthrough from section 1 over real HTTP with the scheduler switched on, including the 409 for cancelling twice and the 400 for a party of 13.

`ReservationTest` covers the aggregate's own invariants.

## 8. Known weak points and open questions

Weak points in this draft:

- **Run one relay per service.** `SKIP LOCKED` lets two relay instances work in parallel without blocking, but then two events of the same aggregate could be sent by different instances and overtake each other. For the course, one instance per service is enough.
- **Published outbox rows are never deleted.** Fine for a semester; a real system needs a cleanup job.
- **Nobody watches the dead-letter topics yet.** A message in `.DLT` is lost to the business until someone looks.
- **The relay sends one message at a time and waits for each acknowledgement.** Slow, but simple and correct.

Open questions (for the team and the supervisors):

1. Where are the supervisors' helper libraries, and what is their API? This decides how much of `outbox` and `inbox` survives.
2. Boundary with workstream 1 (Hyokyung): she covers the stack and installing the producer/consumer libraries, this guide covers using them. Agreed?
3. Topic naming convention (here `<service>.<aggregate>-events`). For the infrastructure ADR.
4. One schema per service in the shared database, or one database per service? For Eldar's ADR. This draft uses schemas.
5. Number of partitions per topic (here 3).
6. Does this match the tactical team's starter platform (package structure, build, guardrails)?
