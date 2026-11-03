package de.thkoeln.ddd.sample;

import de.microservicedungeon.outboxlib.kafka.KafkaRecordMapper;
import de.microservicedungeon.outboxlib.outbox.EntityIdentifier;
import de.microservicedungeon.outboxlib.outbox.EventId;
import de.microservicedungeon.outboxlib.outbox.EventKey;
import de.microservicedungeon.outboxlib.outbox.EventType;
import de.microservicedungeon.outboxlib.outbox.OutboxEventDto;
import de.microservicedungeon.outboxlib.outbox.Partition;
import de.microservicedungeon.outboxlib.outbox.ReadOutbox;
import de.microservicedungeon.outboxlib.outbox.TopicName;
import de.microservicedungeon.outboxlib.outbox.WriteOutbox;
import de.thkoeln.ddd.sample.localrelay.LocalOutboxRelay;
import de.thkoeln.ddd.sample.reservation.application.ReservationService;
import de.thkoeln.ddd.sample.reservation.domain.ReservationId;
import de.thkoeln.ddd.sample.reservationview.domain.ReservationView;
import de.thkoeln.ddd.sample.reservationview.domain.ReservationViewRepository;
import io.zonky.test.db.postgres.embedded.EmbeddedPostgres;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.system.CapturedOutput;
import org.springframework.boot.test.system.OutputCaptureExtension;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.transaction.IllegalTransactionStateException;
import org.springframework.transaction.support.TransactionTemplate;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BooleanSupplier;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Runs against a real PostgreSQL 18 (embedded binaries), an in-process Kafka broker and the
 * supervisors' outbox and consumer libraries. The local relay stands in for the Outbox-Service;
 * its schedule is effectively off (1 h), so each test triggers publishing itself.
 */
@SpringBootTest(properties = {
        "sample.local-relay.enabled=true",
        "sample.local-relay.interval-ms=3600000",
        "spring.kafka.bootstrap-servers=${spring.embedded.kafka.brokers}"
})
@EmbeddedKafka
@ExtendWith(OutputCaptureExtension.class)
class MessagingIntegrationTest {

    private static final EmbeddedPostgres POSTGRES = startPostgres();
    private static final Instant EVENING = Instant.parse("2026-11-06T19:00:00Z");

    @DynamicPropertySource
    static void datasource(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url",
                () -> POSTGRES.getJdbcUrl("postgres", "postgres") + "&currentSchema=sample_service");
        registry.add("spring.datasource.username", () -> "postgres");
        registry.add("spring.datasource.password", () -> "postgres");
    }

    @Autowired ReservationService reservationService;
    @Autowired WriteOutbox writeOutbox;
    @Autowired ReadOutbox readOutbox;
    @Autowired KafkaRecordMapper recordMapper;
    @Autowired LocalOutboxRelay relay;
    @Autowired ReservationViewRepository viewRepository;
    @Autowired JdbcTemplate jdbc;
    @Autowired TransactionTemplate transactionTemplate;
    @Autowired KafkaTemplate<String, byte[]> kafkaTemplate;
    @Value("${sample.topics.reservation-events}") String topic;
    @Value("${sample.topics.reservation-ecst}") String ecstTopic;
    @Value("${spring.embedded.kafka.brokers}") String brokers;

    // --- producer side: what the outbox library writes ----------------------------------------

    @Test
    void savingTheAggregateAlsoWritesTheEventToTheOutbox() {
        ReservationId id = reservationService.confirm("Ada", 4, EVENING);

        assertThat(count("select count(*) from reservation where reservation_id = ?", id.getId())).isOne();
        Map<String, Object> row = jdbc.queryForMap("""
                select topic_name, partition, event_key, event_type, event_type_version, entity_name,
                       convert_from(payload, 'UTF8')::json->>'guestName' as guest
                from event_outbox where entity_id = ? and topic_name = ?""", id.toString(), topic);
        assertThat(row.get("topic_name")).isEqualTo(topic);
        assertThat(row.get("event_key")).as("key = aggregate id").isEqualTo(id.toString());
        assertThat(row.get("event_type")).isEqualTo("reservation.confirmed");
        assertThat(((Number) row.get("event_type_version")).intValue()).isEqualTo(1);
        assertThat(row.get("entity_name")).isEqualTo("reservation");
        assertThat(((Number) row.get("partition")).intValue()).isBetween(0, 2);
        assertThat(row.get("guest")).isEqualTo("Ada");
    }

    @Test
    void rollbackRemovesTheAggregateAndItsEventTogether() {
        AtomicReference<ReservationId> id = new AtomicReference<>();

        assertThatThrownBy(() -> transactionTemplate.executeWithoutResult(status -> {
            id.set(reservationService.confirm("Grace", 2, EVENING));
            throw new IllegalStateException("something fails after the save");
        })).isInstanceOf(IllegalStateException.class);

        assertThat(count("select count(*) from reservation where reservation_id = ?", id.get().getId())).isZero();
        assertThat(count("select count(*) from event_outbox where entity_id = ?", id.get().toString())).isZero();
    }

    @Test
    void outboxCannotBeWrittenOutsideATransaction() {
        assertThatThrownBy(() -> writeOutbox.write(new TestOutboxEvent(topic, "x", "test.event", "{}")))
                .isInstanceOf(IllegalTransactionStateException.class);
    }

    @Test
    void eventsOfOneReservationShareAPartitionAndKeepTheirOrder() {
        ReservationId id = reservationService.confirm("Ada", 4, EVENING);
        reservationService.changePartySize(id, 6);
        reservationService.cancel(id, "ill");

        List<Map<String, Object>> rows = jdbc.queryForList(
                "select partition, event_type from event_outbox where entity_id = ? and topic_name = ? order by sequence_id",
                id.toString(), topic);
        assertThat(rows).extracting(r -> r.get("event_type"))
                .containsExactly("reservation.confirmed", "reservation.party-size-changed", "reservation.cancelled");
        assertThat(rows).extracting(r -> r.get("partition")).containsOnly(rows.getFirst().get("partition"));
    }

    /** ECST, as in the MSD services: after every change, the whole current state on a compacted topic. */
    @Test
    void everyChangeAlsoWritesTheFullStateForTheCompactedTopic() throws Exception {
        ReservationId id = reservationService.confirm("Ada", 4, EVENING);
        reservationService.changePartySize(id, 4); // no change: no event, no new state
        reservationService.changePartySize(id, 6);
        reservationService.cancel(id, "ill");

        List<Map<String, Object>> states = jdbc.queryForList("""
                select event_key, event_type,
                       convert_from(payload, 'UTF8')::json->>'revision' as revision,
                       convert_from(payload, 'UTF8')::json->>'partySize' as party_size,
                       convert_from(payload, 'UTF8')::json->>'status' as status
                from event_outbox where entity_id = ? and topic_name = ? order by sequence_id""",
                id.toString(), ecstTopic);
        assertThat(states).hasSize(3).allSatisfy(row -> {
            assertThat(row.get("event_type")).isEqualTo("reservation.state-transfer");
            assertThat(row.get("event_key")).as("same key as the events: compaction keeps one per reservation")
                    .isEqualTo(id.toString());
        });
        assertThat(states).extracting(row -> row.get("revision")).containsExactly("1", "2", "3");
        assertThat(states.getLast()).containsEntry("party_size", "6").containsEntry("status", "CANCELLED");

        ConfigResource resource = new ConfigResource(ConfigResource.Type.TOPIC, ecstTopic);
        try (Admin admin = Admin.create(Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, brokers))) {
            Config config = admin.describeConfigs(List.of(resource)).all().get().get(resource);
            assertThat(config.get("cleanup.policy").value()).isEqualTo("compact");
        }
    }

    // --- end to end: outbox -> relay -> Kafka -> consumer ---------------------------------------

    @Test
    void relayPublishesAndTheConsumerFollows() {
        ReservationId id = reservationService.confirm("Ada", 4, EVENING);
        reservationService.changePartySize(id, 6);
        reservationService.cancel(id, "ill");

        assertThat(relay.publishPending()).isGreaterThanOrEqualTo(3);

        awaitTrue(() -> viewRepository.findById(id.getId()).map(v -> v.getLastRevision() == 3).orElse(false));
        ReservationView view = viewRepository.findById(id.getId()).orElseThrow();
        assertThat(view.getStatus()).isEqualTo(ReservationView.Status.CANCELLED);
        assertThat(view.getPartySize()).isEqualTo(6);
        assertThat(count("select count(*) from event_outbox where entity_id = ?", id.toString()))
                .as("acknowledged rows are deleted").isZero();
        assertThat(jdbc.queryForList("select sequence_id from processed_event where event_key = ? order by processed_at",
                Long.class, id.toString()))
                .as("received in the order they were written").isSorted().hasSize(3);
    }

    @Test
    void publishedRecordsCarryTheStandardHeaders() {
        ReservationId id = reservationService.confirm("Ada", 4, EVENING);
        relay.publishPending();

        var record = recordsOn(topic).stream().filter(r -> id.toString().equals(r.key())).findFirst().orElseThrow();
        assertThat(header(record, "event_type")).isEqualTo("reservation.confirmed");
        assertThat(record.headers().lastHeader("event_id")).isNotNull();
        assertThat(record.headers().lastHeader("sequence_id")).isNotNull();
        assertThat(record.headers().lastHeader("event_type_version").value()).hasSize(4);
        assertThat(header(record, "entity")).isEqualTo("reservation." + id);
    }

    // --- consumer side: what at-least-once delivery throws at you ------------------------------

    /** The relay sent the events but died before deleting them: the next run sends them again. */
    @Test
    void eventsSentTwiceAfterARelayCrashAreHandledOnce(CapturedOutput output) {
        ReservationId id = reservationService.confirm("Ada", 4, EVENING);
        reservationService.changePartySize(id, 5);

        List<OutboxEventDto> sentBeforeCrash = sendWithoutAcknowledging(id);
        relay.publishPending(); // the "next run" sends the same rows again, then deletes them

        awaitTrue(() -> output.getOut().split("Skipping duplicate", -1).length - 1 >= 2);
        assertThat(sentBeforeCrash).hasSize(2);
        assertThat(count("select count(*) from processed_event where event_key = ?", id.toString())).isEqualTo(2);
        assertThat(viewRepository.findById(id.getId()).orElseThrow().getPartySize()).isEqualTo(5);
    }

    @Test
    void lateEventDoesNotOverwriteNewerState(CapturedOutput output) {
        ReservationId id = reservationService.confirm("Ada", 4, EVENING);
        reservationService.changePartySize(id, 6);

        // Deliver revision 2 before revision 1 (as if revision 1 were redelivered late).
        List<OutboxEventDto> events = outboxRows(id);
        transactionTemplate.executeWithoutResult(s -> {
            events.reversed().forEach(e -> kafkaTemplate.send(recordMapper.toProducerRecord(e)).join());
            jdbc.update("delete from event_outbox where entity_id = ?", id.toString());
        });

        awaitTrue(() -> count("select count(*) from processed_event where event_key = ?", id.toString()) == 2);
        ReservationView view = viewRepository.findById(id.getId()).orElseThrow();
        assertThat(view.getPartySize()).isEqualTo(6);
        assertThat(view.getLastRevision()).isEqualTo(2);
        assertThat(output.getOut())
                .contains("earlier events are late or missing")
                .contains("Ignoring stale reservation.confirmed (revision 1)");
    }

    @Test
    void brokenMessagesGoToTheDeadLetterTopicAndDoNotBlockThePartition() {
        String key = UUID.randomUUID().toString();
        // 1. standard headers, but the payload is not JSON
        OutboxEventDto badPayload = dto(key, "reservation.confirmed", 900_001, "this is not json");
        kafkaTemplate.send(recordMapper.toProducerRecord(badPayload)).join();
        // 2. no headers at all: the consumer library cannot even read it
        kafkaTemplate.send(new ProducerRecord<>(topic, key, "{}".getBytes(StandardCharsets.UTF_8))).join();
        // 3. a good event behind them, same key, same partition
        String good = """
                {"reservationId":"%s","guestName":"Ada","partySize":3,"reservedFor":"2026-11-06T19:00:00Z",\
                "revision":1,"occurredAt":"2026-11-06T10:00:00Z"}""".formatted(key);
        OutboxEventDto goodEvent = dto(key, "reservation.confirmed", 900_002, good);
        kafkaTemplate.send(recordMapper.toProducerRecord(goodEvent)).join();

        awaitTrue(() -> viewRepository.findById(UUID.fromString(key)).isPresent());
        assertThat(count("select count(*) from processed_event where event_id = ?", badPayload.id().getId()))
                .as("failed handling is rolled back, including the processed mark").isZero();
        List<String> deadLetterKeys = recordsOn(topic + ".DLT").stream().map(r -> r.key()).toList();
        assertThat(deadLetterKeys).filteredOn(key::equals).hasSize(2);
    }

    @Test
    void unknownEventTypesAreSkipped() {
        String key = UUID.randomUUID().toString();
        OutboxEventDto unknown = dto(key, "reservation.something-new", 900_003, "{}");
        kafkaTemplate.send(recordMapper.toProducerRecord(unknown)).join();
        String good = """
                {"reservationId":"%s","guestName":"Lin","partySize":2,"reservedFor":"2026-11-06T19:00:00Z",\
                "revision":1,"occurredAt":"2026-11-06T10:00:00Z"}""".formatted(key);
        kafkaTemplate.send(recordMapper.toProducerRecord(dto(key, "reservation.confirmed", 900_004, good))).join();

        awaitTrue(() -> viewRepository.findById(UUID.fromString(key)).isPresent());
        assertThat(count("select count(*) from processed_event where event_id = ?", unknown.id().getId())).isZero();
    }

    // --- helpers -------------------------------------------------------------------------------

    /** Sends the reservation's outbox rows to Kafka but leaves them in the outbox (a crash before the delete). */
    private List<OutboxEventDto> sendWithoutAcknowledging(ReservationId id) {
        List<OutboxEventDto> events = outboxRows(id);
        events.forEach(e -> kafkaTemplate.send(recordMapper.toProducerRecord(e)).join());
        return events;
    }

    /** The reservation's rows as the Outbox-Service would read them, without deleting them. */
    private List<OutboxEventDto> outboxRows(ReservationId id) {
        List<OutboxEventDto> result = new ArrayList<>();
        transactionTemplate.executeWithoutResult(s -> {
            List<Partition> all = IntStream.range(0, 3).mapToObj(Partition::of).toList();
            try (Stream<OutboxEventDto> events = readOutbox.readNext(TopicName.of(topic), all)) {
                events.filter(e -> e.eventKey() != null && e.eventKey().getKey().equals(id.toString()))
                        .forEach(result::add);
            }
            jdbc.update("update event_outbox set is_read = false where entity_id <> ?", id.toString());
        });
        result.sort(Comparator.comparing(OutboxEventDto::sequenceId));
        return result;
    }

    /** A hand-made event in exactly the record format the Outbox-Service publishes. */
    private OutboxEventDto dto(String key, String eventType, long sequenceId, String payload) {
        return new OutboxEventDto(EventId.ofRandomUuid(), sequenceId, TopicName.of(topic),
                Partition.of(Math.abs(key.hashCode()) % 3), ZonedDateTime.now(), null, null, null,
                EventKey.of(key), EventType.of(eventType, 1), EntityIdentifier.of("reservation", key), List.of(),
                payload.getBytes(StandardCharsets.UTF_8));
    }

    private long count(String sql, Object arg) {
        Long result = jdbc.queryForObject(sql, Long.class, arg);
        return result == null ? 0 : result;
    }

    private static String header(org.apache.kafka.clients.consumer.ConsumerRecord<String, byte[]> record, String name) {
        return new String(record.headers().lastHeader(name).value(), StandardCharsets.UTF_8);
    }

    private List<org.apache.kafka.clients.consumer.ConsumerRecord<String, byte[]>> recordsOn(String topicName) {
        Map<String, Object> props = Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers,
                ConsumerConfig.GROUP_ID_CONFIG, "test-" + UUID.randomUUID(),
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
        List<org.apache.kafka.clients.consumer.ConsumerRecord<String, byte[]>> all = new ArrayList<>();
        try (KafkaConsumer<String, byte[]> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(List.of(topicName));
            long deadline = System.nanoTime() + Duration.ofSeconds(15).toNanos();
            while (System.nanoTime() < deadline) {
                ConsumerRecords<String, byte[]> records = consumer.poll(Duration.ofSeconds(2));
                StreamSupport.stream(records.spliterator(), false).forEach(all::add);
                if (!all.isEmpty() && records.isEmpty()) {
                    break;
                }
            }
        }
        return all;
    }

    private static void awaitTrue(BooleanSupplier condition) {
        long deadline = System.nanoTime() + Duration.ofSeconds(25).toNanos();
        while (!condition.getAsBoolean()) {
            if (System.nanoTime() > deadline) {
                throw new AssertionError("Condition not met within 25 seconds");
            }
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new AssertionError(e);
            }
        }
    }

    private static EmbeddedPostgres startPostgres() {
        try {
            return EmbeddedPostgres.builder().start();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
