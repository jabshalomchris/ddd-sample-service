package de.thkoeln.ddd.sample;

import de.thkoeln.ddd.sample.outbox.OutboxRelay;
import de.thkoeln.ddd.sample.outbox.OutboxWriter;
import de.thkoeln.ddd.sample.reservation.application.ReservationService;
import de.thkoeln.ddd.sample.reservation.domain.ReservationId;
import de.thkoeln.ddd.sample.reservationview.domain.ReservationView;
import de.thkoeln.ddd.sample.reservationview.domain.ReservationViewRepository;
import io.zonky.test.db.postgres.embedded.EmbeddedPostgres;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerRecord;
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
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BooleanSupplier;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Runs against a real PostgreSQL 18 (embedded binaries) and an in-process Kafka broker,
 * so no Docker is needed. The scheduler is off; tests call the relay themselves.
 */
@SpringBootTest(properties = {
        "outbox.relay.enabled=false",
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
    @Autowired OutboxWriter outboxWriter;
    @Autowired OutboxRelay outboxRelay;
    @Autowired ReservationViewRepository viewRepository;
    @Autowired JdbcTemplate jdbc;
    @Autowired TransactionTemplate transactionTemplate;
    @Autowired KafkaTemplate<String, String> kafkaTemplate;
    @Value("${sample.topics.reservation-events}") String topic;
    @Value("${spring.embedded.kafka.brokers}") String brokers;

    // --- producer side: outbox -------------------------------------------------------

    @Test
    void savingTheAggregateAlsoWritesTheEventToTheOutbox() {
        ReservationId id = reservationService.confirm("Ada", 4, EVENING);

        assertThat(count("select count(*) from reservation where reservation_id = ?", id.getId())).isOne();
        Map<String, Object> row = jdbc.queryForMap("""
                select event_type, payload->>'guestName' as guest, published_at
                from outbox_message where aggregate_id = ?""", id.toString());
        assertThat(row.get("event_type")).isEqualTo("ReservationConfirmed");
        assertThat(row.get("guest")).isEqualTo("Ada");
        assertThat(row.get("published_at")).as("not sent yet").isNull();
    }

    @Test
    void rollbackRemovesTheAggregateAndItsEventTogether() {
        AtomicReference<ReservationId> id = new AtomicReference<>();

        assertThatThrownBy(() -> transactionTemplate.executeWithoutResult(status -> {
            id.set(reservationService.confirm("Grace", 2, EVENING));
            throw new IllegalStateException("something fails after the save");
        })).isInstanceOf(IllegalStateException.class);

        assertThat(count("select count(*) from reservation where reservation_id = ?", id.get().getId())).isZero();
        assertThat(count("select count(*) from outbox_message where aggregate_id = ?", id.get().toString())).isZero();
    }

    @Test
    void outboxCannotBeWrittenOutsideATransaction() {
        assertThatThrownBy(() -> outboxWriter.append(topic, "Reservation", "x", "Test", Map.of()))
                .isInstanceOf(IllegalTransactionStateException.class);
    }

    // --- end to end: relay -> Kafka -> consumer --------------------------------------

    @Test
    void relayPublishesInOrderAndTheConsumerFollows() {
        ReservationId id = reservationService.confirm("Ada", 4, EVENING);
        reservationService.changePartySize(id, 6);
        reservationService.cancel(id, "ill");

        while (outboxRelay.publishPendingBatch() > 0) {
            // drain everything that is pending, including rows from other tests
        }

        awaitTrue(() -> viewRepository.findById(id.getId())
                .map(v -> v.getLastRevision() == 3).orElse(false));
        ReservationView view = viewRepository.findById(id.getId()).orElseThrow();
        assertThat(view.getStatus()).isEqualTo(ReservationView.Status.CANCELLED);
        assertThat(view.getPartySize()).isEqualTo(6);
        assertThat(view.getGuestName()).isEqualTo("Ada");
        assertThat(count("select count(*) from outbox_message where aggregate_id = ? and published_at is null",
                id.toString())).isZero();
    }

    // --- consumer side: what at-least-once delivery throws at you -----------------------

    @Test
    void duplicateDeliveryIsHandledOnce(CapturedOutput output) {
        UUID reservationId = UUID.randomUUID();
        UUID confirmedId = UUID.randomUUID();
        String confirmed = confirmedJson(reservationId, "Ada", 4, 1);

        send(reservationId, confirmedId, "ReservationConfirmed", confirmed);
        send(reservationId, confirmedId, "ReservationConfirmed", confirmed); // same event ID again
        UUID marker = send(reservationId, UUID.randomUUID(), "ReservationPartySizeChanged",
                partySizeChangedJson(reservationId, 5, 2));

        awaitProcessed(marker);
        assertThat(output.getOut()).containsOnlyOnce("Skipping duplicate ReservationConfirmed " + confirmedId);
        assertThat(viewRepository.findById(reservationId).orElseThrow().getPartySize()).isEqualTo(5);
    }

    @Test
    void lateEventDoesNotOverwriteNewerState(CapturedOutput output) {
        UUID reservationId = UUID.randomUUID();

        send(reservationId, UUID.randomUUID(), "ReservationPartySizeChanged", partySizeChangedJson(reservationId, 6, 2));
        UUID late = send(reservationId, UUID.randomUUID(), "ReservationConfirmed",
                confirmedJson(reservationId, "Late", 2, 1));

        awaitProcessed(late);
        ReservationView view = viewRepository.findById(reservationId).orElseThrow();
        assertThat(view.getPartySize()).isEqualTo(6);
        assertThat(view.getLastRevision()).isEqualTo(2);
        assertThat(output.getOut())
                .contains("earlier events are late or missing")
                .contains("Ignoring stale ReservationConfirmed (revision 1)");
    }

    @Test
    void poisonMessageGoesToTheDeadLetterTopicAndDoesNotBlockThePartition() {
        UUID reservationId = UUID.randomUUID();
        UUID poisonId = send(reservationId, UUID.randomUUID(), "ReservationConfirmed", "this is not json");
        UUID next = send(reservationId, UUID.randomUUID(), "ReservationConfirmed",
                confirmedJson(reservationId, "Ada", 3, 1));

        awaitProcessed(next);
        assertThat(count("select count(*) from processed_event where event_id = ?", poisonId))
                .as("failed handling is rolled back, including the processed mark").isZero();
        assertThat(eventIdsOn(topic + ".DLT")).contains(poisonId.toString());
    }

    // --- helpers -----------------------------------------------------------------------

    private UUID send(UUID key, UUID eventId, String eventType, String json) {
        ProducerRecord<String, String> record = new ProducerRecord<>(topic, key.toString(), json);
        record.headers()
                .add(OutboxRelay.HEADER_EVENT_ID, eventId.toString().getBytes(StandardCharsets.UTF_8))
                .add(OutboxRelay.HEADER_EVENT_TYPE, eventType.getBytes(StandardCharsets.UTF_8));
        kafkaTemplate.send(record).join();
        return eventId;
    }

    private static String confirmedJson(UUID id, String guest, int partySize, long revision) {
        return """
                {"reservationId":"%s","guestName":"%s","partySize":%d,"reservedFor":"2026-11-06T19:00:00Z",\
                "revision":%d,"occurredAt":"2026-11-06T10:00:00Z"}""".formatted(id, guest, partySize, revision);
    }

    private static String partySizeChangedJson(UUID id, int partySize, long revision) {
        return """
                {"reservationId":"%s","partySize":%d,"revision":%d,"occurredAt":"2026-11-06T10:00:00Z"}"""
                .formatted(id, partySize, revision);
    }

    private void awaitProcessed(UUID eventId) {
        awaitTrue(() -> count("select count(*) from processed_event where event_id = ?", eventId) == 1);
    }

    private long count(String sql, Object arg) {
        Long result = jdbc.queryForObject(sql, Long.class, arg);
        return result == null ? 0 : result;
    }

    private List<String> eventIdsOn(String dltTopic) {
        Map<String, Object> props = Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers,
                ConsumerConfig.GROUP_ID_CONFIG, "test-dlt-" + UUID.randomUUID(),
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(List.of(dltTopic));
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(10));
            return java.util.stream.StreamSupport.stream(records.spliterator(), false)
                    .map(r -> new String(r.headers().lastHeader(OutboxRelay.HEADER_EVENT_ID).value(),
                            StandardCharsets.UTF_8))
                    .toList();
        }
    }

    private static void awaitTrue(BooleanSupplier condition) {
        long deadline = System.nanoTime() + Duration.ofSeconds(20).toNanos();
        while (!condition.getAsBoolean()) {
            if (System.nanoTime() > deadline) {
                throw new AssertionError("Condition not met within 20 seconds");
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
