package de.thkoeln.ddd.sample.reservationview.application;

import de.thkoeln.ddd.sample.inbox.IdempotencyGuard;
import de.thkoeln.ddd.sample.outbox.OutboxRelay;
import de.thkoeln.ddd.sample.reservationview.application.ReservationEventMessages.Cancelled;
import de.thkoeln.ddd.sample.reservationview.application.ReservationEventMessages.Confirmed;
import de.thkoeln.ddd.sample.reservationview.application.ReservationEventMessages.PartySizeChanged;
import de.thkoeln.ddd.sample.reservationview.domain.ReservationView;
import de.thkoeln.ddd.sample.reservationview.domain.ReservationViewRepository;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;
import tools.jackson.databind.json.JsonMapper;

import java.nio.charset.StandardCharsets;
import java.util.UUID;

/**
 * Consumes the reservation events the way any neighbouring context should:
 * <ol>
 *   <li>skip duplicates by event ID ({@link IdempotencyGuard}),</li>
 *   <li>skip late events by revision ({@link ReservationView}),</li>
 *   <li>ignore event types it does not know,</li>
 *   <li>do all of it in one database transaction; the offset is committed afterwards.</li>
 * </ol>
 * Exceptions are retried and then sent to the dead-letter topic (see MessagingConfig).
 */
@Component
class ReservationEventListener {

    static final String CONSUMER_NAME = "reservation-view";

    private static final Logger log = LoggerFactory.getLogger(ReservationEventListener.class);

    private final IdempotencyGuard idempotencyGuard;
    private final ReservationViewRepository viewRepository;
    private final JsonMapper jsonMapper;

    ReservationEventListener(IdempotencyGuard idempotencyGuard, ReservationViewRepository viewRepository,
                             JsonMapper jsonMapper) {
        this.idempotencyGuard = idempotencyGuard;
        this.viewRepository = viewRepository;
        this.jsonMapper = jsonMapper;
    }

    @KafkaListener(id = CONSUMER_NAME,
            topics = "${sample.topics.reservation-events}",
            groupId = "sample-service." + CONSUMER_NAME)
    @Transactional
    void on(ConsumerRecord<String, String> record) {
        UUID eventId = UUID.fromString(requiredHeader(record, OutboxRelay.HEADER_EVENT_ID));
        String eventType = requiredHeader(record, OutboxRelay.HEADER_EVENT_TYPE);

        if (!idempotencyGuard.firstDelivery(CONSUMER_NAME, eventId)) {
            log.info("Skipping duplicate {} {}", eventType, eventId);
            return;
        }

        switch (eventType) {
            case "ReservationConfirmed" -> {
                Confirmed event = read(record, Confirmed.class);
                ReservationView view = load(event.reservationId(), event.revision());
                report(view.applyConfirmed(event.guestName(), event.partySize(), event.revision()), eventType, event.revision());
                viewRepository.save(view);
            }
            case "ReservationPartySizeChanged" -> {
                PartySizeChanged event = read(record, PartySizeChanged.class);
                ReservationView view = load(event.reservationId(), event.revision());
                report(view.applyPartySizeChanged(event.partySize(), event.revision()), eventType, event.revision());
                viewRepository.save(view);
            }
            case "ReservationCancelled" -> {
                Cancelled event = read(record, Cancelled.class);
                ReservationView view = load(event.reservationId(), event.revision());
                report(view.applyCancelled(event.revision()), eventType, event.revision());
                viewRepository.save(view);
            }
            default -> log.debug("Ignoring event type {} this consumer does not need", eventType);
        }
    }

    private ReservationView load(UUID reservationId, long revision) {
        ReservationView view = viewRepository.findById(reservationId)
                .orElseGet(() -> ReservationView.notYetSeen(reservationId));
        if (view.hasGapBefore(revision)) {
            log.warn("Reservation {}: got revision {} but last seen was {}; earlier events are late or missing",
                    reservationId, revision, view.getLastRevision());
        }
        return view;
    }

    private static void report(boolean applied, String eventType, long revision) {
        if (!applied) {
            log.info("Ignoring stale {} (revision {})", eventType, revision);
        }
    }

    private <T> T read(ConsumerRecord<String, String> record, Class<T> type) {
        return jsonMapper.readValue(record.value(), type);
    }

    private static String requiredHeader(ConsumerRecord<String, String> record, String name) {
        Header header = record.headers().lastHeader(name);
        if (header == null) {
            throw new IllegalArgumentException("Message without '" + name + "' header at offset " + record.offset());
        }
        return new String(header.value(), StandardCharsets.UTF_8);
    }
}
