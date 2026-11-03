package de.thkoeln.ddd.sample.reservation.application;

import de.microservicedungeon.outboxlib.outbox.EventType;
import de.microservicedungeon.outboxlib.outbox.OutboxWritableEvent;
import de.microservicedungeon.outboxlib.outbox.TopicName;
import de.microservicedungeon.outboxlib.outbox.WriteOutbox;
import de.thkoeln.ddd.sample.reservation.domain.Reservation;
import de.thkoeln.ddd.sample.reservation.domain.ReservationCancelled;
import de.thkoeln.ddd.sample.reservation.domain.ReservationConfirmed;
import de.thkoeln.ddd.sample.reservation.domain.ReservationEvent;
import de.thkoeln.ddd.sample.reservation.domain.ReservationPartySizeChanged;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import tools.jackson.databind.json.JsonMapper;

import java.util.List;

/**
 * Hands the aggregate's recorded events to the outbox library. Called by the service after
 * saving, inside the same transaction. {@link WriteOutbox} refuses to run without one
 * ({@code Propagation.MANDATORY}).
 *
 * <p>The library assigns each event a {@code sequence_id} when it is written, not when it is
 * committed. It therefore asks that concurrent changes to the same entity are serialised; here the
 * aggregate's {@code @Version} (optimistic locking) does that.
 */
@Component
class ReservationEventPublisher {

    /** Schema version of all reservation events. Raise it (v2) when a payload changes incompatibly. */
    static final int SCHEMA_VERSION = 1;

    private final WriteOutbox writeOutbox;
    private final JsonMapper jsonMapper;
    private final TopicName topic;

    ReservationEventPublisher(WriteOutbox writeOutbox, JsonMapper jsonMapper,
                              @Value("${sample.topics.reservation-events}") String topic) {
        this.writeOutbox = writeOutbox;
        this.jsonMapper = jsonMapper;
        this.topic = TopicName.of(topic);
    }

    void publishPendingEvents(Reservation reservation) {
        List<ReservationEvent> events = reservation.pullEvents();
        if (events.isEmpty()) {
            return;
        }
        List<OutboxWritableEvent> outboxEvents = events.stream().<OutboxWritableEvent>map(this::toOutboxEvent).toList();
        writeOutbox.write(outboxEvents);
    }

    private ReservationOutboxEvent toOutboxEvent(ReservationEvent event) {
        return new ReservationOutboxEvent(
                topic,
                event.reservationId().toString(),
                EventType.of(eventTypeOf(event), SCHEMA_VERSION),
                jsonMapper.writeValueAsBytes(event));
    }

    /** Event type names as published: lowercase, {@code <entity>.<event>}. Part of the event API. */
    static String eventTypeOf(ReservationEvent event) {
        return switch (event) {
            case ReservationConfirmed e -> "reservation.confirmed";
            case ReservationPartySizeChanged e -> "reservation.party-size-changed";
            case ReservationCancelled e -> "reservation.cancelled";
        };
    }
}
