package de.thkoeln.ddd.sample.reservation.application;

import de.thkoeln.ddd.sample.outbox.OutboxWriter;
import de.thkoeln.ddd.sample.reservation.domain.Reservation;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

/**
 * Moves the aggregate's recorded events into the outbox. Called by the service after
 * saving, inside the same transaction.
 */
@Component
class ReservationEventPublisher {

    private static final String AGGREGATE_TYPE = "Reservation";

    private final OutboxWriter outboxWriter;
    private final String topic;

    ReservationEventPublisher(OutboxWriter outboxWriter,
                              @Value("${sample.topics.reservation-events}") String topic) {
        this.outboxWriter = outboxWriter;
        this.topic = topic;
    }

    void publishPendingEvents(Reservation reservation) {
        String key = reservation.getId().getId().toString();
        reservation.pullEvents().forEach(event ->
                outboxWriter.append(topic, AGGREGATE_TYPE, key, event.eventType(), event));
    }
}
