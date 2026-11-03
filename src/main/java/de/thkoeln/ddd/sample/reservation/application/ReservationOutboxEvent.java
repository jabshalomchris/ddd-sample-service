package de.thkoeln.ddd.sample.reservation.application;

import de.microservicedungeon.outboxlib.outbox.CustomHeader;
import de.microservicedungeon.outboxlib.outbox.DelayInterval;
import de.microservicedungeon.outboxlib.outbox.EntityIdentifier;
import de.microservicedungeon.outboxlib.outbox.EventKey;
import de.microservicedungeon.outboxlib.outbox.EventType;
import de.microservicedungeon.outboxlib.outbox.OutboxWritableEvent;
import de.microservicedungeon.outboxlib.outbox.TopicName;

import java.util.List;
import java.util.Optional;

/**
 * One domain event in the shape the outbox library expects ({@link OutboxWritableEvent}).
 *
 * <ul>
 *   <li><b>key</b> = reservation ID: same key, same partition, events of one reservation stay in order</li>
 *   <li><b>eventType</b> = e.g. {@code reservation.cancelled}, version 1 (lowercase letters, dots and dashes only)</li>
 *   <li><b>entity</b> = {@code reservation.<id>}, sent as a header for debugging</li>
 *   <li><b>payload</b> = the event as UTF-8 JSON</li>
 * </ul>
 */
record ReservationOutboxEvent(
        TopicName topic,
        String reservationId,
        EventType eventType,
        byte[] payload) implements OutboxWritableEvent {

    static final String ENTITY_NAME = "reservation";

    @Override
    public TopicName getTopicName() {
        return topic;
    }

    @Override
    public Optional<EventKey> getKey() {
        return Optional.of(EventKey.of(reservationId));
    }

    @Override
    public Optional<DelayInterval> getDelayInterval() {
        return Optional.empty(); // a delay would break ordering and duplicate detection by sequence id
    }

    @Override
    public Optional<EntityIdentifier> getEntityIdentifier() {
        return Optional.of(EntityIdentifier.of(ENTITY_NAME, reservationId));
    }

    @Override
    public EventType getEventType() {
        return eventType;
    }

    @Override
    public Optional<String> getTraceId() {
        return Optional.empty();
    }

    @Override
    public List<CustomHeader> getCustomHeaders() {
        return List.of();
    }

    @Override
    public byte[] getPayload() {
        return payload;
    }
}
