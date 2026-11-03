package de.thkoeln.ddd.sample;

import de.microservicedungeon.outboxlib.outbox.CustomHeader;
import de.microservicedungeon.outboxlib.outbox.DelayInterval;
import de.microservicedungeon.outboxlib.outbox.EntityIdentifier;
import de.microservicedungeon.outboxlib.outbox.EventKey;
import de.microservicedungeon.outboxlib.outbox.EventType;
import de.microservicedungeon.outboxlib.outbox.OutboxWritableEvent;
import de.microservicedungeon.outboxlib.outbox.TopicName;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Optional;

/** Minimal {@link OutboxWritableEvent} for tests. */
record TestOutboxEvent(String topic, String key, String type, String json) implements OutboxWritableEvent {

    @Override
    public TopicName getTopicName() {
        return TopicName.of(topic);
    }

    @Override
    public Optional<EventKey> getKey() {
        return Optional.of(EventKey.of(key));
    }

    @Override
    public Optional<DelayInterval> getDelayInterval() {
        return Optional.empty();
    }

    @Override
    public Optional<EntityIdentifier> getEntityIdentifier() {
        return Optional.of(EntityIdentifier.of("test", key));
    }

    @Override
    public EventType getEventType() {
        return EventType.of(type, 1);
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
        return json.getBytes(StandardCharsets.UTF_8);
    }
}
