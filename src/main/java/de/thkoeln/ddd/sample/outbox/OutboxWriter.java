package de.thkoeln.ddd.sample.outbox;

import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;
import tools.jackson.databind.json.JsonMapper;

import java.time.Clock;
import java.util.UUID;

/**
 * Writes an event into the outbox table.
 *
 * <p>{@code Propagation.MANDATORY} is the important line: the call fails unless it runs
 * inside the transaction that also saves the aggregate. That is what makes
 * "state changed" and "event will be published" one atomic step.
 */
@Component
public class OutboxWriter {

    private final OutboxRepository outboxRepository;
    private final JsonMapper jsonMapper;
    private final Clock clock;

    OutboxWriter(OutboxRepository outboxRepository, JsonMapper jsonMapper, Clock clock) {
        this.outboxRepository = outboxRepository;
        this.jsonMapper = jsonMapper;
        this.clock = clock;
    }

    /**
     * @param aggregateId used as the Kafka message key, so all events of one aggregate
     *                    land on the same partition and keep their order
     * @return the generated event ID (sent as the {@code eventId} header)
     */
    @Transactional(propagation = Propagation.MANDATORY)
    public UUID append(String topic, String aggregateType, String aggregateId, String eventType, Object payload) {
        UUID eventId = UUID.randomUUID();
        String json = jsonMapper.writeValueAsString(payload);
        outboxRepository.save(new OutboxMessage(
                eventId, aggregateType, aggregateId, eventType, topic, json, clock.instant()));
        return eventId;
    }
}
