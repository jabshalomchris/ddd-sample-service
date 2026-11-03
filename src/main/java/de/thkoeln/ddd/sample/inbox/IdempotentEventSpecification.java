package de.thkoeln.ddd.sample.inbox;

import de.microservicedungeon.consumerlib.processor.EventSpecification;
import de.microservicedungeon.consumerlib.processor.kafka.KafkaDefaultHeader;
import de.microservicedungeon.consumerlib.processor.kafka.KafkaEventContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;
import java.util.UUID;
import java.util.function.BiConsumer;

/**
 * Base class for the consumer library's {@link EventSpecification}: one subclass per event type.
 *
 * <p>Adds the one thing the library does not do yet: skipping duplicates. Every event is checked
 * against {@link IdempotencyGuard} by its {@code event_id} before {@link #handle} runs.
 *
 * <p>Why {@code event_id} and not a high-watermark on {@code sequence_id}: the outbox assigns the
 * sequence id when an event is written, not when its transaction commits. Two transactions on
 * different entities can commit in the opposite order, so a lower id can be published after a
 * higher one. A watermark would drop that event; the event id check never does.
 *
 * <p>No error handler is provided ({@link #onError()} is empty), so the library rethrows, the
 * transaction rolls back, and Spring Kafka retries and finally moves the record to the DLT.
 */
public abstract class IdempotentEventSpecification
        implements EventSpecification<KafkaEventContext<KafkaDefaultHeader, byte[]>> {

    private static final Logger log = LoggerFactory.getLogger(IdempotentEventSpecification.class);

    private final IdempotencyGuard guard;
    private final String consumerName;

    protected IdempotentEventSpecification(IdempotencyGuard guard, String consumerName) {
        this.guard = guard;
        this.consumerName = consumerName;
    }

    @Override
    public final void process(KafkaEventContext<KafkaDefaultHeader, byte[]> context) {
        KafkaDefaultHeader headers = context.headers();
        UUID eventId = UUID.fromString(headers.eventId().getId());
        String key = context.metadata().key() == null ? null : context.metadata().key().getKey();
        boolean first = guard.firstDelivery(consumerName, eventId, key,
                headers.eventType().getType(), headers.sequenceId().getSequenceId());
        if (!first) {
            log.info("Skipping duplicate {} {}", headers.eventType().getType(), eventId);
            return;
        }
        handle(context);
    }

    /** Called once per event. Runs inside the listener's transaction. */
    protected abstract void handle(KafkaEventContext<KafkaDefaultHeader, byte[]> context);

    @Override
    public Optional<BiConsumer<KafkaEventContext<KafkaDefaultHeader, byte[]>, Throwable>> onError() {
        return Optional.empty();
    }
}
