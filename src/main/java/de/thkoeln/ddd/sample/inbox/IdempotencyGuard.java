package de.thkoeln.ddd.sample.inbox;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.util.UUID;

/**
 * Remembers which events a consumer has already handled, by their {@code event_id} header.
 *
 * <p>Needed because the consumer library (1.0.0) does not detect duplicates yet, and the outbox
 * delivers at least once. Must run in the same transaction as the handler's own writes: if the
 * handler fails, the "processed" mark is rolled back too and the redelivered event is handled again.
 */
@Component
public class IdempotencyGuard {

    private final JdbcTemplate jdbcTemplate;

    IdempotencyGuard(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    /** @return true the first time this consumer sees the event, false for every duplicate */
    @Transactional(propagation = Propagation.MANDATORY)
    public boolean firstDelivery(String consumer, UUID eventId, String eventKey, String eventType, long sequenceId) {
        int inserted = jdbcTemplate.update("""
                insert into processed_event (consumer, event_id, event_key, event_type, sequence_id)
                values (?, ?, ?, ?, ?)
                on conflict do nothing
                """, consumer, eventId, eventKey, eventType, sequenceId);
        return inserted == 1;
    }
}
