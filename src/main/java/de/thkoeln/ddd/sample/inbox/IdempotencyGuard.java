package de.thkoeln.ddd.sample.inbox;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.util.UUID;

/**
 * Remembers which events a consumer has already handled.
 *
 * <p>Must run in the same transaction as the handler's own writes: if the handler fails,
 * the "processed" mark is rolled back too and the redelivered event is handled again.
 *
 * <p>STAND-IN, like the outbox package: replace with the provided library if it covers this.
 */
@Component
public class IdempotencyGuard {

    private final JdbcTemplate jdbcTemplate;

    IdempotencyGuard(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    /** @return true the first time this consumer sees the event, false for every duplicate */
    @Transactional(propagation = Propagation.MANDATORY)
    public boolean firstDelivery(String consumer, UUID eventId) {
        int inserted = jdbcTemplate.update("""
                insert into processed_event (consumer, event_id) values (?, ?)
                on conflict do nothing
                """, consumer, eventId);
        return inserted == 1;
    }
}
