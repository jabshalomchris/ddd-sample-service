package de.thkoeln.ddd.sample.reservation.domain;

import java.time.Instant;
import java.util.UUID;

public record ReservationCancelled(
        UUID reservationId,
        String reason,
        long revision,
        Instant occurredAt) implements ReservationEvent {
}
