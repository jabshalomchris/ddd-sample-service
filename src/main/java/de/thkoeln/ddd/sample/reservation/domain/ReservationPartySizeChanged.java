package de.thkoeln.ddd.sample.reservation.domain;

import java.time.Instant;
import java.util.UUID;

public record ReservationPartySizeChanged(
        UUID reservationId,
        int partySize,
        long revision,
        Instant occurredAt) implements ReservationEvent {
}
