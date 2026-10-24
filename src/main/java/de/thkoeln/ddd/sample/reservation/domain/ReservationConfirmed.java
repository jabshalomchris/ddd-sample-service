package de.thkoeln.ddd.sample.reservation.domain;

import java.time.Instant;
import java.util.UUID;

public record ReservationConfirmed(
        UUID reservationId,
        String guestName,
        int partySize,
        Instant reservedFor,
        long revision,
        Instant occurredAt) implements ReservationEvent {
}
