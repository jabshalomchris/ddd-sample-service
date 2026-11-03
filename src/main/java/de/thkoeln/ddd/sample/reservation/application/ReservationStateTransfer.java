package de.thkoeln.ddd.sample.reservation.application;

import de.thkoeln.ddd.sample.reservation.domain.Reservation;
import de.thkoeln.ddd.sample.reservation.domain.ReservationStatus;

import java.time.Instant;
import java.util.UUID;

/**
 * Payload of {@code reservation.state-transfer} on {@code db.reservation.ecst.v1}: the whole
 * current state of one reservation, published after every change (ECST, event-carried state
 * transfer, as the MSD services do with {@code game.state-transfer} and {@code player.state-transfer}).
 *
 * <p>The topic is log-compacted and keyed by reservation ID, so Kafka eventually keeps only the
 * newest state per reservation. A context that only needs "what does reservation X look like now"
 * reads this topic and never has to replay the business events. {@code revision} lets it ignore
 * a state that arrives late.
 *
 * <p>Part of the published API: renaming or removing a field needs a new version (v2).
 */
record ReservationStateTransfer(
        UUID reservationId,
        String guestName,
        int partySize,
        Instant reservedFor,
        ReservationStatus status,
        long revision) {

    static ReservationStateTransfer of(Reservation reservation) {
        return new ReservationStateTransfer(
                reservation.getId().getId(),
                reservation.getGuestName(),
                reservation.getPartySize().value(),
                reservation.getReservedFor(),
                reservation.getStatus(),
                reservation.getRevision());
    }
}
