package de.thkoeln.ddd.sample.reservationview.application;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.time.Instant;
import java.util.UUID;

/**
 * The consumer's own view of the producer's event payloads, written from the AsyncAPI
 * document rather than by importing the producer's classes. Contexts share the contract,
 * not the code. Fields the consumer does not need are simply left out (tolerant reader).
 */
final class ReservationEventMessages {

    private ReservationEventMessages() {
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    record Confirmed(UUID reservationId, String guestName, int partySize, Instant reservedFor, long revision) {
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    record PartySizeChanged(UUID reservationId, int partySize, long revision) {
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    record Cancelled(UUID reservationId, long revision) {
    }
}
