package de.thkoeln.ddd.sample.reservation.domain;

import java.time.Instant;
import java.util.UUID;

/**
 * The events this service publishes. Together they are its event API
 * (documented in docs/asyncapi.yaml).
 *
 * <p>Every event carries the aggregate's {@code revision}: 1 for the first event of a
 * reservation, then +1 per change. Consumers use it to spot events that arrive late.
 */
public sealed interface ReservationEvent
        permits ReservationConfirmed, ReservationPartySizeChanged, ReservationCancelled {

    UUID reservationId();

    long revision();

    Instant occurredAt();

    /** Name used in the {@code eventType} header. Not part of the JSON payload. */
    default String eventType() {
        return getClass().getSimpleName();
    }
}
