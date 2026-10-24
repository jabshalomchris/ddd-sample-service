package de.thkoeln.ddd.sample.reservation.application;

import de.thkoeln.ddd.sample.reservation.domain.ReservationId;

public class ReservationNotFoundException extends RuntimeException {

    public ReservationNotFoundException(ReservationId id) {
        super("Reservation " + id + " not found");
    }
}
