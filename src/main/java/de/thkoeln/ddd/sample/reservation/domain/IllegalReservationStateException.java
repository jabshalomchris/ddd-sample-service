package de.thkoeln.ddd.sample.reservation.domain;

public class IllegalReservationStateException extends RuntimeException {

    public IllegalReservationStateException(String message) {
        super(message);
    }
}
