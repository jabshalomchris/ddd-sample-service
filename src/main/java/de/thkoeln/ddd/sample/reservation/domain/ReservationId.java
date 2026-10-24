package de.thkoeln.ddd.sample.reservation.domain;

import de.thkoeln.ddd.sample.shared.domain.GenericId;
import jakarta.persistence.Embeddable;

import java.util.UUID;

@Embeddable
public class ReservationId extends GenericId {

    protected ReservationId() {
        // for JPA only
    }

    public ReservationId(UUID id) {
        super(id);
    }

    public static ReservationId newId() {
        return new ReservationId(UUID.randomUUID());
    }
}
