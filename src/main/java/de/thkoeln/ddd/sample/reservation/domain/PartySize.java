package de.thkoeln.ddd.sample.reservation.domain;

import jakarta.persistence.Column;
import jakarta.persistence.Embeddable;

/** Value object: a party has between 1 and 12 guests. */
@Embeddable
public record PartySize(@Column(name = "party_size", nullable = false) int value) {

    public static final int MIN = 1;
    public static final int MAX = 12;

    public PartySize {
        if (value < MIN || value > MAX) {
            throw new IllegalArgumentException(
                    "Party size must be between " + MIN + " and " + MAX + ", was " + value);
        }
    }
}
