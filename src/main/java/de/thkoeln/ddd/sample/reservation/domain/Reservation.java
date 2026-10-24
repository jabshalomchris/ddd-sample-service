package de.thkoeln.ddd.sample.reservation.domain;

import jakarta.persistence.AttributeOverride;
import jakarta.persistence.Column;
import jakarta.persistence.Embedded;
import jakarta.persistence.EmbeddedId;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.Transient;
import jakarta.persistence.Version;

import java.time.Clock;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Aggregate root. Invariant: a cancelled reservation cannot change any more.
 *
 * <p>Every state change records a domain event. The application service pulls the
 * events after saving and hands them to the outbox in the same transaction, so the
 * aggregate never talks to Kafka itself.
 */
@Entity
public class Reservation {

    @EmbeddedId
    @AttributeOverride(name = "id", column = @Column(name = "reservation_id"))
    private ReservationId id;

    @Column(nullable = false)
    private String guestName;

    @Embedded
    private PartySize partySize;

    @Column(nullable = false)
    private Instant reservedFor;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false)
    private ReservationStatus status;

    /** Domain revision: 1 after creation, +1 per change. Published with every event. */
    @Column(nullable = false)
    private long revision;

    /** Optimistic locking only; not part of the domain model. */
    @Version
    private long version;

    @Transient
    private final List<ReservationEvent> pendingEvents = new ArrayList<>();

    protected Reservation() {
        // for JPA only
    }

    private Reservation(ReservationId id, String guestName, PartySize partySize, Instant reservedFor) {
        this.id = Objects.requireNonNull(id);
        this.guestName = requireText(guestName);
        this.partySize = Objects.requireNonNull(partySize);
        this.reservedFor = Objects.requireNonNull(reservedFor);
    }

    public static Reservation confirm(String guestName, PartySize partySize, Instant reservedFor, Clock clock) {
        Reservation reservation = new Reservation(ReservationId.newId(), guestName, partySize, reservedFor);
        reservation.status = ReservationStatus.CONFIRMED;
        reservation.revision = 1;
        reservation.pendingEvents.add(new ReservationConfirmed(
                reservation.id.getId(), reservation.guestName, partySize.value(),
                reservedFor, reservation.revision, clock.instant()));
        return reservation;
    }

    public void changePartySize(PartySize newPartySize, Clock clock) {
        requireNotCancelled("change the party size of");
        Objects.requireNonNull(newPartySize);
        if (newPartySize.equals(partySize)) {
            return; // no change, no event
        }
        partySize = newPartySize;
        revision++;
        pendingEvents.add(new ReservationPartySizeChanged(id.getId(), newPartySize.value(), revision, clock.instant()));
    }

    public void cancel(String reason, Clock clock) {
        requireNotCancelled("cancel");
        status = ReservationStatus.CANCELLED;
        revision++;
        pendingEvents.add(new ReservationCancelled(id.getId(), reason, revision, clock.instant()));
    }

    /** Returns the events recorded since the last call and forgets them. */
    public List<ReservationEvent> pullEvents() {
        List<ReservationEvent> events = List.copyOf(pendingEvents);
        pendingEvents.clear();
        return events;
    }

    private void requireNotCancelled(String action) {
        if (status == ReservationStatus.CANCELLED) {
            throw new IllegalReservationStateException(
                    "Cannot " + action + " reservation " + id + ": it is already cancelled");
        }
    }

    private static String requireText(String value) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException("Guest name must not be blank");
        }
        return value.strip();
    }

    public ReservationId getId() {
        return id;
    }

    public String getGuestName() {
        return guestName;
    }

    public PartySize getPartySize() {
        return partySize;
    }

    public Instant getReservedFor() {
        return reservedFor;
    }

    public ReservationStatus getStatus() {
        return status;
    }

    public long getRevision() {
        return revision;
    }
}
