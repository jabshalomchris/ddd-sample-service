package de.thkoeln.ddd.sample.reservationview.domain;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.Id;

import java.util.UUID;

/**
 * A consumer's own copy of what it needs to know about a reservation, built only from
 * events. It stands in for any neighbouring context (e.g. kitchen or table planning).
 *
 * <p>{@code lastRevision} protects it against late events: an event whose revision is
 * not newer than what the view already has is ignored.
 */
@Entity
public class ReservationView {

    public enum Status { CONFIRMED, CANCELLED }

    @Id
    private UUID reservationId;

    private String guestName;

    private Integer partySize;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false)
    private Status status;

    @Column(nullable = false)
    private long lastRevision;

    protected ReservationView() {
        // for JPA only
    }

    public static ReservationView notYetSeen(UUID reservationId) {
        ReservationView view = new ReservationView();
        view.reservationId = reservationId;
        view.lastRevision = 0;
        return view;
    }

    /** @return false if the event is stale (not newer than the view) and was ignored */
    public boolean applyConfirmed(String guestName, int partySize, long revision) {
        if (!isNewer(revision)) return false;
        this.guestName = guestName;
        this.partySize = partySize;
        this.status = Status.CONFIRMED;
        this.lastRevision = revision;
        return true;
    }

    public boolean applyPartySizeChanged(int partySize, long revision) {
        if (!isNewer(revision)) return false;
        this.partySize = partySize;
        if (status == null) {
            status = Status.CONFIRMED; // only confirmed reservations can change their size
        }
        this.lastRevision = revision;
        return true;
    }

    public boolean applyCancelled(long revision) {
        if (!isNewer(revision)) return false;
        this.status = Status.CANCELLED;
        this.lastRevision = revision;
        return true;
    }

    /** True if events between the last one seen and this one have not arrived (yet). */
    public boolean hasGapBefore(long revision) {
        return revision > lastRevision + 1;
    }

    private boolean isNewer(long revision) {
        return revision > lastRevision;
    }

    public UUID getReservationId() {
        return reservationId;
    }

    public String getGuestName() {
        return guestName;
    }

    public Integer getPartySize() {
        return partySize;
    }

    public Status getStatus() {
        return status;
    }

    public long getLastRevision() {
        return lastRevision;
    }
}
