package de.thkoeln.ddd.sample.reservation.domain;

import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ReservationTest {

    private final Clock clock = Clock.fixed(Instant.parse("2026-11-06T10:00:00Z"), ZoneOffset.UTC);
    private final Instant evening = Instant.parse("2026-11-06T19:00:00Z");

    @Test
    void confirmingRecordsOneEventWithRevisionOne() {
        Reservation reservation = Reservation.confirm("Ada", new PartySize(4), evening, clock);

        assertThat(reservation.pullEvents()).singleElement().isInstanceOfSatisfying(ReservationConfirmed.class, e -> {
            assertThat(e.reservationId()).isEqualTo(reservation.getId().getId());
            assertThat(e.partySize()).isEqualTo(4);
            assertThat(e.revision()).isEqualTo(1);
        });
        assertThat(reservation.pullEvents()).as("events are handed out only once").isEmpty();
    }

    @Test
    void everyChangeIncrementsTheRevision() {
        Reservation reservation = Reservation.confirm("Ada", new PartySize(4), evening, clock);
        reservation.changePartySize(new PartySize(6), clock);
        reservation.cancel("ill", clock);

        assertThat(reservation.pullEvents())
                .extracting(ReservationEvent::revision)
                .containsExactly(1L, 2L, 3L);
    }

    @Test
    void unchangedPartySizeRecordsNoEvent() {
        Reservation reservation = Reservation.confirm("Ada", new PartySize(4), evening, clock);
        reservation.pullEvents();

        reservation.changePartySize(new PartySize(4), clock);

        assertThat(reservation.pullEvents()).isEmpty();
        assertThat(reservation.getRevision()).isEqualTo(1);
    }

    @Test
    void cancelledReservationCannotChange() {
        Reservation reservation = Reservation.confirm("Ada", new PartySize(4), evening, clock);
        reservation.cancel("ill", clock);

        assertThatThrownBy(() -> reservation.cancel("again", clock))
                .isInstanceOf(IllegalReservationStateException.class);
        assertThatThrownBy(() -> reservation.changePartySize(new PartySize(2), clock))
                .isInstanceOf(IllegalReservationStateException.class);
    }

    @Test
    void partySizeMustBeBetweenOneAndTwelve() {
        assertThatThrownBy(() -> new PartySize(0)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new PartySize(13)).isInstanceOf(IllegalArgumentException.class);
    }
}
