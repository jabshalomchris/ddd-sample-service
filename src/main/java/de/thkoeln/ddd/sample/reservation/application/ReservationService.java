package de.thkoeln.ddd.sample.reservation.application;

import de.thkoeln.ddd.sample.reservation.domain.PartySize;
import de.thkoeln.ddd.sample.reservation.domain.Reservation;
import de.thkoeln.ddd.sample.reservation.domain.ReservationId;
import de.thkoeln.ddd.sample.reservation.domain.ReservationRepository;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Clock;
import java.time.Instant;

/**
 * Each public method is one transaction: load, call the aggregate, save, write the
 * events to the outbox. Nothing here talks to Kafka.
 */
@Service
public class ReservationService {

    private final ReservationRepository reservationRepository;
    private final ReservationEventPublisher eventPublisher;
    private final Clock clock;

    ReservationService(ReservationRepository reservationRepository,
                       ReservationEventPublisher eventPublisher, Clock clock) {
        this.reservationRepository = reservationRepository;
        this.eventPublisher = eventPublisher;
        this.clock = clock;
    }

    @Transactional
    public ReservationId confirm(String guestName, int partySize, Instant reservedFor) {
        Reservation reservation = Reservation.confirm(guestName, new PartySize(partySize), reservedFor, clock);
        reservationRepository.save(reservation);
        eventPublisher.publishPendingEvents(reservation);
        return reservation.getId();
    }

    @Transactional
    public void changePartySize(ReservationId id, int partySize) {
        Reservation reservation = load(id);
        reservation.changePartySize(new PartySize(partySize), clock);
        eventPublisher.publishPendingEvents(reservation);
    }

    @Transactional
    public void cancel(ReservationId id, String reason) {
        Reservation reservation = load(id);
        reservation.cancel(reason, clock);
        eventPublisher.publishPendingEvents(reservation);
    }

    @Transactional(readOnly = true)
    public Reservation get(ReservationId id) {
        return load(id);
    }

    private Reservation load(ReservationId id) {
        return reservationRepository.findById(id).orElseThrow(() -> new ReservationNotFoundException(id));
    }
}
