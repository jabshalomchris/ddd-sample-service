package de.thkoeln.ddd.sample.reservationview.application;

import de.thkoeln.ddd.sample.reservationview.domain.ReservationView;
import de.thkoeln.ddd.sample.reservationview.domain.ReservationViewRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.UUID;
import java.util.function.Predicate;

/**
 * Applies one event to the consumer's own copy of a reservation. Shared by the three handlers.
 * Late events (revision not newer than the view) are ignored by {@link ReservationView} itself.
 */
@Component
class ReservationViewProjection {

    private static final Logger log = LoggerFactory.getLogger(ReservationViewProjection.class);

    private final ReservationViewRepository viewRepository;

    ReservationViewProjection(ReservationViewRepository viewRepository) {
        this.viewRepository = viewRepository;
    }

    void apply(UUID reservationId, long revision, String eventType, Predicate<ReservationView> change) {
        ReservationView view = viewRepository.findById(reservationId)
                .orElseGet(() -> ReservationView.notYetSeen(reservationId));
        if (view.hasGapBefore(revision)) {
            log.warn("Reservation {}: got revision {} but last seen was {}; earlier events are late or missing",
                    reservationId, revision, view.getLastRevision());
        }
        if (!change.test(view)) {
            log.info("Ignoring stale {} (revision {})", eventType, revision);
        }
        viewRepository.save(view);
    }
}
