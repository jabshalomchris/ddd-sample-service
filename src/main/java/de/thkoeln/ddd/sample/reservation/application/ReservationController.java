package de.thkoeln.ddd.sample.reservation.application;

import de.thkoeln.ddd.sample.reservation.domain.IllegalReservationStateException;
import de.thkoeln.ddd.sample.reservation.domain.Reservation;
import de.thkoeln.ddd.sample.reservation.domain.ReservationId;
import org.springframework.http.HttpStatus;
import org.springframework.http.ProblemDetail;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;

import java.net.URI;
import java.time.Instant;
import java.util.UUID;

/** Thin REST adapter so the event flow can be triggered by hand (see README). */
@RestController
@RequestMapping("/reservations")
class ReservationController {

    record ConfirmReservationRequest(String guestName, int partySize, Instant reservedFor) {
    }

    record ChangePartySizeRequest(int partySize) {
    }

    record CancelReservationRequest(String reason) {
    }

    record ReservationResponse(UUID id, String guestName, int partySize, Instant reservedFor,
                               String status, long revision) {

        static ReservationResponse of(Reservation reservation) {
            return new ReservationResponse(reservation.getId().getId(), reservation.getGuestName(),
                    reservation.getPartySize().value(), reservation.getReservedFor(),
                    reservation.getStatus().name(), reservation.getRevision());
        }
    }

    private final ReservationService reservationService;

    ReservationController(ReservationService reservationService) {
        this.reservationService = reservationService;
    }

    @PostMapping
    ResponseEntity<ReservationResponse> confirm(@RequestBody ConfirmReservationRequest request) {
        ReservationId id = reservationService.confirm(request.guestName(), request.partySize(), request.reservedFor());
        return ResponseEntity.created(URI.create("/reservations/" + id))
                .body(ReservationResponse.of(reservationService.get(id)));
    }

    @GetMapping("/{id}")
    ReservationResponse get(@PathVariable UUID id) {
        return ReservationResponse.of(reservationService.get(new ReservationId(id)));
    }

    @PutMapping("/{id}/party-size")
    @ResponseStatus(HttpStatus.NO_CONTENT)
    void changePartySize(@PathVariable UUID id, @RequestBody ChangePartySizeRequest request) {
        reservationService.changePartySize(new ReservationId(id), request.partySize());
    }

    @PostMapping("/{id}/cancellation")
    @ResponseStatus(HttpStatus.NO_CONTENT)
    void cancel(@PathVariable UUID id, @RequestBody CancelReservationRequest request) {
        reservationService.cancel(new ReservationId(id), request.reason());
    }

    @ExceptionHandler(ReservationNotFoundException.class)
    ProblemDetail notFound(ReservationNotFoundException e) {
        return ProblemDetail.forStatusAndDetail(HttpStatus.NOT_FOUND, e.getMessage());
    }

    @ExceptionHandler(IllegalReservationStateException.class)
    ProblemDetail conflict(IllegalReservationStateException e) {
        return ProblemDetail.forStatusAndDetail(HttpStatus.CONFLICT, e.getMessage());
    }

    @ExceptionHandler(IllegalArgumentException.class)
    ProblemDetail badRequest(IllegalArgumentException e) {
        return ProblemDetail.forStatusAndDetail(HttpStatus.BAD_REQUEST, e.getMessage());
    }
}
