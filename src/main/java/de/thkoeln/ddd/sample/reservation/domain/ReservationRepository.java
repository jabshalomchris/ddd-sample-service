package de.thkoeln.ddd.sample.reservation.domain;

import org.springframework.data.jpa.repository.JpaRepository;

public interface ReservationRepository extends JpaRepository<Reservation, ReservationId> {
}
