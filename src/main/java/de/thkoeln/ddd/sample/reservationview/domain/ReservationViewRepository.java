package de.thkoeln.ddd.sample.reservationview.domain;

import org.springframework.data.jpa.repository.JpaRepository;

import java.util.UUID;

public interface ReservationViewRepository extends JpaRepository<ReservationView, UUID> {
}
