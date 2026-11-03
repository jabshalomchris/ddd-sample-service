package de.thkoeln.ddd.sample.reservationview.application;

import de.microservicedungeon.consumerlib.processor.EventType;
import de.microservicedungeon.consumerlib.processor.kafka.KafkaDefaultHeader;
import de.microservicedungeon.consumerlib.processor.kafka.KafkaEventContext;
import de.thkoeln.ddd.sample.inbox.IdempotencyGuard;
import de.thkoeln.ddd.sample.inbox.IdempotentEventSpecification;
import de.thkoeln.ddd.sample.reservationview.application.ReservationEventMessages.Cancelled;
import de.thkoeln.ddd.sample.reservationview.application.ReservationEventMessages.Confirmed;
import de.thkoeln.ddd.sample.reservationview.application.ReservationEventMessages.PartySizeChanged;
import org.springframework.stereotype.Component;
import tools.jackson.databind.json.JsonMapper;

/**
 * One handler per event type, as the consumer library expects. Each one names the
 * {@code event_type} + version it accepts and reads the payload into the consumer's own DTO,
 * written from the producer's AsyncAPI file (no shared classes between contexts).
 * Event types without a handler are skipped by the library.
 */
final class ReservationEventHandlers {

    static final String CONSUMER_NAME = "reservation-view";

    private ReservationEventHandlers() {
    }

    @Component
    static class ConfirmedHandler extends IdempotentEventSpecification {

        private final ReservationViewProjection projection;
        private final JsonMapper jsonMapper;

        ConfirmedHandler(IdempotencyGuard guard, ReservationViewProjection projection, JsonMapper jsonMapper) {
            super(guard, CONSUMER_NAME);
            this.projection = projection;
            this.jsonMapper = jsonMapper;
        }

        @Override
        public EventType getEventType() {
            return EventType.build("reservation.confirmed", 1);
        }

        @Override
        protected void handle(KafkaEventContext<KafkaDefaultHeader, byte[]> context) {
            Confirmed e = jsonMapper.readValue(context.value(), Confirmed.class);
            projection.apply(e.reservationId(), e.revision(), "reservation.confirmed",
                    view -> view.applyConfirmed(e.guestName(), e.partySize(), e.revision()));
        }
    }

    @Component
    static class PartySizeChangedHandler extends IdempotentEventSpecification {

        private final ReservationViewProjection projection;
        private final JsonMapper jsonMapper;

        PartySizeChangedHandler(IdempotencyGuard guard, ReservationViewProjection projection, JsonMapper jsonMapper) {
            super(guard, CONSUMER_NAME);
            this.projection = projection;
            this.jsonMapper = jsonMapper;
        }

        @Override
        public EventType getEventType() {
            return EventType.build("reservation.party-size-changed", 1);
        }

        @Override
        protected void handle(KafkaEventContext<KafkaDefaultHeader, byte[]> context) {
            PartySizeChanged e = jsonMapper.readValue(context.value(), PartySizeChanged.class);
            projection.apply(e.reservationId(), e.revision(), "reservation.party-size-changed",
                    view -> view.applyPartySizeChanged(e.partySize(), e.revision()));
        }
    }

    @Component
    static class CancelledHandler extends IdempotentEventSpecification {

        private final ReservationViewProjection projection;
        private final JsonMapper jsonMapper;

        CancelledHandler(IdempotencyGuard guard, ReservationViewProjection projection, JsonMapper jsonMapper) {
            super(guard, CONSUMER_NAME);
            this.projection = projection;
            this.jsonMapper = jsonMapper;
        }

        @Override
        public EventType getEventType() {
            return EventType.build("reservation.cancelled", 1);
        }

        @Override
        protected void handle(KafkaEventContext<KafkaDefaultHeader, byte[]> context) {
            Cancelled e = jsonMapper.readValue(context.value(), Cancelled.class);
            projection.apply(e.reservationId(), e.revision(), "reservation.cancelled",
                    view -> view.applyCancelled(e.revision()));
        }
    }
}
