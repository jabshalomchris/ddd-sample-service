package de.thkoeln.ddd.sample.reservationview.application;

import de.microservicedungeon.consumerlib.processor.ProcessorChain;
import de.microservicedungeon.consumerlib.processor.kafka.KafkaDefaultHeader;
import de.microservicedungeon.consumerlib.processor.kafka.KafkaEventContext;
import de.microservicedungeon.consumerlib.processor.kafka.KafkaProcessorChainFactory;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

/**
 * Consumes the reservation events the way any neighbouring context should, using the
 * consumer library:
 * <ol>
 *   <li>the library reads the standard headers and picks the handler by {@code event_type};
 *       unknown event types are skipped,</li>
 *   <li>each handler skips duplicates by {@code event_id} and late events by revision,</li>
 *   <li>everything runs in one database transaction; the offset is committed afterwards.</li>
 * </ol>
 * Exceptions (including records with missing or broken headers) are retried and then sent to
 * the dead-letter topic (see MessagingConfig).
 */
@Component
class ReservationEventListener {

    private final ProcessorChain<KafkaEventContext<KafkaDefaultHeader, byte[]>, ConsumerRecord<String, byte[]>> chain;

    ReservationEventListener(KafkaProcessorChainFactory chainFactory,
                             ReservationEventHandlers.ConfirmedHandler confirmed,
                             ReservationEventHandlers.PartySizeChangedHandler partySizeChanged,
                             ReservationEventHandlers.CancelledHandler cancelled) {
        this.chain = chainFactory.defaultBuilder()
                .register(confirmed)
                .register(partySizeChanged)
                .register(cancelled)
                .build();
    }

    @KafkaListener(id = ReservationEventHandlers.CONSUMER_NAME,
            topics = "${sample.topics.reservation-events}",
            groupId = "sample-service." + ReservationEventHandlers.CONSUMER_NAME)
    @Transactional
    void on(ConsumerRecord<String, byte[]> record) {
        chain.process(record);
    }
}
