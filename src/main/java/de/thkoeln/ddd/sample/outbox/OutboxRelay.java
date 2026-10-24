package de.thkoeln.ddd.sample.outbox;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Sends unpublished outbox messages to Kafka, oldest first, and marks them as published.
 *
 * <p>Delivery is at least once: if the service dies after Kafka acknowledged a message
 * but before {@code published_at} is committed, the message is sent again on restart.
 * Consumers must therefore tolerate duplicates (see the README).
 */
@Component
public class OutboxRelay {

    public static final String HEADER_EVENT_ID = "eventId";
    public static final String HEADER_EVENT_TYPE = "eventType";
    public static final String HEADER_AGGREGATE_TYPE = "aggregateType";

    private static final Logger log = LoggerFactory.getLogger(OutboxRelay.class);

    private final OutboxRepository outboxRepository;
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final Clock clock;
    private final int batchSize;

    OutboxRelay(OutboxRepository outboxRepository, KafkaTemplate<String, String> kafkaTemplate, Clock clock,
                @Value("${outbox.relay.batch-size:100}") int batchSize) {
        this.outboxRepository = outboxRepository;
        this.kafkaTemplate = kafkaTemplate;
        this.clock = clock;
        this.batchSize = batchSize;
    }

    /** @return number of messages published in this run */
    @Transactional
    public int publishPendingBatch() {
        List<OutboxMessage> batch = outboxRepository.lockNextUnpublished(batchSize);
        int published = 0;
        for (OutboxMessage message : batch) {
            try {
                kafkaTemplate.send(toRecord(message)).get(10, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            } catch (Exception e) {
                // Stop here instead of skipping: a later event of the same aggregate must not
                // overtake this one. Everything sent so far is still marked as published.
                log.warn("Could not publish outbox message {} ({}), retrying in the next run",
                        message.getId(), message.getEventType(), e);
                break;
            }
            message.markPublished(clock.instant());
            published++;
        }
        return published;
    }

    private static ProducerRecord<String, String> toRecord(OutboxMessage message) {
        ProducerRecord<String, String> record =
                new ProducerRecord<>(message.getTopic(), message.getAggregateId(), message.getPayload());
        record.headers()
                .add(HEADER_EVENT_ID, bytes(message.getEventId().toString()))
                .add(HEADER_EVENT_TYPE, bytes(message.getEventType()))
                .add(HEADER_AGGREGATE_TYPE, bytes(message.getAggregateType()));
        return record;
    }

    private static byte[] bytes(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }
}
