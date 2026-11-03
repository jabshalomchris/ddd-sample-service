package de.thkoeln.ddd.sample.localrelay;

import de.microservicedungeon.outboxlib.config.OutboxLibConfig;
import de.microservicedungeon.outboxlib.kafka.KafkaRecordMapper;
import de.microservicedungeon.outboxlib.outbox.OutboxEventDto;
import de.microservicedungeon.outboxlib.outbox.Partition;
import de.microservicedungeon.outboxlib.outbox.ReadOutbox;
import de.microservicedungeon.outboxlib.outbox.TopicName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.support.TransactionTemplate;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.IntStream;
import java.util.stream.Stream;

/**
 * LOCAL STAND-IN for the Outbox-Service. Off by default.
 *
 * <p>In the real setup, writing and publishing are split: this service only writes events into
 * its {@code event_outbox} table (outbox-lib-jpa), and a separate Outbox-Service container reads
 * them, sends them to Kafka and deletes them. This class does the same in-process, with the same
 * library calls and the same Kafka record format, so the sample also runs when that container is
 * not available.
 *
 * <p>Switch it on with {@code sample.local-relay.enabled=true} (profile {@code local-relay}).
 * Never run it next to an Outbox-Service on the same database: each partition must have exactly
 * one reader.
 */
@Component
@EnableScheduling
@ConditionalOnProperty(name = "sample.local-relay.enabled", havingValue = "true")
public class LocalOutboxRelay {

    private static final Logger log = LoggerFactory.getLogger(LocalOutboxRelay.class);

    private final ReadOutbox readOutbox;
    private final KafkaRecordMapper recordMapper;
    private final KafkaTemplate<String, byte[]> kafkaTemplate;
    private final OutboxLibConfig outboxConfig;
    private final TransactionTemplate transaction;

    LocalOutboxRelay(ReadOutbox readOutbox, KafkaRecordMapper recordMapper,
                     KafkaTemplate<String, byte[]> kafkaTemplate, OutboxLibConfig outboxConfig,
                     PlatformTransactionManager transactionManager) {
        this.readOutbox = readOutbox;
        this.recordMapper = recordMapper;
        this.kafkaTemplate = kafkaTemplate;
        this.outboxConfig = outboxConfig;
        // Same isolation as the Outbox-Service. Programmatic, so the scheduled call below gets it too.
        this.transaction = new TransactionTemplate(transactionManager);
        this.transaction.setIsolationLevel(TransactionDefinition.ISOLATION_READ_COMMITTED);
    }

    @Scheduled(fixedDelayString = "${sample.local-relay.interval-ms:500}",
            initialDelayString = "${sample.local-relay.interval-ms:500}")
    void run() {
        try {
            publishPending();
        } catch (Exception e) {
            // Like the Outbox-Service: log, keep the rows, try again in the next run.
            log.warn("Publishing the outbox failed, retrying in the next run: {}", e.getMessage());
        }
    }

    /**
     * One polling run over all configured topics and their partitions.
     * Mirrors the Outbox-Service: read, send everything, wait for Kafka's confirmation of every
     * record, and only then acknowledge (delete) the rows. If anything fails, the rows stay and are
     * sent again, so consumers may see duplicates.
     *
     * @return number of events published
     */
    public int publishPending() {
        int published = 0;
        for (OutboxLibConfig.Topic topic : outboxConfig.getTopics()) {
            Integer count = transaction.execute(status -> publishTopic(topic));
            published += count == null ? 0 : count;
        }
        return published;
    }

    private int publishTopic(OutboxLibConfig.Topic topic) {
        TopicName name = TopicName.of(topic.getName());
        List<Partition> partitions = IntStream.range(0, topic.getPartitions()).mapToObj(Partition::of).toList();
        List<CompletableFuture<SendResult<String, byte[]>>> sends = new ArrayList<>();
        try (Stream<OutboxEventDto> events = readOutbox.readNext(name, partitions)) {
            events.forEach(event -> sends.add(kafkaTemplate.send(recordMapper.toProducerRecord(event))));
        }
        if (sends.isEmpty()) {
            return 0;
        }
        CompletableFuture.allOf(sends.toArray(CompletableFuture[]::new)).join();
        readOutbox.acknowledge(name, partitions);
        return sends.size();
    }
}
