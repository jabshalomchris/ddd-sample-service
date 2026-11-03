package de.thkoeln.ddd.sample.shared.application;

import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.TopicPartition;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.FixedBackOff;

import java.time.Clock;

@Configuration
public class MessagingConfig {

    @Bean
    Clock clock() {
        return Clock.systemUTC();
    }

    /**
     * Created on startup if missing. Three partitions on purpose: with one partition,
     * ordering bugs (missing message keys) never show up locally.
     *
     * <p>The outbox library checks at startup that every topic in {@code outbox.topics} exists
     * and reads its partition count, so the topic must exist before the first event is written.
     * Here Spring's KafkaAdmin creates it while the context starts, which is early enough.
     * In the shared stack, topics may instead be created centrally (open question for the ADR).
     */
    @Bean
    NewTopic reservationEventsTopic(@Value("${sample.topics.reservation-events}") String name,
                                    @Value("${sample.topics.partitions:3}") int partitions) {
        return TopicBuilder.name(name).partitions(partitions).replicas(1).build();
    }

    /**
     * Current state per reservation. Log-compacted ({@code cleanup.policy=compact}): Kafka keeps
     * at least the newest record per key, so a new consumer can rebuild the state of every
     * reservation without replaying its whole history. Same partition count as the events topic.
     */
    @Bean
    NewTopic reservationEcstTopic(@Value("${sample.topics.reservation-ecst}") String name,
                                  @Value("${sample.topics.partitions:3}") int partitions) {
        return TopicBuilder.name(name).partitions(partitions).replicas(1).compact().build();
    }

    /**
     * A listener that keeps failing is retried twice, one second apart. After that the
     * record goes to "&lt;topic&gt;.DLT" so it does not block the partition forever.
     */
    @Bean
    DefaultErrorHandler kafkaErrorHandler(KafkaTemplate<String, byte[]> kafkaTemplate) {
        var recoverer = new DeadLetterPublishingRecoverer(kafkaTemplate,
                (record, exception) -> new TopicPartition(record.topic() + ".DLT", -1));
        return new DefaultErrorHandler(recoverer, new FixedBackOff(1000L, 2));
    }
}
