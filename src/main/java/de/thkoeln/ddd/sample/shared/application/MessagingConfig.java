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
     */
    @Bean
    NewTopic reservationEventsTopic(@Value("${sample.topics.reservation-events}") String name,
                                    @Value("${sample.topics.partitions:3}") int partitions) {
        return TopicBuilder.name(name).partitions(partitions).replicas(1).build();
    }

    /**
     * A listener that keeps failing is retried twice, one second apart. After that the
     * record goes to "&lt;topic&gt;.DLT" so it does not block the partition forever.
     */
    @Bean
    DefaultErrorHandler kafkaErrorHandler(KafkaTemplate<String, String> kafkaTemplate) {
        var recoverer = new DeadLetterPublishingRecoverer(kafkaTemplate,
                (record, exception) -> new TopicPartition(record.topic() + ".DLT", -1));
        return new DefaultErrorHandler(recoverer, new FixedBackOff(1000L, 2));
    }
}
