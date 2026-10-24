package de.thkoeln.ddd.sample.outbox;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;

/** Runs the relay periodically. Tests switch it off and call the relay directly. */
@Configuration
@EnableScheduling
@ConditionalOnProperty(name = "outbox.relay.enabled", havingValue = "true", matchIfMissing = true)
class OutboxRelayScheduler {

    private final OutboxRelay relay;

    OutboxRelayScheduler(OutboxRelay relay) {
        this.relay = relay;
    }

    @Scheduled(fixedDelayString = "${outbox.relay.interval-ms:500}")
    void run() {
        relay.publishPendingBatch();
    }
}
