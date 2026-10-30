package de.thkoeln.ddd.sample;

import io.zonky.test.db.postgres.embedded.EmbeddedPostgres;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.time.Duration;

/**
 * Not a test: starts the service with an embedded PostgreSQL and an in-process Kafka,
 * so the demo page can be tried without Docker:
 *
 * <pre>./mvnw test -Dtest=DemoPageRunner -Ddemo.run=true</pre>
 *
 * Then open http://localhost:8080. The relay runs every 4 seconds here (instead of 0.5),
 * so you can see events wait in the outbox before they are sent. Stops after 30 minutes
 * or Ctrl+C. Skipped in normal builds.
 */
@EnabledIfSystemProperty(named = "demo.run", matches = "true")
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.DEFINED_PORT, properties = {
        "server.port=8080",
        "outbox.relay.enabled=true",
        "outbox.relay.interval-ms=4000",
        "spring.kafka.bootstrap-servers=${spring.embedded.kafka.brokers}"
})
@EmbeddedKafka
class DemoPageRunner {

    private static final EmbeddedPostgres POSTGRES;

    static {
        try {
            POSTGRES = EmbeddedPostgres.builder().start();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @DynamicPropertySource
    static void datasource(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url",
                () -> POSTGRES.getJdbcUrl("postgres", "postgres") + "&currentSchema=sample_service");
        registry.add("spring.datasource.username", () -> "postgres");
        registry.add("spring.datasource.password", () -> "postgres");
    }

    @Test
    void runForThirtyMinutes() throws InterruptedException {
        System.out.println("\n=== Demo page running at http://localhost:8080 (Ctrl+C to stop) ===\n");
        Thread.sleep(Duration.ofMinutes(Long.getLong("demo.minutes", 30)).toMillis());
    }
}
