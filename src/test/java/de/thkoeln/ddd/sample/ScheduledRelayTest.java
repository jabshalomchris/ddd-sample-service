package de.thkoeln.ddd.sample;

import de.thkoeln.ddd.sample.reservationview.domain.ReservationView;
import de.thkoeln.ddd.sample.reservationview.domain.ReservationViewRepository;
import io.zonky.test.db.postgres.embedded.EmbeddedPostgres;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import tools.jackson.databind.json.JsonMapper;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Runtime setup: real HTTP calls, and the scheduler drives the relay. This is the
 * walkthrough from the README, section 1, as a test.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT, properties = {
        "outbox.relay.enabled=true",
        "outbox.relay.interval-ms=200",
        "spring.kafka.bootstrap-servers=${spring.embedded.kafka.brokers}"
})
@EmbeddedKafka
class ScheduledRelayTest {

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

    @Value("${local.server.port}") int port;
    @Autowired ReservationViewRepository viewRepository;
    @Autowired JsonMapper jsonMapper;

    private final HttpClient http = HttpClient.newHttpClient();

    @Test
    void readmeWalkthroughEndsWithACancelledViewAtRevisionThree() throws Exception {
        HttpResponse<String> created = call("POST", "/reservations",
                "{\"guestName\":\"Ada\",\"partySize\":4,\"reservedFor\":\"2026-11-06T19:00:00Z\"}");
        assertThat(created.statusCode()).isEqualTo(201);
        UUID id = UUID.fromString(jsonMapper.readTree(created.body()).get("id").asString());

        assertThat(call("PUT", "/reservations/" + id + "/party-size", "{\"partySize\":6}").statusCode()).isEqualTo(204);
        assertThat(call("POST", "/reservations/" + id + "/cancellation", "{\"reason\":\"ill\"}").statusCode()).isEqualTo(204);
        assertThat(call("POST", "/reservations/" + id + "/cancellation", "{\"reason\":\"again\"}").statusCode())
                .as("the aggregate's invariant surfaces as 409").isEqualTo(409);
        assertThat(call("PUT", "/reservations/" + id + "/party-size", "{\"partySize\":13}").statusCode())
                .as("invalid value object surfaces as 400").isEqualTo(400);

        long deadline = System.nanoTime() + Duration.ofSeconds(20).toNanos();
        while (viewRepository.findById(id).map(v -> v.getLastRevision() < 3).orElse(true)
                && System.nanoTime() < deadline) {
            Thread.sleep(100);
        }

        assertThat(viewRepository.findById(id)).hasValueSatisfying(view -> {
            assertThat(view.getStatus()).isEqualTo(ReservationView.Status.CANCELLED);
            assertThat(view.getPartySize()).isEqualTo(6);
            assertThat(view.getLastRevision()).isEqualTo(3);
        });

        // The demo page and its read-only endpoints show the same state.
        HttpResponse<String> page = call("GET", "/", "");
        assertThat(page.statusCode()).isEqualTo(200);
        assertThat(page.body()).contains("Outbox demo");
        assertThat(jsonMapper.readTree(call("GET", "/demo/reservations", "").body()).toString())
                .contains(id.toString()).contains("CANCELLED");
        String outbox = call("GET", "/demo/outbox", "").body();
        assertThat(outbox).contains("ReservationConfirmed", "ReservationPartySizeChanged", "ReservationCancelled");
        assertThat(call("GET", "/demo/view", "").body()).contains(id.toString()).contains("\"lastRevision\":3");
    }

    private HttpResponse<String> call(String method, String path, String json) throws Exception {
        HttpRequest request = HttpRequest.newBuilder(URI.create("http://localhost:" + port + path))
                .header("Content-Type", "application/json")
                .method(method, HttpRequest.BodyPublishers.ofString(json))
                .build();
        return http.send(request, HttpResponse.BodyHandlers.ofString());
    }
}
