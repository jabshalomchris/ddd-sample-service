package de.thkoeln.ddd.sample.demo;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.Map;

/**
 * DEMO ONLY. Read-only endpoints that feed the demo page (src/main/resources/static/index.html).
 *
 * <p>They read the tables directly so the page can show what happens inside the service:
 * the aggregate, the outbox and the consumer's view. A real service would never expose
 * its outbox or another context's data like this; do not copy this package.
 */
@RestController
@RequestMapping("/demo")
class DemoController {

    private final JdbcTemplate jdbc;

    DemoController(JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    /** Producer side: the Reservation aggregates, most recently created first. */
    @GetMapping("/reservations")
    List<Map<String, Object>> reservations() {
        return jdbc.queryForList("""
                select r.reservation_id as id, r.guest_name as "guestName", r.party_size as "partySize",
                       r.reserved_for as "reservedFor", r.status, r.revision
                from reservation r
                order by (select min(o.id) from outbox_message o
                          where o.aggregate_id = r.reservation_id::text) desc nulls last
                limit 20
                """);
    }

    /** Producer side: the outbox, newest first. publishedAt null = still waiting for Kafka. */
    @GetMapping("/outbox")
    List<Map<String, Object>> outbox() {
        return jdbc.queryForList("""
                select id, event_type as "eventType", aggregate_id as "aggregateId",
                       payload->>'revision' as revision, created_at as "createdAt", published_at as "publishedAt"
                from outbox_message order by id desc limit 20
                """);
    }

    /** Consumer side: the view the listener built from events alone. */
    @GetMapping("/view")
    List<Map<String, Object>> view() {
        return jdbc.queryForList("""
                select reservation_id as id, guest_name as "guestName", party_size as "partySize",
                       status, last_revision as "lastRevision"
                from reservation_view v
                order by (select min(o.id) from outbox_message o
                          where o.aggregate_id = v.reservation_id::text) desc nulls last
                limit 20
                """);
    }
}
