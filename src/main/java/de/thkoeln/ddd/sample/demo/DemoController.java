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
 * the aggregate, the outbox and what the consumer received. A real service would never expose
 * its outbox or another context's data like this; do not copy this package.
 */
@RestController
@RequestMapping("/demo")
class DemoController {

    /** Newest first: reservations still waiting in the outbox, then by their first delivered event. */
    private static final String NEWEST_FIRST = """
            order by (select max(o.sequence_id) from event_outbox o where o.entity_id = %1$s::text) desc nulls last,
                     (select min(p.sequence_id) from processed_event p where p.event_key = %1$s::text) desc nulls first
            """;

    private final JdbcTemplate jdbc;

    DemoController(JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    /** Producer side: the Reservation aggregates. */
    @GetMapping("/reservations")
    List<Map<String, Object>> reservations() {
        return jdbc.queryForList("""
                select r.reservation_id as id, r.guest_name as "guestName", r.party_size as "partySize",
                       r.reserved_for as "reservedFor", r.status, r.revision
                from reservation r
                """ + NEWEST_FIRST.formatted("r.reservation_id") + " limit 20");
    }

    /**
     * Producer side: events still in the outbox. The Outbox-Service (or the local relay) deletes
     * a row once Kafka has confirmed it, so everything listed here is waiting to be sent.
     */
    @GetMapping("/outbox")
    List<Map<String, Object>> outbox() {
        return jdbc.queryForList("""
                select sequence_id as "sequenceId", event_type as "eventType", entity_id as "aggregateId",
                       partition, created_at as "createdAt"
                from event_outbox order by sequence_id desc limit 20
                """);
    }

    /** Consumer side: events the listener has processed, newest first. */
    @GetMapping("/delivered")
    List<Map<String, Object>> delivered() {
        return jdbc.queryForList("""
                select sequence_id as "sequenceId", event_type as "eventType", event_key as "aggregateId",
                       processed_at as "processedAt"
                from processed_event order by processed_at desc, sequence_id desc limit 20
                """);
    }

    /** Consumer side: the view the listener built from events alone. */
    @GetMapping("/view")
    List<Map<String, Object>> view() {
        return jdbc.queryForList("""
                select v.reservation_id as id, v.guest_name as "guestName", v.party_size as "partySize",
                       v.status, v.last_revision as "lastRevision"
                from reservation_view v
                """ + NEWEST_FIRST.formatted("v.reservation_id") + " limit 20");
    }
}
