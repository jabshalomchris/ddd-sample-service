package de.thkoeln.ddd.sample.outbox;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import org.hibernate.annotations.JdbcTypeCode;
import org.hibernate.type.SqlTypes;

import java.time.Instant;
import java.util.UUID;

/**
 * One event waiting to be published (or already published) to Kafka.
 *
 * <p>STAND-IN: the supervisors provide a helper library for the outbox. Once it is
 * available, this package is replaced by that library; the rules in the README stay.
 */
@Entity
public class OutboxMessage {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(nullable = false, unique = true, updatable = false)
    private UUID eventId;

    @Column(nullable = false, updatable = false)
    private String aggregateType;

    @Column(nullable = false, updatable = false)
    private String aggregateId;

    @Column(nullable = false, updatable = false)
    private String eventType;

    @Column(nullable = false, updatable = false)
    private String topic;

    @JdbcTypeCode(SqlTypes.JSON)
    @Column(nullable = false, updatable = false, columnDefinition = "jsonb")
    private String payload;

    @Column(nullable = false, updatable = false)
    private Instant createdAt;

    private Instant publishedAt;

    protected OutboxMessage() {
        // for JPA only
    }

    OutboxMessage(UUID eventId, String aggregateType, String aggregateId, String eventType,
                  String topic, String payload, Instant createdAt) {
        this.eventId = eventId;
        this.aggregateType = aggregateType;
        this.aggregateId = aggregateId;
        this.eventType = eventType;
        this.topic = topic;
        this.payload = payload;
        this.createdAt = createdAt;
    }

    void markPublished(Instant when) {
        this.publishedAt = when;
    }

    public Long getId() {
        return id;
    }

    public UUID getEventId() {
        return eventId;
    }

    public String getAggregateType() {
        return aggregateType;
    }

    public String getAggregateId() {
        return aggregateId;
    }

    public String getEventType() {
        return eventType;
    }

    public String getTopic() {
        return topic;
    }

    public String getPayload() {
        return payload;
    }

    public Instant getCreatedAt() {
        return createdAt;
    }

    public Instant getPublishedAt() {
        return publishedAt;
    }
}
