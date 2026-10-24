package de.thkoeln.ddd.sample.outbox;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import java.util.List;

interface OutboxRepository extends JpaRepository<OutboxMessage, Long> {

    /**
     * Locks the oldest unpublished messages. SKIP LOCKED lets a second relay
     * instance pick other rows instead of waiting.
     */
    @Query(value = """
            select * from outbox_message
            where published_at is null
            order by id
            limit :limit
            for update skip locked
            """, nativeQuery = true)
    List<OutboxMessage> lockNextUnpublished(@Param("limit") int limit);

    long countByPublishedAtIsNull();
}
