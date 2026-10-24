package de.thkoeln.ddd.sample.shared.domain;

import jakarta.persistence.Column;
import jakarta.persistence.MappedSuperclass;

import java.io.Serializable;
import java.util.Objects;
import java.util.UUID;

/**
 * Base class for typed aggregate IDs, following the archi-lab page
 * "Aggregate-Referenzen über typed IDs" (without Lombok).
 */
@MappedSuperclass
public abstract class GenericId implements Serializable {

    @Column(nullable = false, updatable = false)
    private UUID id;

    protected GenericId() {
        // for JPA only
    }

    protected GenericId(UUID id) {
        this.id = Objects.requireNonNull(id, "id must not be null");
    }

    public UUID getId() {
        return id;
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) return true;
        if (other == null || getClass() != other.getClass()) return false;
        return Objects.equals(id, ((GenericId) other).id);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(id);
    }

    @Override
    public String toString() {
        return String.valueOf(id);
    }
}
