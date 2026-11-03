package de.thkoeln.ddd.sample;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.persistence.autoconfigure.EntityScan;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;

/**
 * The outbox library brings its own JPA entity and repository (package {@code de.microservicedungeon}).
 * Spring only scans the application's own package by default, so both packages are listed here.
 */
@SpringBootApplication
@EnableJpaRepositories({"de.thkoeln.ddd.sample", "de.microservicedungeon"})
@EntityScan({"de.thkoeln.ddd.sample", "de.microservicedungeon"})
public class SampleServiceApplication {

    public static void main(String[] args) {
        SpringApplication.run(SampleServiceApplication.class, args);
    }
}
