package com.fintech.payments.integration;

import com.github.tomakehurst.wiremock.WireMockServer;
import org.junit.jupiter.api.BeforeEach;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.kafka.ConfluentKafkaContainer;
import org.testcontainers.utility.DockerImageName;

import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.options;

/**
 * Boots the whole application against a real PostgreSQL, a real Kafka broker and a stubbed PSP.
 * Containers are static so all integration tests share one set.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
public abstract class AbstractIntegrationTest {

    protected static final PostgreSQLContainer<?> POSTGRES =
            new PostgreSQLContainer<>(DockerImageName.parse("postgres:16-alpine"))
                    .withDatabaseName("payments")
                    .withUsername("payments")
                    .withPassword("payments");

    protected static final ConfluentKafkaContainer KAFKA =
            new ConfluentKafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.6.1"));

    protected static final WireMockServer PSP = new WireMockServer(options().dynamicPort());

    static {
        POSTGRES.start();
        KAFKA.start();
        PSP.start();
    }

    @DynamicPropertySource
    static void properties(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", POSTGRES::getJdbcUrl);
        registry.add("spring.datasource.username", POSTGRES::getUsername);
        registry.add("spring.datasource.password", POSTGRES::getPassword);
        registry.add("spring.kafka.bootstrap-servers", KAFKA::getBootstrapServers);
        registry.add("payments.psp.base-url", () -> "http://localhost:" + PSP.port());
        registry.add("payments.psp.max-attempts", () -> 2);
        registry.add("payments.psp.retry-backoff", () -> "50ms");
        registry.add("payments.outbox.poll-interval-ms", () -> 200);
    }

    @BeforeEach
    void resetPsp() {
        PSP.resetAll();
    }
}
