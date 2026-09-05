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
 * The harness: the whole application against a real PostgreSQL, a real Kafka broker and two
 * stubbed providers (WireMock): the card PSP of SPEC-001 and the PIX provider of SPEC-002.
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

    /** Card PSP (SPEC-001). */
    protected static final WireMockServer PSP = new WireMockServer(options().dynamicPort());

    /** PIX provider (SPEC-002): a separate server, so misrouting is observable. */
    protected static final WireMockServer PIX = new WireMockServer(options().dynamicPort());

    static {
        POSTGRES.start();
        KAFKA.start();
        PSP.start();
        PIX.start();
    }

    @DynamicPropertySource
    static void properties(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", POSTGRES::getJdbcUrl);
        registry.add("spring.datasource.username", POSTGRES::getUsername);
        registry.add("spring.datasource.password", POSTGRES::getPassword);
        registry.add("spring.kafka.bootstrap-servers", KAFKA::getBootstrapServers);
        // Short producer timeouts so a "Kafka unavailable" experiment fails fast instead of
        // blocking the outbox poller for 30s.
        registry.add("spring.kafka.producer.properties.delivery.timeout.ms", () -> 3000);
        registry.add("spring.kafka.producer.properties.request.timeout.ms", () -> 1000);
        registry.add("spring.kafka.producer.properties.max.block.ms", () -> 2000);
        registry.add("payments.psp.base-url", () -> "http://localhost:" + PSP.port());
        registry.add("payments.psp.connect-timeout", () -> "300ms");
        registry.add("payments.psp.read-timeout", () -> "500ms");
        registry.add("payments.psp.max-attempts", () -> 3);
        registry.add("payments.psp.retry-backoff", () -> "20ms");
        registry.add("payments.pix.base-url", () -> "http://localhost:" + PIX.port());
        registry.add("payments.pix.connect-timeout", () -> "300ms");
        registry.add("payments.pix.read-timeout", () -> "500ms");
        registry.add("payments.pix.max-attempts", () -> 3);
        registry.add("payments.pix.retry-backoff", () -> "20ms");
        registry.add("payments.outbox.poll-interval-ms", () -> 200);
    }

    @BeforeEach
    void resetProviders() {
        PSP.resetAll();
        PIX.resetAll();
    }
}
