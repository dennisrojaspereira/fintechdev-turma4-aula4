package com.fintech.payments.integration;

import com.github.tomakehurst.wiremock.WireMockServer;
import org.junit.jupiter.api.BeforeEach;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.kafka.ConfluentKafkaContainer;
import org.testcontainers.utility.DockerImageName;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;

import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.options;
import static org.awaitility.Awaitility.await;

/**
 * The harness: the whole application against a real PostgreSQL (logical WAL), a real Kafka
 * broker, a real Debezium Kafka Connect shipping the outbox (SPEC-003), and two stubbed
 * providers (WireMock): the card PSP of SPEC-001 and the PIX provider of SPEC-002.
 * Containers are static so all integration tests share one set.
 *
 * <p>The connector is registered once, after the Spring context (and Flyway) is up, because
 * the outbox table must exist before Debezium snapshots it and creates the publication.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
public abstract class AbstractIntegrationTest {

    private static final Logger log = LoggerFactory.getLogger(AbstractIntegrationTest.class);

    public static final String CONNECTOR_NAME = "payments-outbox";
    /** Listener the Debezium container uses to reach the broker inside the Docker network. */
    private static final String KAFKA_INTERNAL_LISTENER = "kafka:19092";

    protected static final Network NETWORK = Network.newNetwork();

    protected static final PostgreSQLContainer<?> POSTGRES =
            new PostgreSQLContainer<>(DockerImageName.parse("postgres:16-alpine"))
                    .withDatabaseName("payments")
                    .withUsername("payments")
                    .withPassword("payments")
                    .withNetwork(NETWORK)
                    .withNetworkAliases("postgres")
                    // Debezium needs logical decoding (ADR-005 D8).
                    .withCommand("postgres", "-c", "wal_level=logical", "-c", "fsync=off");

    protected static final ConfluentKafkaContainer KAFKA =
            new ConfluentKafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.6.1"))
                    .withNetwork(NETWORK)
                    .withNetworkAliases("kafka")
                    .withListener(KAFKA_INTERNAL_LISTENER);

    /** Debezium Kafka Connect, the CDC publisher of the outbox. */
    protected static final GenericContainer<?> DEBEZIUM =
            new GenericContainer<>(DockerImageName.parse("debezium/connect:2.7.3.Final"))
                    .withNetwork(NETWORK)
                    .withNetworkAliases("connect")
                    .withExposedPorts(8083)
                    .withEnv("BOOTSTRAP_SERVERS", KAFKA_INTERNAL_LISTENER)
                    .withEnv("GROUP_ID", "payments-connect")
                    .withEnv("CONFIG_STORAGE_TOPIC", "connect.configs")
                    .withEnv("OFFSET_STORAGE_TOPIC", "connect.offsets")
                    .withEnv("STATUS_STORAGE_TOPIC", "connect.status")
                    .withEnv("CONFIG_STORAGE_REPLICATION_FACTOR", "1")
                    .withEnv("OFFSET_STORAGE_REPLICATION_FACTOR", "1")
                    .withEnv("STATUS_STORAGE_REPLICATION_FACTOR", "1")
                    .waitingFor(Wait.forHttp("/connectors").forPort(8083).forStatusCode(200)
                            .withStartupTimeout(Duration.ofMinutes(3)));

    /** Card PSP (SPEC-001). */
    protected static final WireMockServer PSP = new WireMockServer(options().dynamicPort());

    /** PIX provider (SPEC-002): a separate server, so misrouting is observable. */
    protected static final WireMockServer PIX = new WireMockServer(options().dynamicPort());

    private static volatile boolean connectorRegistered;

    static {
        POSTGRES.start();
        KAFKA.start();
        DEBEZIUM.start();
        PSP.start();
        PIX.start();
    }

    @DynamicPropertySource
    static void properties(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", POSTGRES::getJdbcUrl);
        registry.add("spring.datasource.username", POSTGRES::getUsername);
        registry.add("spring.datasource.password", POSTGRES::getPassword);
        registry.add("spring.kafka.bootstrap-servers", KAFKA::getBootstrapServers);
        // Short producer timeouts so a "Kafka unavailable" experiment fails fast.
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
        // SPEC-003: Debezium publishes; the poller must not exist. Short processing timeout so
        // the "dead worker" experiments finish in seconds (production: 30s, ADR-005 D10).
        registry.add("payments.outbox.publisher", () -> "cdc");
        registry.add("payments.worker.processing-timeout", () -> "3s");
    }

    @BeforeEach
    void resetProviders() {
        PSP.resetAll();
        PIX.resetAll();
        registerConnectorOnce();
    }

    // ------------------------------------------------------------ Debezium

    protected static String connectUrl() {
        return "http://" + DEBEZIUM.getHost() + ":" + DEBEZIUM.getMappedPort(8083);
    }

    /** Mirrors docker/debezium/payments-outbox-connector.json, pointed at the test network. */
    private static String connectorConfig() {
        return """
                {
                  "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
                  "tasks.max": "1",
                  "plugin.name": "pgoutput",
                  "database.hostname": "postgres",
                  "database.port": "5432",
                  "database.user": "%s",
                  "database.password": "%s",
                  "database.dbname": "%s",
                  "topic.prefix": "payments-db",
                  "slot.name": "payments_outbox",
                  "publication.name": "payments_outbox",
                  "publication.autocreate.mode": "filtered",
                  "table.include.list": "public.outbox_messages",
                  "snapshot.mode": "initial",
                  "tombstones.on.delete": "false",
                  "poll.interval.ms": "100",
                  "transforms": "outbox",
                  "transforms.outbox.type": "io.debezium.transforms.outbox.EventRouter",
                  "transforms.outbox.route.by.field": "topic",
                  "transforms.outbox.route.topic.replacement": "${routedByValue}",
                  "transforms.outbox.route.tombstone.on.empty.payload": "false",
                  "transforms.outbox.table.field.event.id": "id",
                  "transforms.outbox.table.field.event.key": "message_key",
                  "transforms.outbox.table.field.event.payload": "payload",
                  "transforms.outbox.table.expand.json.payload": "false",
                  "transforms.outbox.table.fields.additional.placement": "id:header:eventId,event_type:header:eventType,correlation_id:header:correlationId",
                  "transforms.outbox.table.op.invalid.behavior": "warn",
                  "key.converter": "org.apache.kafka.connect.storage.StringConverter",
                  "value.converter": "org.apache.kafka.connect.storage.StringConverter",
                  "header.converter": "org.apache.kafka.connect.storage.SimpleHeaderConverter"
                }
                """.formatted(POSTGRES.getUsername(), POSTGRES.getPassword(), POSTGRES.getDatabaseName());
    }

    private static synchronized void registerConnectorOnce() {
        if (connectorRegistered) {
            return;
        }
        try {
            HttpClient http = HttpClient.newHttpClient();
            HttpResponse<String> response = http.send(HttpRequest.newBuilder()
                            .uri(URI.create(connectUrl() + "/connectors/" + CONNECTOR_NAME + "/config"))
                            .header("Content-Type", "application/json")
                            .PUT(HttpRequest.BodyPublishers.ofString(connectorConfig()))
                            .build(),
                    HttpResponse.BodyHandlers.ofString());
            if (response.statusCode() / 100 != 2) {
                throw new IllegalStateException("Connector registration failed: "
                        + response.statusCode() + " " + response.body());
            }
            await().atMost(Duration.ofSeconds(60)).pollInterval(Duration.ofMillis(500))
                    .until(() -> connectorStatus(http).contains("\"tasks\":[{\"id\":0,\"state\":\"RUNNING\""));
            log.info("Debezium connector {} RUNNING: {}", CONNECTOR_NAME, connectorStatus(http));
            connectorRegistered = true;
        } catch (IOException e) {
            throw new IllegalStateException("Could not register the Debezium connector", e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted while registering the Debezium connector", e);
        }
    }

    /** The connector's status document; the task state is the last {@code "state"} in it. */
    protected static String connectorStatus(HttpClient http) throws IOException, InterruptedException {
        return http.send(HttpRequest.newBuilder()
                        .uri(URI.create(connectUrl() + "/connectors/" + CONNECTOR_NAME + "/status"))
                        .GET().build(),
                HttpResponse.BodyHandlers.ofString()).body();
    }
}
