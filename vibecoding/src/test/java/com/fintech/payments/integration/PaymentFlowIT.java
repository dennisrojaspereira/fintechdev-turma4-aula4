package com.fintech.payments.integration;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fintech.payments.api.dto.CreatePaymentRequest;
import com.fintech.payments.api.dto.PaymentResponse;
import com.fintech.payments.config.PaymentsProperties;
import com.fintech.payments.domain.OutboxRepository;
import com.fintech.payments.domain.Payment;
import com.fintech.payments.domain.PaymentMethod;
import com.fintech.payments.domain.PaymentRepository;
import com.fintech.payments.domain.PaymentStatus;
import com.fintech.payments.messaging.PaymentCompletedEvent;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.equalTo;
import static com.github.tomakehurst.wiremock.client.WireMock.post;
import static com.github.tomakehurst.wiremock.client.WireMock.postRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/** End-to-end: HTTP in, PSP call, PostgreSQL row, PaymentCompleted event on Kafka. */
class PaymentFlowIT extends AbstractIntegrationTest {

    @Autowired
    private TestRestTemplate rest;

    @Autowired
    private PaymentRepository payments;

    @Autowired
    private OutboxRepository outbox;

    @Autowired
    private ObjectMapper objectMapper;

    @Autowired
    private PaymentsProperties properties;

    private KafkaConsumer<String, String> consumer;

    /** Every record polled during a test, so an event can be asserted on more than once. */
    private final List<ConsumerRecord<String, String>> received = new ArrayList<>();

    @BeforeEach
    void subscribe() {
        Properties config = new Properties();
        config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
        config.put(ConsumerConfig.GROUP_ID_CONFIG, "it-" + UUID.randomUUID());
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

        received.clear();
        consumer = new KafkaConsumer<>(config);
        consumer.subscribe(List.of(properties.topics().paymentCompleted()));
        // Force the initial assignment so records produced after this point are never missed.
        consumer.poll(Duration.ofSeconds(5));
    }

    @AfterEach
    void unsubscribe() {
        consumer.close();
    }

    // ---------------------------------------------------------------- helpers

    private void stubPspApproved(String transactionId, String authorizationCode) {
        PSP.stubFor(post(urlEqualTo("/v1/charges")).willReturn(aResponse()
                .withStatus(200)
                .withHeader("Content-Type", "application/json")
                .withBody("""
                        {"transactionId":"%s","status":"APPROVED","authorizationCode":"%s"}
                        """.formatted(transactionId, authorizationCode))));
    }

    private void stubPspDeclined(String transactionId, String reason) {
        PSP.stubFor(post(urlEqualTo("/v1/charges")).willReturn(aResponse()
                .withStatus(200)
                .withHeader("Content-Type", "application/json")
                .withBody("""
                        {"transactionId":"%s","status":"DECLINED","declineReason":"%s"}
                        """.formatted(transactionId, reason))));
    }

    private ResponseEntity<PaymentResponse> createPayment(String idempotencyKey,
                                                          CreatePaymentRequest body) {
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        headers.set("Idempotency-Key", idempotencyKey);
        return rest.exchange("/api/v1/payments", HttpMethod.POST,
                new HttpEntity<>(body, headers), PaymentResponse.class);
    }

    private static CreatePaymentRequest request(String amount) {
        return new CreatePaymentRequest("merchant-1", "customer-1",
                new BigDecimal(amount), "BRL", PaymentMethod.CREDIT_CARD);
    }

    /** Polls Kafka once, accumulating everything seen so far into {@link #received}. */
    private void drain(Duration timeout) {
        ConsumerRecords<String, String> batch = consumer.poll(timeout);
        batch.forEach(received::add);
    }

    private List<ConsumerRecord<String, String>> eventsFor(UUID paymentId) {
        return received.stream()
                .filter(r -> r.value().contains(paymentId.toString()))
                .toList();
    }

    /** Polls Kafka until an event for the given payment arrives, or fails after the timeout. */
    private ConsumerRecord<String, String> awaitEventFor(UUID paymentId) {
        await().atMost(Duration.ofSeconds(20)).pollInterval(Duration.ofMillis(200))
                .until(() -> {
                    drain(Duration.ofMillis(300));
                    return !eventsFor(paymentId).isEmpty();
                });
        return eventsFor(paymentId).getFirst();
    }

    /**
     * Keeps polling for a while so that a *second*, unwanted event would have time to show up,
     * then counts what actually arrived for this payment.
     */
    private long countEventsFor(UUID paymentId) {
        long deadline = System.currentTimeMillis() + 3000;
        while (System.currentTimeMillis() < deadline) {
            drain(Duration.ofMillis(300));
        }
        return eventsFor(paymentId).size();
    }

    private PaymentCompletedEvent parse(ConsumerRecord<String, String> record) {
        try {
            return objectMapper.readValue(record.value(), PaymentCompletedEvent.class);
        } catch (Exception e) {
            throw new AssertionError("Event payload is not a PaymentCompletedEvent: "
                    + record.value(), e);
        }
    }

    private static String header(ConsumerRecord<String, String> record, String name) {
        var header = record.headers().lastHeader(name);
        return header == null ? null : new String(header.value(), StandardCharsets.UTF_8);
    }

    // ------------------------------------------------------------------ tests

    @Test
    @DisplayName("approved payment: stored in PostgreSQL and published to Kafka")
    void approvedPaymentIsStoredAndPublished() {
        stubPspApproved("psp-tx-100", "AUTH-100");

        ResponseEntity<PaymentResponse> response = createPayment("idem-approved", request("250.75"));

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.CREATED);
        assertThat(response.getHeaders().getLocation()).isNotNull();
        PaymentResponse body = response.getBody();
        assertThat(body).isNotNull();
        assertThat(body.status()).isEqualTo("APPROVED");
        assertThat(body.pspTransactionId()).isEqualTo("psp-tx-100");
        assertThat(body.authorizationCode()).isEqualTo("AUTH-100");

        // PostgreSQL
        Payment stored = payments.findById(body.id()).orElseThrow();
        assertThat(stored.getStatus()).isEqualTo(PaymentStatus.APPROVED);
        assertThat(stored.getAmount()).isEqualByComparingTo("250.75");
        assertThat(stored.getCurrency()).isEqualTo("BRL");
        assertThat(stored.getIdempotencyKey()).isEqualTo("idem-approved");

        // Kafka
        ConsumerRecord<String, String> record = awaitEventFor(body.id());
        assertThat(record.key()).isEqualTo(body.id().toString());
        assertThat(header(record, "eventType")).isEqualTo("PaymentCompleted");
        assertThat(header(record, "eventId")).isNotBlank();

        PaymentCompletedEvent event = parse(record);
        assertThat(event.eventType()).isEqualTo("PaymentCompleted");
        assertThat(event.paymentId()).isEqualTo(body.id());
        assertThat(event.status()).isEqualTo("APPROVED");
        assertThat(event.amount()).isEqualByComparingTo("250.75");
        assertThat(event.currency()).isEqualTo("BRL");
        assertThat(event.merchantId()).isEqualTo("merchant-1");
        assertThat(event.pspTransactionId()).isEqualTo("psp-tx-100");
        assertThat(event.authorizationCode()).isEqualTo("AUTH-100");
        assertThat(event.occurredAt()).isNotNull();

        // The outbox drains completely.
        await().atMost(Duration.ofSeconds(10))
                .untilAsserted(() -> assertThat(outbox.countByPublishedAtIsNull()).isZero());
    }

    @Test
    @DisplayName("declined payment is persisted and published with status DECLINED")
    void declinedPaymentIsPublished() {
        stubPspDeclined("psp-tx-200", "INSUFFICIENT_FUNDS");

        ResponseEntity<PaymentResponse> response = createPayment("idem-declined", request("10.00"));

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.CREATED);
        UUID id = response.getBody().id();
        assertThat(payments.findById(id).orElseThrow().getStatus())
                .isEqualTo(PaymentStatus.DECLINED);

        PaymentCompletedEvent event = parse(awaitEventFor(id));
        assertThat(event.status()).isEqualTo("DECLINED");
        assertThat(event.failureReason()).isEqualTo("INSUFFICIENT_FUNDS");
        assertThat(event.authorizationCode()).isNull();
    }

    @Test
    @DisplayName("replaying an idempotency key charges the PSP once and publishes one event")
    void idempotentRetryDoesNotChargeTwice() {
        stubPspApproved("psp-tx-300", "AUTH-300");

        ResponseEntity<PaymentResponse> first = createPayment("idem-repeat", request("99.99"));
        ResponseEntity<PaymentResponse> second = createPayment("idem-repeat", request("99.99"));

        assertThat(first.getStatusCode()).isEqualTo(HttpStatus.CREATED);
        assertThat(second.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(second.getBody().id()).isEqualTo(first.getBody().id());

        PSP.verify(1, postRequestedFor(urlEqualTo("/v1/charges"))
                .withHeader("Idempotency-Key", equalTo("idem-repeat")));

        awaitEventFor(first.getBody().id());
        long eventsForPayment = countEventsFor(first.getBody().id());
        assertThat(eventsForPayment).isEqualTo(1);
    }

    @Test
    @DisplayName("an unreachable PSP answers 502, stores FAILED and publishes nothing")
    void pspOutageIsNotADecline() {
        PSP.stubFor(post(urlEqualTo("/v1/charges"))
                .willReturn(aResponse().withStatus(503).withBody("psp down")));

        ResponseEntity<PaymentResponse> response = createPayment("idem-outage", request("42.00"));

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.BAD_GATEWAY);
        UUID id = response.getBody().id();

        Payment stored = payments.findById(id).orElseThrow();
        assertThat(stored.getStatus()).isEqualTo(PaymentStatus.FAILED);
        assertThat(stored.getFailureReason()).contains("PSP unavailable");

        // maxAttempts is 2 in tests: the client retried once before giving up.
        PSP.verify(2, postRequestedFor(urlEqualTo("/v1/charges")));

        // No PaymentCompleted event: the outcome is unknown, so nothing is announced downstream.
        assertThat(countEventsFor(id)).isZero();
    }

    @Test
    @DisplayName("validation rejects a bad request before any PSP call or database write")
    void invalidRequestNeverReachesThePsp() {
        stubPspApproved("psp-tx-400", "AUTH-400");
        long before = payments.count();

        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        headers.set("Idempotency-Key", "idem-invalid");
        ResponseEntity<Map> response = rest.exchange("/api/v1/payments", HttpMethod.POST,
                new HttpEntity<>(Map.of(
                        "merchantId", "merchant-1",
                        "customerId", "customer-1",
                        "amount", "-5.00",
                        "currency", "BRL",
                        "paymentMethod", "CREDIT_CARD"), headers),
                Map.class);

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);
        assertThat(payments.count()).isEqualTo(before);
        PSP.verify(0, postRequestedFor(urlEqualTo("/v1/charges")));
    }

    @Test
    @DisplayName("a stored payment can be read back")
    void paymentCanBeFetched() {
        stubPspApproved("psp-tx-500", "AUTH-500");
        UUID id = createPayment("idem-fetch", request("15.00")).getBody().id();

        ResponseEntity<PaymentResponse> fetched =
                rest.getForEntity("/api/v1/payments/{id}", PaymentResponse.class, id);

        assertThat(fetched.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(fetched.getBody().id()).isEqualTo(id);
        assertThat(fetched.getBody().status()).isEqualTo("APPROVED");
    }

    @Test
    @DisplayName("an unknown payment id answers 404")
    void unknownPaymentIsNotFound() {
        ResponseEntity<String> fetched = rest.getForEntity(
                "/api/v1/payments/{id}", String.class, UUID.randomUUID());

        assertThat(fetched.getStatusCode()).isEqualTo(HttpStatus.NOT_FOUND);
    }
}
