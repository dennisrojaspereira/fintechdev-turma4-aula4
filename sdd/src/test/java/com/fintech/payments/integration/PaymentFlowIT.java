package com.fintech.payments.integration;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fintech.payments.api.dto.CreatePaymentRequest;
import com.fintech.payments.api.dto.PaymentResponse;
import com.fintech.payments.config.PaymentsProperties;
import com.fintech.payments.domain.LedgerEntryRepository;
import com.fintech.payments.domain.OutboxMessage;
import com.fintech.payments.domain.OutboxRepository;
import com.fintech.payments.domain.Payment;
import com.fintech.payments.domain.PaymentMethod;
import com.fintech.payments.domain.PaymentProvider;
import com.fintech.payments.domain.PaymentRepository;
import com.fintech.payments.domain.PaymentStatus;
import com.fintech.payments.domain.ProcessedEventRepository;
import com.fintech.payments.messaging.KafkaHeaders;
import com.fintech.payments.messaging.OutboxPublisher;
import com.fintech.payments.messaging.PaymentCompletedEvent;
import com.fintech.payments.service.PaymentStore;
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
import org.springframework.kafka.core.KafkaTemplate;

import java.math.BigDecimal;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.equalTo;
import static com.github.tomakehurst.wiremock.client.WireMock.post;
import static com.github.tomakehurst.wiremock.client.WireMock.postRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Harness experiments (harness/README.md) as executable evidence for the SPEC-001 acceptance
 * criteria: HTTP in, PSP call, PostgreSQL rows, Kafka event, idempotent consumer.
 */
class PaymentFlowIT extends AbstractIntegrationTest {

    @Autowired
    private TestRestTemplate rest;

    @Autowired
    private PaymentRepository payments;

    @Autowired
    private OutboxRepository outbox;

    @Autowired
    private ProcessedEventRepository processedEvents;

    @Autowired
    private LedgerEntryRepository ledger;

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

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
        stubPspApproved(transactionId, authorizationCode, 0);
    }

    private void stubPspApproved(String transactionId, String authorizationCode, int delayMs) {
        PSP.stubFor(post(urlEqualTo("/v1/charges")).willReturn(aResponse()
                .withStatus(200)
                .withHeader("Content-Type", "application/json")
                .withFixedDelay(delayMs)
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
        return createPayment(idempotencyKey, body, null);
    }

    private ResponseEntity<PaymentResponse> createPayment(String idempotencyKey,
                                                          CreatePaymentRequest body,
                                                          String correlationId) {
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        headers.set("Idempotency-Key", idempotencyKey);
        if (correlationId != null) {
            headers.set("X-Correlation-Id", correlationId);
        }
        return rest.exchange("/api/v1/payments", HttpMethod.POST,
                new HttpEntity<>(body, headers), PaymentResponse.class);
    }

    private static CreatePaymentRequest request(String amount) {
        return new CreatePaymentRequest("merchant-1", "customer-1",
                new BigDecimal(amount), "BRL", PaymentMethod.CREDIT_CARD);
    }

    private void drain(Duration timeout) {
        ConsumerRecords<String, String> batch = consumer.poll(timeout);
        batch.forEach(received::add);
    }

    private List<ConsumerRecord<String, String>> eventsFor(UUID paymentId) {
        return received.stream()
                .filter(r -> r.value().contains(paymentId.toString()))
                .toList();
    }

    private ConsumerRecord<String, String> awaitEventFor(UUID paymentId) {
        await().atMost(Duration.ofSeconds(20)).pollInterval(Duration.ofMillis(200))
                .until(() -> {
                    drain(Duration.ofMillis(300));
                    return !eventsFor(paymentId).isEmpty();
                });
        return eventsFor(paymentId).getFirst();
    }

    /** Keeps polling long enough for an unwanted second event to show up, then counts. */
    private long countEventsFor(UUID paymentId, long waitMs) {
        long deadline = System.currentTimeMillis() + waitMs;
        while (System.currentTimeMillis() < deadline) {
            drain(Duration.ofMillis(300));
        }
        return eventsFor(paymentId).size();
    }

    private PaymentCompletedEvent parse(ConsumerRecord<String, String> record) {
        try {
            return objectMapper.readValue(record.value(), PaymentCompletedEvent.class);
        } catch (Exception e) {
            throw new AssertionError("Not a PaymentCompletedEvent: " + record.value(), e);
        }
    }

    private long pspCalls() {
        return PSP.countRequestsMatching(postRequestedFor(urlEqualTo("/v1/charges")).build()).getCount();
    }

    // ================================================================== tests

    @Test
    @DisplayName("Experiment 1 — PSP success: 201, APPROVED in PostgreSQL, event on Kafka, ledger credited once")
    void approvedPaymentIsStoredPublishedAndCredited() {
        stubPspApproved("psp-tx-100", "AUTH-100");

        ResponseEntity<PaymentResponse> response =
                createPayment("idem-approved", request("250.75"), "corr-approved");

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.CREATED);
        assertThat(response.getHeaders().getLocation()).isNotNull();
        assertThat(response.getHeaders().getFirst("X-Correlation-Id")).isEqualTo("corr-approved");
        PaymentResponse body = response.getBody();
        assertThat(body).isNotNull();
        assertThat(body.status()).isEqualTo("APPROVED");
        assertThat(body.pspTransactionId()).isEqualTo("psp-tx-100");
        assertThat(body.correlationId()).isEqualTo("corr-approved");
        assertThat(body.provider()).isEqualTo("CARD_PSP");

        // PSP received the identifiers
        PSP.verify(1, postRequestedFor(urlEqualTo("/v1/charges"))
                .withHeader("Idempotency-Key", equalTo("idem-approved"))
                .withHeader("X-Correlation-Id", equalTo("corr-approved")));
        // ... and the PIX provider received nothing (SPEC-002 routing).
        assertThat(pixCalls()).isZero();

        // PostgreSQL
        Payment stored = payments.findById(body.id()).orElseThrow();
        assertThat(stored.getStatus()).isEqualTo(PaymentStatus.APPROVED);
        assertThat(stored.getAmount()).isEqualByComparingTo("250.75");
        OutboxMessage intent = outbox.findByAggregateTypeAndAggregateIdAndEventType(
                PaymentStore.AGGREGATE_TYPE, body.id().toString(), PaymentCompletedEvent.EVENT_TYPE)
                .orElseThrow();

        // Kafka
        ConsumerRecord<String, String> record = awaitEventFor(body.id());
        assertThat(record.key()).isEqualTo(body.id().toString());
        assertThat(KafkaHeaders.read(record.headers(), KafkaHeaders.EVENT_TYPE)).isEqualTo("PaymentCompleted");
        assertThat(KafkaHeaders.read(record.headers(), KafkaHeaders.EVENT_ID)).isEqualTo(intent.getId().toString());
        assertThat(KafkaHeaders.read(record.headers(), KafkaHeaders.CORRELATION_ID)).isEqualTo("corr-approved");

        PaymentCompletedEvent event = parse(record);
        assertThat(event.eventId()).isEqualTo(intent.getId());
        assertThat(event.paymentId()).isEqualTo(body.id());
        assertThat(event.status()).isEqualTo("APPROVED");
        assertThat(event.amount()).isEqualByComparingTo("250.75");
        assertThat(event.correlationId()).isEqualTo("corr-approved");

        // Consumer: one credit, one inbox row
        await().atMost(Duration.ofSeconds(15)).untilAsserted(() ->
                assertThat(ledger.findByPaymentId(body.id())).hasSize(1));
        assertThat(processedEvents.existsById(intent.getId())).isTrue();
        assertThat(ledger.findByPaymentId(body.id()).getFirst().getAmount()).isEqualByComparingTo("250.75");

        await().atMost(Duration.ofSeconds(10))
                .untilAsserted(() -> assertThat(outbox.countByPublishedAtIsNull()).isZero());
    }

    @Test
    @DisplayName("declined payment is persisted, published as DECLINED and credits nothing")
    void declinedPaymentIsPublishedWithoutLedgerEffect() {
        stubPspDeclined("psp-tx-200", "INSUFFICIENT_FUNDS");

        ResponseEntity<PaymentResponse> response = createPayment("idem-declined", request("10.00"));

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.CREATED);
        UUID id = response.getBody().id();
        assertThat(payments.findById(id).orElseThrow().getStatus()).isEqualTo(PaymentStatus.DECLINED);

        PaymentCompletedEvent event = parse(awaitEventFor(id));
        assertThat(event.status()).isEqualTo("DECLINED");
        assertThat(event.failureReason()).isEqualTo("INSUFFICIENT_FUNDS");

        await().atMost(Duration.ofSeconds(15)).untilAsserted(() ->
                assertThat(processedEvents.existsById(event.eventId())).isTrue());
        assertThat(ledger.findByPaymentId(id)).isEmpty();
    }

    @Test
    @DisplayName("Experiment 4 — duplicate request: same key twice charges the PSP once and publishes one event")
    void duplicateRequestDoesNotChargeTwice() {
        stubPspApproved("psp-tx-300", "AUTH-300");

        ResponseEntity<PaymentResponse> first = createPayment("idem-repeat", request("99.99"));
        ResponseEntity<PaymentResponse> second = createPayment("idem-repeat", request("99.99"));

        assertThat(first.getStatusCode()).isEqualTo(HttpStatus.CREATED);
        assertThat(second.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(second.getBody().id()).isEqualTo(first.getBody().id());

        PSP.verify(1, postRequestedFor(urlEqualTo("/v1/charges"))
                .withHeader("Idempotency-Key", equalTo("idem-repeat")));

        awaitEventFor(first.getBody().id());
        assertThat(countEventsFor(first.getBody().id(), 3000)).isEqualTo(1);
        assertThat(ledger.findByPaymentId(first.getBody().id())).hasSize(1);
    }

    @Test
    @DisplayName("Experiment 4b — concurrent duplicates: two parallel requests, one PSP charge, one stable id")
    void concurrentDuplicatesChargeOnce() throws Exception {
        stubPspApproved("psp-tx-350", "AUTH-350", 400);
        int clients = 4;
        ExecutorService pool = Executors.newFixedThreadPool(clients);
        CountDownLatch go = new CountDownLatch(1);
        try {
            List<Future<ResponseEntity<PaymentResponse>>> futures = new ArrayList<>();
            for (int i = 0; i < clients; i++) {
                futures.add(pool.submit(() -> {
                    go.await();
                    return createPayment("idem-race", request("77.00"));
                }));
            }
            go.countDown();

            List<ResponseEntity<PaymentResponse>> responses = new ArrayList<>();
            for (var f : futures) {
                responses.add(f.get());
            }

            assertThat(responses).extracting(r -> r.getBody().id()).containsOnly(responses.getFirst().getBody().id());
            assertThat(responses).extracting(ResponseEntity::getStatusCode)
                    .containsOnly(HttpStatus.CREATED, HttpStatus.OK);
            assertThat(responses).filteredOn(r -> r.getStatusCode() == HttpStatus.CREATED).hasSize(1);
        } finally {
            pool.shutdownNow();
        }

        PSP.verify(1, postRequestedFor(urlEqualTo("/v1/charges"))
                .withHeader("Idempotency-Key", equalTo("idem-race")));
        assertThat(payments.findByIdempotencyKey("idem-race")).isPresent();
    }

    @Test
    @DisplayName("same key with a different body is rejected with 422 and never reaches the PSP")
    void sameKeyDifferentBodyIsRejected() {
        stubPspApproved("psp-tx-380", "AUTH-380");
        createPayment("idem-conflict", request("10.00"));

        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        headers.set("Idempotency-Key", "idem-conflict");
        ResponseEntity<Map> response = rest.exchange("/api/v1/payments", HttpMethod.POST,
                new HttpEntity<>(request("20.00"), headers), Map.class);

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.UNPROCESSABLE_ENTITY);
        assertThat(response.getBody().get("error")).isEqualTo("idempotency_key_conflict");
        assertThat(pspCalls()).isEqualTo(1);
    }

    @Test
    @DisplayName("Experiment 3 — PSP processes but the answer is lost: 202 UNKNOWN, no retry, no event, replay is safe")
    void lostPspAnswerIsUnknownAndReplaySafe() {
        // The PSP answers APPROVED, but only after our read timeout (500ms): the charge exists on
        // the PSP side and we never see the answer.
        stubPspApproved("psp-tx-400", "AUTH-400", 2000);

        ResponseEntity<PaymentResponse> response =
                createPayment("idem-lost", request("42.00"), "corr-lost");

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.ACCEPTED);
        assertThat(response.getHeaders().getLocation()).isNotNull();
        UUID id = response.getBody().id();
        assertThat(response.getBody().status()).isEqualTo("UNKNOWN");

        Payment stored = payments.findById(id).orElseThrow();
        assertThat(stored.getStatus()).isEqualTo(PaymentStatus.UNKNOWN);
        assertThat(stored.getStatus()).isNotEqualTo(PaymentStatus.FAILED);
        assertThat(stored.getFailureReason()).contains("READ_TIMEOUT");

        // GUARDRAIL: a read timeout is never retried.
        PSP.verify(1, postRequestedFor(urlEqualTo("/v1/charges"))
                .withHeader("Idempotency-Key", equalTo("idem-lost")));

        // The client retries the same logical attempt: no second charge.
        ResponseEntity<PaymentResponse> replay = createPayment("idem-lost", request("42.00"));
        assertThat(replay.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(replay.getBody().id()).isEqualTo(id);
        assertThat(replay.getBody().status()).isEqualTo("UNKNOWN");
        PSP.verify(1, postRequestedFor(urlEqualTo("/v1/charges")));

        // No PaymentCompleted, no outbox intent, no ledger credit.
        assertThat(outbox.findByAggregateTypeAndAggregateIdAndEventType(
                PaymentStore.AGGREGATE_TYPE, id.toString(), PaymentCompletedEvent.EVENT_TYPE)).isEmpty();
        assertThat(countEventsFor(id, 2000)).isZero();
        assertThat(ledger.findByPaymentId(id)).isEmpty();
    }

    @Test
    @DisplayName("a PSP 5xx is UNKNOWN after exactly one attempt and publishes nothing")
    void serverErrorIsUnknownWithoutRetry() {
        PSP.stubFor(post(urlEqualTo("/v1/charges"))
                .willReturn(aResponse().withStatus(503).withBody("psp down")));

        ResponseEntity<PaymentResponse> response = createPayment("idem-5xx", request("42.00"));

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.ACCEPTED);
        UUID id = response.getBody().id();
        assertThat(payments.findById(id).orElseThrow().getStatus()).isEqualTo(PaymentStatus.UNKNOWN);
        PSP.verify(1, postRequestedFor(urlEqualTo("/v1/charges")));
        assertThat(countEventsFor(id, 1500)).isZero();
    }

    @Test
    @DisplayName("a redirecting PSP is retried at most 3 times with the same key, then UNKNOWN")
    void redirectIsRetriedThreeTimes() {
        PSP.stubFor(post(urlEqualTo("/v1/charges"))
                .willReturn(aResponse().withStatus(302).withHeader("Location", "/elsewhere")));

        ResponseEntity<PaymentResponse> response = createPayment("idem-redirect", request("42.00"));

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.ACCEPTED);
        assertThat(response.getBody().status()).isEqualTo("UNKNOWN");
        PSP.verify(3, postRequestedFor(urlEqualTo("/v1/charges"))
                .withHeader("Idempotency-Key", equalTo("idem-redirect")));
    }

    @Test
    @DisplayName("a PSP 4xx is FAILED (definitive): 201, no retry, no event")
    void rejectionIsFailed() {
        PSP.stubFor(post(urlEqualTo("/v1/charges"))
                .willReturn(aResponse().withStatus(400).withBody("invalid merchant")));

        ResponseEntity<PaymentResponse> response = createPayment("idem-4xx", request("42.00"));

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.CREATED);
        UUID id = response.getBody().id();
        Payment stored = payments.findById(id).orElseThrow();
        assertThat(stored.getStatus()).isEqualTo(PaymentStatus.FAILED);
        assertThat(stored.getFailureReason()).contains("invalid merchant");
        PSP.verify(1, postRequestedFor(urlEqualTo("/v1/charges")));
        assertThat(countEventsFor(id, 1500)).isZero();
    }

    @Test
    @DisplayName("Experiment 5 — duplicate Kafka event: the same eventId delivered twice credits the ledger once")
    void duplicateEventHasOneEffect() throws Exception {
        UUID paymentId = UUID.randomUUID();
        UUID eventId = UUID.randomUUID();
        var event = new PaymentCompletedEvent(eventId, "PaymentCompleted", paymentId, "merchant-dup",
                "customer-1", new BigDecimal("15.00"), "BRL", "PIX", "PIX_PROVIDER", "APPROVED", "psp-tx-500",
                "AUTH", null, "corr-dup", java.time.Instant.now());
        OutboxMessage message = OutboxMessage.of(eventId, "Payment", paymentId.toString(),
                "PaymentCompleted", properties.topics().paymentCompleted(), paymentId.toString(),
                objectMapper.writeValueAsString(event), "corr-dup", java.time.Instant.now());

        // Simulates a publisher restart right after the broker acknowledged: the same row is sent twice.
        kafkaTemplate.send(OutboxPublisher.toRecord(message)).get();
        kafkaTemplate.send(OutboxPublisher.toRecord(message)).get();

        await().atMost(Duration.ofSeconds(15)).untilAsserted(() ->
                assertThat(processedEvents.existsById(eventId)).isTrue());
        // Both copies reached the consumer group.
        await().atMost(Duration.ofSeconds(10)).until(() -> countEventsFor(paymentId, 300) >= 2);

        assertThat(ledger.findByPaymentId(paymentId)).hasSize(1);
        assertThat(processedEvents.count()).isGreaterThanOrEqualTo(1);
    }

    @Test
    @DisplayName("Experiment 6 — Kafka unavailable: the payment completes, the intent survives in the outbox and is published later")
    void kafkaUnavailableDoesNotLoseTheIntent() {
        stubPspApproved("psp-tx-600", "AUTH-600");
        var docker = KAFKA.getDockerClient();

        docker.pauseContainerCmd(KAFKA.getContainerId()).exec();
        UUID id;
        try {
            ResponseEntity<PaymentResponse> response = createPayment("idem-kafka-down", request("60.00"));

            // The client is not affected: the payment is APPROVED and durable.
            assertThat(response.getStatusCode()).isEqualTo(HttpStatus.CREATED);
            id = response.getBody().id();
            assertThat(payments.findById(id).orElseThrow().getStatus()).isEqualTo(PaymentStatus.APPROVED);

            OutboxMessage intent = outbox.findByAggregateTypeAndAggregateIdAndEventType(
                    PaymentStore.AGGREGATE_TYPE, id.toString(), PaymentCompletedEvent.EVENT_TYPE)
                    .orElseThrow();
            assertThat(intent.isPublished()).isFalse();

            // The poller tries, fails, and keeps the row.
            await().atMost(Duration.ofSeconds(20)).untilAsserted(() -> {
                OutboxMessage retried = outbox.findById(intent.getId()).orElseThrow();
                assertThat(retried.getAttempts()).isGreaterThanOrEqualTo(1);
                assertThat(retried.isPublished()).isFalse();
            });
        } finally {
            docker.unpauseContainerCmd(KAFKA.getContainerId()).exec();
        }

        // Broker back: the event is delivered and consumed exactly once.
        ConsumerRecord<String, String> record = awaitEventFor(id);
        assertThat(parse(record).status()).isEqualTo("APPROVED");
        await().atMost(Duration.ofSeconds(15)).untilAsserted(() ->
                assertThat(ledger.findByPaymentId(id)).hasSize(1));
        await().atMost(Duration.ofSeconds(10)).untilAsserted(() ->
                assertThat(outbox.findByAggregateTypeAndAggregateIdAndEventType(
                        PaymentStore.AGGREGATE_TYPE, id.toString(), PaymentCompletedEvent.EVENT_TYPE)
                        .orElseThrow().isPublished()).isTrue());
    }

    @Test
    @DisplayName("validation rejects a bad request before any PSP call or database write")
    void invalidRequestNeverReachesThePsp() {
        stubPspApproved("psp-tx-700", "AUTH-700");
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
        assertThat(pspCalls()).isZero();
    }

    @Test
    @DisplayName("a stored payment can be read back; an unknown id answers 404")
    void paymentCanBeFetched() {
        stubPspApproved("psp-tx-800", "AUTH-800");
        UUID id = createPayment("idem-fetch", request("15.00")).getBody().id();

        ResponseEntity<PaymentResponse> fetched =
                rest.getForEntity("/api/v1/payments/{id}", PaymentResponse.class, id);
        assertThat(fetched.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(fetched.getBody().status()).isEqualTo("APPROVED");

        ResponseEntity<String> missing = rest.getForEntity(
                "/api/v1/payments/{id}", String.class, UUID.randomUUID());
        assertThat(missing.getStatusCode()).isEqualTo(HttpStatus.NOT_FOUND);
    }

    // ================================================================== SPEC-002 (PIX)

    private static final String PIX_PATH = "/v1/pix/payments";

    private void stubPixConfirmed(String endToEndId, int delayMs) {
        PIX.stubFor(post(urlEqualTo(PIX_PATH)).willReturn(aResponse()
                .withStatus(200)
                .withHeader("Content-Type", "application/json")
                .withFixedDelay(delayMs)
                .withBody("""
                        {"endToEndId":"%s","status":"CONFIRMED"}
                        """.formatted(endToEndId))));
    }

    private static CreatePaymentRequest pixRequest(String amount) {
        return new CreatePaymentRequest("merchant-1", "customer-1",
                new BigDecimal(amount), "BRL", PaymentMethod.PIX);
    }

    private long pixCalls() {
        return PIX.countRequestsMatching(postRequestedFor(urlEqualTo(PIX_PATH)).build()).getCount();
    }

    @Test
    @DisplayName("SPEC-002 — PIX confirmed: routed to the PIX provider (never the PSP), APPROVED, provider recorded, event, ledger once")
    void pixPaymentIsRoutedToPixProviderAndSettled() {
        stubPixConfirmed("E2026090500001", 0);
        stubPspApproved("psp-must-not-be-used", "NOPE");

        ResponseEntity<PaymentResponse> response =
                createPayment("idem-pix-ok", pixRequest("120.00"), "corr-pix-ok");

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.CREATED);
        PaymentResponse body = response.getBody();
        assertThat(body).isNotNull();
        assertThat(body.status()).isEqualTo("APPROVED");
        assertThat(body.paymentMethod()).isEqualTo("PIX");
        assertThat(body.provider()).isEqualTo("PIX_PROVIDER");
        assertThat(body.pspTransactionId()).isEqualTo("E2026090500001");
        assertThat(body.authorizationCode()).isNull();

        // Routing: the PIX provider got the identifiers, the card PSP got nothing.
        PIX.verify(1, postRequestedFor(urlEqualTo(PIX_PATH))
                .withHeader("Idempotency-Key", equalTo("idem-pix-ok"))
                .withHeader("X-Correlation-Id", equalTo("corr-pix-ok")));
        assertThat(pspCalls()).isZero();

        // PostgreSQL
        Payment stored = payments.findById(body.id()).orElseThrow();
        assertThat(stored.getProvider()).isEqualTo(PaymentProvider.PIX_PROVIDER);
        assertThat(stored.getStatus()).isEqualTo(PaymentStatus.APPROVED);

        // Kafka: same event, provider added.
        PaymentCompletedEvent event = parse(awaitEventFor(body.id()));
        assertThat(event.status()).isEqualTo("APPROVED");
        assertThat(event.paymentMethod()).isEqualTo("PIX");
        assertThat(event.provider()).isEqualTo("PIX_PROVIDER");
        assertThat(event.pspTransactionId()).isEqualTo("E2026090500001");

        // Consumer: one credit.
        await().atMost(Duration.ofSeconds(15)).untilAsserted(() ->
                assertThat(ledger.findByPaymentId(body.id())).hasSize(1));
        assertThat(ledger.findByPaymentId(body.id()).getFirst().getAmount()).isEqualByComparingTo("120.00");
    }

    @Test
    @DisplayName("SPEC-002 — PIX rejected by the payer's bank is DECLINED, published, and credits nothing")
    void pixRejectedIsDeclinedWithoutLedgerEffect() {
        PIX.stubFor(post(urlEqualTo(PIX_PATH)).willReturn(aResponse()
                .withStatus(200)
                .withHeader("Content-Type", "application/json")
                .withBody("{\"endToEndId\":\"E2026090500002\",\"status\":\"REJECTED\",\"rejectionReason\":\"PAYER_LIMIT_EXCEEDED\"}")));

        ResponseEntity<PaymentResponse> response = createPayment("idem-pix-declined", pixRequest("10.00"));

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.CREATED);
        UUID id = response.getBody().id();
        assertThat(response.getBody().status()).isEqualTo("DECLINED");
        assertThat(response.getBody().failureReason()).isEqualTo("PAYER_LIMIT_EXCEEDED");

        PaymentCompletedEvent event = parse(awaitEventFor(id));
        assertThat(event.status()).isEqualTo("DECLINED");
        assertThat(event.provider()).isEqualTo("PIX_PROVIDER");
        await().atMost(Duration.ofSeconds(15)).untilAsserted(() ->
                assertThat(processedEvents.existsById(event.eventId())).isTrue());
        assertThat(ledger.findByPaymentId(id)).isEmpty();
        assertThat(pspCalls()).isZero();
    }

    @Test
    @DisplayName("SPEC-002 — PIX provider settles but the answer is lost: 202 UNKNOWN, one call, no event, replay is safe")
    void pixLostAnswerIsUnknownAndReplaySafe() {
        // Confirms only after our read timeout (500ms): the PIX may be settled and we never know.
        stubPixConfirmed("E2026090500003", 2000);

        ResponseEntity<PaymentResponse> response =
                createPayment("idem-pix-lost", pixRequest("42.00"), "corr-pix-lost");

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.ACCEPTED);
        UUID id = response.getBody().id();
        assertThat(response.getBody().status()).isEqualTo("UNKNOWN");
        assertThat(response.getBody().provider()).isEqualTo("PIX_PROVIDER");

        Payment stored = payments.findById(id).orElseThrow();
        assertThat(stored.getStatus()).isEqualTo(PaymentStatus.UNKNOWN);
        assertThat(stored.getStatus()).isNotEqualTo(PaymentStatus.FAILED);
        assertThat(stored.getFailureReason()).contains("READ_TIMEOUT").contains("PIX_PROVIDER");

        // GUARDRAIL: a read timeout is never retried; and no fallback to the other provider.
        PIX.verify(1, postRequestedFor(urlEqualTo(PIX_PATH))
                .withHeader("Idempotency-Key", equalTo("idem-pix-lost")));
        assertThat(pspCalls()).isZero();

        // The client retries the same logical attempt: no second PIX.
        ResponseEntity<PaymentResponse> replay = createPayment("idem-pix-lost", pixRequest("42.00"));
        assertThat(replay.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(replay.getBody().id()).isEqualTo(id);
        assertThat(replay.getBody().status()).isEqualTo("UNKNOWN");
        assertThat(pixCalls()).isEqualTo(1);

        // No PaymentCompleted, no outbox intent, no ledger credit.
        assertThat(outbox.findByAggregateTypeAndAggregateIdAndEventType(
                PaymentStore.AGGREGATE_TYPE, id.toString(), PaymentCompletedEvent.EVENT_TYPE)).isEmpty();
        assertThat(countEventsFor(id, 2000)).isZero();
        assertThat(ledger.findByPaymentId(id)).isEmpty();
    }

    @Test
    @DisplayName("SPEC-002 — PIX provider 4xx is FAILED (definitive): 201, no retry, no event")
    void pixRejectionIsFailed() {
        PIX.stubFor(post(urlEqualTo(PIX_PATH))
                .willReturn(aResponse().withStatus(400).withBody("merchant not enabled for PIX")));

        ResponseEntity<PaymentResponse> response = createPayment("idem-pix-4xx", pixRequest("42.00"));

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.CREATED);
        UUID id = response.getBody().id();
        Payment stored = payments.findById(id).orElseThrow();
        assertThat(stored.getStatus()).isEqualTo(PaymentStatus.FAILED);
        assertThat(stored.getFailureReason()).contains("merchant not enabled for PIX");
        PIX.verify(1, postRequestedFor(urlEqualTo(PIX_PATH)));
        assertThat(countEventsFor(id, 1500)).isZero();
    }

    @Test
    @DisplayName("SPEC-002 — a PIX provider outage does not affect card payments (and vice-versa)")
    void pixOutageDoesNotAffectCardPayments() {
        PIX.stubFor(post(urlEqualTo(PIX_PATH))
                .willReturn(aResponse().withStatus(503).withBody("SPI unavailable")));
        stubPspApproved("psp-tx-isolated", "AUTH-ISO");

        ResponseEntity<PaymentResponse> pixResponse = createPayment("idem-iso-pix", pixRequest("30.00"));
        ResponseEntity<PaymentResponse> cardResponse = createPayment("idem-iso-card", request("30.00"));

        assertThat(pixResponse.getStatusCode()).isEqualTo(HttpStatus.ACCEPTED);
        assertThat(pixResponse.getBody().status()).isEqualTo("UNKNOWN");
        assertThat(pixResponse.getBody().provider()).isEqualTo("PIX_PROVIDER");

        assertThat(cardResponse.getStatusCode()).isEqualTo(HttpStatus.CREATED);
        assertThat(cardResponse.getBody().status()).isEqualTo("APPROVED");
        assertThat(cardResponse.getBody().provider()).isEqualTo("CARD_PSP");

        assertThat(pixCalls()).isEqualTo(1);
        assertThat(pspCalls()).isEqualTo(1);
        await().atMost(Duration.ofSeconds(15)).untilAsserted(() ->
                assertThat(ledger.findByPaymentId(cardResponse.getBody().id())).hasSize(1));
        assertThat(ledger.findByPaymentId(pixResponse.getBody().id())).isEmpty();
    }
}
