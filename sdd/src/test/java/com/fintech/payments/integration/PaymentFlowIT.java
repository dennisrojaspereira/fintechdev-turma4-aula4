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
import com.fintech.payments.messaging.PaymentRequestedEvent;
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
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.core.KafkaTemplate;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
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
 * Harness experiments (harness/README.md) as executable evidence for SPEC-001, SPEC-002 and
 * SPEC-003: HTTP in (202), outbox row, Debezium → Kafka, worker → provider, PostgreSQL rows,
 * PaymentCompleted → idempotent ledger consumer.
 */
class PaymentFlowIT extends AbstractIntegrationTest {

    private static final String PSP_PATH = "/v1/charges";
    private static final String PIX_PATH = "/v1/pix/payments";

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
    private PaymentStore store;

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    @Autowired
    private JdbcTemplate jdbc;

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
        consumer.subscribe(List.of(properties.topics().paymentRequested(),
                properties.topics().paymentCompleted()));
        // Force the initial assignment so records produced after this point are never missed.
        consumer.poll(Duration.ofSeconds(5));
    }

    @AfterEach
    void unsubscribe() {
        consumer.close();
    }

    // ---------------------------------------------------------------- stubs

    private void stubPspApproved(String transactionId, String authorizationCode) {
        stubPspApproved(transactionId, authorizationCode, 0);
    }

    private void stubPspApproved(String transactionId, String authorizationCode, int delayMs) {
        PSP.stubFor(post(urlEqualTo(PSP_PATH)).willReturn(aResponse()
                .withStatus(200)
                .withHeader("Content-Type", "application/json")
                .withFixedDelay(delayMs)
                .withBody("""
                        {"transactionId":"%s","status":"APPROVED","authorizationCode":"%s"}
                        """.formatted(transactionId, authorizationCode))));
    }

    private void stubPspDeclined(String transactionId, String reason) {
        PSP.stubFor(post(urlEqualTo(PSP_PATH)).willReturn(aResponse()
                .withStatus(200)
                .withHeader("Content-Type", "application/json")
                .withBody("""
                        {"transactionId":"%s","status":"DECLINED","declineReason":"%s"}
                        """.formatted(transactionId, reason))));
    }

    private void stubPixConfirmed(String endToEndId, int delayMs) {
        PIX.stubFor(post(urlEqualTo(PIX_PATH)).willReturn(aResponse()
                .withStatus(200)
                .withHeader("Content-Type", "application/json")
                .withFixedDelay(delayMs)
                .withBody("""
                        {"endToEndId":"%s","status":"CONFIRMED"}
                        """.formatted(endToEndId))));
    }

    private long pspCalls() {
        return PSP.countRequestsMatching(postRequestedFor(urlEqualTo(PSP_PATH)).build()).getCount();
    }

    private long pixCalls() {
        return PIX.countRequestsMatching(postRequestedFor(urlEqualTo(PIX_PATH)).build()).getCount();
    }

    // ---------------------------------------------------------------- HTTP

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

    private static CreatePaymentRequest pixRequest(String amount) {
        return new CreatePaymentRequest("merchant-1", "customer-1",
                new BigDecimal(amount), "BRL", PaymentMethod.PIX);
    }

    /** Asserts the SPEC-003 contract for a new payment: 202, Location, PENDING, nothing charged yet. */
    private UUID accepted(ResponseEntity<PaymentResponse> response) {
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.ACCEPTED);
        assertThat(response.getHeaders().getLocation()).isNotNull();
        assertThat(response.getBody()).isNotNull();
        assertThat(response.getBody().status()).isEqualTo("PENDING");
        assertThat(response.getBody().pspTransactionId()).isNull();
        return response.getBody().id();
    }

    // ---------------------------------------------------------------- database

    private Payment stored(UUID id) {
        return payments.findById(id).orElseThrow();
    }

    private Payment awaitStatus(UUID id, PaymentStatus expected) {
        await().atMost(Duration.ofSeconds(30)).pollInterval(Duration.ofMillis(200))
                .untilAsserted(() -> assertThat(stored(id).getStatus()).isEqualTo(expected));
        return stored(id);
    }

    private OutboxMessage outboxRow(UUID paymentId, String eventType) {
        return outbox.findByAggregateTypeAndAggregateIdAndEventType(
                PaymentStore.AGGREGATE_TYPE, paymentId.toString(), eventType).orElseThrow();
    }

    private boolean hasOutboxRow(UUID paymentId, String eventType) {
        return outbox.findByAggregateTypeAndAggregateIdAndEventType(
                PaymentStore.AGGREGATE_TYPE, paymentId.toString(), eventType).isPresent();
    }

    /**
     * A payment persisted directly and claimed, as if a worker had taken it and died;
     * {@code claimedAt} backdates the claim so the processing timeout (3s here) can be crossed.
     */
    private Payment claimedPayment(String key, Instant claimedAt) {
        Payment payment = payments.save(Payment.pending(key, "fp", "corr-" + key, "merchant-1",
                "customer-1", new BigDecimal("33.00"), "BRL", PaymentMethod.CREDIT_CARD, claimedAt));
        assertThat(store.claim(payment.getId())).isTrue();
        jdbc.update("update payments set updated_at = ? where id = ?",
                java.sql.Timestamp.from(claimedAt), payment.getId());
        return stored(payment.getId());
    }

    /** Sends a PaymentRequested for {@code payment} exactly as Debezium/the poller would. */
    private UUID sendPaymentRequested(Payment payment) throws Exception {
        UUID eventId = UUID.randomUUID();
        var event = PaymentRequestedEvent.from(eventId, payment, Instant.now());
        OutboxMessage row = OutboxMessage.of(eventId, PaymentStore.AGGREGATE_TYPE,
                payment.getId().toString(), PaymentRequestedEvent.EVENT_TYPE,
                properties.topics().paymentRequested(), payment.getId().toString(),
                objectMapper.writeValueAsString(event), payment.getCorrelationId(), Instant.now());
        resend(row);
        return eventId;
    }

    /** Re-delivers an outbox row byte for byte (same eventId), like a connector/publisher restart. */
    private void resend(OutboxMessage row) throws Exception {
        kafkaTemplate.send(OutboxPublisher.toRecord(row)).get();
    }

    // ---------------------------------------------------------------- Kafka

    private void drain(Duration timeout) {
        ConsumerRecords<String, String> batch = consumer.poll(timeout);
        batch.forEach(received::add);
    }

    private List<ConsumerRecord<String, String>> eventsFor(UUID paymentId, String topic) {
        return received.stream()
                .filter(r -> r.topic().equals(topic))
                .filter(r -> r.value().contains(paymentId.toString()))
                .toList();
    }

    private ConsumerRecord<String, String> awaitEventFor(UUID paymentId, String topic) {
        await().atMost(Duration.ofSeconds(30)).pollInterval(Duration.ofMillis(200))
                .until(() -> {
                    drain(Duration.ofMillis(300));
                    return !eventsFor(paymentId, topic).isEmpty();
                });
        return eventsFor(paymentId, topic).getFirst();
    }

    private ConsumerRecord<String, String> awaitRequestedEvent(UUID paymentId) {
        return awaitEventFor(paymentId, properties.topics().paymentRequested());
    }

    private ConsumerRecord<String, String> awaitCompletedEvent(UUID paymentId) {
        return awaitEventFor(paymentId, properties.topics().paymentCompleted());
    }

    /** Keeps polling long enough for an unwanted event to show up, then counts. */
    private long countEventsFor(UUID paymentId, String topic, long waitMs) {
        long deadline = System.currentTimeMillis() + waitMs;
        while (System.currentTimeMillis() < deadline) {
            drain(Duration.ofMillis(300));
        }
        return eventsFor(paymentId, topic).size();
    }

    private long countCompletedEventsFor(UUID paymentId, long waitMs) {
        return countEventsFor(paymentId, properties.topics().paymentCompleted(), waitMs);
    }

    private PaymentCompletedEvent parseCompleted(ConsumerRecord<String, String> record) {
        try {
            return objectMapper.readValue(record.value(), PaymentCompletedEvent.class);
        } catch (Exception e) {
            throw new AssertionError("Not a PaymentCompletedEvent: " + record.value(), e);
        }
    }

    private PaymentRequestedEvent parseRequested(ConsumerRecord<String, String> record) {
        try {
            return objectMapper.readValue(record.value(), PaymentRequestedEvent.class);
        } catch (Exception e) {
            throw new AssertionError("Not a PaymentRequestedEvent: " + record.value(), e);
        }
    }

    private static void sleep(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    // ================================================================== SPEC-003 core

    @Test
    @DisplayName("Experiment 1 — card approved: 202 PENDING, outbox row, Debezium publishes PaymentRequested, worker charges once, APPROVED, PaymentCompleted, ledger credited once")
    void approvedCardPaymentIsAcceptedThenProcessedAsynchronously() {
        stubPspApproved("psp-tx-100", "AUTH-100");

        ResponseEntity<PaymentResponse> response =
                createPayment("idem-approved", request("250.75"), "corr-approved");

        // D7: the request never waits for the provider.
        UUID id = accepted(response);
        assertThat(response.getHeaders().getFirst("X-Correlation-Id")).isEqualTo("corr-approved");
        assertThat(response.getBody().provider()).isEqualTo("CARD_PSP");
        assertThat(response.getBody().correlationId()).isEqualTo("corr-approved");

        // Same transaction: the PENDING row and its PaymentRequested intent exist together.
        OutboxMessage intent = outboxRow(id, PaymentRequestedEvent.EVENT_TYPE);
        assertThat(intent.getTopic()).isEqualTo(properties.topics().paymentRequested());
        assertThat(intent.getMessageKey()).isEqualTo(id.toString());

        // D8: Debezium routed the row by its topic column; key, headers and payload are the
        // outbox row, byte for byte (the poller's contract).
        ConsumerRecord<String, String> requested = awaitRequestedEvent(id);
        assertThat(requested.key()).isEqualTo(id.toString());
        assertThat(KafkaHeaders.read(requested.headers(), KafkaHeaders.EVENT_TYPE)).isEqualTo("PaymentRequested");
        assertThat(KafkaHeaders.read(requested.headers(), KafkaHeaders.EVENT_ID)).isEqualTo(intent.getId().toString());
        assertThat(KafkaHeaders.read(requested.headers(), KafkaHeaders.CORRELATION_ID)).isEqualTo("corr-approved");
        assertThat(requested.value()).isEqualTo(intent.getPayload());
        PaymentRequestedEvent requestedEvent = parseRequested(requested);
        assertThat(requestedEvent.eventId()).isEqualTo(intent.getId());
        assertThat(requestedEvent.paymentId()).isEqualTo(id);
        assertThat(requestedEvent.idempotencyKey()).isEqualTo("idem-approved");
        assertThat(requestedEvent.provider()).isEqualTo("CARD_PSP");

        // D9: the worker claimed, charged once with the identifiers, settled.
        Payment approved = awaitStatus(id, PaymentStatus.APPROVED);
        assertThat(approved.getPspTransactionId()).isEqualTo("psp-tx-100");
        assertThat(approved.getAmount()).isEqualByComparingTo("250.75");
        PSP.verify(1, postRequestedFor(urlEqualTo(PSP_PATH))
                .withHeader("Idempotency-Key", equalTo("idem-approved"))
                .withHeader("X-Correlation-Id", equalTo("corr-approved")));
        assertThat(pixCalls()).isZero();
        assertThat(processedEvents.existsById(intent.getId())).isTrue();

        // PaymentCompleted, unchanged contract (SPEC-001 consumers do not know who published).
        OutboxMessage completion = outboxRow(id, PaymentCompletedEvent.EVENT_TYPE);
        ConsumerRecord<String, String> completed = awaitCompletedEvent(id);
        assertThat(completed.key()).isEqualTo(id.toString());
        assertThat(KafkaHeaders.read(completed.headers(), KafkaHeaders.EVENT_TYPE)).isEqualTo("PaymentCompleted");
        assertThat(KafkaHeaders.read(completed.headers(), KafkaHeaders.EVENT_ID)).isEqualTo(completion.getId().toString());
        PaymentCompletedEvent event = parseCompleted(completed);
        assertThat(event.eventId()).isEqualTo(completion.getId());
        assertThat(event.status()).isEqualTo("APPROVED");
        assertThat(event.amount()).isEqualByComparingTo("250.75");
        assertThat(event.correlationId()).isEqualTo("corr-approved");

        // Ledger: one credit, one inbox row per event.
        await().atMost(Duration.ofSeconds(15)).untilAsserted(() ->
                assertThat(ledger.findByPaymentId(id)).hasSize(1));
        assertThat(processedEvents.existsById(completion.getId())).isTrue();
        assertThat(ledger.findByPaymentId(id).getFirst().getAmount()).isEqualByComparingTo("250.75");

        // With CDC the connector does not write back: published_at stays null by design (D8).
        assertThat(outboxRow(id, PaymentRequestedEvent.EVENT_TYPE).isPublished()).isFalse();

        // GET is the way to follow the outcome (D7).
        ResponseEntity<PaymentResponse> fetched =
                rest.getForEntity("/api/v1/payments/{id}", PaymentResponse.class, id);
        assertThat(fetched.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(fetched.getBody().status()).isEqualTo("APPROVED");
        assertThat(fetched.getBody().authorizationCode()).isEqualTo("AUTH-100");
    }

    @Test
    @DisplayName("declined payment is 202, then DECLINED, published as DECLINED and credits nothing")
    void declinedPaymentIsPublishedWithoutLedgerEffect() {
        stubPspDeclined("psp-tx-200", "INSUFFICIENT_FUNDS");

        UUID id = accepted(createPayment("idem-declined", request("10.00")));

        Payment declined = awaitStatus(id, PaymentStatus.DECLINED);
        assertThat(declined.getFailureReason()).isEqualTo("INSUFFICIENT_FUNDS");
        PaymentCompletedEvent event = parseCompleted(awaitCompletedEvent(id));
        assertThat(event.status()).isEqualTo("DECLINED");
        assertThat(event.failureReason()).isEqualTo("INSUFFICIENT_FUNDS");

        await().atMost(Duration.ofSeconds(15)).untilAsserted(() ->
                assertThat(processedEvents.existsById(event.eventId())).isTrue());
        assertThat(ledger.findByPaymentId(id)).isEmpty();
    }

    @Test
    @DisplayName("Experiment 1 (PIX) — routed to the PIX provider, never the PSP; APPROVED with endToEndId; event; ledger once")
    void pixPaymentIsRoutedToPixProviderAndSettled() {
        stubPixConfirmed("E2026090500001", 0);
        stubPspApproved("psp-must-not-be-used", "NOPE");

        ResponseEntity<PaymentResponse> response =
                createPayment("idem-pix-ok", pixRequest("120.00"), "corr-pix-ok");
        UUID id = accepted(response);
        assertThat(response.getBody().paymentMethod()).isEqualTo("PIX");
        assertThat(response.getBody().provider()).isEqualTo("PIX_PROVIDER");

        Payment approved = awaitStatus(id, PaymentStatus.APPROVED);
        assertThat(approved.getProvider()).isEqualTo(PaymentProvider.PIX_PROVIDER);
        assertThat(approved.getPspTransactionId()).isEqualTo("E2026090500001");
        assertThat(approved.getPspAuthorizationCode()).isNull();

        PIX.verify(1, postRequestedFor(urlEqualTo(PIX_PATH))
                .withHeader("Idempotency-Key", equalTo("idem-pix-ok"))
                .withHeader("X-Correlation-Id", equalTo("corr-pix-ok")));
        assertThat(pspCalls()).isZero();

        PaymentCompletedEvent event = parseCompleted(awaitCompletedEvent(id));
        assertThat(event.status()).isEqualTo("APPROVED");
        assertThat(event.paymentMethod()).isEqualTo("PIX");
        assertThat(event.provider()).isEqualTo("PIX_PROVIDER");
        await().atMost(Duration.ofSeconds(15)).untilAsserted(() ->
                assertThat(ledger.findByPaymentId(id)).hasSize(1));
    }

    @Test
    @DisplayName("PIX rejected by the payer's bank is DECLINED, published, and credits nothing")
    void pixRejectedIsDeclinedWithoutLedgerEffect() {
        PIX.stubFor(post(urlEqualTo(PIX_PATH)).willReturn(aResponse()
                .withStatus(200)
                .withHeader("Content-Type", "application/json")
                .withBody("{\"endToEndId\":\"E2026090500002\",\"status\":\"REJECTED\",\"rejectionReason\":\"PAYER_LIMIT_EXCEEDED\"}")));

        UUID id = accepted(createPayment("idem-pix-declined", pixRequest("10.00")));

        Payment declined = awaitStatus(id, PaymentStatus.DECLINED);
        assertThat(declined.getFailureReason()).isEqualTo("PAYER_LIMIT_EXCEEDED");
        PaymentCompletedEvent event = parseCompleted(awaitCompletedEvent(id));
        assertThat(event.status()).isEqualTo("DECLINED");
        assertThat(event.provider()).isEqualTo("PIX_PROVIDER");
        await().atMost(Duration.ofSeconds(15)).untilAsserted(() ->
                assertThat(processedEvents.existsById(event.eventId())).isTrue());
        assertThat(ledger.findByPaymentId(id)).isEmpty();
        assertThat(pspCalls()).isZero();
    }

    // ================================================================== idempotency (client)

    @Test
    @DisplayName("Experiment 4 — duplicate request: same key twice → one payment, one intent, one charge, one event")
    void duplicateRequestDoesNotChargeTwice() {
        stubPspApproved("psp-tx-300", "AUTH-300");

        ResponseEntity<PaymentResponse> first = createPayment("idem-repeat", request("99.99"));
        ResponseEntity<PaymentResponse> second = createPayment("idem-repeat", request("99.99"));

        UUID id = accepted(first);
        assertThat(second.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(second.getHeaders().getLocation()).isNull();
        assertThat(second.getBody().id()).isEqualTo(id);

        awaitStatus(id, PaymentStatus.APPROVED);
        PSP.verify(1, postRequestedFor(urlEqualTo(PSP_PATH))
                .withHeader("Idempotency-Key", equalTo("idem-repeat")));

        // A replay after the outcome returns it, still without a new intent or charge.
        ResponseEntity<PaymentResponse> later = createPayment("idem-repeat", request("99.99"));
        assertThat(later.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(later.getBody().status()).isEqualTo("APPROVED");

        awaitCompletedEvent(id);
        assertThat(countCompletedEventsFor(id, 3000)).isEqualTo(1);
        assertThat(pspCalls()).isEqualTo(1);
        assertThat(ledger.findByPaymentId(id)).hasSize(1);
    }

    @Test
    @DisplayName("Experiment 4b — concurrent duplicates: 4 parallel requests, one 202, one stable id, one charge")
    void concurrentDuplicatesChargeOnce() throws Exception {
        stubPspApproved("psp-tx-350", "AUTH-350");
        int clients = 4;
        ExecutorService pool = Executors.newFixedThreadPool(clients);
        CountDownLatch go = new CountDownLatch(1);
        List<ResponseEntity<PaymentResponse>> responses = new ArrayList<>();
        try {
            List<Future<ResponseEntity<PaymentResponse>>> futures = new ArrayList<>();
            for (int i = 0; i < clients; i++) {
                futures.add(pool.submit(() -> {
                    go.await();
                    return createPayment("idem-race", request("77.00"));
                }));
            }
            go.countDown();
            for (var f : futures) {
                responses.add(f.get());
            }
        } finally {
            pool.shutdownNow();
        }

        UUID id = responses.getFirst().getBody().id();
        assertThat(responses).extracting(r -> r.getBody().id()).containsOnly(id);
        assertThat(responses).extracting(ResponseEntity::getStatusCode)
                .containsOnly(HttpStatus.ACCEPTED, HttpStatus.OK);
        assertThat(responses).filteredOn(r -> r.getStatusCode() == HttpStatus.ACCEPTED).hasSize(1);

        awaitStatus(id, PaymentStatus.APPROVED);
        PSP.verify(1, postRequestedFor(urlEqualTo(PSP_PATH))
                .withHeader("Idempotency-Key", equalTo("idem-race")));
        assertThat(outbox.findByAggregateTypeAndAggregateIdAndEventType(
                PaymentStore.AGGREGATE_TYPE, id.toString(), PaymentRequestedEvent.EVENT_TYPE)).isPresent();
    }

    @Test
    @DisplayName("same key with a different body is rejected with 422 and creates no second intent")
    void sameKeyDifferentBodyIsRejected() {
        stubPspApproved("psp-tx-380", "AUTH-380");
        UUID id = accepted(createPayment("idem-conflict", request("10.00")));

        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        headers.set("Idempotency-Key", "idem-conflict");
        ResponseEntity<Map> response = rest.exchange("/api/v1/payments", HttpMethod.POST,
                new HttpEntity<>(request("20.00"), headers), Map.class);

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.UNPROCESSABLE_ENTITY);
        assertThat(response.getBody().get("error")).isEqualTo("idempotency_key_conflict");
        assertThat(payments.findByIdempotencyKey("idem-conflict").orElseThrow().getId()).isEqualTo(id);
        awaitStatus(id, PaymentStatus.APPROVED);
        assertThat(pspCalls()).isEqualTo(1);
    }

    // ================================================================== idempotency (worker)

    @Test
    @DisplayName("Experiment 5 (worker) — the same PaymentRequested delivered 3 times charges the provider once")
    void redeliveredPaymentRequestedChargesOnce() throws Exception {
        stubPspApproved("psp-tx-500", "AUTH-500");
        UUID id = accepted(createPayment("idem-redeliver", request("50.00")));
        awaitStatus(id, PaymentStatus.APPROVED);
        OutboxMessage intent = outboxRow(id, PaymentRequestedEvent.EVENT_TYPE);
        assertThat(processedEvents.existsById(intent.getId())).isTrue();

        // Connector restart / snapshot replay: the same row, same eventId, two more times.
        resend(intent);
        resend(intent);

        // All three copies reached the topic...
        await().atMost(Duration.ofSeconds(15)).until(() ->
                countEventsFor(id, properties.topics().paymentRequested(), 300) >= 3);
        // ... and nothing happened: one charge, one completion, one credit, state untouched.
        sleep(1500);
        assertThat(pspCalls()).isEqualTo(1);
        assertThat(stored(id).getStatus()).isEqualTo(PaymentStatus.APPROVED);
        assertThat(stored(id).getPspTransactionId()).isEqualTo("psp-tx-500");
        assertThat(countCompletedEventsFor(id, 1000)).isEqualTo(1);
        await().atMost(Duration.ofSeconds(15)).untilAsserted(() ->
                assertThat(ledger.findByPaymentId(id)).hasSize(1));
    }

    @Test
    @DisplayName("a late PaymentRequested (new eventId) for an already resolved payment is recorded and never calls the provider")
    void lateEventForResolvedPaymentDoesNotCallProvider() throws Exception {
        stubPspApproved("psp-tx-510", "AUTH-510");
        UUID id = accepted(createPayment("idem-late", request("51.00")));
        Payment approved = awaitStatus(id, PaymentStatus.APPROVED);

        UUID lateEventId = sendPaymentRequested(approved);

        await().atMost(Duration.ofSeconds(15)).untilAsserted(() ->
                assertThat(processedEvents.existsById(lateEventId)).isTrue());
        assertThat(pspCalls()).isEqualTo(1);
        assertThat(stored(id).getStatus()).isEqualTo(PaymentStatus.APPROVED);
        assertThat(countCompletedEventsFor(id, 1000)).isEqualTo(1);
    }

    @Test
    @DisplayName("Experiment 7 (worker) — worker died after the claim: PROCESSING past the timeout becomes UNKNOWN (never FAILED) with no second charge")
    void deadWorkerLeavesUnknownWithoutSecondCharge() throws Exception {
        stubPspApproved("psp-must-not-be-called", "NOPE");
        // Claimed 10s ago (timeout is 3s in this harness): the worker is presumed dead.
        Payment orphan = claimedPayment("idem-dead-worker", Instant.now().minusSeconds(10));
        assertThat(orphan.getStatus()).isEqualTo(PaymentStatus.PROCESSING);

        UUID eventId = sendPaymentRequested(orphan);

        Payment unknown = awaitStatus(orphan.getId(), PaymentStatus.UNKNOWN);
        assertThat(unknown.getStatus()).isNotEqualTo(PaymentStatus.FAILED);
        assertThat(unknown.getFailureReason()).contains("Worker interrupted").contains("CARD_PSP");
        assertThat(processedEvents.existsById(eventId)).isTrue();
        assertThat(pspCalls()).isZero();
        assertThat(hasOutboxRow(orphan.getId(), PaymentCompletedEvent.EVENT_TYPE)).isFalse();
        assertThat(countCompletedEventsFor(orphan.getId(), 1500)).isZero();

        assertThat(pspCalls()).isZero();
    }

    @Test
    @DisplayName("PROCESSING claimed recently by another worker is retried, not charged; after the timeout it becomes UNKNOWN")
    void recentProcessingIsRetriedThenTimesOut() throws Exception {
        stubPspApproved("psp-must-not-be-called", "NOPE");
        Payment inFlight = claimedPayment("idem-in-flight", Instant.now());

        UUID eventId = sendPaymentRequested(inFlight);

        // Within the timeout: the event is being retried (no inbox row), nothing changed.
        sleep(1500);
        assertThat(stored(inFlight.getId()).getStatus()).isEqualTo(PaymentStatus.PROCESSING);
        assertThat(processedEvents.existsById(eventId)).isFalse();
        assertThat(pspCalls()).isZero();

        // Past the timeout the redelivery resolves it as UNKNOWN, still without a charge.
        Payment unknown = awaitStatus(inFlight.getId(), PaymentStatus.UNKNOWN);
        assertThat(unknown.getFailureReason()).contains("Worker interrupted");
        await().atMost(Duration.ofSeconds(10)).untilAsserted(() ->
                assertThat(processedEvents.existsById(eventId)).isTrue());
        assertThat(pspCalls()).isZero();
    }

    // ================================================================== provider failures (worker)

    @Test
    @DisplayName("Experiment 3 — provider processes but the answer is lost: UNKNOWN, one call, no event, replay is safe")
    void lostProviderAnswerIsUnknownAndReplaySafe() {
        // The PSP answers APPROVED, but only after our read timeout (500ms): the charge exists on
        // the PSP side and we never see the answer.
        stubPspApproved("psp-tx-400", "AUTH-400", 2000);

        UUID id = accepted(createPayment("idem-lost", request("42.00"), "corr-lost"));

        Payment unknown = awaitStatus(id, PaymentStatus.UNKNOWN);
        assertThat(unknown.getStatus()).isNotEqualTo(PaymentStatus.FAILED);
        assertThat(unknown.getFailureReason()).contains("READ_TIMEOUT");

        // GUARDRAIL: a read timeout is never retried, by the client nor by the worker.
        await().atMost(Duration.ofSeconds(5)).untilAsserted(() ->
                PSP.verify(1, postRequestedFor(urlEqualTo(PSP_PATH))
                        .withHeader("Idempotency-Key", equalTo("idem-lost"))));

        // The client retries the same logical attempt: no second intent, no second charge.
        ResponseEntity<PaymentResponse> replay = createPayment("idem-lost", request("42.00"));
        assertThat(replay.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(replay.getBody().id()).isEqualTo(id);
        assertThat(replay.getBody().status()).isEqualTo("UNKNOWN");

        // No PaymentCompleted, no outbox intent for it, no ledger credit.
        assertThat(hasOutboxRow(id, PaymentCompletedEvent.EVENT_TYPE)).isFalse();
        assertThat(countCompletedEventsFor(id, 2000)).isZero();
        assertThat(ledger.findByPaymentId(id)).isEmpty();
        assertThat(pspCalls()).isEqualTo(1);
    }

    @Test
    @DisplayName("a provider 5xx is UNKNOWN after exactly one attempt and publishes nothing")
    void serverErrorIsUnknownWithoutRetry() {
        PSP.stubFor(post(urlEqualTo(PSP_PATH))
                .willReturn(aResponse().withStatus(503).withBody("psp down")));

        UUID id = accepted(createPayment("idem-5xx", request("42.00")));

        awaitStatus(id, PaymentStatus.UNKNOWN);
        sleep(1000);
        PSP.verify(1, postRequestedFor(urlEqualTo(PSP_PATH)));
        assertThat(countCompletedEventsFor(id, 1500)).isZero();
    }

    @Test
    @DisplayName("a redirecting provider is retried at most 3 times with the same key, then UNKNOWN")
    void redirectIsRetriedThreeTimes() {
        PSP.stubFor(post(urlEqualTo(PSP_PATH))
                .willReturn(aResponse().withStatus(302).withHeader("Location", "/elsewhere")));

        UUID id = accepted(createPayment("idem-redirect", request("42.00")));

        awaitStatus(id, PaymentStatus.UNKNOWN);
        sleep(1000);
        PSP.verify(3, postRequestedFor(urlEqualTo(PSP_PATH))
                .withHeader("Idempotency-Key", equalTo("idem-redirect")));
    }

    @Test
    @DisplayName("a provider 4xx is FAILED (definitive): no retry, no event")
    void rejectionIsFailed() {
        PSP.stubFor(post(urlEqualTo(PSP_PATH))
                .willReturn(aResponse().withStatus(400).withBody("invalid merchant")));

        UUID id = accepted(createPayment("idem-4xx", request("42.00")));

        Payment failed = awaitStatus(id, PaymentStatus.FAILED);
        assertThat(failed.getFailureReason()).contains("invalid merchant");
        sleep(1000);
        PSP.verify(1, postRequestedFor(urlEqualTo(PSP_PATH)));
        assertThat(hasOutboxRow(id, PaymentCompletedEvent.EVENT_TYPE)).isFalse();
        assertThat(countCompletedEventsFor(id, 1500)).isZero();
    }

    @Test
    @DisplayName("PIX provider settles but the answer is lost: UNKNOWN, one call, no fallback to the PSP, replay is safe")
    void pixLostAnswerIsUnknownAndReplaySafe() {
        stubPixConfirmed("E2026090500003", 2000);

        UUID id = accepted(createPayment("idem-pix-lost", pixRequest("42.00"), "corr-pix-lost"));

        Payment unknown = awaitStatus(id, PaymentStatus.UNKNOWN);
        assertThat(unknown.getProvider()).isEqualTo(PaymentProvider.PIX_PROVIDER);
        assertThat(unknown.getFailureReason()).contains("READ_TIMEOUT").contains("PIX_PROVIDER");
        await().atMost(Duration.ofSeconds(5)).untilAsserted(() ->
                PIX.verify(1, postRequestedFor(urlEqualTo(PIX_PATH))
                        .withHeader("Idempotency-Key", equalTo("idem-pix-lost"))));
        assertThat(pspCalls()).isZero();

        ResponseEntity<PaymentResponse> replay = createPayment("idem-pix-lost", pixRequest("42.00"));
        assertThat(replay.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(replay.getBody().status()).isEqualTo("UNKNOWN");
        assertThat(pixCalls()).isEqualTo(1);
        assertThat(countCompletedEventsFor(id, 1500)).isZero();
        assertThat(ledger.findByPaymentId(id)).isEmpty();
    }

    @Test
    @DisplayName("a PIX provider outage does not affect card payments (and vice-versa)")
    void pixOutageDoesNotAffectCardPayments() {
        PIX.stubFor(post(urlEqualTo(PIX_PATH))
                .willReturn(aResponse().withStatus(503).withBody("SPI unavailable")));
        stubPspApproved("psp-tx-isolated", "AUTH-ISO");

        UUID pixId = accepted(createPayment("idem-iso-pix", pixRequest("30.00")));
        UUID cardId = accepted(createPayment("idem-iso-card", request("30.00")));

        awaitStatus(pixId, PaymentStatus.UNKNOWN);
        awaitStatus(cardId, PaymentStatus.APPROVED);
        assertThat(pixCalls()).isEqualTo(1);
        assertThat(pspCalls()).isEqualTo(1);
        await().atMost(Duration.ofSeconds(15)).untilAsserted(() ->
                assertThat(ledger.findByPaymentId(cardId)).hasSize(1));
        assertThat(ledger.findByPaymentId(pixId)).isEmpty();
    }

    // ================================================================== infrastructure failures

    @Test
    @DisplayName("Experiment 6 — Kafka unavailable: 202 anyway, the intent waits in the outbox, processed when the broker is back")
    void kafkaUnavailableDoesNotLoseTheIntent() {
        stubPspApproved("psp-tx-600", "AUTH-600");
        var docker = KAFKA.getDockerClient();

        docker.pauseContainerCmd(KAFKA.getContainerId()).exec();
        UUID id;
        try {
            ResponseEntity<PaymentResponse> response = createPayment("idem-kafka-down", request("60.00"));

            // The client is not affected: PENDING and durable, with its intent.
            id = accepted(response);
            assertThat(outboxRow(id, PaymentRequestedEvent.EVENT_TYPE).isPublished()).isFalse();

            // Nobody can process it while the broker is paused.
            sleep(2000);
            assertThat(stored(id).getStatus()).isEqualTo(PaymentStatus.PENDING);
            assertThat(pspCalls()).isZero();
        } finally {
            docker.unpauseContainerCmd(KAFKA.getContainerId()).exec();
        }

        // Broker back: Debezium ships the row, the worker charges once, the ledger is credited once.
        awaitStatus(id, PaymentStatus.APPROVED);
        PSP.verify(1, postRequestedFor(urlEqualTo(PSP_PATH))
                .withHeader("Idempotency-Key", equalTo("idem-kafka-down")));
        assertThat(parseCompleted(awaitCompletedEvent(id)).status()).isEqualTo("APPROVED");
        await().atMost(Duration.ofSeconds(15)).untilAsserted(() ->
                assertThat(ledger.findByPaymentId(id)).hasSize(1));
        assertThat(countEventsFor(id, properties.topics().paymentRequested(), 1000)).isEqualTo(1);
    }

    @Test
    @DisplayName("Experiment 6b — Debezium unavailable: 202 anyway, the row waits in the WAL/outbox, published exactly once when the connector is back")
    void debeziumUnavailableDoesNotLoseTheIntent() {
        stubPspApproved("psp-tx-650", "AUTH-650");
        var docker = DEBEZIUM.getDockerClient();

        docker.pauseContainerCmd(DEBEZIUM.getContainerId()).exec();
        UUID id;
        try {
            id = accepted(createPayment("idem-debezium-down", request("65.00")));
            assertThat(outboxRow(id, PaymentRequestedEvent.EVENT_TYPE).isPublished()).isFalse();

            // No publisher: nothing on the topic, nothing processed.
            assertThat(countEventsFor(id, properties.topics().paymentRequested(), 2000)).isZero();
            assertThat(stored(id).getStatus()).isEqualTo(PaymentStatus.PENDING);
            assertThat(pspCalls()).isZero();
        } finally {
            docker.unpauseContainerCmd(DEBEZIUM.getContainerId()).exec();
        }

        awaitRequestedEvent(id);
        awaitStatus(id, PaymentStatus.APPROVED);
        PSP.verify(1, postRequestedFor(urlEqualTo(PSP_PATH))
                .withHeader("Idempotency-Key", equalTo("idem-debezium-down")));
        await().atMost(Duration.ofSeconds(15)).untilAsserted(() ->
                assertThat(ledger.findByPaymentId(id)).hasSize(1));
        assertThat(countEventsFor(id, properties.topics().paymentRequested(), 1000)).isEqualTo(1);
    }

    // ================================================================== SPEC-001 consumer (unchanged)

    @Test
    @DisplayName("Experiment 5 — duplicate PaymentCompleted: the same eventId delivered twice credits the ledger once")
    void duplicateCompletedEventHasOneEffect() throws Exception {
        UUID paymentId = UUID.randomUUID();
        UUID eventId = UUID.randomUUID();
        var event = new PaymentCompletedEvent(eventId, "PaymentCompleted", paymentId, "merchant-dup",
                "customer-1", new BigDecimal("15.00"), "BRL", "PIX", "PIX_PROVIDER", "APPROVED", "psp-tx-500",
                "AUTH", null, "corr-dup", Instant.now());
        OutboxMessage message = OutboxMessage.of(eventId, "Payment", paymentId.toString(),
                "PaymentCompleted", properties.topics().paymentCompleted(), paymentId.toString(),
                objectMapper.writeValueAsString(event), "corr-dup", Instant.now());

        resend(message);
        resend(message);

        await().atMost(Duration.ofSeconds(15)).untilAsserted(() ->
                assertThat(processedEvents.existsById(eventId)).isTrue());
        await().atMost(Duration.ofSeconds(10)).until(() -> countCompletedEventsFor(paymentId, 300) >= 2);

        assertThat(ledger.findByPaymentId(paymentId)).hasSize(1);
    }

    // ================================================================== validation / GET

    @Test
    @DisplayName("validation rejects a bad request before any database write or intent")
    void invalidRequestNeverReachesTheProvider() {
        stubPspApproved("psp-tx-700", "AUTH-700");
        long before = payments.count();
        long outboxBefore = outbox.count();

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
        assertThat(outbox.count()).isEqualTo(outboxBefore);
        sleep(1000);
        assertThat(pspCalls()).isZero();
    }

    @Test
    @DisplayName("GET follows the payment from PENDING to its outcome; an unknown id answers 404")
    void paymentCanBeFetched() {
        // 300ms: a window to observe PENDING/PROCESSING, still under the 500ms read timeout.
        stubPspApproved("psp-tx-800", "AUTH-800", 300);
        UUID id = accepted(createPayment("idem-fetch", request("15.00")));

        ResponseEntity<PaymentResponse> fetched =
                rest.getForEntity("/api/v1/payments/{id}", PaymentResponse.class, id);
        assertThat(fetched.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(fetched.getBody().status()).isIn("PENDING", "PROCESSING", "APPROVED");

        awaitStatus(id, PaymentStatus.APPROVED);
        fetched = rest.getForEntity("/api/v1/payments/{id}", PaymentResponse.class, id);
        assertThat(fetched.getBody().status()).isEqualTo("APPROVED");
        assertThat(fetched.getBody().pspTransactionId()).isEqualTo("psp-tx-800");

        ResponseEntity<String> missing = rest.getForEntity(
                "/api/v1/payments/{id}", String.class, UUID.randomUUID());
        assertThat(missing.getStatusCode()).isEqualTo(HttpStatus.NOT_FOUND);
    }
}
