package com.fintech.payments.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fintech.payments.config.PaymentsProperties;
import com.fintech.payments.domain.OutboxMessage;
import com.fintech.payments.domain.OutboxRepository;
import com.fintech.payments.domain.Payment;
import com.fintech.payments.domain.PaymentRepository;
import com.fintech.payments.domain.ProcessedEventRepository;
import com.fintech.payments.messaging.PaymentCompletedEvent;
import com.fintech.payments.messaging.PaymentRequestedEvent;
import com.fintech.payments.psp.PspChargeResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Clock;
import java.time.Instant;
import java.util.Optional;
import java.util.UUID;

/**
 * Owns every database transaction touching a payment.
 *
 * <p>Kept apart from {@link PaymentService} and {@link PaymentProcessor} on purpose: the provider
 * call must happen *outside* a transaction (a remote call must never hold a database connection
 * open), while each business step must happen *inside a single* transaction:
 * <pre>
 *   API     BEGIN  insert Payment(PENDING)  + insert Outbox(PaymentRequested)                 COMMIT
 *   worker  BEGIN  update Payment(PENDING→PROCESSING)                                         COMMIT
 *   worker  BEGIN  update Payment(outcome)  + insert Outbox(PaymentCompleted)? + insert Inbox  COMMIT
 * </pre>
 * Kafka and Debezium do not participate in these transactions and do not need to be available:
 * the outbox row is the durable intent, captured later from the database log.
 */
@Service
public class PaymentStore {

    private static final Logger log = LoggerFactory.getLogger(PaymentStore.class);

    public static final String AGGREGATE_TYPE = "Payment";

    private final PaymentRepository payments;
    private final OutboxRepository outbox;
    private final ProcessedEventRepository inbox;
    private final ObjectMapper objectMapper;
    private final PaymentsProperties properties;
    private final Clock clock;

    public PaymentStore(PaymentRepository payments,
                        OutboxRepository outbox,
                        ProcessedEventRepository inbox,
                        ObjectMapper objectMapper,
                        PaymentsProperties properties,
                        Clock clock) {
        this.payments = payments;
        this.outbox = outbox;
        this.inbox = inbox;
        this.objectMapper = objectMapper;
        this.properties = properties;
        this.clock = clock;
    }

    @Transactional(readOnly = true)
    public Optional<Payment> findByIdempotencyKey(String key) {
        return payments.findByIdempotencyKey(key);
    }

    @Transactional(readOnly = true)
    public Optional<Payment> findById(UUID id) {
        return payments.findById(id);
    }

    /** Whether the inbox already holds this event: the shortcut of the worker protocol (D9). */
    @Transactional(readOnly = true)
    public boolean isProcessed(UUID eventId) {
        return inbox.existsById(eventId);
    }

    // ------------------------------------------------------------------ API side

    /**
     * Records the intent to charge before any money moves (SPEC-003): the PENDING payment and the
     * {@code PaymentRequested} outbox row commit together, so a payment can never exist without
     * its durable request to process it, nor the other way round.
     */
    @Transactional
    public Payment savePending(PaymentCommand command, String fingerprint) {
        Instant now = clock.instant();
        Payment payment = payments.saveAndFlush(Payment.pending(
                command.idempotencyKey(),
                fingerprint,
                command.correlationId(),
                command.merchantId(),
                command.customerId(),
                command.amount(),
                command.currency(),
                command.paymentMethod(),
                now));

        UUID eventId = UUID.randomUUID();
        enqueue(payment, eventId, PaymentRequestedEvent.EVENT_TYPE,
                properties.topics().paymentRequested(),
                serialize(PaymentRequestedEvent.from(eventId, payment, now)), now);
        return payment;
    }

    // --------------------------------------------------------------- worker side

    /**
     * Atomic claim (ADR-005 D9): {@code PENDING → PROCESSING} for exactly one caller. Returns
     * false when the payment is not PENDING any more, in which case the caller must not call the
     * provider and has to decide by the current state instead.
     */
    @Transactional
    public boolean claim(UUID paymentId) {
        return payments.claim(paymentId, clock.instant());
    }

    /**
     * Applies the provider answer, enqueues the PaymentCompleted event and records the consumed
     * event in the inbox, atomically. Either the new status, the durable publish intent and the
     * proof of processing are all committed, or none is.
     */
    @Transactional
    public Payment settle(Payment detached, PspChargeResponse response, InboxEntry processed) {
        Payment payment = reload(detached);
        Instant now = clock.instant();

        if (response.isApproved()) {
            payment.approve(response.transactionId(), response.authorizationCode(), now);
        } else {
            payment.decline(response.transactionId(), response.declineReason(), now);
        }

        payments.save(payment);
        UUID eventId = UUID.randomUUID();
        enqueue(payment, eventId, PaymentCompletedEvent.EVENT_TYPE,
                properties.topics().paymentCompleted(),
                serialize(PaymentCompletedEvent.from(eventId, payment, now)), now);
        recordInbox(processed, now);
        return payment;
    }

    /** The provider refused the request: definitive, no financial effect, no completion event. */
    @Transactional
    public Payment markFailed(Payment detached, String reason, InboxEntry processed) {
        Payment payment = reload(detached);
        payment.fail(reason, clock.instant());
        recordInbox(processed, clock.instant());
        return payments.save(payment);
    }

    /**
     * We never learned the provider outcome (no answer, or a worker died mid-call). No
     * PaymentCompleted event is emitted; the payment stays unresolved (UNKNOWN) until
     * reconciliation, and a replay of the same key returns it as is.
     */
    @Transactional
    public Payment markUnknown(Payment detached, String reason, InboxEntry processed) {
        Payment payment = reload(detached);
        payment.markUnknown(reason, clock.instant());
        recordInbox(processed, clock.instant());
        return payments.save(payment);
    }

    /**
     * Records an event that needs no business effect (the payment it refers to is already
     * resolved). Returns false if the inbox already had it.
     */
    @Transactional
    public boolean recordProcessed(InboxEntry processed) {
        return insertInbox(processed, clock.instant()) == 1;
    }

    // ------------------------------------------------------------------- internals

    private Payment reload(Payment detached) {
        return payments.findById(detached.getId())
                .orElseThrow(() -> new IllegalStateException(
                        "Payment " + detached.getId() + " disappeared between claim and settle"));
    }

    private void recordInbox(InboxEntry processed, Instant now) {
        if (insertInbox(processed, now) == 0) {
            // Cannot happen under the claim protocol (only the claimer reaches here), but the
            // outcome is real money and must be persisted regardless: log, never roll back.
            log.warn("Inbox already had eventId={} while recording the outcome of payment {}",
                    processed.eventId(), processed.aggregateId());
        }
    }

    private int insertInbox(InboxEntry processed, Instant now) {
        return inbox.insertIfAbsent(processed.eventId(), processed.eventType(),
                processed.aggregateId(), processed.correlationId(), now);
    }

    /**
     * The outbox row id *is* the eventId: stable across redeliveries, so consumers can
     * deduplicate on it. Keyed by payment id so all events of one payment stay ordered in one
     * partition.
     */
    private void enqueue(Payment payment, UUID eventId, String eventType, String topic,
                         String payload, Instant now) {
        outbox.save(OutboxMessage.of(
                eventId,
                AGGREGATE_TYPE,
                payment.getId().toString(),
                eventType,
                topic,
                payment.getId().toString(),
                payload,
                payment.getCorrelationId(),
                now));
    }

    private String serialize(Object event) {
        try {
            return objectMapper.writeValueAsString(event);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Could not serialize " + event.getClass().getSimpleName(), e);
        }
    }
}
