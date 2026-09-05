package com.fintech.payments.service;

import com.fintech.payments.config.PaymentsProperties;
import com.fintech.payments.domain.Payment;
import com.fintech.payments.domain.PaymentMethod;
import com.fintech.payments.domain.PaymentProvider;
import com.fintech.payments.domain.PaymentStatus;
import com.fintech.payments.messaging.PaymentRequestedEvent;
import com.fintech.payments.messaging.PoisonEventException;
import com.fintech.payments.psp.ProviderRouter;
import com.fintech.payments.psp.PspChargeRequest;
import com.fintech.payments.psp.PspChargeResponse;
import com.fintech.payments.psp.PspClient;
import com.fintech.payments.psp.PspFailureKind;
import com.fintech.payments.psp.PspOutcomeUnknownException;
import com.fintech.payments.psp.PspRejectedException;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.math.BigDecimal;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Optional;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

/**
 * The worker protocol (ADR-005 D9/D10): no provider call without a claim, never two charges,
 * UNKNOWN is never FAILED.
 */
@ExtendWith(MockitoExtension.class)
class PaymentProcessorTest {

    private static final Instant NOW = Instant.parse("2026-09-05T12:00:00Z");
    private static final Clock CLOCK = Clock.fixed(NOW, ZoneOffset.UTC);
    private static final Duration TIMEOUT = Duration.ofSeconds(30);

    @Mock
    private PaymentStore store;

    @Mock
    private ProviderRouter router;

    /** The card PSP client (SPEC-001). */
    @Mock
    private PspClient psp;

    /** The PIX provider client (SPEC-002). */
    @Mock
    private PspClient pix;

    private SimpleMeterRegistry registry;
    private PaymentProcessor processor;

    private Payment payment;
    private PaymentRequestedEvent event;

    @BeforeEach
    void setUp() {
        registry = new SimpleMeterRegistry();
        var properties = new PaymentsProperties(
                new PaymentsProperties.Outbox("cdc", 100, 10, 500),
                new PaymentsProperties.Topics("payments.payment-completed.v1", "payments.payment-requested.v1"),
                new PaymentsProperties.Consumer("ledger"),
                new PaymentsProperties.Worker("worker", TIMEOUT));
        processor = new PaymentProcessor(store, router, new PaymentMetrics(registry), properties, CLOCK);
        lenient().when(router.clientFor(PaymentProvider.CARD_PSP)).thenReturn(psp);
        lenient().when(router.clientFor(PaymentProvider.PIX_PROVIDER)).thenReturn(pix);

        payment = Payment.pending("idem-1", "fp", "corr-1", "merchant-1", "customer-1",
                new BigDecimal("199.90"), "BRL", PaymentMethod.CREDIT_CARD, NOW.minusSeconds(1));
        event = PaymentRequestedEvent.from(UUID.randomUUID(), payment, NOW.minusSeconds(1));
    }

    private void claimable() {
        when(store.isProcessed(event.eventId())).thenReturn(false);
        when(store.findById(payment.getId())).thenReturn(Optional.of(payment));
        when(store.claim(payment.getId())).thenReturn(true);
    }

    private static InboxEntry inboxFor(PaymentRequestedEvent event, Payment payment) {
        return new InboxEntry(event.eventId(), "PaymentRequested", payment.getId().toString(),
                payment.getCorrelationId());
    }

    private double counter(String name, String... tags) {
        var c = registry.find(name).tags(tags).counter();
        return c == null ? 0 : c.count();
    }

    // ------------------------------------------------------------- happy paths

    @Test
    @DisplayName("claim → provider → settle with the event in the inbox, all in one transaction")
    void claimsChargesAndSettles() {
        claimable();
        var response = new PspChargeResponse("psp-tx-1", PspChargeResponse.PspStatus.APPROVED, "AUTH123", null);
        when(psp.charge(any())).thenReturn(response);
        when(store.settle(eq(payment), eq(response), any())).thenAnswer(inv -> {
            payment.claim(NOW);
            payment.approve("psp-tx-1", "AUTH123", NOW);
            return payment;
        });

        PaymentProcessor.Outcome outcome = processor.process(event);

        assertThat(outcome).isEqualTo(PaymentProcessor.Outcome.PROCESSED);
        ArgumentCaptor<PspChargeRequest> request = ArgumentCaptor.forClass(PspChargeRequest.class);
        verify(psp).charge(request.capture());
        assertThat(request.getValue().idempotencyKey()).isEqualTo("idem-1");
        assertThat(request.getValue().correlationId()).isEqualTo("corr-1");
        assertThat(request.getValue().amount()).isEqualByComparingTo("199.90");
        assertThat(request.getValue().paymentMethod()).isEqualTo("CREDIT_CARD");
        verify(store).settle(payment, response, inboxFor(event, payment));
        verifyNoInteractions(pix);
        assertThat(counter("payments.outcome", "status", "APPROVED", "provider", "CARD_PSP")).isEqualTo(1);
    }

    @Test
    @DisplayName("declined charge is settled as DECLINED, not treated as an error")
    void declinedIsSettled() {
        claimable();
        var response = new PspChargeResponse("psp-tx-2", PspChargeResponse.PspStatus.DECLINED, null, "INSUFFICIENT_FUNDS");
        when(psp.charge(any())).thenReturn(response);
        when(store.settle(eq(payment), eq(response), any())).thenReturn(payment);

        processor.process(event);

        verify(store).settle(payment, response, inboxFor(event, payment));
        verify(store, never()).markFailed(any(), anyString(), any());
    }

    @Test
    @DisplayName("a PIX payment goes to the PIX provider and never touches the card PSP")
    void routesPixToPixProvider() {
        Payment pixPayment = Payment.pending("idem-pix", "fp", "corr-pix", "merchant-1", "customer-1",
                new BigDecimal("50.00"), "BRL", PaymentMethod.PIX, NOW);
        PaymentRequestedEvent pixEvent = PaymentRequestedEvent.from(UUID.randomUUID(), pixPayment, NOW);
        when(store.isProcessed(pixEvent.eventId())).thenReturn(false);
        when(store.findById(pixPayment.getId())).thenReturn(Optional.of(pixPayment));
        when(store.claim(pixPayment.getId())).thenReturn(true);
        var confirmed = new PspChargeResponse("E123", PspChargeResponse.PspStatus.APPROVED, null, null);
        when(pix.charge(any())).thenReturn(confirmed);
        when(store.settle(eq(pixPayment), eq(confirmed), any())).thenReturn(pixPayment);

        processor.process(pixEvent);

        verify(router).clientFor(PaymentProvider.PIX_PROVIDER);
        verify(router, never()).clientFor(PaymentProvider.CARD_PSP);
        ArgumentCaptor<PspChargeRequest> request = ArgumentCaptor.forClass(PspChargeRequest.class);
        verify(pix).charge(request.capture());
        assertThat(request.getValue().idempotencyKey()).isEqualTo("idem-pix");
        assertThat(request.getValue().paymentMethod()).isEqualTo("PIX");
        verifyNoInteractions(psp);
    }

    // ---------------------------------------------------------- provider errors

    @Test
    @DisplayName("a provider timeout becomes UNKNOWN (never FAILED) with the event in the inbox, no completion")
    void timeoutBecomesUnknownNotFailed() {
        claimable();
        when(psp.charge(any())).thenThrow(new PspOutcomeUnknownException(
                PspFailureKind.READ_TIMEOUT, 1, "READ_TIMEOUT after 1 attempt(s)", null));
        when(store.markUnknown(eq(payment), anyString(), any())).thenAnswer(inv -> {
            payment.claim(NOW);
            payment.markUnknown(inv.getArgument(1), NOW);
            return payment;
        });

        PaymentProcessor.Outcome outcome = processor.process(event);

        assertThat(outcome).isEqualTo(PaymentProcessor.Outcome.PROCESSED);
        assertThat(payment.getStatus()).isEqualTo(PaymentStatus.UNKNOWN);
        assertThat(payment.getFailureReason()).contains("READ_TIMEOUT");
        verify(store).markUnknown(eq(payment), anyString(), eq(inboxFor(event, payment)));
        verify(store, never()).settle(any(), any(), any());
        verify(store, never()).markFailed(any(), anyString(), any());
        assertThat(counter("payments.psp.unknown", "provider", "CARD_PSP", "kind", "READ_TIMEOUT")).isEqualTo(1);
        assertThat(counter("payments.outcome", "status", "UNKNOWN", "provider", "CARD_PSP")).isEqualTo(1);
    }

    @Test
    @DisplayName("a 5xx from the provider also becomes UNKNOWN")
    void serverErrorBecomesUnknown() {
        claimable();
        when(psp.charge(any())).thenThrow(new PspOutcomeUnknownException(
                PspFailureKind.SERVER_ERROR, 1, "PSP responded 503", null));
        when(store.markUnknown(eq(payment), anyString(), any())).thenReturn(payment);

        processor.process(event);

        verify(store).markUnknown(eq(payment), anyString(), any());
        verify(store, never()).settle(any(), any(), any());
    }

    @Test
    @DisplayName("a provider rejection (4xx) is FAILED: definitive, inbox recorded, no event")
    void rejectionBecomesFailed() {
        claimable();
        when(psp.charge(any())).thenThrow(new PspRejectedException(400, "PSP rejected the request with 400"));
        when(store.markFailed(eq(payment), anyString(), any())).thenAnswer(inv -> {
            payment.claim(NOW);
            payment.fail(inv.getArgument(1), NOW);
            return payment;
        });

        processor.process(event);

        assertThat(payment.getStatus()).isEqualTo(PaymentStatus.FAILED);
        verify(store).markFailed(eq(payment), anyString(), eq(inboxFor(event, payment)));
        verify(store, never()).settle(any(), any(), any());
        verify(store, never()).markUnknown(any(), anyString(), any());
    }

    // ----------------------------------------------------------- idempotency

    @Test
    @DisplayName("a redelivered event (eventId already in the inbox) does nothing at all")
    void duplicateDeliveryDoesNothing() {
        when(store.isProcessed(event.eventId())).thenReturn(true);

        PaymentProcessor.Outcome outcome = processor.process(event);

        assertThat(outcome).isEqualTo(PaymentProcessor.Outcome.DUPLICATE);
        verify(store, never()).claim(any());
        verify(store, never()).findById(any());
        verifyNoInteractions(router, psp, pix);
        assertThat(counter("payments.worker.duplicate", "eventType", "PaymentRequested")).isEqualTo(1);
    }

    @Test
    @DisplayName("an event for an already resolved payment records the inbox and never calls the provider")
    void lateEventForResolvedPaymentIsRecordedOnly() {
        payment.claim(NOW.minusSeconds(10));
        payment.approve("psp-tx-1", "AUTH", NOW.minusSeconds(5));
        when(store.isProcessed(event.eventId())).thenReturn(false);
        when(store.findById(payment.getId())).thenReturn(Optional.of(payment));
        when(store.claim(payment.getId())).thenReturn(false);

        PaymentProcessor.Outcome outcome = processor.process(event);

        assertThat(outcome).isEqualTo(PaymentProcessor.Outcome.ALREADY_RESOLVED);
        verify(store).recordProcessed(inboxFor(event, payment));
        verify(store, never()).settle(any(), any(), any());
        verify(store, never()).markUnknown(any(), anyString(), any());
        verifyNoInteractions(router, psp, pix);
        assertThat(counter("payments.worker.duplicate", "eventType", "PaymentRequested")).isEqualTo(1);
    }

    @Test
    @DisplayName("an event for an UNKNOWN payment is recorded only: reconciliation owns it, not the worker")
    void eventForUnknownPaymentIsRecordedOnly() {
        payment.markUnknown("lost", NOW.minusSeconds(5));
        when(store.isProcessed(event.eventId())).thenReturn(false);
        when(store.findById(payment.getId())).thenReturn(Optional.of(payment));
        when(store.claim(payment.getId())).thenReturn(false);

        assertThat(processor.process(event)).isEqualTo(PaymentProcessor.Outcome.ALREADY_RESOLVED);
        verify(store).recordProcessed(inboxFor(event, payment));
        verifyNoInteractions(router, psp, pix);
    }

    // ------------------------------------------------------- PROCESSING (D10)

    @Test
    @DisplayName("PROCESSING claimed recently by another worker: retry later, no provider, no state change")
    void recentProcessingIsRetried() {
        payment.claim(NOW.minusSeconds(5));
        when(store.isProcessed(event.eventId())).thenReturn(false);
        when(store.findById(payment.getId())).thenReturn(Optional.of(payment));
        when(store.claim(payment.getId())).thenReturn(false);

        assertThatThrownBy(() -> processor.process(event))
                .isInstanceOf(PaymentInFlightException.class)
                .hasMessageContaining(payment.getId().toString());

        verify(store, never()).markUnknown(any(), anyString(), any());
        verify(store, never()).recordProcessed(any());
        verifyNoInteractions(router, psp, pix);
    }

    @Test
    @DisplayName("PROCESSING older than the timeout: the worker died mid-call → UNKNOWN, inbox, no second charge")
    void staleProcessingBecomesUnknownWithoutCharging() {
        payment.claim(NOW.minus(TIMEOUT).minusSeconds(1));
        when(store.isProcessed(event.eventId())).thenReturn(false);
        when(store.findById(payment.getId())).thenReturn(Optional.of(payment));
        when(store.claim(payment.getId())).thenReturn(false);
        when(store.markUnknown(eq(payment), anyString(), any())).thenAnswer(inv -> {
            payment.markUnknown(inv.getArgument(1), NOW);
            return payment;
        });

        PaymentProcessor.Outcome outcome = processor.process(event);

        assertThat(outcome).isEqualTo(PaymentProcessor.Outcome.IN_FLIGHT_UNKNOWN);
        assertThat(payment.getStatus()).isEqualTo(PaymentStatus.UNKNOWN);
        assertThat(payment.getStatus()).isNotEqualTo(PaymentStatus.FAILED);
        assertThat(payment.getFailureReason()).contains("Worker interrupted").contains("CARD_PSP");
        verify(store).markUnknown(eq(payment), anyString(), eq(inboxFor(event, payment)));
        verify(store, never()).markFailed(any(), anyString(), any());
        verifyNoInteractions(router, psp, pix);
        assertThat(counter("payments.worker.inflight_unknown")).isEqualTo(1);
        assertThat(counter("payments.outcome", "status", "UNKNOWN", "provider", "CARD_PSP")).isEqualTo(1);
    }

    @Test
    @DisplayName("PROCESSING exactly at the timeout boundary is still considered in flight")
    void processingAtBoundaryIsStillInFlight() {
        payment.claim(NOW.minus(TIMEOUT).plusMillis(1));
        when(store.isProcessed(event.eventId())).thenReturn(false);
        when(store.findById(payment.getId())).thenReturn(Optional.of(payment));
        when(store.claim(payment.getId())).thenReturn(false);

        assertThatThrownBy(() -> processor.process(event)).isInstanceOf(PaymentInFlightException.class);
        verifyNoInteractions(router, psp, pix);
    }

    @Test
    @DisplayName("claim lost but the row still reads PENDING: retry, never call the provider without a claim")
    void lostClaimOnPendingIsRetried() {
        when(store.isProcessed(event.eventId())).thenReturn(false);
        when(store.findById(payment.getId())).thenReturn(Optional.of(payment));
        when(store.claim(payment.getId())).thenReturn(false);

        assertThatThrownBy(() -> processor.process(event)).isInstanceOf(PaymentInFlightException.class);
        verifyNoInteractions(router, psp, pix);
    }

    // ---------------------------------------------------------------- poison

    @Test
    @DisplayName("an event for a payment that does not exist is poison: skipped, nothing claimed")
    void unknownPaymentIsPoison() {
        when(store.isProcessed(event.eventId())).thenReturn(false);
        when(store.findById(payment.getId())).thenReturn(Optional.empty());

        assertThatThrownBy(() -> processor.process(event))
                .isInstanceOf(PoisonEventException.class)
                .hasMessageContaining(payment.getId().toString());

        verify(store, never()).claim(any());
        verifyNoInteractions(router, psp, pix);
    }
}
