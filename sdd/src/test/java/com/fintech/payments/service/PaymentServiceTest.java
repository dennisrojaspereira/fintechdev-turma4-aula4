package com.fintech.payments.service;

import com.fintech.payments.domain.Payment;
import com.fintech.payments.domain.PaymentMethod;
import com.fintech.payments.domain.PaymentProvider;
import com.fintech.payments.domain.PaymentStatus;
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
import org.springframework.dao.DataIntegrityViolationException;

import java.math.BigDecimal;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Optional;

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

@ExtendWith(MockitoExtension.class)
class PaymentServiceTest {

    private static final Clock CLOCK =
            Clock.fixed(Instant.parse("2026-09-03T12:00:00Z"), ZoneOffset.UTC);

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

    private PaymentService service;

    private PaymentCommand command;
    private String fingerprint;
    private Payment pending;

    @BeforeEach
    void setUp() {
        service = new PaymentService(store, router, new PaymentMetrics(new SimpleMeterRegistry()));
        // Routing as fixed by ADR-004; lenient because replay paths never route at all.
        lenient().when(router.clientFor(PaymentProvider.CARD_PSP)).thenReturn(psp);
        lenient().when(router.clientFor(PaymentProvider.PIX_PROVIDER)).thenReturn(pix);

        command = new PaymentCommand("idem-1", "corr-1", "merchant-1", "customer-1",
                new BigDecimal("199.90"), "BRL", PaymentMethod.CREDIT_CARD);
        fingerprint = RequestFingerprint.of(command);
        pending = Payment.pending("idem-1", fingerprint, "corr-1", "merchant-1", "customer-1",
                new BigDecimal("199.90"), "BRL", PaymentMethod.CREDIT_CARD, CLOCK.instant());
    }

    private void newPaymentFlow() {
        when(store.findByIdempotencyKey("idem-1")).thenReturn(Optional.empty());
        when(store.savePending(command, fingerprint)).thenReturn(pending);
    }

    // ---------------------------------------------------------------- outcomes

    @Test
    @DisplayName("approved charge is settled (payment + outbox) and reported as not replayed")
    void approvesPayment() {
        var response = new PspChargeResponse("psp-tx-1",
                PspChargeResponse.PspStatus.APPROVED, "AUTH123", null);
        newPaymentFlow();
        when(psp.charge(any())).thenReturn(response);
        when(store.settle(pending, response)).thenAnswer(inv -> {
            pending.approve("psp-tx-1", "AUTH123", CLOCK.instant());
            return pending;
        });

        PaymentResult result = service.pay(command);

        assertThat(result.replayed()).isFalse();
        assertThat(result.payment().getStatus()).isEqualTo(PaymentStatus.APPROVED);
        verify(store).settle(pending, response);
    }

    @Test
    @DisplayName("declined charge is settled as DECLINED, not treated as an error")
    void declinesPayment() {
        var response = new PspChargeResponse("psp-tx-2",
                PspChargeResponse.PspStatus.DECLINED, null, "INSUFFICIENT_FUNDS");
        newPaymentFlow();
        when(psp.charge(any())).thenReturn(response);
        when(store.settle(pending, response)).thenAnswer(inv -> {
            pending.decline("psp-tx-2", "INSUFFICIENT_FUNDS", CLOCK.instant());
            return pending;
        });

        PaymentResult result = service.pay(command);

        assertThat(result.payment().getStatus()).isEqualTo(PaymentStatus.DECLINED);
        assertThat(result.payment().getFailureReason()).isEqualTo("INSUFFICIENT_FUNDS");
    }

    @Test
    @DisplayName("a PSP timeout becomes UNKNOWN, never FAILED, and emits no completion event")
    void timeoutBecomesUnknownNotFailed() {
        newPaymentFlow();
        when(psp.charge(any())).thenThrow(new PspOutcomeUnknownException(
                PspFailureKind.READ_TIMEOUT, 1, "READ_TIMEOUT after 1 attempt(s)", null));
        when(store.markUnknown(eq(pending), anyString())).thenAnswer(inv -> {
            pending.markUnknown(inv.getArgument(1), CLOCK.instant());
            return pending;
        });

        PaymentResult result = service.pay(command);

        assertThat(result.payment().getStatus()).isEqualTo(PaymentStatus.UNKNOWN);
        assertThat(result.payment().getStatus()).isNotEqualTo(PaymentStatus.FAILED);
        assertThat(result.payment().getFailureReason()).contains("READ_TIMEOUT");
        verify(store, never()).settle(any(), any());
        verify(store, never()).markFailed(any(), anyString());
    }

    @Test
    @DisplayName("a 5xx from the PSP also becomes UNKNOWN")
    void serverErrorBecomesUnknown() {
        newPaymentFlow();
        when(psp.charge(any())).thenThrow(new PspOutcomeUnknownException(
                PspFailureKind.SERVER_ERROR, 1, "PSP responded 503", null));
        when(store.markUnknown(eq(pending), anyString())).thenAnswer(inv -> {
            pending.markUnknown(inv.getArgument(1), CLOCK.instant());
            return pending;
        });

        assertThat(service.pay(command).payment().getStatus()).isEqualTo(PaymentStatus.UNKNOWN);
        verify(store, never()).settle(any(), any());
    }

    @Test
    @DisplayName("a PSP rejection (4xx) is FAILED: definitive, no event, no reconciliation")
    void rejectionBecomesFailed() {
        newPaymentFlow();
        when(psp.charge(any())).thenThrow(new PspRejectedException(400, "PSP rejected the request with 400"));
        when(store.markFailed(eq(pending), anyString())).thenAnswer(inv -> {
            pending.fail(inv.getArgument(1), CLOCK.instant());
            return pending;
        });

        PaymentResult result = service.pay(command);

        assertThat(result.payment().getStatus()).isEqualTo(PaymentStatus.FAILED);
        verify(store, never()).settle(any(), any());
        verify(store, never()).markUnknown(any(), anyString());
    }

    // ---------------------------------------------------------------- routing (SPEC-002)

    @Test
    @DisplayName("a card payment goes to the card PSP and never touches the PIX provider")
    void routesCardsToCardPsp() {
        newPaymentFlow();
        when(psp.charge(any())).thenReturn(new PspChargeResponse("psp-tx-1",
                PspChargeResponse.PspStatus.APPROVED, "AUTH123", null));
        when(store.settle(any(), any())).thenReturn(pending);

        service.pay(command);

        assertThat(pending.getProvider()).isEqualTo(PaymentProvider.CARD_PSP);
        verify(router).clientFor(PaymentProvider.CARD_PSP);
        verify(router, never()).clientFor(PaymentProvider.PIX_PROVIDER);
        verify(psp).charge(any());
        verifyNoInteractions(pix);
    }

    @Test
    @DisplayName("a PIX payment goes to the PIX provider and never touches the card PSP")
    void routesPixToPixProvider() {
        PaymentCommand pixCommand = new PaymentCommand("idem-pix", "corr-pix", "merchant-1",
                "customer-1", new BigDecimal("50.00"), "BRL", PaymentMethod.PIX);
        String pixFingerprint = RequestFingerprint.of(pixCommand);
        Payment pixPending = Payment.pending("idem-pix", pixFingerprint, "corr-pix", "merchant-1",
                "customer-1", new BigDecimal("50.00"), "BRL", PaymentMethod.PIX, CLOCK.instant());
        when(store.findByIdempotencyKey("idem-pix")).thenReturn(Optional.empty());
        when(store.savePending(pixCommand, pixFingerprint)).thenReturn(pixPending);
        var confirmed = new PspChargeResponse("E123", PspChargeResponse.PspStatus.APPROVED, null, null);
        when(pix.charge(any())).thenReturn(confirmed);
        when(store.settle(pixPending, confirmed)).thenAnswer(inv -> {
            pixPending.approve("E123", null, CLOCK.instant());
            return pixPending;
        });

        PaymentResult result = service.pay(pixCommand);

        assertThat(result.payment().getProvider()).isEqualTo(PaymentProvider.PIX_PROVIDER);
        assertThat(result.payment().getStatus()).isEqualTo(PaymentStatus.APPROVED);
        assertThat(result.payment().getPspTransactionId()).isEqualTo("E123");
        verify(router).clientFor(PaymentProvider.PIX_PROVIDER);
        verify(router, never()).clientFor(PaymentProvider.CARD_PSP);
        ArgumentCaptor<PspChargeRequest> captor = ArgumentCaptor.forClass(PspChargeRequest.class);
        verify(pix).charge(captor.capture());
        assertThat(captor.getValue().idempotencyKey()).isEqualTo("idem-pix");
        assertThat(captor.getValue().paymentMethod()).isEqualTo("PIX");
        verifyNoInteractions(psp);
    }

    @Test
    @DisplayName("a PIX provider timeout is UNKNOWN for the PIX provider, and the card PSP is never asked instead")
    void pixTimeoutIsUnknownWithoutFallback() {
        PaymentCommand pixCommand = new PaymentCommand("idem-pix", "corr-pix", "merchant-1",
                "customer-1", new BigDecimal("50.00"), "BRL", PaymentMethod.PIX);
        String pixFingerprint = RequestFingerprint.of(pixCommand);
        Payment pixPending = Payment.pending("idem-pix", pixFingerprint, "corr-pix", "merchant-1",
                "customer-1", new BigDecimal("50.00"), "BRL", PaymentMethod.PIX, CLOCK.instant());
        when(store.findByIdempotencyKey("idem-pix")).thenReturn(Optional.empty());
        when(store.savePending(pixCommand, pixFingerprint)).thenReturn(pixPending);
        when(pix.charge(any())).thenThrow(new PspOutcomeUnknownException(
                PspFailureKind.READ_TIMEOUT, 1, "READ_TIMEOUT after 1 attempt(s) at PIX_PROVIDER", null));
        when(store.markUnknown(eq(pixPending), anyString())).thenAnswer(inv -> {
            pixPending.markUnknown(inv.getArgument(1), CLOCK.instant());
            return pixPending;
        });

        PaymentResult result = service.pay(pixCommand);

        assertThat(result.payment().getStatus()).isEqualTo(PaymentStatus.UNKNOWN);
        assertThat(result.payment().getProvider()).isEqualTo(PaymentProvider.PIX_PROVIDER);
        verifyNoInteractions(psp);
        verify(store, never()).settle(any(), any());
    }

    // ------------------------------------------------------------- idempotency

    @Test
    @DisplayName("the idempotency key and correlation id are forwarded to the PSP")
    void forwardsIdentifiersToPsp() {
        newPaymentFlow();
        when(psp.charge(any())).thenReturn(new PspChargeResponse("psp-tx-1",
                PspChargeResponse.PspStatus.APPROVED, "AUTH123", null));
        when(store.settle(any(), any())).thenReturn(pending);

        service.pay(command);

        ArgumentCaptor<PspChargeRequest> captor = ArgumentCaptor.forClass(PspChargeRequest.class);
        verify(psp).charge(captor.capture());
        assertThat(captor.getValue().idempotencyKey()).isEqualTo("idem-1");
        assertThat(captor.getValue().correlationId()).isEqualTo("corr-1");
        assertThat(captor.getValue().amount()).isEqualByComparingTo("199.90");
    }

    @Test
    @DisplayName("a replayed key returns the original payment without calling any provider")
    void replaysExistingPayment() {
        pending.approve("psp-tx-1", "AUTH123", CLOCK.instant());
        when(store.findByIdempotencyKey("idem-1")).thenReturn(Optional.of(pending));

        PaymentResult result = service.pay(command);

        assertThat(result.replayed()).isTrue();
        assertThat(result.payment()).isSameAs(pending);
        verifyNoInteractions(psp, pix, router);
        verify(store, never()).savePending(any(), anyString());
    }

    @Test
    @DisplayName("replaying a key whose payment is UNKNOWN does NOT charge again")
    void replayOfUnknownPaymentNeverChargesAgain() {
        pending.markUnknown("PSP outcome unknown: READ_TIMEOUT", CLOCK.instant());
        when(store.findByIdempotencyKey("idem-1")).thenReturn(Optional.of(pending));

        PaymentResult result = service.pay(command);

        assertThat(result.replayed()).isTrue();
        assertThat(result.payment().getStatus()).isEqualTo(PaymentStatus.UNKNOWN);
        verifyNoInteractions(psp, pix);
        verify(store, never()).savePending(any(), anyString());
    }

    @Test
    @DisplayName("replaying a key whose payment is still PENDING (in flight) does not charge again")
    void replayOfPendingPaymentNeverChargesAgain() {
        when(store.findByIdempotencyKey("idem-1")).thenReturn(Optional.of(pending));

        PaymentResult result = service.pay(command);

        assertThat(result.replayed()).isTrue();
        assertThat(result.payment().getStatus()).isEqualTo(PaymentStatus.PENDING);
        verifyNoInteractions(psp, pix);
    }

    @Test
    @DisplayName("the same key with a different body is a conflict, not a replay")
    void sameKeyDifferentBodyIsConflict() {
        when(store.findByIdempotencyKey("idem-1")).thenReturn(Optional.of(pending));
        PaymentCommand different = new PaymentCommand("idem-1", "corr-2", "merchant-1",
                "customer-1", new BigDecimal("500.00"), "BRL", PaymentMethod.CREDIT_CARD);

        assertThatThrownBy(() -> service.pay(different))
                .isInstanceOf(IdempotencyKeyConflictException.class)
                .hasMessageContaining("idem-1");
        verifyNoInteractions(psp, pix);
    }

    @Test
    @DisplayName("the same key with another payment method is a conflict: it never reaches a second provider")
    void sameKeyDifferentMethodIsConflict() {
        when(store.findByIdempotencyKey("idem-1")).thenReturn(Optional.of(pending));
        PaymentCommand asPix = new PaymentCommand("idem-1", "corr-2", "merchant-1",
                "customer-1", new BigDecimal("199.90"), "BRL", PaymentMethod.PIX);

        assertThatThrownBy(() -> service.pay(asPix))
                .isInstanceOf(IdempotencyKeyConflictException.class);
        verifyNoInteractions(psp, pix, router);
    }

    @Test
    @DisplayName("a concurrent duplicate loses the unique-index race and replays the winner")
    void handlesConcurrentDuplicate() {
        when(store.findByIdempotencyKey("idem-1"))
                .thenReturn(Optional.empty())
                .thenReturn(Optional.of(pending));
        when(store.savePending(command, fingerprint))
                .thenThrow(new DataIntegrityViolationException("uk_payments_idempotency_key"));

        PaymentResult result = service.pay(command);

        assertThat(result.replayed()).isTrue();
        assertThat(result.payment()).isSameAs(pending);
        verifyNoInteractions(psp, pix);
    }

    @Test
    @DisplayName("the fingerprint ignores insignificant decimal differences")
    void fingerprintIsCanonical() {
        PaymentCommand a = new PaymentCommand("k", "c1", "m", "c",
                new BigDecimal("10.5"), "BRL", PaymentMethod.PIX);
        PaymentCommand b = new PaymentCommand("k", "c2", "m", "c",
                new BigDecimal("10.50"), "BRL", PaymentMethod.PIX);
        PaymentCommand c = new PaymentCommand("k", "c1", "m", "c",
                new BigDecimal("10.51"), "BRL", PaymentMethod.PIX);

        assertThat(RequestFingerprint.of(a)).isEqualTo(RequestFingerprint.of(b));
        assertThat(RequestFingerprint.of(a)).isNotEqualTo(RequestFingerprint.of(c));
    }
}
