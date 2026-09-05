package com.fintech.payments.service;

import com.fintech.payments.domain.Payment;
import com.fintech.payments.domain.PaymentMethod;
import com.fintech.payments.domain.PaymentProvider;
import com.fintech.payments.domain.PaymentStatus;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
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
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

/**
 * The request side after SPEC-003: the service only persists intent. Everything that touches a
 * provider lives in {@link PaymentProcessorTest}.
 */
@ExtendWith(MockitoExtension.class)
class PaymentServiceTest {

    private static final Clock CLOCK =
            Clock.fixed(Instant.parse("2026-09-03T12:00:00Z"), ZoneOffset.UTC);

    @Mock
    private PaymentStore store;

    private PaymentService service;

    private PaymentCommand command;
    private String fingerprint;
    private Payment pending;

    @BeforeEach
    void setUp() {
        service = new PaymentService(store, new PaymentMetrics(new SimpleMeterRegistry()));
        command = new PaymentCommand("idem-1", "corr-1", "merchant-1", "customer-1",
                new BigDecimal("199.90"), "BRL", PaymentMethod.CREDIT_CARD);
        fingerprint = RequestFingerprint.of(command);
        pending = Payment.pending("idem-1", fingerprint, "corr-1", "merchant-1", "customer-1",
                new BigDecimal("199.90"), "BRL", PaymentMethod.CREDIT_CARD, CLOCK.instant());
    }

    // ------------------------------------------------------------- new payment

    @Test
    @DisplayName("SPEC-003 — a new payment is persisted PENDING with its intent and nothing else happens")
    void newPaymentIsOnlyPersisted() {
        when(store.findByIdempotencyKey("idem-1")).thenReturn(Optional.empty());
        when(store.savePending(command, fingerprint)).thenReturn(pending);

        PaymentResult result = service.pay(command);

        assertThat(result.replayed()).isFalse();
        assertThat(result.payment()).isSameAs(pending);
        assertThat(result.payment().getStatus()).isEqualTo(PaymentStatus.PENDING);
        assertThat(result.payment().getProvider()).isEqualTo(PaymentProvider.CARD_PSP);
        // savePending is the single write: the outbox row is part of it (same transaction).
        verify(store).savePending(command, fingerprint);
        verify(store, never()).claim(any());
        verify(store, never()).settle(any(), any(), any());
        verify(store, never()).markFailed(any(), anyString(), any());
        verify(store, never()).markUnknown(any(), anyString(), any());
        verifyNoMoreInteractions(store);
    }

    @Test
    @DisplayName("a PIX payment is also just persisted, routed to the PIX provider for later")
    void pixPaymentIsOnlyPersisted() {
        PaymentCommand pixCommand = new PaymentCommand("idem-pix", "corr-pix", "merchant-1",
                "customer-1", new BigDecimal("50.00"), "BRL", PaymentMethod.PIX);
        String pixFingerprint = RequestFingerprint.of(pixCommand);
        Payment pixPending = Payment.pending("idem-pix", pixFingerprint, "corr-pix", "merchant-1",
                "customer-1", new BigDecimal("50.00"), "BRL", PaymentMethod.PIX, CLOCK.instant());
        when(store.findByIdempotencyKey("idem-pix")).thenReturn(Optional.empty());
        when(store.savePending(pixCommand, pixFingerprint)).thenReturn(pixPending);

        PaymentResult result = service.pay(pixCommand);

        assertThat(result.payment().getStatus()).isEqualTo(PaymentStatus.PENDING);
        assertThat(result.payment().getProvider()).isEqualTo(PaymentProvider.PIX_PROVIDER);
        verify(store, never()).claim(any());
    }

    // ------------------------------------------------------------- idempotency

    @Test
    @DisplayName("a replayed key returns the original payment without creating a second intent")
    void replaysExistingPayment() {
        pending.approve("psp-tx-1", "AUTH123", CLOCK.instant());
        when(store.findByIdempotencyKey("idem-1")).thenReturn(Optional.of(pending));

        PaymentResult result = service.pay(command);

        assertThat(result.replayed()).isTrue();
        assertThat(result.payment()).isSameAs(pending);
        verify(store, never()).savePending(any(), anyString());
    }

    @Test
    @DisplayName("replaying a key whose payment is UNKNOWN does NOT create a new attempt")
    void replayOfUnknownPaymentNeverChargesAgain() {
        pending.markUnknown("PSP outcome unknown: READ_TIMEOUT", CLOCK.instant());
        when(store.findByIdempotencyKey("idem-1")).thenReturn(Optional.of(pending));

        PaymentResult result = service.pay(command);

        assertThat(result.replayed()).isTrue();
        assertThat(result.payment().getStatus()).isEqualTo(PaymentStatus.UNKNOWN);
        verify(store, never()).savePending(any(), anyString());
    }

    @Test
    @DisplayName("replaying a key whose payment is PENDING or PROCESSING returns it as is")
    void replayOfInFlightPaymentReturnsItAsIs() {
        when(store.findByIdempotencyKey("idem-1")).thenReturn(Optional.of(pending));
        assertThat(service.pay(command).payment().getStatus()).isEqualTo(PaymentStatus.PENDING);

        pending.claim(CLOCK.instant());
        PaymentResult result = service.pay(command);
        assertThat(result.replayed()).isTrue();
        assertThat(result.payment().getStatus()).isEqualTo(PaymentStatus.PROCESSING);
        verify(store, never()).savePending(any(), anyString());
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
        verify(store, never()).savePending(any(), anyString());
    }

    @Test
    @DisplayName("the same key with another payment method is a conflict: it never becomes a second intent")
    void sameKeyDifferentMethodIsConflict() {
        when(store.findByIdempotencyKey("idem-1")).thenReturn(Optional.of(pending));
        PaymentCommand asPix = new PaymentCommand("idem-1", "corr-2", "merchant-1",
                "customer-1", new BigDecimal("199.90"), "BRL", PaymentMethod.PIX);

        assertThatThrownBy(() -> service.pay(asPix))
                .isInstanceOf(IdempotencyKeyConflictException.class);
        verify(store, never()).savePending(any(), anyString());
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
