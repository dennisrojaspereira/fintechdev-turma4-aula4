package com.fintech.payments.service;

import com.fintech.payments.domain.Payment;
import com.fintech.payments.domain.PaymentMethod;
import com.fintech.payments.domain.PaymentStatus;
import com.fintech.payments.psp.PspChargeRequest;
import com.fintech.payments.psp.PspChargeResponse;
import com.fintech.payments.psp.PspClient;
import com.fintech.payments.psp.PspException;
import com.fintech.payments.psp.PspUnavailableException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
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
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class PaymentServiceTest {

    private static final Clock CLOCK =
            Clock.fixed(Instant.parse("2026-09-02T12:00:00Z"), ZoneOffset.UTC);

    @Mock
    private PaymentStore store;

    @Mock
    private PspClient psp;

    @InjectMocks
    private PaymentService service;

    private PaymentCommand command;
    private Payment pending;

    @BeforeEach
    void setUp() {
        command = new PaymentCommand("idem-1", "merchant-1", "customer-1",
                new BigDecimal("199.90"), "BRL", PaymentMethod.CREDIT_CARD);
        pending = Payment.pending("idem-1", "merchant-1", "customer-1",
                new BigDecimal("199.90"), "BRL", PaymentMethod.CREDIT_CARD, CLOCK.instant());
    }

    @Test
    @DisplayName("approved charge is settled and reported as not replayed")
    void approvesPayment() {
        var response = new PspChargeResponse("psp-tx-1",
                PspChargeResponse.PspStatus.APPROVED, "AUTH123", null);

        when(store.findByIdempotencyKey("idem-1")).thenReturn(Optional.empty());
        when(store.savePending(command)).thenReturn(pending);
        when(psp.charge(any())).thenReturn(response);
        when(store.settle(pending, response)).thenAnswer(invocation -> {
            pending.approve("psp-tx-1", "AUTH123", CLOCK.instant());
            return pending;
        });

        PaymentResult result = service.pay(command);

        assertThat(result.replayed()).isFalse();
        assertThat(result.payment().getStatus()).isEqualTo(PaymentStatus.APPROVED);
        assertThat(result.payment().getPspAuthorizationCode()).isEqualTo("AUTH123");
    }

    @Test
    @DisplayName("the idempotency key is forwarded to the PSP so its retries are safe too")
    void forwardsIdempotencyKeyToPsp() {
        when(store.findByIdempotencyKey("idem-1")).thenReturn(Optional.empty());
        when(store.savePending(command)).thenReturn(pending);
        when(psp.charge(any())).thenReturn(new PspChargeResponse("psp-tx-1",
                PspChargeResponse.PspStatus.APPROVED, "AUTH123", null));
        when(store.settle(any(), any())).thenReturn(pending);

        service.pay(command);

        ArgumentCaptor<PspChargeRequest> captor = ArgumentCaptor.forClass(PspChargeRequest.class);
        verify(psp).charge(captor.capture());
        assertThat(captor.getValue().idempotencyKey()).isEqualTo("idem-1");
        assertThat(captor.getValue().amount()).isEqualByComparingTo("199.90");
        assertThat(captor.getValue().paymentMethod()).isEqualTo("CREDIT_CARD");
    }

    @Test
    @DisplayName("declined charge is settled as DECLINED, not treated as an error")
    void declinesPayment() {
        var response = new PspChargeResponse("psp-tx-2",
                PspChargeResponse.PspStatus.DECLINED, null, "INSUFFICIENT_FUNDS");

        when(store.findByIdempotencyKey("idem-1")).thenReturn(Optional.empty());
        when(store.savePending(command)).thenReturn(pending);
        when(psp.charge(any())).thenReturn(response);
        when(store.settle(pending, response)).thenAnswer(invocation -> {
            pending.decline("psp-tx-2", "INSUFFICIENT_FUNDS", CLOCK.instant());
            return pending;
        });

        PaymentResult result = service.pay(command);

        assertThat(result.payment().getStatus()).isEqualTo(PaymentStatus.DECLINED);
        assertThat(result.payment().getFailureReason()).isEqualTo("INSUFFICIENT_FUNDS");
    }

    @Test
    @DisplayName("a replayed idempotency key returns the original payment without calling the PSP")
    void replaysExistingPayment() {
        pending.approve("psp-tx-1", "AUTH123", CLOCK.instant());
        when(store.findByIdempotencyKey("idem-1")).thenReturn(Optional.of(pending));

        PaymentResult result = service.pay(command);

        assertThat(result.replayed()).isTrue();
        assertThat(result.payment()).isSameAs(pending);
        verifyNoInteractions(psp);
        verify(store, never()).savePending(any());
    }

    @Test
    @DisplayName("a concurrent duplicate loses the unique-index race and replays instead")
    void handlesConcurrentDuplicate() {
        when(store.findByIdempotencyKey("idem-1"))
                .thenReturn(Optional.empty())
                .thenReturn(Optional.of(pending));
        when(store.savePending(command))
                .thenThrow(new DataIntegrityViolationException("uk_payments_idempotency_key"));

        PaymentResult result = service.pay(command);

        assertThat(result.replayed()).isTrue();
        verifyNoInteractions(psp);
    }

    @Test
    @DisplayName("an unreachable PSP marks the payment FAILED and emits no completion event")
    void marksFailedWhenPspUnavailable() {
        when(store.findByIdempotencyKey("idem-1")).thenReturn(Optional.empty());
        when(store.savePending(command)).thenReturn(pending);
        when(psp.charge(any())).thenThrow(new PspUnavailableException("read timed out"));
        when(store.markFailed(eq(pending), anyString())).thenAnswer(invocation -> {
            pending.fail(invocation.getArgument(1), CLOCK.instant());
            return pending;
        });

        PaymentResult result = service.pay(command);

        assertThat(result.payment().getStatus()).isEqualTo(PaymentStatus.FAILED);
        assertThat(result.payment().getFailureReason()).contains("read timed out");
        verify(store, never()).settle(any(), any());
    }

    @Test
    @DisplayName("a PSP protocol error also lands as FAILED, never as DECLINED")
    void marksFailedWhenPspRejectsRequest() {
        when(store.findByIdempotencyKey("idem-1")).thenReturn(Optional.empty());
        when(store.savePending(command)).thenReturn(pending);
        when(psp.charge(any())).thenThrow(new PspException("PSP rejected the request with 400"));
        when(store.markFailed(eq(pending), anyString())).thenAnswer(invocation -> {
            pending.fail(invocation.getArgument(1), CLOCK.instant());
            return pending;
        });

        PaymentResult result = service.pay(command);

        assertThat(result.payment().getStatus()).isEqualTo(PaymentStatus.FAILED);
        verify(store, never()).settle(any(), any());
    }

    @Test
    @DisplayName("a settled payment cannot be settled again")
    void terminalPaymentCannotChangeState() {
        pending.approve("psp-tx-1", "AUTH123", CLOCK.instant());

        assertThatThrownBy(() -> pending.decline("psp-tx-2", "nope", CLOCK.instant()))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("terminal state APPROVED");
    }
}
