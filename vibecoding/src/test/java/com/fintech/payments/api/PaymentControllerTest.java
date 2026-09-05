package com.fintech.payments.api;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fintech.payments.api.dto.CreatePaymentRequest;
import com.fintech.payments.domain.Payment;
import com.fintech.payments.domain.PaymentMethod;
import com.fintech.payments.service.PaymentCommand;
import com.fintech.payments.service.PaymentResult;
import com.fintech.payments.service.PaymentService;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;

import java.math.BigDecimal;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Optional;
import java.util.UUID;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.header;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@WebMvcTest(PaymentController.class)
class PaymentControllerTest {

    private static final Clock CLOCK =
            Clock.fixed(Instant.parse("2026-09-02T12:00:00Z"), ZoneOffset.UTC);

    @Autowired
    private MockMvc mockMvc;

    @Autowired
    private ObjectMapper objectMapper;

    @MockBean
    private PaymentService paymentService;

    private static Payment pending() {
        return Payment.pending("idem-1", "merchant-1", "customer-1",
                new BigDecimal("199.90"), "BRL", PaymentMethod.CREDIT_CARD, CLOCK.instant());
    }

    private static CreatePaymentRequest validRequest() {
        return new CreatePaymentRequest("merchant-1", "customer-1",
                new BigDecimal("199.90"), "BRL", PaymentMethod.CREDIT_CARD);
    }

    private String json(Object body) throws Exception {
        return objectMapper.writeValueAsString(body);
    }

    @Test
    @DisplayName("an approved payment answers 201 with a Location header")
    void createsApprovedPayment() throws Exception {
        Payment payment = pending();
        payment.approve("psp-tx-1", "AUTH123", CLOCK.instant());
        when(paymentService.pay(any())).thenReturn(new PaymentResult(payment, false));

        mockMvc.perform(post("/api/v1/payments")
                        .header("Idempotency-Key", "idem-1")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(json(validRequest())))
                .andExpect(status().isCreated())
                .andExpect(header().string("Location",
                        "http://localhost/api/v1/payments/" + payment.getId()))
                .andExpect(jsonPath("$.status").value("APPROVED"))
                .andExpect(jsonPath("$.authorizationCode").value("AUTH123"))
                .andExpect(jsonPath("$.pspTransactionId").value("psp-tx-1"))
                .andExpect(jsonPath("$.amount").value(199.90));
    }

    @Test
    @DisplayName("a decline is still a successful request: 201 with status DECLINED")
    void createsDeclinedPayment() throws Exception {
        Payment payment = pending();
        payment.decline("psp-tx-2", "INSUFFICIENT_FUNDS", CLOCK.instant());
        when(paymentService.pay(any())).thenReturn(new PaymentResult(payment, false));

        mockMvc.perform(post("/api/v1/payments")
                        .header("Idempotency-Key", "idem-1")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(json(validRequest())))
                .andExpect(status().isCreated())
                .andExpect(jsonPath("$.status").value("DECLINED"))
                .andExpect(jsonPath("$.failureReason").value("INSUFFICIENT_FUNDS"))
                .andExpect(jsonPath("$.authorizationCode").doesNotExist());
    }

    @Test
    @DisplayName("an unknown PSP outcome answers 502 but still returns the stored payment")
    void reportsBadGatewayOnFailure() throws Exception {
        Payment payment = pending();
        payment.fail("PSP unavailable: read timed out", CLOCK.instant());
        when(paymentService.pay(any())).thenReturn(new PaymentResult(payment, false));

        mockMvc.perform(post("/api/v1/payments")
                        .header("Idempotency-Key", "idem-1")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(json(validRequest())))
                .andExpect(status().isBadGateway())
                .andExpect(jsonPath("$.id").value(payment.getId().toString()))
                .andExpect(jsonPath("$.status").value("FAILED"));
    }

    @Test
    @DisplayName("a replayed idempotency key answers 200, not 201")
    void replayAnswersOk() throws Exception {
        Payment payment = pending();
        payment.approve("psp-tx-1", "AUTH123", CLOCK.instant());
        when(paymentService.pay(any())).thenReturn(new PaymentResult(payment, true));

        mockMvc.perform(post("/api/v1/payments")
                        .header("Idempotency-Key", "idem-1")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(json(validRequest())))
                .andExpect(status().isOk())
                .andExpect(header().doesNotExist("Location"))
                .andExpect(jsonPath("$.status").value("APPROVED"));
    }

    @Test
    @DisplayName("the Idempotency-Key header is required")
    void rejectsMissingIdempotencyKey() throws Exception {
        mockMvc.perform(post("/api/v1/payments")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(json(validRequest())))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.error").value("missing_header"));

        verify(paymentService, never()).pay(any());
    }

    @Test
    @DisplayName("a non-positive amount is rejected before any money moves")
    void rejectsNonPositiveAmount() throws Exception {
        var request = new CreatePaymentRequest("merchant-1", "customer-1",
                new BigDecimal("0.00"), "BRL", PaymentMethod.CREDIT_CARD);

        mockMvc.perform(post("/api/v1/payments")
                        .header("Idempotency-Key", "idem-1")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(json(request)))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.error").value("validation_failed"))
                .andExpect(jsonPath("$.violations[0].field").value("amount"));

        verify(paymentService, never()).pay(any());
    }

    @Test
    @DisplayName("a malformed currency is rejected")
    void rejectsInvalidCurrency() throws Exception {
        var request = new CreatePaymentRequest("merchant-1", "customer-1",
                new BigDecimal("10.00"), "brl", PaymentMethod.CREDIT_CARD);

        mockMvc.perform(post("/api/v1/payments")
                        .header("Idempotency-Key", "idem-1")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(json(request)))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.violations[0].field").value("currency"));
    }

    @Test
    @DisplayName("a blank merchant is rejected")
    void rejectsBlankMerchant() throws Exception {
        var request = new CreatePaymentRequest("  ", "customer-1",
                new BigDecimal("10.00"), "BRL", PaymentMethod.CREDIT_CARD);

        mockMvc.perform(post("/api/v1/payments")
                        .header("Idempotency-Key", "idem-1")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(json(request)))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.violations[0].field").value("merchantId"));
    }

    @Test
    @DisplayName("a malformed body answers 400, not 500")
    void rejectsMalformedBody() throws Exception {
        mockMvc.perform(post("/api/v1/payments")
                        .header("Idempotency-Key", "idem-1")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{ not json"))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.error").value("malformed_request"));
    }

    @Test
    @DisplayName("the command carries the header key through to the service")
    void passesIdempotencyKeyToService() throws Exception {
        Payment payment = pending();
        payment.approve("psp-tx-1", "AUTH123", CLOCK.instant());
        when(paymentService.pay(any())).thenReturn(new PaymentResult(payment, false));

        mockMvc.perform(post("/api/v1/payments")
                        .header("Idempotency-Key", "idem-42")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(json(validRequest())))
                .andExpect(status().isCreated());

        verify(paymentService).pay(new PaymentCommand("idem-42", "merchant-1", "customer-1",
                new BigDecimal("199.90"), "BRL", PaymentMethod.CREDIT_CARD));
    }

    @Test
    @DisplayName("fetching a known payment returns it")
    void getsPayment() throws Exception {
        Payment payment = pending();
        payment.approve("psp-tx-1", "AUTH123", CLOCK.instant());
        when(paymentService.findById(payment.getId())).thenReturn(Optional.of(payment));

        mockMvc.perform(get("/api/v1/payments/{id}", payment.getId()))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.id").value(payment.getId().toString()))
                .andExpect(jsonPath("$.status").value("APPROVED"));
    }

    @Test
    @DisplayName("fetching an unknown payment returns 404")
    void getsMissingPayment() throws Exception {
        UUID id = UUID.randomUUID();
        when(paymentService.findById(id)).thenReturn(Optional.empty());

        mockMvc.perform(get("/api/v1/payments/{id}", id))
                .andExpect(status().isNotFound());
    }

    @Test
    @DisplayName("a non-UUID path variable answers 400")
    void rejectsMalformedId() throws Exception {
        mockMvc.perform(get("/api/v1/payments/{id}", "not-a-uuid"))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.error").value("invalid_parameter"));
    }
}
