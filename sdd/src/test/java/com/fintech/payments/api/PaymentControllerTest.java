package com.fintech.payments.api;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fintech.payments.api.dto.CreatePaymentRequest;
import com.fintech.payments.domain.Payment;
import com.fintech.payments.domain.PaymentMethod;
import com.fintech.payments.service.IdempotencyKeyConflictException;
import com.fintech.payments.service.PaymentCommand;
import com.fintech.payments.service.PaymentResult;
import com.fintech.payments.service.PaymentService;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.Optional;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.header;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/** The public HTTP contract (ADR-002). */
@WebMvcTest(controllers = PaymentController.class)
class PaymentControllerTest {

    private static final Instant NOW = Instant.parse("2026-09-03T12:00:00Z");

    @Autowired
    private MockMvc mockMvc;

    @Autowired
    private ObjectMapper objectMapper;

    @MockBean
    private PaymentService paymentService;

    private static Payment pending() {
        return Payment.pending("idem-1", "fp", "corr-1", "merchant-1", "customer-1",
                new BigDecimal("199.90"), "BRL", PaymentMethod.CREDIT_CARD, NOW);
    }

    private static CreatePaymentRequest validRequest() {
        return new CreatePaymentRequest("merchant-1", "customer-1",
                new BigDecimal("199.90"), "BRL", PaymentMethod.CREDIT_CARD);
    }

    private MvcResult postPayment(String idempotencyKey, Object body, String correlationId) throws Exception {
        var request = post("/api/v1/payments")
                .contentType(MediaType.APPLICATION_JSON)
                .content(objectMapper.writeValueAsString(body));
        if (idempotencyKey != null) {
            request.header("Idempotency-Key", idempotencyKey);
        }
        if (correlationId != null) {
            request.header("X-Correlation-Id", correlationId);
        }
        return mockMvc.perform(request).andReturn();
    }

    @Test
    @DisplayName("201 Created with Location when the PSP approved")
    void createdWhenApproved() throws Exception {
        Payment payment = pending();
        payment.approve("psp-tx-1", "AUTH", NOW);
        when(paymentService.pay(any())).thenReturn(new PaymentResult(payment, false));

        mockMvc.perform(post("/api/v1/payments")
                        .header("Idempotency-Key", "idem-1")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(validRequest())))
                .andExpect(status().isCreated())
                .andExpect(header().string("Location", "http://localhost/api/v1/payments/" + payment.getId()))
                .andExpect(jsonPath("$.id").value(payment.getId().toString()))
                .andExpect(jsonPath("$.status").value("APPROVED"))
                .andExpect(jsonPath("$.authorizationCode").value("AUTH"))
                .andExpect(jsonPath("$.provider").value("CARD_PSP"))
                .andExpect(jsonPath("$.correlationId").value("corr-1"));
    }

    @Test
    @DisplayName("201 Created with status FAILED when the PSP rejected the request")
    void createdWhenFailed() throws Exception {
        Payment payment = pending();
        payment.fail("PSP rejected: 400", NOW);
        when(paymentService.pay(any())).thenReturn(new PaymentResult(payment, false));

        mockMvc.perform(post("/api/v1/payments")
                        .header("Idempotency-Key", "idem-1")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(validRequest())))
                .andExpect(status().isCreated())
                .andExpect(jsonPath("$.status").value("FAILED"))
                .andExpect(jsonPath("$.failureReason").value("PSP rejected: 400"));
    }

    @Test
    @DisplayName("202 Accepted with a stable id when the PSP outcome is UNKNOWN")
    void acceptedWhenUnknown() throws Exception {
        Payment payment = pending();
        payment.markUnknown("PSP outcome unknown: READ_TIMEOUT", NOW);
        when(paymentService.pay(any())).thenReturn(new PaymentResult(payment, false));

        mockMvc.perform(post("/api/v1/payments")
                        .header("Idempotency-Key", "idem-1")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(validRequest())))
                .andExpect(status().isAccepted())
                .andExpect(header().exists("Location"))
                .andExpect(jsonPath("$.id").value(payment.getId().toString()))
                .andExpect(jsonPath("$.status").value("UNKNOWN"));
    }

    @Test
    @DisplayName("200 OK without Location when the idempotency key is replayed")
    void okWhenReplayed() throws Exception {
        Payment payment = pending();
        payment.approve("psp-tx-1", "AUTH", NOW);
        when(paymentService.pay(any())).thenReturn(new PaymentResult(payment, true));

        mockMvc.perform(post("/api/v1/payments")
                        .header("Idempotency-Key", "idem-1")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(validRequest())))
                .andExpect(status().isOk())
                .andExpect(header().doesNotExist("Location"))
                .andExpect(jsonPath("$.id").value(payment.getId().toString()));
    }

    @Test
    @DisplayName("422 when the idempotency key is reused with a different body")
    void unprocessableOnConflict() throws Exception {
        when(paymentService.pay(any()))
                .thenThrow(new IdempotencyKeyConflictException("idem-1", UUID.randomUUID()));

        mockMvc.perform(post("/api/v1/payments")
                        .header("Idempotency-Key", "idem-1")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(validRequest())))
                .andExpect(status().isUnprocessableEntity())
                .andExpect(jsonPath("$.error").value("idempotency_key_conflict"));
    }

    @Test
    @DisplayName("400 when the Idempotency-Key header is missing; nothing is charged")
    void badRequestWithoutIdempotencyKey() throws Exception {
        mockMvc.perform(post("/api/v1/payments")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(validRequest())))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.error").value("missing_header"));

        verify(paymentService, never()).pay(any());
    }

    @Test
    @DisplayName("400 with field violations for an invalid body; nothing is charged")
    void badRequestOnInvalidBody() throws Exception {
        var invalid = new CreatePaymentRequest("", "customer-1",
                new BigDecimal("-1"), "brl", PaymentMethod.PIX);

        mockMvc.perform(post("/api/v1/payments")
                        .header("Idempotency-Key", "idem-1")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(invalid)))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.error").value("validation_failed"))
                .andExpect(jsonPath("$.violations.length()").value(3));

        verify(paymentService, never()).pay(any());
    }

    @Test
    @DisplayName("the correlation id from the request is echoed back and passed to the service")
    void correlationIdIsPropagated() throws Exception {
        Payment payment = pending();
        payment.approve("psp-tx-1", "AUTH", NOW);
        when(paymentService.pay(any())).thenReturn(new PaymentResult(payment, false));

        MvcResult result = postPayment("idem-1", validRequest(), "trace-abc-123");

        assertThat(result.getResponse().getHeader("X-Correlation-Id")).isEqualTo("trace-abc-123");
        ArgumentCaptor<PaymentCommand> captor = ArgumentCaptor.forClass(PaymentCommand.class);
        verify(paymentService).pay(captor.capture());
        assertThat(captor.getValue().correlationId()).isEqualTo("trace-abc-123");
        assertThat(captor.getValue().idempotencyKey()).isEqualTo("idem-1");
    }

    @Test
    @DisplayName("a correlation id is generated when the client sends none")
    void correlationIdIsGenerated() throws Exception {
        Payment payment = pending();
        payment.approve("psp-tx-1", "AUTH", NOW);
        when(paymentService.pay(any())).thenReturn(new PaymentResult(payment, false));

        MvcResult result = postPayment("idem-1", validRequest(), null);

        String generated = result.getResponse().getHeader("X-Correlation-Id");
        assertThat(generated).isNotBlank();
        assertThat(UUID.fromString(generated)).isNotNull();
    }

    @Test
    @DisplayName("GET returns 200 for a known payment and 404 otherwise")
    void getPayment() throws Exception {
        Payment payment = pending();
        when(paymentService.findById(payment.getId())).thenReturn(Optional.of(payment));
        when(paymentService.findById(any())).thenReturn(Optional.empty());
        when(paymentService.findById(payment.getId())).thenReturn(Optional.of(payment));

        mockMvc.perform(get("/api/v1/payments/{id}", payment.getId()))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.status").value("PENDING"));

        mockMvc.perform(get("/api/v1/payments/{id}", UUID.randomUUID()))
                .andExpect(status().isNotFound());

        mockMvc.perform(get("/api/v1/payments/{id}", "not-a-uuid"))
                .andExpect(status().isBadRequest());
    }
}
