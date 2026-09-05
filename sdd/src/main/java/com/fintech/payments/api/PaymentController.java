package com.fintech.payments.api;

import com.fintech.payments.api.dto.CreatePaymentRequest;
import com.fintech.payments.api.dto.PaymentResponse;
import com.fintech.payments.domain.Payment;
import com.fintech.payments.service.PaymentCommand;
import com.fintech.payments.service.PaymentResult;
import com.fintech.payments.service.PaymentService;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Size;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.util.UriComponentsBuilder;

import java.net.URI;
import java.util.UUID;

/**
 * Public HTTP contract of SPEC-001 (ADR-002), made asynchronous by SPEC-003 (ADR-005 D7).
 *
 * <p>{@code POST /api/v1/payments} with a mandatory {@code Idempotency-Key} header:
 * <ul>
 *   <li>202 Accepted + Location: a new payment was recorded ({@code status: PENDING}) and will
 *       be processed by the worker; poll {@code GET} for the outcome. Also used for any other
 *       non-terminal state (UNKNOWN). The client must NOT retry with a new key.</li>
 *   <li>201 Created + Location: a new payment that is already terminal. Does not occur in the
 *       asynchronous flow; kept so the mapping stays total.</li>
 *   <li>200 OK: the key was already used with the same body; the existing payment is returned
 *       as is, whatever its state, and nothing was charged again.</li>
 *   <li>422: the key was already used with a different body.</li>
 *   <li>400: validation failure or missing header.</li>
 * </ul>
 */
@RestController
@RequestMapping("/api/v1/payments")
@Validated
public class PaymentController {

    private final PaymentService paymentService;

    public PaymentController(PaymentService paymentService) {
        this.paymentService = paymentService;
    }

    @PostMapping
    public ResponseEntity<PaymentResponse> create(
            @RequestHeader("Idempotency-Key") @NotBlank @Size(max = 100) String idempotencyKey,
            @Valid @RequestBody CreatePaymentRequest request,
            HttpServletRequest servletRequest,
            UriComponentsBuilder uriBuilder) {

        PaymentResult result = paymentService.pay(new PaymentCommand(
                idempotencyKey,
                correlationIdOf(servletRequest),
                request.merchantId(),
                request.customerId(),
                request.amount(),
                request.currency(),
                request.paymentMethod()));

        Payment payment = result.payment();
        PaymentResponse body = PaymentResponse.from(payment);

        if (result.replayed()) {
            return ResponseEntity.ok(body);
        }

        URI location = uriBuilder.path("/api/v1/payments/{id}")
                .buildAndExpand(payment.getId())
                .toUri();
        HttpStatus status = payment.getStatus().isTerminal()
                ? HttpStatus.CREATED
                : HttpStatus.ACCEPTED;
        return ResponseEntity.status(status).location(location).body(body);
    }

    @GetMapping("/{id}")
    public ResponseEntity<PaymentResponse> get(@PathVariable UUID id) {
        return paymentService.findById(id)
                .map(PaymentResponse::from)
                .map(ResponseEntity::ok)
                .orElseGet(() -> ResponseEntity.notFound().build());
    }

    private static String correlationIdOf(HttpServletRequest request) {
        Object attribute = request.getAttribute(CorrelationIdFilter.ATTRIBUTE);
        return attribute instanceof String s ? s : UUID.randomUUID().toString();
    }
}
