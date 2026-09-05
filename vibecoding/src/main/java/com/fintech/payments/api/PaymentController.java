package com.fintech.payments.api;

import com.fintech.payments.api.dto.CreatePaymentRequest;
import com.fintech.payments.api.dto.PaymentResponse;
import com.fintech.payments.domain.Payment;
import com.fintech.payments.domain.PaymentStatus;
import com.fintech.payments.service.PaymentCommand;
import com.fintech.payments.service.PaymentResult;
import com.fintech.payments.service.PaymentService;
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

@RestController
@RequestMapping("/api/v1/payments")
@Validated
public class PaymentController {

    private final PaymentService paymentService;

    public PaymentController(PaymentService paymentService) {
        this.paymentService = paymentService;
    }

    /**
     * Creates a payment.
     *
     * <p>The {@code Idempotency-Key} header is mandatory: it is what makes a client retry safe,
     * both against this API and against the PSP.
     *
     * <ul>
     *   <li>201 — authorized or declined by the PSP (check {@code status})</li>
     *   <li>200 — the idempotency key was already used; the original payment is returned</li>
     *   <li>502 — the PSP outcome is unknown; the payment is stored as FAILED for reconciliation</li>
     * </ul>
     */
    @PostMapping
    public ResponseEntity<PaymentResponse> create(
            @RequestHeader("Idempotency-Key") @NotBlank @Size(max = 100) String idempotencyKey,
            @Valid @RequestBody CreatePaymentRequest request,
            UriComponentsBuilder uriBuilder) {

        PaymentResult result = paymentService.pay(new PaymentCommand(
                idempotencyKey,
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
        if (payment.getStatus() == PaymentStatus.FAILED) {
            return ResponseEntity.status(HttpStatus.BAD_GATEWAY).body(body);
        }

        URI location = uriBuilder.path("/api/v1/payments/{id}")
                .buildAndExpand(payment.getId())
                .toUri();
        return ResponseEntity.created(location).body(body);
    }

    @GetMapping("/{id}")
    public ResponseEntity<PaymentResponse> get(@PathVariable UUID id) {
        return paymentService.findById(id)
                .map(PaymentResponse::from)
                .map(ResponseEntity::ok)
                .orElseGet(() -> ResponseEntity.notFound().build());
    }
}
