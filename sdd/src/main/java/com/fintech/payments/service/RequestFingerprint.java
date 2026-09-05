package com.fintech.payments.service;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;

/**
 * SHA-256 of the business fields of a request. Two requests with the same Idempotency-Key are
 * the same logical attempt only if their fingerprints match; otherwise the key is being reused
 * for a different payment, which is rejected instead of silently replayed.
 */
public final class RequestFingerprint {

    private RequestFingerprint() {
    }

    public static String of(PaymentCommand command) {
        String canonical = String.join("|",
                command.merchantId(),
                command.customerId(),
                command.amount().stripTrailingZeros().toPlainString(),
                command.currency(),
                command.paymentMethod().name());
        try {
            byte[] digest = MessageDigest.getInstance("SHA-256")
                    .digest(canonical.getBytes(StandardCharsets.UTF_8));
            return HexFormat.of().formatHex(digest);
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 not available", e);
        }
    }
}
