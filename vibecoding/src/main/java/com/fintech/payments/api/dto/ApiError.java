package com.fintech.payments.api.dto;

import com.fasterxml.jackson.annotation.JsonInclude;

import java.time.Instant;
import java.util.List;

@JsonInclude(JsonInclude.Include.NON_NULL)
public record ApiError(
        int status,
        String error,
        String message,
        List<FieldViolation> violations,
        Instant timestamp) {

    public record FieldViolation(String field, String message) {
    }

    public static ApiError of(int status, String error, String message) {
        return new ApiError(status, error, message, null, Instant.now());
    }

    public static ApiError of(int status, String error, String message,
                              List<FieldViolation> violations) {
        return new ApiError(status, error, message, violations, Instant.now());
    }
}
