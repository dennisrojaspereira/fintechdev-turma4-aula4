package com.fintech.payments.api;

import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import io.micrometer.tracing.Span;
import io.micrometer.tracing.Tracer;
import org.slf4j.MDC;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;
import org.springframework.web.filter.OncePerRequestFilter;

import java.io.IOException;
import java.util.UUID;

/**
 * Accepts or generates {@code X-Correlation-Id}, exposes it to logs (MDC), to the controller
 * (request attribute) and echoes it back on the response. The same value is forwarded to the PSP
 * and carried on the Kafka event, so one logical attempt can be traced end to end.
 *
 * <p>Ordered right after Spring Boot's server observation filter ({@code HIGHEST_PRECEDENCE + 1})
 * so the request span already exists and gets the correlationId as a tag: the API trace and the
 * worker trace of one payment share it, even though Debezium breaks trace propagation.
 */
@Component
@Order(Ordered.HIGHEST_PRECEDENCE + 2)
public class CorrelationIdFilter extends OncePerRequestFilter {

    public static final String SPAN_TAG = "correlationId";

    private final ObjectProvider<Tracer> tracer;

    public CorrelationIdFilter(ObjectProvider<Tracer> tracer) {
        this.tracer = tracer;
    }

    public static final String HEADER = "X-Correlation-Id";
    public static final String MDC_KEY = "correlationId";
    public static final String ATTRIBUTE = CorrelationIdFilter.class.getName() + ".correlationId";

    private static final int MAX_LENGTH = 64;

    @Override
    protected void doFilterInternal(HttpServletRequest request, HttpServletResponse response,
                                    FilterChain chain) throws ServletException, IOException {
        String correlationId = sanitize(request.getHeader(HEADER));
        MDC.put(MDC_KEY, correlationId);
        request.setAttribute(ATTRIBUTE, correlationId);
        response.setHeader(HEADER, correlationId);
        tagCurrentSpan(correlationId);
        try {
            chain.doFilter(request, response);
        } finally {
            MDC.remove(MDC_KEY);
        }
    }

    private void tagCurrentSpan(String correlationId) {
        Tracer t = tracer.getIfAvailable();
        Span span = t == null ? null : t.currentSpan();
        if (span != null) {
            span.tag(SPAN_TAG, correlationId);
        }
    }

    static String sanitize(String header) {
        if (header == null || header.isBlank()) {
            return UUID.randomUUID().toString();
        }
        String trimmed = header.strip().replaceAll("[^A-Za-z0-9_\\-.:]", "");
        if (trimmed.isEmpty()) {
            return UUID.randomUUID().toString();
        }
        return trimmed.length() <= MAX_LENGTH ? trimmed : trimmed.substring(0, MAX_LENGTH);
    }
}
