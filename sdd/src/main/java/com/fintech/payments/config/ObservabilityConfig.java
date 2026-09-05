package com.fintech.payments.config;

import io.micrometer.observation.ObservationPredicate;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.server.observation.ServerRequestObservationContext;

/**
 * Observability harness tuning. Metrics come from Micrometer (Prometheus), traces from
 * Micrometer Tracing + OpenTelemetry (Tempo); see {@code docs/observability.md}.
 */
@Configuration
public class ObservabilityConfig {

    /**
     * Prometheus scrapes {@code /actuator/prometheus} every 5s and Docker polls the health
     * endpoint: those requests must not become traces (nor {@code http_server_requests} series),
     * or they drown the payments in Tempo.
     */
    @Bean
    ObservationPredicate noActuatorObservations() {
        return (name, context) -> !(context instanceof ServerRequestObservationContext http
                && http.getCarrier().getRequestURI().startsWith("/actuator"));
    }
}
