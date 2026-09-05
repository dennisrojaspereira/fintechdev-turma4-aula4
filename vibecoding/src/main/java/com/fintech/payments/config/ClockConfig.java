package com.fintech.payments.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.time.Clock;

@Configuration
public class ClockConfig {

    /** Injected everywhere instead of Instant.now(), so time can be pinned in tests. */
    @Bean
    Clock clock() {
        return Clock.systemUTC();
    }
}
