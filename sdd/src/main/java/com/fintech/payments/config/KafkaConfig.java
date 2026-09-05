package com.fintech.payments.config;

import com.fintech.payments.messaging.PoisonEventException;
import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.listener.CommonErrorHandler;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.FixedBackOff;

@Configuration
public class KafkaConfig {

    @Bean
    NewTopic paymentCompletedTopic(PaymentsProperties properties) {
        return TopicBuilder.name(properties.topics().paymentCompleted())
                .partitions(6)
                .replicas(1)
                .build();
    }

    /**
     * A consumer that produces a financial effect must never silently drop an event: transient
     * failures (database down) are retried until they succeed, blocking the partition. Only an
     * event that can never be processed (unparseable) is skipped, and logged at ERROR.
     */
    @Bean
    CommonErrorHandler kafkaErrorHandler() {
        DefaultErrorHandler handler =
                new DefaultErrorHandler(new FixedBackOff(1000L, FixedBackOff.UNLIMITED_ATTEMPTS));
        handler.addNotRetryableExceptions(PoisonEventException.class);
        return handler;
    }
}
