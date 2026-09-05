package com.fintech.payments.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

@Configuration
public class KafkaConfig {

    @Bean
    NewTopic paymentCompletedTopic(PaymentsProperties properties) {
        return TopicBuilder.name(properties.topics().paymentCompleted())
                .partitions(6)
                .replicas(1)
                .build();
    }
}
