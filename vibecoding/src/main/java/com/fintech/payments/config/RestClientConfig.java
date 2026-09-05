package com.fintech.payments.config;

import com.fintech.payments.psp.PspProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.client.ClientHttpRequestFactory;
import org.springframework.http.client.SimpleClientHttpRequestFactory;
import org.springframework.web.client.RestClient;

@Configuration
public class RestClientConfig {

    /**
     * Dedicated client for the PSP. Both timeouts are mandatory: without a read timeout a stalled
     * PSP would pin request threads until the container runs out of them.
     */
    @Bean
    public RestClient pspRestClient(RestClient.Builder builder, PspProperties properties) {
        SimpleClientHttpRequestFactory factory = new SimpleClientHttpRequestFactory();
        factory.setConnectTimeout((int) properties.connectTimeout().toMillis());
        factory.setReadTimeout((int) properties.readTimeout().toMillis());

        return builder
                .baseUrl(properties.baseUrl())
                .requestFactory((ClientHttpRequestFactory) factory)
                .defaultHeader("Authorization", "Bearer " + properties.apiKey())
                .build();
    }
}
