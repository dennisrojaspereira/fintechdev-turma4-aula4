package com.fintech.payments.config;

import com.fintech.payments.psp.PixProperties;
import com.fintech.payments.psp.ProviderSettings;
import com.fintech.payments.psp.PspProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.client.JdkClientHttpRequestFactory;
import org.springframework.web.client.RestClient;

import java.net.http.HttpClient;

/**
 * One dedicated {@link RestClient} per synchronous provider, all built the same way (see
 * {@link #build}). Beans are named after the provider so each client picks its own by qualifier.
 */
@Configuration
public class RestClientConfig {

    /** Card PSP (SPEC-001). */
    @Bean
    public RestClient pspRestClient(RestClient.Builder builder, PspProperties properties) {
        return build(builder, properties);
    }

    /** PIX provider (SPEC-002). */
    @Bean
    public RestClient pixRestClient(RestClient.Builder builder, PixProperties properties) {
        return build(builder, properties);
    }

    /**
     * On the JDK HttpClient because it distinguishes a connect timeout
     * ({@code HttpConnectTimeoutException}) from a read timeout, which the retry policy depends
     * on. Redirects are never followed: a redirect is not a processed charge.
     */
    public static RestClient build(RestClient.Builder builder, ProviderSettings settings) {
        HttpClient httpClient = HttpClient.newBuilder()
                .connectTimeout(settings.connectTimeout())
                .followRedirects(HttpClient.Redirect.NEVER)
                .version(HttpClient.Version.HTTP_1_1)
                .build();

        JdkClientHttpRequestFactory factory = new JdkClientHttpRequestFactory(httpClient);
        factory.setReadTimeout(settings.readTimeout());

        return builder
                .baseUrl(settings.baseUrl())
                .requestFactory(factory)
                .defaultHeader("Authorization", "Bearer " + settings.apiKey())
                .build();
    }
}
