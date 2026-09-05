package com.fintech.payments.messaging;

import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;

import java.nio.charset.StandardCharsets;

/** Names of the custom record headers this service writes and reads. */
public final class KafkaHeaders {

    public static final String EVENT_ID = "eventId";
    public static final String EVENT_TYPE = "eventType";
    public static final String CORRELATION_ID = "correlationId";

    private KafkaHeaders() {
    }

    public static String read(Headers headers, String name) {
        Header header = headers.lastHeader(name);
        return header == null ? null : new String(header.value(), StandardCharsets.UTF_8);
    }
}
