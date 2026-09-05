package com.fintech.payments.service;

import java.util.UUID;

/**
 * Identity of the consumed event that a database transaction must record in the inbox
 * ({@code processed_events}) together with its business effect, so a redelivery of the same
 * event is recognised.
 */
public record InboxEntry(UUID eventId, String eventType, String aggregateId, String correlationId) {
}
