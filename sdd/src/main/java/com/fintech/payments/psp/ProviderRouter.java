package com.fintech.payments.psp;

import com.fintech.payments.domain.PaymentMethod;
import com.fintech.payments.domain.PaymentProvider;
import org.springframework.stereotype.Component;

import java.util.Arrays;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;

/**
 * Finds the {@link PspClient} for a {@link PaymentProvider} (ADR-004, D4).
 *
 * <p>Fails at startup, not at the first payment, if any provider has no client or two clients:
 * a payment method must never be left without a destination, and never have two.
 */
@Component
public class ProviderRouter {

    private final Map<PaymentProvider, PspClient> clients = new EnumMap<>(PaymentProvider.class);

    public ProviderRouter(List<PspClient> available) {
        for (PspClient client : available) {
            PspClient previous = clients.putIfAbsent(client.provider(), client);
            if (previous != null) {
                throw new IllegalStateException("Two PspClients claim provider " + client.provider()
                        + ": " + previous.getClass().getSimpleName() + " and "
                        + client.getClass().getSimpleName());
            }
        }
        for (PaymentProvider provider : PaymentProvider.values()) {
            if (!clients.containsKey(provider)) {
                throw new IllegalStateException("No PspClient for provider " + provider
                        + " (payment methods " + methodsOf(provider) + " would have no destination)");
            }
        }
    }

    /** Never null: every provider has exactly one client, checked in the constructor. */
    public PspClient clientFor(PaymentProvider provider) {
        return clients.get(provider);
    }

    private static List<PaymentMethod> methodsOf(PaymentProvider provider) {
        return Arrays.stream(PaymentMethod.values())
                .filter(method -> method.provider() == provider)
                .toList();
    }
}
