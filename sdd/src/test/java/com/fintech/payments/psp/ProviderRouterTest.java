package com.fintech.payments.psp;

import com.fintech.payments.domain.PaymentMethod;
import com.fintech.payments.domain.PaymentProvider;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** ADR-004 D4: exactly one client per provider, checked when the application starts. */
class ProviderRouterTest {

    private static PspClient clientFor(PaymentProvider provider) {
        return new PspClient() {
            @Override
            public PaymentProvider provider() {
                return provider;
            }

            @Override
            public PspChargeResponse charge(PspChargeRequest request) {
                throw new UnsupportedOperationException("not exercised");
            }
        };
    }

    private final PspClient card = clientFor(PaymentProvider.CARD_PSP);
    private final PspClient pix = clientFor(PaymentProvider.PIX_PROVIDER);

    @Test
    @DisplayName("each provider is routed to its own client")
    void routesEachProviderToItsClient() {
        ProviderRouter router = new ProviderRouter(List.of(pix, card));

        assertThat(router.clientFor(PaymentProvider.CARD_PSP)).isSameAs(card);
        assertThat(router.clientFor(PaymentProvider.PIX_PROVIDER)).isSameAs(pix);
    }

    @ParameterizedTest
    @EnumSource(PaymentMethod.class)
    @DisplayName("every payment method ends up at a client for its provider")
    void everyPaymentMethodHasADestination(PaymentMethod method) {
        ProviderRouter router = new ProviderRouter(List.of(card, pix));

        assertThat(router.clientFor(method.provider()).provider()).isEqualTo(method.provider());
    }

    @Test
    @DisplayName("a provider without a client fails at startup, naming the orphaned payment methods")
    void missingClientFailsAtStartup() {
        assertThatThrownBy(() -> new ProviderRouter(List.of(card)))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("PIX_PROVIDER")
                .hasMessageContaining("PIX");
    }

    @Test
    @DisplayName("two clients for the same provider fail at startup")
    void duplicateClientFailsAtStartup() {
        assertThatThrownBy(() -> new ProviderRouter(List.of(card, pix, clientFor(PaymentProvider.CARD_PSP))))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Two PspClients claim provider CARD_PSP");
    }
}
