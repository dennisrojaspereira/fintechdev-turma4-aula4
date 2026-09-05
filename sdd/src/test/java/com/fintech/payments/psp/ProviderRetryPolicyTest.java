package com.fintech.payments.psp;

import com.fintech.payments.domain.PaymentProvider;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.math.BigDecimal;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The retry policy shared by every provider client, as a test of the GUARDRAILS and RULES:
 * retry ONLY on connect timeout / too many redirects, never anything else, at most 3 attempts,
 * exponential backoff, definitive rejections propagate untouched.
 */
class ProviderRetryPolicyTest {

    private final PspChargeRequest request = new PspChargeRequest(
            "idem-1", "corr-1", "merchant-1", "customer-1", new BigDecimal("10.00"), "BRL", "PIX");

    private final ProviderRetryPolicy policy =
            new ProviderRetryPolicy(PaymentProvider.PIX_PROVIDER, 3, Duration.ofMillis(10));

    private final List<Integer> attemptsSeen = new ArrayList<>();

    private ProviderRetryPolicy.Attempt<String> alwaysFailingWith(PspFailureKind kind) {
        return attempt -> {
            attemptsSeen.add(attempt);
            throw new AttemptFailure(kind, kind + " simulated", null);
        };
    }

    @ParameterizedTest
    @EnumSource(value = PspFailureKind.class, names = {"CONNECT_TIMEOUT", "TOO_MANY_REDIRECTS"})
    @DisplayName("a retryable failure is retried up to 3 times, then the outcome is unknown")
    void retryableKindIsRetriedUpToThreeTimes(PspFailureKind kind) {
        assertThatThrownBy(() -> policy.execute(request, alwaysFailingWith(kind)))
                .isInstanceOf(PspOutcomeUnknownException.class)
                .satisfies(e -> {
                    var unknown = (PspOutcomeUnknownException) e;
                    assertThat(unknown.kind()).isEqualTo(kind);
                    assertThat(unknown.attempts()).isEqualTo(3);
                })
                .hasMessageContaining("PIX_PROVIDER");

        assertThat(attemptsSeen).containsExactly(1, 2, 3);
    }

    @ParameterizedTest
    @EnumSource(value = PspFailureKind.class,
            names = {"READ_TIMEOUT", "SERVER_ERROR", "TRANSPORT_ERROR", "MALFORMED_RESPONSE"})
    @DisplayName("GUARDRAIL: any other failure is NEVER retried, the outcome is unknown after one attempt")
    void nonRetryableKindIsNeverRetried(PspFailureKind kind) {
        assertThatThrownBy(() -> policy.execute(request, alwaysFailingWith(kind)))
                .isInstanceOf(PspOutcomeUnknownException.class)
                .satisfies(e -> {
                    var unknown = (PspOutcomeUnknownException) e;
                    assertThat(unknown.kind()).isEqualTo(kind);
                    assertThat(unknown.attempts()).isEqualTo(1);
                });

        assertThat(attemptsSeen).containsExactly(1);
    }

    @Test
    @DisplayName("a definitive rejection (4xx) propagates as is, without any retry")
    void rejectionPropagatesWithoutRetry() {
        var rejection = new PspRejectedException(400, "invalid merchant");

        assertThatThrownBy(() -> policy.execute(request, attempt -> {
            attemptsSeen.add(attempt);
            throw rejection;
        })).isSameAs(rejection);

        assertThat(attemptsSeen).containsExactly(1);
    }

    @Test
    @DisplayName("a retryable failure followed by an answer succeeds on the second attempt")
    void succeedsOnSecondAttempt() {
        String result = policy.execute(request, attempt -> {
            attemptsSeen.add(attempt);
            if (attempt == 1) {
                throw new AttemptFailure(PspFailureKind.CONNECT_TIMEOUT, "connect timed out", null);
            }
            return "answered";
        });

        assertThat(result).isEqualTo("answered");
        assertThat(attemptsSeen).containsExactly(1, 2);
    }

    @Test
    @DisplayName("RULE: backoff between attempts is exponential")
    void backoffIsExponential() {
        long start = System.nanoTime();
        assertThatThrownBy(() -> policy.execute(request, alwaysFailingWith(PspFailureKind.CONNECT_TIMEOUT)))
                .isInstanceOf(PspOutcomeUnknownException.class);
        long elapsedMs = (System.nanoTime() - start) / 1_000_000;

        // 10ms after the first failure, 20ms after the second.
        assertThat(elapsedMs).isGreaterThanOrEqualTo(30);
    }

    @Test
    @DisplayName("RULE: the policy never exceeds 3 attempts, whatever is configured")
    void capsAttemptsAtThree() {
        ProviderRetryPolicy generous =
                new ProviderRetryPolicy(PaymentProvider.CARD_PSP, 10, Duration.ofMillis(1));

        assertThat(generous.maxAttempts()).isEqualTo(ProviderSettings.MAX_ATTEMPTS_ALLOWED);
        assertThatThrownBy(() -> generous.execute(request, alwaysFailingWith(PspFailureKind.CONNECT_TIMEOUT)))
                .isInstanceOf(PspOutcomeUnknownException.class);
        assertThat(attemptsSeen).hasSize(3);
    }

    @Test
    @DisplayName("the policy carries the provider it protects")
    void knowsItsProvider() {
        assertThat(policy.provider()).isEqualTo(PaymentProvider.PIX_PROVIDER);
    }
}
