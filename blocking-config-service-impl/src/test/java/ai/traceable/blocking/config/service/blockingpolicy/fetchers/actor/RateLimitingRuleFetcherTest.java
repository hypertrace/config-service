package ai.traceable.blocking.config.service.blockingpolicy.fetchers.actor;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

import ai.traceable.ratelimiting.config.service.v2.Category;
import ai.traceable.ratelimiting.config.service.v2.EnvironmentScope;
import ai.traceable.ratelimiting.config.service.v2.GetRateLimitingRulesFilter;
import ai.traceable.ratelimiting.config.service.v2.GetRateLimitingRulesRequest;
import ai.traceable.ratelimiting.config.service.v2.GetRateLimitingRulesResponse;
import ai.traceable.ratelimiting.config.service.v2.RateLimitingConfigServiceGrpc.RateLimitingConfigServiceBlockingStub;
import ai.traceable.ratelimiting.config.service.v2.RateLimitingRule;
import ai.traceable.ratelimiting.config.service.v2.RateLimitingRuleData;
import ai.traceable.ratelimiting.config.service.v2.RuleConfigScope;
import java.util.Optional;
import java.util.Set;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class RateLimitingRuleFetcherTest {
  private static final String TENANT_ID = "tenant-id";
  private static final String ENVIRONMENT_ID = "environment-id";
  private static final RequestContext REQUEST_CONTEXT = RequestContext.forTenantId(TENANT_ID);

  private RateLimitingConfigServiceBlockingStub rateLimitingBlockingStub;
  private RateLimitingRuleFetcher rateLimitingRuleFetcher;

  @BeforeEach
  void setUp() {
    rateLimitingBlockingStub = mock(RateLimitingConfigServiceBlockingStub.class);
    rateLimitingRuleFetcher = new RateLimitingRuleFetcher(rateLimitingBlockingStub);
  }

  @Test
  void getCustomIpBasedRulesTestWithoutEnvironment() {
    doReturn(sampleResponse)
        .when(rateLimitingBlockingStub)
        .getRateLimitingRules(
            GetRateLimitingRulesRequest.newBuilder()
                .setRulesFilter(
                    GetRateLimitingRulesFilter.newBuilder()
                        .addCategories(Category.CATEGORY_RATE_LIMITING)
                        .setScope(
                            RuleConfigScope.newBuilder()
                                .setEnvironmentScope(EnvironmentScope.newBuilder())))
                .build());

    Set<String> activeRateLimitingRules =
        rateLimitingRuleFetcher.getRateLimitingRules(REQUEST_CONTEXT, Optional.empty());

    assertEquals(Set.of("rule-id-enabled-0", "rule-id-enabled-1"), activeRateLimitingRules);
  }

  @Test
  void getCustomIpBasedRulesTestWithEnvironment() {
    doReturn(sampleResponse)
        .when(rateLimitingBlockingStub)
        .getRateLimitingRules(
            GetRateLimitingRulesRequest.newBuilder()
                .setRulesFilter(
                    GetRateLimitingRulesFilter.newBuilder()
                        .addCategories(Category.CATEGORY_RATE_LIMITING)
                        .setScope(
                            RuleConfigScope.newBuilder()
                                .setEnvironmentScope(
                                    EnvironmentScope.newBuilder()
                                        .addEnvironmentIds(ENVIRONMENT_ID))))
                .build());

    Set<String> activeRateLimitingRules =
        rateLimitingRuleFetcher.getRateLimitingRules(REQUEST_CONTEXT, Optional.of(ENVIRONMENT_ID));

    assertEquals(Set.of("rule-id-enabled-0", "rule-id-enabled-1"), activeRateLimitingRules);
  }

  private static final GetRateLimitingRulesResponse sampleResponse =
      GetRateLimitingRulesResponse.newBuilder()
          .addRules(
              RateLimitingRule.newBuilder()
                  .setId("rule-id-enabled-0")
                  .setData(RateLimitingRuleData.newBuilder().setEnabled(true)))
          .addRules(
              RateLimitingRule.newBuilder()
                  .setId("rule-id-enabled-1")
                  .setData(RateLimitingRuleData.newBuilder().setEnabled(true)))
          .addRules(
              RateLimitingRule.newBuilder()
                  .setId("rule-id-not-enabled")
                  .setData(RateLimitingRuleData.newBuilder().setEnabled(false)))
          .build();
}
