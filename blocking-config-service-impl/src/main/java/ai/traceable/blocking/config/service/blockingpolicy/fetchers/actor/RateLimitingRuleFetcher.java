package ai.traceable.blocking.config.service.blockingpolicy.fetchers.actor;

import ai.traceable.ratelimiting.config.service.v2.Category;
import ai.traceable.ratelimiting.config.service.v2.EnvironmentScope;
import ai.traceable.ratelimiting.config.service.v2.GetRateLimitingRulesFilter;
import ai.traceable.ratelimiting.config.service.v2.GetRateLimitingRulesRequest;
import ai.traceable.ratelimiting.config.service.v2.GetRateLimitingRulesResponse;
import ai.traceable.ratelimiting.config.service.v2.RateLimitingConfigServiceGrpc.RateLimitingConfigServiceBlockingStub;
import ai.traceable.ratelimiting.config.service.v2.RateLimitingRule;
import ai.traceable.ratelimiting.config.service.v2.RuleConfigScope;
import com.google.inject.Inject;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import org.hypertrace.core.grpcutils.context.RequestContext;

public class RateLimitingRuleFetcher {
  private final RateLimitingConfigServiceBlockingStub rateLimitingBlockingStub;

  @Inject
  public RateLimitingRuleFetcher(RateLimitingConfigServiceBlockingStub rateLimitingBlockingStub) {
    this.rateLimitingBlockingStub = rateLimitingBlockingStub;
  }

  public Set<String> getRateLimitingRules(
      RequestContext requestContext, Optional<String> environmentId) {
    GetRateLimitingRulesRequest request =
        GetRateLimitingRulesRequest.newBuilder()
            .setRulesFilter(
                GetRateLimitingRulesFilter.newBuilder()
                    .addCategories(Category.CATEGORY_RATE_LIMITING)
                    .setScope(
                        RuleConfigScope.newBuilder()
                            .setEnvironmentScope(
                                environmentId
                                    .map(id -> EnvironmentScope.newBuilder().addEnvironmentIds(id))
                                    .orElse(EnvironmentScope.newBuilder()))))
            .build();

    GetRateLimitingRulesResponse response =
        requestContext.call(() -> rateLimitingBlockingStub.getRateLimitingRules(request));

    return response.getRulesList().stream()
        .filter(rateLimitingRule -> rateLimitingRule.getData().getEnabled())
        .map(RateLimitingRule::getId)
        .collect(Collectors.toUnmodifiableSet());
  }
}
