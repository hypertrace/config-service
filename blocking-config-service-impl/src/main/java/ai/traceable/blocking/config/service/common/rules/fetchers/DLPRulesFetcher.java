package ai.traceable.blocking.config.service.common.rules.fetchers;

import ai.traceable.ratelimiting.config.service.v2.EnvironmentScope;
import ai.traceable.ratelimiting.config.service.v2.GetRateLimitingModsecRulesFilter;
import ai.traceable.ratelimiting.config.service.v2.GetRateLimitingModsecRulesFilter.RuleAction;
import ai.traceable.ratelimiting.config.service.v2.GetRateLimitingRuleModsecRulesRequest;
import ai.traceable.ratelimiting.config.service.v2.GetRateLimitingRuleModsecRulesResponse;
import ai.traceable.ratelimiting.config.service.v2.RateLimitingConfigServiceGrpc.RateLimitingConfigServiceBlockingStub;
import ai.traceable.ratelimiting.config.service.v2.RuleConfigScope;
import java.util.List;
import java.util.Optional;
import javax.inject.Inject;
import lombok.extern.slf4j.Slf4j;
import org.hypertrace.core.grpcutils.context.RequestContext;

@Slf4j
public class DLPRulesFetcher implements RulesFetcher<GetRateLimitingRuleModsecRulesResponse> {
  private final RateLimitingConfigServiceBlockingStub rateLimitingConfigServiceBlockingStub;

  @Inject
  DLPRulesFetcher(RateLimitingConfigServiceBlockingStub rateLimitingConfigServiceBlockingStub) {
    this.rateLimitingConfigServiceBlockingStub = rateLimitingConfigServiceBlockingStub;
  }

  public GetRateLimitingRuleModsecRulesResponse fetchRules(
      RequestContext requestContext, Optional<String> environmentId) {

    GetRateLimitingRuleModsecRulesRequest rulesRequest =
        GetRateLimitingRuleModsecRulesRequest.newBuilder()
            .setRulesFilter(
                GetRateLimitingModsecRulesFilter.newBuilder()
                    .setDisabled(false)
                    .addAllRuleActions(
                        List.of(
                            RuleAction.RULE_ACTION_TRANSACTION_BLOCKED,
                            RuleAction.RULE_ACTION_TRANSACTION_ALLOWED))
                    .setScope(
                        RuleConfigScope.newBuilder()
                            .setEnvironmentScope(
                                environmentId
                                    .map(
                                        id ->
                                            EnvironmentScope.newBuilder()
                                                .addEnvironmentIds(id)
                                                .build())
                                    .orElse(EnvironmentScope.getDefaultInstance()))))
            .build();

    return requestContext.call(
        () -> rateLimitingConfigServiceBlockingStub.getRateLimitingRuleModsecRules(rulesRequest));
  }
}
