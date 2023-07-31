package ai.traceable.blocking.config.service.common.rules.fetchers;

import ai.traceable.malicioussources.config.service.v1.EnvironmentScope;
import ai.traceable.malicioussources.config.service.v1.GetMaliciousSourcesRulesRequest;
import ai.traceable.malicioussources.config.service.v1.GetRulesFilter;
import ai.traceable.malicioussources.config.service.v1.MaliciousSourcesConfigServiceGrpc.MaliciousSourcesConfigServiceBlockingStub;
import ai.traceable.malicioussources.config.service.v1.MaliciousSourcesRule;
import ai.traceable.malicioussources.config.service.v1.MaliciousSourcesRuleAction;
import ai.traceable.malicioussources.config.service.v1.MaliciousSourcesRuleScope;
import ai.traceable.malicioussources.config.service.v1.RuleActionType;
import com.google.protobuf.Timestamp;
import com.google.protobuf.util.Timestamps;
import java.time.Clock;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import javax.inject.Inject;
import lombok.extern.slf4j.Slf4j;
import org.hypertrace.core.grpcutils.context.RequestContext;

@Slf4j
public class MaliciousSourcesRulesFetcher implements RulesFetcher<List<MaliciousSourcesRule>> {

  private static final GetRulesFilter DEFAULT_RULES_FILTER =
      GetRulesFilter.newBuilder()
          .setDisabled(false)
          .addAllRuleActionTypes(
              List.of(
                  RuleActionType.RULE_ACTION_TYPE_BLOCK,
                  RuleActionType.RULE_ACTION_TYPE_ALLOW,
                  RuleActionType.RULE_ACTION_TYPE_BLOCK_ALL_EXCEPT))
          .build();

  private final Clock clock;
  private final MaliciousSourcesConfigServiceBlockingStub maliciousSourcesConfigServiceBlockingStub;

  @Inject
  public MaliciousSourcesRulesFetcher(
      Clock clock,
      MaliciousSourcesConfigServiceBlockingStub maliciousSourcesConfigServiceBlockingStub) {
    this.clock = clock;
    this.maliciousSourcesConfigServiceBlockingStub = maliciousSourcesConfigServiceBlockingStub;
  }

  public List<MaliciousSourcesRule> fetchRules(
      RequestContext requestContext, Optional<String> environmentId) {
    GetMaliciousSourcesRulesRequest rulesRequest =
        GetMaliciousSourcesRulesRequest.newBuilder()
            .setFilter(
                DEFAULT_RULES_FILTER.toBuilder()
                    .setRuleScope(
                        MaliciousSourcesRuleScope.newBuilder()
                            .setEnvironmentScope(
                                environmentId
                                    .map(
                                        id ->
                                            EnvironmentScope.newBuilder()
                                                .addEnvironmentIds(id)
                                                .build())
                                    .orElse(EnvironmentScope.getDefaultInstance()))))
            .build();

    return requestContext
        .call(
            () -> maliciousSourcesConfigServiceBlockingStub.getMaliciousSourcesRules(rulesRequest))
        .getRulesList()
        .stream()
        .filter(rule -> isRuleActive(clock.millis(), rule.getRuleInfo().getRuleAction()))
        .collect(Collectors.toList());
  }

  private boolean isRuleActive(long currentTimeMillis, MaliciousSourcesRuleAction ruleAction) {
    if (!ruleAction.hasExpirationDetails()) {
      return true;
    }
    Timestamp timestamp = ruleAction.getExpirationDetails().getExpirationTimestamp();
    long expirationMillis = Timestamps.toMillis(timestamp);
    return expirationMillis == 0 || expirationMillis > currentTimeMillis;
  }
}
