package ai.traceable.blocking.config.service.iptype;

import ai.traceable.malicioussources.config.service.v1.EnvironmentScope;
import ai.traceable.malicioussources.config.service.v1.GetMaliciousSourcesRulesRequest;
import ai.traceable.malicioussources.config.service.v1.GetRulesFilter;
import ai.traceable.malicioussources.config.service.v1.IpLocationType;
import ai.traceable.malicioussources.config.service.v1.MaliciousSourcesConfigServiceGrpc.MaliciousSourcesConfigServiceBlockingStub;
import ai.traceable.malicioussources.config.service.v1.MaliciousSourcesRule;
import ai.traceable.malicioussources.config.service.v1.MaliciousSourcesRuleScope;
import ai.traceable.malicioussources.config.service.v1.RuleActionType;
import com.google.inject.Inject;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;
import org.hypertrace.core.grpcutils.context.RequestContext;

public class BlockingIpTypesClient {
  private final MaliciousSourcesConfigServiceBlockingStub maliciousSourcesConfigServiceBlockingStub;

  @Inject
  public BlockingIpTypesClient(
      MaliciousSourcesConfigServiceBlockingStub maliciousSourcesConfigServiceBlockingStub) {
    this.maliciousSourcesConfigServiceBlockingStub = maliciousSourcesConfigServiceBlockingStub;
  }

  public List<MaliciousSourcesRule> fetchMaliciousSourceRules(
      RequestContext requestContext, Optional<String> environmentId) {
    GetMaliciousSourcesRulesRequest rulesRequest =
        GetMaliciousSourcesRulesRequest.newBuilder()
            .setFilter(
                GetRulesFilter.newBuilder()
                    .setDisabled(false)
                    .addRuleActionTypes(RuleActionType.RULE_ACTION_TYPE_BLOCK)
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
        .getRulesList();
  }

  public static List<IpLocationType> getBlockingIpTypes(MaliciousSourcesRule maliciousSourcesRule) {
    return maliciousSourcesRule.getRuleInfo().getConditionsList().stream()
        .flatMap(
            condition -> condition.getIpLocationTypeCondition().getIpLocationTypesList().stream())
        .distinct()
        .filter(Objects::nonNull)
        .collect(Collectors.toUnmodifiableList());
  }
}
