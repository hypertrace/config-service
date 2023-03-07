package ai.traceable.blocking.config.service.common.blockingpolicy.fetchers.malicioussources;

import ai.traceable.blocking.config.service.common.blockingpolicy.BlockingPolicyData;
import ai.traceable.blocking.config.service.common.blockingpolicy.BlockingPolicyDataBucket;
import ai.traceable.blocking.config.service.common.blockingpolicy.fetchers.malicioussources.handlers.MaliciousSourceDataHandler;
import ai.traceable.malicioussources.config.service.v1.EnvironmentScope;
import ai.traceable.malicioussources.config.service.v1.GetMaliciousSourcesRulesRequest;
import ai.traceable.malicioussources.config.service.v1.GetRulesFilter;
import ai.traceable.malicioussources.config.service.v1.MaliciousSourcesConfigServiceGrpc.MaliciousSourcesConfigServiceBlockingStub;
import ai.traceable.malicioussources.config.service.v1.MaliciousSourcesRule;
import ai.traceable.malicioussources.config.service.v1.MaliciousSourcesRuleCondition;
import ai.traceable.malicioussources.config.service.v1.MaliciousSourcesRuleScope;
import ai.traceable.malicioussources.config.service.v1.RuleActionType;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import javax.inject.Inject;
import lombok.extern.slf4j.Slf4j;
import org.hypertrace.core.grpcutils.context.RequestContext;

@Slf4j
public class MaliciousSourcesDataFetcher {
  private final MaliciousSourcesConfigServiceBlockingStub maliciousSourcesConfigServiceBlockingStub;
  private final Map<MaliciousSourcesRuleCondition.ConditionCase, MaliciousSourceDataHandler>
      maliciousSourceDataHandlerMap;

  @Inject
  public MaliciousSourcesDataFetcher(
      MaliciousSourcesConfigServiceBlockingStub maliciousSourcesConfigServiceBlockingStub,
      Map<MaliciousSourcesRuleCondition.ConditionCase, MaliciousSourceDataHandler>
          maliciousSourceDataHandlerMap) {
    this.maliciousSourceDataHandlerMap = maliciousSourceDataHandlerMap;
    this.maliciousSourcesConfigServiceBlockingStub = maliciousSourcesConfigServiceBlockingStub;
  }

  public Map<BlockingPolicyDataBucket, List<BlockingPolicyData>>
      getMaliciousSourceRuleBlockingDetails(
          RequestContext requestContext, Optional<String> environmentId) {
    List<MaliciousSourcesRule> ruleList = fetchMaliciousSourceRules(requestContext, environmentId);
    Optional<BlockingPolicyData> emptyOptionalOfBlockingPolicyData = Optional.empty();
    return ruleList.stream()
        .map(
            maliciousSourcesRule -> {
              MaliciousSourceDataHandler maliciousSourceDataHandler =
                  maliciousSourceDataHandlerMap.get(
                      maliciousSourcesRule.getRuleInfo().getConditions(0).getConditionCase());
              if (maliciousSourceDataHandler == null) {
                log.info(
                    "maliciousSourceDataHandler could not find class for condition case : {} and rule id : {}",
                    maliciousSourcesRule.getRuleInfo().getConditions(0).getConditionCase(),
                    maliciousSourcesRule.getId());
                return emptyOptionalOfBlockingPolicyData;
              } else {
                return maliciousSourceDataHandler.getBlockingDetails(maliciousSourcesRule);
              }
            })
        .filter(Optional::isPresent)
        .map(Optional::get)
        .collect(Collectors.groupingBy(BlockingPolicyData::getBucket));
  }

  private List<MaliciousSourcesRule> fetchMaliciousSourceRules(
      RequestContext requestContext, Optional<String> environmentId) {
    GetMaliciousSourcesRulesRequest rulesRequest =
        GetMaliciousSourcesRulesRequest.newBuilder()
            .setFilter(
                GetRulesFilter.newBuilder()
                    .setDisabled(false)
                    .addAllRuleActionTypes(
                        List.of(
                            RuleActionType.RULE_ACTION_TYPE_BLOCK,
                            RuleActionType.RULE_ACTION_TYPE_ALLOW,
                            RuleActionType.RULE_ACTION_TYPE_BLOCK_ALL_EXCEPT))
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
}
