package ai.traceable.anomaly.config.service.exclusion.handlers;

import ai.traceable.anomaly.config.service.common.AnomalyConfigScopeMatcher;
import ai.traceable.anomaly.config.service.exclusion.utils.FilterUtils;
import ai.traceable.anomaly.config.service.v1.exclusion.AnomalyExclusionRuleConfig;
import ai.traceable.anomaly.config.service.v1.exclusion.GetAnomalyExclusionRulesRequest;
import ai.traceable.anomaly.config.service.v1.exclusion.GetAnomalyExclusionRulesResponse;
import ai.traceable.anomaly.config.service.v1.exclusion.GetRulesFilter;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import javax.inject.Inject;
import org.hypertrace.core.grpcutils.context.RequestContext;

public class GetAnomalyExclusionRuleHandler {
  private final ConfigServiceHandler configServiceHandler;
  private final AnomalyConfigScopeMatcher anomalyConfigScopeMatcher;
  private final FilterUtils filterUtils;

  @Inject
  GetAnomalyExclusionRuleHandler(
      ConfigServiceHandler configServiceHandler,
      AnomalyConfigScopeMatcher anomalyConfigScopeMatcher,
      FilterUtils filterUtils) {
    this.configServiceHandler = configServiceHandler;
    this.anomalyConfigScopeMatcher = anomalyConfigScopeMatcher;
    this.filterUtils = filterUtils;
  }

  public GetAnomalyExclusionRulesResponse getRules(
      GetAnomalyExclusionRulesRequest request, RequestContext requestContext) {
    List<AnomalyExclusionRuleConfig> anomalyExclusionRuleConfigList =
        configServiceHandler.getAllExclusionConfigs(requestContext);

    if (!request.hasFilter()) {
      return GetAnomalyExclusionRulesResponse.newBuilder()
          .addAllConfigs(anomalyExclusionRuleConfigList)
          .build();
    }

    List<AnomalyExclusionRuleConfig> filteredExclusionConfigs =
        getFilteredConfigs(anomalyExclusionRuleConfigList, request.getFilter());

    return GetAnomalyExclusionRulesResponse.newBuilder()
        .addAllConfigs(filteredExclusionConfigs)
        .build();
  }

  private List<AnomalyExclusionRuleConfig> getFilteredConfigs(
      List<AnomalyExclusionRuleConfig> configs, GetRulesFilter ruleFilter) {
    Set<String> ruleIdsSet = new HashSet<>(ruleFilter.getRuleIdsList());
    Set<String> anomalyActorIdsSet = new HashSet<>(ruleFilter.getAnomalyActorIdsList());
    return configs.stream()
        .filter(
            config ->
                filterUtils.filterRuleIds(config, ruleIdsSet)
                    && filterUtils.filterAnomalyActorIds(config, anomalyActorIdsSet)
                    && filterUtils.filterEventIds(config, ruleFilter.getEventTypeIdsList())
                    && filterUtils.filterEventFamilies(config, ruleFilter.getEventFamiliesList())
                    && (!ruleFilter.hasAnomalyConfigScope()
                        || anomalyConfigScopeMatcher.isParentScope(
                            config.getRuleData().getAnomalyConfigScope(),
                            ruleFilter.getAnomalyConfigScope())))
        .collect(Collectors.toUnmodifiableList());
  }
}
