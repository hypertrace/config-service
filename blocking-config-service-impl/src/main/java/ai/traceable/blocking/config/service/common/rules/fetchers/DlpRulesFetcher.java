package ai.traceable.blocking.config.service.common.rules.fetchers;

import ai.traceable.ratelimiting.config.service.v2.Category;
import ai.traceable.ratelimiting.config.service.v2.EnvironmentScope;
import ai.traceable.ratelimiting.config.service.v2.GetRateLimitingModsecRulesFilter;
import ai.traceable.ratelimiting.config.service.v2.GetRateLimitingModsecRulesFilter.RuleAction;
import ai.traceable.ratelimiting.config.service.v2.GetRateLimitingRuleModsecRulesRequest;
import ai.traceable.ratelimiting.config.service.v2.GetRateLimitingRuleModsecRulesResponse;
import ai.traceable.ratelimiting.config.service.v2.GetRateLimitingRulesFilter;
import ai.traceable.ratelimiting.config.service.v2.RateLimitingConfigServiceGrpc.RateLimitingConfigServiceBlockingStub;
import ai.traceable.ratelimiting.config.service.v2.RateLimitingModsecRule;
import ai.traceable.ratelimiting.config.service.v2.RuleConfigScope;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import javax.inject.Inject;
import lombok.EqualsAndHashCode;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;
import org.hypertrace.core.grpcutils.context.RequestContext;

@Slf4j
public class DlpRulesFetcher implements RulesFetcher {
  private final RateLimitingConfigServiceBlockingStub rateLimitingConfigServiceBlockingStub;

  @Inject
  DlpRulesFetcher(RateLimitingConfigServiceBlockingStub rateLimitingConfigServiceBlockingStub) {
    this.rateLimitingConfigServiceBlockingStub = rateLimitingConfigServiceBlockingStub;
  }

  @Nullable
  public Map<String, DlpModsecRulesData> fetchDlpModsecRules(
      RequestContext requestContext, Optional<String> environmentId, Set<String> serviceNames) {
    if (serviceNames.isEmpty()) {
      return Collections.emptyMap();
    }

    GetRateLimitingRuleModsecRulesRequest rulesRequest =
        GetRateLimitingRuleModsecRulesRequest.newBuilder()
            .setRulesFilter(
                GetRateLimitingModsecRulesFilter.newBuilder()
                    .setRulesFilter(
                        GetRateLimitingRulesFilter.newBuilder()
                            .setScope(
                                RuleConfigScope.newBuilder()
                                    .setEnvironmentScope(
                                        environmentId
                                            .map(
                                                id ->
                                                    EnvironmentScope.newBuilder()
                                                        .addEnvironmentIds(id)
                                                        .build())
                                            .orElse(EnvironmentScope.getDefaultInstance())))
                            .setDisabled(false)
                            .addCategories(Category.CATEGORY_DATA_EXFILTRATION))
                    .addAllRuleActions(
                        List.of(
                            RuleAction.RULE_ACTION_TRANSACTION_BLOCKED,
                            RuleAction.RULE_ACTION_TRANSACTION_ALLOWED))
                    .addAllServiceNames(serviceNames))
            .build();

    GetRateLimitingRuleModsecRulesResponse response =
        requestContext.call(
            () ->
                rateLimitingConfigServiceBlockingStub.getRateLimitingRuleModsecRules(rulesRequest));

    Map<String, DlpModsecRulesData> dlpModsecRulesDataMap =
        response.getModsecBlobsDataList().stream()
            .flatMap(
                data -> {
                  DlpModsecRulesData dlpModsecRulesData =
                      new DlpModsecRulesData(
                          response.getModsecDirectivesBlob(),
                          data.getModsecBlob(),
                          data.getRuleIdsList(),
                          response.getRulesList());
                  return data.getServiceNamesList().stream()
                      .map(serviceName -> Map.entry(serviceName, dlpModsecRulesData));
                })
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

    // add empty entry for serviceNames which do not have dlp rules
    serviceNames.forEach(
        serviceName ->
            dlpModsecRulesDataMap.computeIfAbsent(serviceName, sName -> new DlpModsecRulesData()));
    return dlpModsecRulesDataMap;
  }

  @Value
  @EqualsAndHashCode
  public static class DlpModsecRulesData {
    String modsecDirectivesBlob;
    String modsecRulesBlob;
    List<RateLimitingModsecRule> rules;

    public DlpModsecRulesData(
        String modsecDirectivesBlob,
        String modsecRulesBlob,
        List<String> ruleIds,
        Collection<RateLimitingModsecRule> rules) {
      this.modsecDirectivesBlob = modsecDirectivesBlob;
      this.modsecRulesBlob = modsecRulesBlob;
      this.rules =
          rules.stream()
              .filter(rule -> ruleIds.contains(rule.getId()))
              .collect(Collectors.toList());
    }

    public DlpModsecRulesData() {
      modsecDirectivesBlob = "";
      modsecRulesBlob = "";
      rules = Collections.emptyList();
    }
  }
}
