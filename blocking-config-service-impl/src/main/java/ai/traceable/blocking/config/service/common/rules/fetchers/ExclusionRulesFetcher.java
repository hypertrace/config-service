package ai.traceable.blocking.config.service.common.rules.fetchers;

import ai.traceable.blocking.config.service.common.rules.ModsecRulesData;
import ai.traceable.detection.exclusion.config.service.v1.DetectionExclusionConfigServiceGrpc.DetectionExclusionConfigServiceBlockingStub;
import ai.traceable.detection.exclusion.config.service.v1.DetectionExclusionModsecRule;
import ai.traceable.detection.exclusion.config.service.v1.DetectionExclusionRuleScope;
import ai.traceable.detection.exclusion.config.service.v1.EnvironmentScope;
import ai.traceable.detection.exclusion.config.service.v1.ExclusionTarget;
import ai.traceable.detection.exclusion.config.service.v1.GetExclusionModsecRulesRequest;
import ai.traceable.detection.exclusion.config.service.v1.GetExclusionModsecRulesResponse;
import ai.traceable.detection.exclusion.config.service.v1.GetRulesFilter;
import ai.traceable.detection.exclusion.config.service.v1.RuleSource;
import jakarta.inject.Inject;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;
import org.hypertrace.config.objectstore.ClientConfig;
import org.hypertrace.core.grpcutils.context.RequestContext;

@Slf4j
public class ExclusionRulesFetcher implements RulesFetcher {
  private static final List<RuleSource> ruleSourceList =
      List.of(
          RuleSource.RULE_SOURCE_DEFAULT,
          RuleSource.RULE_SOURCE_CUSTOMER,
          RuleSource.RULE_SOURCE_TRACEABLE,
          RuleSource.RULE_SOURCE_SYSTEM,
          RuleSource.RULE_SOURCE_OLD_API);

  private final DetectionExclusionConfigServiceBlockingStub
      detectionExclusionConfigServiceBlockingStub;
  private final ClientConfig clientConfig;

  @Inject
  ExclusionRulesFetcher(
      DetectionExclusionConfigServiceBlockingStub detectionExclusionConfigServiceBlockingStub,
      ClientConfig clientConfig) {
    this.detectionExclusionConfigServiceBlockingStub = detectionExclusionConfigServiceBlockingStub;
    this.clientConfig = clientConfig;
  }

  @Nullable
  public Map<String, ModsecRulesData<DetectionExclusionModsecRule>> fetchExclusionModsecRules(
      RequestContext requestContext, Optional<String> environmentId, Set<String> serviceNames) {
    if (serviceNames.isEmpty()) {
      return Collections.emptyMap();
    }

    GetExclusionModsecRulesRequest rulesRequest =
        GetExclusionModsecRulesRequest.newBuilder()
            .setRulesFilter(
                GetRulesFilter.newBuilder()
                    .setRuleScope(
                        DetectionExclusionRuleScope.newBuilder()
                            .setEnvironmentScope(
                                environmentId
                                    .map(
                                        id ->
                                            EnvironmentScope.newBuilder()
                                                .addEnvironmentIds(id)
                                                .build())
                                    .orElse(EnvironmentScope.getDefaultInstance())))
                    .setDisabled(false)
                    .addExclusionTargets(ExclusionTarget.EXCLUSION_TARGET_ALLOW)
                    .addExclusionTargets(ExclusionTarget.EXCLUSION_TARGET_BLOCK)
                    .addAllRuleCreationSources(ruleSourceList))
            .addAllServiceNames(serviceNames)
            .build();

    GetExclusionModsecRulesResponse response =
        requestContext.call(
            () ->
                detectionExclusionConfigServiceBlockingStub
                    .withDeadlineAfter(clientConfig.getTimeout().toMillis(), TimeUnit.MILLISECONDS)
                    .getExclusionModsecRules(rulesRequest));

    Map<String, ModsecRulesData<DetectionExclusionModsecRule>> modsecRulesDataMap =
        response.getModsecBlobsDataList().stream()
            .flatMap(
                data -> {
                  ModsecRulesData<DetectionExclusionModsecRule> modsecRulesData =
                      new ModsecRulesData<>(
                          response.getModsecDirectivesBlob(),
                          data.getModsecBlob(),
                          data.getRuleIdsList(),
                          response.getModsecRulesList(),
                          rule -> rule.getRule().getId());
                  return data.getServiceNamesList().stream()
                      .map(serviceName -> Map.entry(serviceName, modsecRulesData));
                })
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

    // add empty entry for serviceNames which do not have exclusion rules
    serviceNames.forEach(
        serviceName ->
            modsecRulesDataMap.computeIfAbsent(serviceName, sName -> new ModsecRulesData<>()));
    return modsecRulesDataMap;
  }
}
