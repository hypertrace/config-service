package ai.traceable.anomaly.config.service.apiprotect.protection.engine.cache;

import static ai.traceable.anomaly.config.service.common.AnomalyConfigScopeUtils.ANOMALY_CONFIG_SCOPE_COMPARATOR;

import ai.traceable.anomaly.config.service.common.AnomalyConfigScopeUtils;
import ai.traceable.anomaly.config.service.detector.anomalydetection.AnomalyDetectionConfigManager;
import ai.traceable.anomaly.config.service.global.status.GlobalAnomalyConfigStatusManager;
import ai.traceable.anomaly.config.service.v1.AnomalyConfigScope;
import ai.traceable.anomaly.config.service.v1.AnomalyRuleAction;
import ai.traceable.anomaly.config.service.v1.RuleEvaluationPoint;
import ai.traceable.anomaly.config.service.v1.RuleVersion;
import ai.traceable.anomaly.config.service.v1.RuleVersionData;
import ai.traceable.anomaly.config.service.v1.apiprotect.GetApiProtectEvaluationConfigContextRequest;
import ai.traceable.anomaly.config.service.v1.detector.AnomalyDetectionConfig;
import ai.traceable.anomaly.config.service.v1.detector.AnomalyDetectionConfigType;
import ai.traceable.anomaly.config.service.v1.detector.AnomalySubRuleConfig;
import ai.traceable.anomaly.config.service.v1.detector.GetAnomalyDetectionConfigsFilter;
import ai.traceable.anomaly.config.service.v1.detector.ScopedAnomalyDetectionConfig;
import ai.traceable.anomaly.config.service.v1.global.ScopedAnomalyConfigStatus;
import ai.traceable.config.service.feature.caching.client.FeatureCachingClient;
import ai.traceable.entity.fetcher.cache.CachedApiMappingProvider;
import ai.traceable.entity.fetcher.cache.CachedServiceMappingProvider;
import ai.traceable.protection.engine.config.apiprotect.v1.ApiProtectionConfigContext;
import ai.traceable.protection.engine.config.apiprotect.v1.ApiProtectionRuleConfig;
import ai.traceable.protection.engine.config.apiprotect.v1.ApiProtectionRuleConfig.BlackBoxEvaluationRule;
import ai.traceable.protection.engine.config.apiprotect.v1.ApiProtectionRulesContext;
import ai.traceable.protection.processing.common.v1.ScopeContext;
import ai.traceable.protection.rules.apiprotect.v1.ApiProtectThreatRule;
import ai.traceable.protection.rules.apiprotect.v1.ApiProtectVersionedRules;
import ai.traceable.protection.rules.apiprotect.v1.ApiProtectVersionedRulesFilter;
import ai.traceable.protection.rules.apiprotect.v1.ApiProtectionRulesProvider;
import ai.traceable.protection.rules.apiprotect.v1.BlackBoxEvaluation;
import jakarta.inject.Inject;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeSet;
import java.util.function.Function;
import java.util.stream.Collectors;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.hypertrace.core.grpcutils.context.RequestContext;

@Slf4j
@AllArgsConstructor(onConstructor_ = @Inject)
public class ApiProtectConfigContextClientProvider implements ApiProtectConfigContextProvider {
  private static final GetAnomalyDetectionConfigsFilter ANOMALY_DETECTION_CONFIGS_FILTER =
      GetAnomalyDetectionConfigsFilter.newBuilder()
          .addAnomalyDetectionConfigTypes(
              AnomalyDetectionConfigType.ANOMALY_DETECTION_CONFIG_TYPE_API_PROTECT)
          .build();

  protected final FeatureCachingClient featureCachingClient;
  protected final AnomalyDetectionConfigManager anomalyDetectionConfigManager;
  protected final GlobalAnomalyConfigStatusManager globalAnomalyConfigStatusManager;
  protected final ApiProtectionRulesProvider apiProtectionRulesProvider;
  protected final CachedServiceMappingProvider cachedServiceMappingProvider;
  protected final CachedApiMappingProvider cachedApiMappingProvider;
  private static final AnomalyConfigScopeUtils ANOMALY_CONFIG_SCOPE_UTILS =
      new AnomalyConfigScopeUtils();

  @Override
  public ApiProtectionConfigContext getApiProtectionConfigContext(
      RequestContext requestContext, GetApiProtectEvaluationConfigContextRequest request) {
    return loadApiProtectionConfigContext(requestContext, request);
  }

  protected ApiProtectionConfigContext loadApiProtectionConfigContext(
      RequestContext requestContext, GetApiProtectEvaluationConfigContextRequest request) {

    Map<AnomalyConfigScope, ScopedAnomalyDetectionConfig> scopedAnomalyDetectionConfigMap =
        filterByRequestScope(
            getScopedAnomalyDetectionConfigMap(requestContext), request.getConfigScope());
    Map<AnomalyConfigScope, ScopedAnomalyConfigStatus> scopedAnomalyConfigStatusMap =
        filterByRequestScope(
            getScopedAnomalyConfigStatusMap(requestContext), request.getConfigScope());
    log.debug(
        "Retrieved scopedAnomalyDetectionConfigMap with {} entries and scopedAnomalyConfigStatusMap with {} entries",
        scopedAnomalyDetectionConfigMap.size(),
        scopedAnomalyConfigStatusMap.size());

    TreeSet<AnomalyConfigScope> configScopes = new TreeSet<>(ANOMALY_CONFIG_SCOPE_COMPARATOR);
    configScopes.addAll(scopedAnomalyDetectionConfigMap.keySet());
    configScopes.addAll(scopedAnomalyConfigStatusMap.keySet());
    log.debug(
        "Combined config scopes: {} total scopes and Sorted config scopes by priority: {}",
        configScopes.size(),
        configScopes);

    List<ApiProtectionRulesContext> apiProtectionRulesContextList = new ArrayList<>();
    Map<AnomalyConfigScope, ScopeContext> scopeContextMap =
        AnomalyConfigScopeUtils.getScopeContextMap(
            requestContext, configScopes, cachedApiMappingProvider, cachedServiceMappingProvider);

    for (AnomalyConfigScope configScope : configScopes) {
      List<AnomalyConfigScope> scopesWithDecreasingPriority =
          AnomalyConfigScopeUtils.getConfigScopesWithDecreasingPriority(configScope);

      ScopedAnomalyConfigStatus scopedAnomalyConfigStatus =
          scopesWithDecreasingPriority.stream()
              .filter(scopedAnomalyConfigStatusMap::containsKey)
              .findFirst()
              .map(scopedAnomalyConfigStatusMap::get)
              .orElse(ScopedAnomalyConfigStatus.getDefaultInstance());

      ScopedAnomalyDetectionConfig scopedAnomalyDetectionConfig =
          scopesWithDecreasingPriority.stream()
              .filter(scopedAnomalyDetectionConfigMap::containsKey)
              .findFirst()
              .map(scopedAnomalyDetectionConfigMap::get)
              .orElse(ScopedAnomalyDetectionConfig.getDefaultInstance());

      RuleVersionData ruleVersionData =
          scopedAnomalyConfigStatus.getGlobalApiConfig().getRuleVersionData();
      RuleVersion ruleVersion = getRuleVersion(ruleVersionData, requestContext);

      if (!scopedAnomalyDetectionConfig.equals(ScopedAnomalyDetectionConfig.getDefaultInstance())) {
        List<ApiProtectionRuleConfig> ruleConfigs =
            getApiProtectionRuleConfigs(
                scopedAnomalyDetectionConfig, request.getRuleEvaluationPoint(), ruleVersion);

        if (!ruleConfigs.isEmpty()) {
          apiProtectionRulesContextList.add(
              ApiProtectionRulesContext.newBuilder()
                  .setScopeContext(scopeContextMap.get(configScope))
                  .addAllRuleConfigs(ruleConfigs)
                  .build());
        } else {
          log.debug("No rule configs found for scope, skipping");
        }
      }
    }
    return ApiProtectionConfigContext.newBuilder()
        .addAllRuleContexts(apiProtectionRulesContextList)
        .build();
  }

  private List<ApiProtectionRuleConfig> getApiProtectionRuleConfigs(
      ScopedAnomalyDetectionConfig scopedAnomalyDetectionConfig,
      RuleEvaluationPoint ruleEvaluationPoint,
      RuleVersion ruleVersion) {
    log.debug(
        "getApiProtectionRuleConfigs called with ruleEvaluationPoint={}, ruleVersion={}",
        ruleEvaluationPoint,
        ruleVersion);

    Map<String, AnomalyDetectionConfig> anomalyRuleConfigMap =
        getApiProtectAnomalyRuleConfigMap(scopedAnomalyDetectionConfig);
    Map<String, ApiProtectThreatRule> threatRuleMap = getApiProtectThreatRuleMap(ruleVersion);

    List<ApiProtectionRuleConfig> ruleConfigs = new ArrayList<>();
    for (Map.Entry<String, AnomalyDetectionConfig> entry : anomalyRuleConfigMap.entrySet()) {
      AnomalyDetectionConfig detectionConfig = entry.getValue();
      Map<String, AnomalySubRuleConfig> subRuleConfigMap =
          getApiProtectSubRuleConfigMap(detectionConfig);

      for (Map.Entry<String, AnomalySubRuleConfig> subRuleEntry : subRuleConfigMap.entrySet()) {
        String subRuleId = subRuleEntry.getKey();
        AnomalySubRuleConfig subRuleConfig = subRuleEntry.getValue();

        if (isDisabled(detectionConfig, subRuleConfig, ruleEvaluationPoint)) {
          log.debug("Skipping disabled rule: {}", subRuleId);
          continue;
        }
        ApiProtectionRuleConfig.Builder builder =
            ApiProtectionRuleConfig.newBuilder()
                .setRuleId(subRuleId)
                .putAllEvaluationConfigs(subRuleConfig.getConfigParamsMap());
        Optional.ofNullable(threatRuleMap.get(subRuleId))
            .filter(rule -> rule.getRuleEvaluation().hasBlackBoxEvaluation())
            .ifPresent(rule -> builder.setBlackBoxEvaluation(getBlackBoxEvaluationConfig(rule)));

        ruleConfigs.add(builder.build());
      }
    }
    return ruleConfigs;
  }

  private Map<String, AnomalyDetectionConfig> getApiProtectAnomalyRuleConfigMap(
      ScopedAnomalyDetectionConfig scopedAnomalyDetectionConfig) {
    return scopedAnomalyDetectionConfig.getAnomalyDetectionConfigsList().stream()
        .filter(
            detectionConfig ->
                detectionConfig.getApiProtectAnomalyDetectionConfig().hasApiProtectAnomalyRule())
        .collect(
            Collectors.toUnmodifiableMap(
                detectionConfig ->
                    detectionConfig
                        .getApiProtectAnomalyDetectionConfig()
                        .getApiProtectAnomalyRule()
                        .getAnomalyRuleId(),
                Function.identity()));
  }

  private Map<String, ApiProtectThreatRule> getApiProtectThreatRuleMap(RuleVersion ruleVersion) {
    List<ApiProtectVersionedRules> threatRules =
        apiProtectionRulesProvider.getApiProtectVersionedRules(
            ApiProtectVersionedRulesFilter.newBuilder()
                .addRuleVersions(ruleVersion.getVersion())
                .build());
    if (threatRules.isEmpty()) {
      log.warn("No threat rules found for rule version: {}", ruleVersion);
      return Collections.emptyMap();
    }
    return threatRules.get(0).getRulesData().getThreatRulesList().stream()
        .collect(
            Collectors.toUnmodifiableMap(ApiProtectThreatRule::getRuleId, Function.identity()));
  }

  private BlackBoxEvaluationRule getBlackBoxEvaluationConfig(ApiProtectThreatRule threatRule) {
    BlackBoxEvaluation blackBoxEvaluation = threatRule.getRuleEvaluation().getBlackBoxEvaluation();
    return BlackBoxEvaluationRule.newBuilder()
        .setBlackBoxEvaluationIdentifier(blackBoxEvaluation.getBlackBoxIdentifier())
        .putAllRuleParameters(blackBoxEvaluation.getRuleParametersMap())
        .build();
  }

  private Map<String, AnomalySubRuleConfig> getApiProtectSubRuleConfigMap(
      AnomalyDetectionConfig detectionConfig) {
    return detectionConfig
        .getApiProtectAnomalyDetectionConfig()
        .getApiProtectAnomalyRule()
        .getSubRuleConfigsList()
        .stream()
        .collect(
            Collectors.toUnmodifiableMap(AnomalySubRuleConfig::getSubRuleId, Function.identity()));
  }

  private boolean isDisabled(
      AnomalyDetectionConfig detectionConfig,
      AnomalySubRuleConfig subRuleConfig,
      RuleEvaluationPoint ruleEvaluationPoint) {
    switch (ruleEvaluationPoint) {
      case RULE_EVALUATION_POINT_EDGE:
        return detectionConfig.getConfigStatus().getDisabled()
            || !subRuleConfig
                .getAnomalyRuleAction()
                .equals(AnomalyRuleAction.ANOMALY_RULE_ACTION_BLOCK);
      case RULE_EVALUATION_POINT_PLATFORM:
        return detectionConfig.getConfigStatus().getDisabled()
            || subRuleConfig
                .getAnomalyRuleAction()
                .equals(AnomalyRuleAction.ANOMALY_RULE_ACTION_DISABLE);
      default:
        log.error("Unsupported rule evaluation point: {}", ruleEvaluationPoint);
        throw new IllegalArgumentException(
            "Unsupported rule evaluation point: " + ruleEvaluationPoint);
    }
  }

  private Map<AnomalyConfigScope, ScopedAnomalyDetectionConfig> getScopedAnomalyDetectionConfigMap(
      RequestContext requestContext) {
    return anomalyDetectionConfigManager
        .getAllGlobalResolvedScopedAnomalyDetectionConfigs(
            requestContext, ANOMALY_DETECTION_CONFIGS_FILTER)
        .stream()
        .collect(
            Collectors.toUnmodifiableMap(
                ScopedAnomalyDetectionConfig::getConfigScope, Function.identity()));
  }

  private Map<AnomalyConfigScope, ScopedAnomalyConfigStatus> getScopedAnomalyConfigStatusMap(
      RequestContext requestContext) {
    return globalAnomalyConfigStatusManager
        .getAllScopedAnomalyConfigStatusConfigs(requestContext, Collections.emptyList())
        .stream()
        .collect(
            Collectors.toUnmodifiableMap(
                ScopedAnomalyConfigStatus::getConfigScope, Function.identity()));
  }

  private <T> Map<AnomalyConfigScope, T> filterByRequestScope(
      Map<AnomalyConfigScope, T> configMap, AnomalyConfigScope requestScope) {
    if (requestScope.getScopeCase() == AnomalyConfigScope.ScopeCase.SCOPE_NOT_SET) {
      return configMap;
    }
    return configMap.entrySet().stream()
        .filter(entry -> ANOMALY_CONFIG_SCOPE_UTILS.isParentScope(entry.getKey(), requestScope))
        .collect(Collectors.toUnmodifiableMap(Map.Entry::getKey, Map.Entry::getValue));
  }

  private RuleVersion getRuleVersion(
      RuleVersionData ruleVersionData, RequestContext requestContext) {
    if (featureCachingClient.isApiProtectConfigPoliciesRevampEnabled(requestContext)) {
      return ruleVersionData.getCurrentVersion();
    }
    return RuleVersion.getDefaultInstance();
  }
}
