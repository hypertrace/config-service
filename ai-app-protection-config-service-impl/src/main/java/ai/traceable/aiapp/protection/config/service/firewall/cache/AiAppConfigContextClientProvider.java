package ai.traceable.aiapp.protection.config.service.firewall.cache;

import static ai.traceable.anomaly.config.service.common.AnomalyConfigScopeUtils.ANOMALY_CONFIG_SCOPE_COMPARATOR;

import ai.traceable.aiapp.protection.config.service.firewall.converter.DatatypeRuleToCustomSignatureConfigConverter;
import ai.traceable.aiapp.protection.config.service.v1.AiAppConfigServiceGrpc;
import ai.traceable.aiapp.protection.config.service.v1.AiAppCustomRule;
import ai.traceable.aiapp.protection.config.service.v1.AiAppSubRule;
import ai.traceable.aiapp.protection.config.service.v1.GetAiAppEvaluationConfigContextRequest;
import ai.traceable.aiapp.protection.config.service.v1.GetAiAppRulesRequest;
import ai.traceable.aiapp.protection.config.service.v1.GetAiAppRulesResponse;
import ai.traceable.aiapp.protection.config.service.v1.RuleEvaluationPoint;
import ai.traceable.aiapp.protection.config.service.v1.RuleScope;
import ai.traceable.anomaly.config.service.common.AnomalyConfigScopeUtils;
import ai.traceable.anomaly.config.service.detector.anomalydetection.AnomalyDetectionConfigManager;
import ai.traceable.anomaly.config.service.v1.AnomalyConfigScope;
import ai.traceable.anomaly.config.service.v1.AnomalyCustomerScope;
import ai.traceable.anomaly.config.service.v1.AnomalyEnvironmentScope;
import ai.traceable.anomaly.config.service.v1.AnomalyRuleAction;
import ai.traceable.anomaly.config.service.v1.detector.AnomalyDetectionConfig;
import ai.traceable.anomaly.config.service.v1.detector.AnomalyDetectionConfigType;
import ai.traceable.anomaly.config.service.v1.detector.AnomalySubRuleConfig;
import ai.traceable.anomaly.config.service.v1.detector.GetAnomalyDetectionConfigsFilter;
import ai.traceable.anomaly.config.service.v1.detector.ScopedAnomalyDetectionConfig;
import ai.traceable.data.classification.cache.client.DataClassificationClient;
import ai.traceable.data.classification.cache.info.DataClassificationInfo;
import ai.traceable.data.classification.config.service.v1.DataClassificationConfigServiceGrpc;
import ai.traceable.data.classification.config.service.v1.GetDataTypesRequest;
import ai.traceable.data.classification.config.service.v1.GetDataTypesRequest.DataTypeFilter;
import ai.traceable.data.classification.config.service.v1.GetDataTypesRequest.DataTypeOrdering;
import ai.traceable.entity.fetcher.cache.CachedApiMappingProvider;
import ai.traceable.entity.fetcher.cache.CachedServiceMappingProvider;
import ai.traceable.protection.engine.config.aifirewall.v1.AiFirewallConfigContext;
import ai.traceable.protection.engine.config.aifirewall.v1.AiFirewallScopedConfigContext;
import ai.traceable.protection.engine.config.aifirewall.v1.ModelBasedEvaluationConfig;
import ai.traceable.protection.engine.config.aifirewall.v1.ModelBasedRuleConfig;
import ai.traceable.protection.engine.config.aifirewall.v1.SecRulesEvaluationConfig;
import ai.traceable.protection.engine.config.customsignature.v1.CustomSignatureConfigContext;
import ai.traceable.protection.engine.config.customsignature.v1.CustomSignatureRulesContext;
import ai.traceable.protection.processing.common.v1.ScopeContext;
import ai.traceable.protection.processor.datatype.v1.DataType;
import ai.traceable.protection.rules.aiapp.v1.AiAppRules;
import ai.traceable.protection.rules.aiapp.v1.AiAppRulesProvider;
import ai.traceable.protection.rules.aiapp.v1.AiAppThreatRule;
import com.google.protobuf.Value;
import jakarta.inject.Inject;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.stream.Collectors;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.hypertrace.core.grpcutils.context.RequestContext;

@Slf4j
@AllArgsConstructor(onConstructor_ = @Inject)
public class AiAppConfigContextClientProvider implements AiAppConfigContextProvider {

  private static final String MODEL_ID = "model_id";
  private static final GetAnomalyDetectionConfigsFilter ANOMALY_DETECTION_CONFIGS_FILTER =
      GetAnomalyDetectionConfigsFilter.newBuilder()
          .addAnomalyDetectionConfigTypes(
              AnomalyDetectionConfigType.ANOMALY_DETECTION_CONFIG_TYPE_GEN_AI)
          .build();

  private static final AnomalyConfigScopeUtils ANOMALY_CONFIG_SCOPE_UTILS =
      new AnomalyConfigScopeUtils();
  public static final String MATCHED_CATEGORY = "matched_category";

  protected final AnomalyDetectionConfigManager anomalyDetectionConfigManager;
  protected final AiAppRulesProvider aiAppRulesProvider;
  protected final CachedServiceMappingProvider cachedServiceMappingProvider;
  protected final CachedApiMappingProvider cachedApiMappingProvider;
  protected final DataClassificationConfigServiceGrpc.DataClassificationConfigServiceBlockingStub
      dataClassificationConfigServiceStub;
  protected final ProtectionEngineDataTypeTranslator protectionEngineDataTypeTranslator;
  protected final DataClassificationClient dataClassificationClient;
  protected final AiAppConfigServiceGrpc.AiAppConfigServiceBlockingStub aiAppConfigService;
  protected final DatatypeRuleToCustomSignatureConfigConverter
      datatypeRuleToCustomSignatureConfigConverter;

  @Override
  public AiFirewallConfigContext getAiFirewallConfigContext(
      RequestContext requestContext, GetAiAppEvaluationConfigContextRequest request) {
    return loadAiFirewallConfigContext(requestContext, request);
  }

  protected AiFirewallConfigContext loadAiFirewallConfigContext(
      RequestContext requestContext, GetAiAppEvaluationConfigContextRequest request) {

    AnomalyConfigScope anomalyConfigScope =
        convertRuleScopeToAnomalyConfigScope(request.getRuleScope());
    CompletableFuture<Map<AnomalyConfigScope, ScopedAnomalyDetectionConfig>> configMapFuture =
        CompletableFuture.supplyAsync(
            () ->
                filterByRequestScope(
                    getScopedAnomalyDetectionConfigMap(requestContext), anomalyConfigScope));

    AiAppRules aiAppRules = aiAppRulesProvider.getAiAppRules();
    Set<String> secRuleEvaluatedRuleIds = getSecRuleEvaluatedRuleIds(aiAppRules);
    Set<String> modelBasedRuleIds = getModelBasedRuleIds(aiAppRules);

    Map<AnomalyConfigScope, ScopedAnomalyDetectionConfig> scopedAnomalyDetectionConfigMap =
        configMapFuture.join();
    log.debug(
        "Retrieved scopedAnomalyDetectionConfigMap with {} entries",
        scopedAnomalyDetectionConfigMap.size());

    TreeSet<AnomalyConfigScope> configScopes = new TreeSet<>(ANOMALY_CONFIG_SCOPE_COMPARATOR);
    configScopes.addAll(scopedAnomalyDetectionConfigMap.keySet());

    List<AiFirewallScopedConfigContext> scopedConfigContextList = new ArrayList<>();
    Map<AnomalyConfigScope, ScopeContext> scopeContextMap =
        AnomalyConfigScopeUtils.getScopeContextMap(
            requestContext, configScopes, cachedApiMappingProvider, cachedServiceMappingProvider);

    for (AnomalyConfigScope configScope : configScopes) {
      List<AnomalyConfigScope> scopesWithDecreasingPriority =
          AnomalyConfigScopeUtils.getConfigScopesWithDecreasingPriority(configScope);

      ScopedAnomalyDetectionConfig scopedAnomalyDetectionConfig =
          scopesWithDecreasingPriority.stream()
              .filter(scopedAnomalyDetectionConfigMap::containsKey)
              .findFirst()
              .map(scopedAnomalyDetectionConfigMap::get)
              .orElse(ScopedAnomalyDetectionConfig.getDefaultInstance());

      if (!scopedAnomalyDetectionConfig.equals(ScopedAnomalyDetectionConfig.getDefaultInstance())) {
        List<String> disabledSecRuleIds = new ArrayList<>();

        populateEvaluationConfigs(
            scopedAnomalyDetectionConfig,
            request.getRuleEvaluationPoint(),
            secRuleEvaluatedRuleIds,
            disabledSecRuleIds);

        ModelBasedEvaluationConfig modelBasedEvaluationConfig =
            buildModelBasedEvaluationConfig(
                modelBasedRuleIds, scopedAnomalyDetectionConfig, request.getRuleEvaluationPoint());

        AiFirewallScopedConfigContext scopedConfigContext =
            AiFirewallScopedConfigContext.newBuilder()
                .setScopeContext(scopeContextMap.get(configScope))
                .setSecRulesEvaluationConfig(
                    SecRulesEvaluationConfig.newBuilder()
                        .addAllDisabledSecRuleIds(disabledSecRuleIds))
                .setModelBasedEvaluationConfig(modelBasedEvaluationConfig)
                .build();

        scopedConfigContextList.add(scopedConfigContext);
      }
    }
    AiFirewallConfigContext.Builder configContextBuilder =
        AiFirewallConfigContext.newBuilder()
            .addAllScopedConfigContexts(scopedConfigContextList)
            .setSecRulesBlob(aiAppRules.getAiAppRulesBlob());

    buildAndSetCustomSignatureConfigContext(
        requestContext,
        request,
        configContextBuilder,
        scopedAnomalyDetectionConfigMap,
        anomalyConfigScope);

    return configContextBuilder.build();
  }

  private void buildAndSetCustomSignatureConfigContext(
      RequestContext requestContext,
      GetAiAppEvaluationConfigContextRequest request,
      AiFirewallConfigContext.Builder configContextBuilder,
      Map<AnomalyConfigScope, ScopedAnomalyDetectionConfig> scopedAnomalyDetectionConfigMap,
      AnomalyConfigScope anomalyConfigScope) {
    CustomSignatureConfigContext.Builder customSigBuilder =
        CustomSignatureConfigContext.newBuilder();

    if (request.getRuleEvaluationPoint() != RuleEvaluationPoint.RULE_EVALUATION_POINT_PLATFORM) {
      List<DataType> dataTypes = fetchDataTypes(requestContext, request);
      if (!isPiiDetectionInPromptDisabled(scopedAnomalyDetectionConfigMap, anomalyConfigScope)) {
        addDatatypeRulesContext(
            requestContext, request.getRuleScope(), customSigBuilder, dataTypes);
      } else {
        log.debug(
            "GenAI detection config is disabled for scope {}; skipping datatype-based rules",
            anomalyConfigScope);
      }
    }

    CustomSignatureConfigContext customSigContext = customSigBuilder.build();
    if (customSigContext.getRuleContextsCount() > 0) {
      configContextBuilder.setCustomSignatureConfigContext(customSigContext);
    }
  }

  private boolean isPiiDetectionInPromptDisabled(
      Map<AnomalyConfigScope, ScopedAnomalyDetectionConfig> filteredConfigMap,
      AnomalyConfigScope requestScope) {
    return AnomalyConfigScopeUtils.getConfigScopesWithDecreasingPriority(requestScope).stream()
        .filter(filteredConfigMap::containsKey)
        .findFirst()
        .map(filteredConfigMap::get)
        .map(
            scopedConfig ->
                scopedConfig.getAnomalyDetectionConfigsList().stream()
                    .filter(
                        detectionConfig ->
                            detectionConfig
                                .getGenAiAnomalyDetectionConfig()
                                .hasPiiDetectedInPrompt())
                    .anyMatch(c -> c.getConfigStatus().getDisabled()))
        .orElse(false);
  }

  private List<DataType> fetchDataTypes(
      RequestContext requestContext, GetAiAppEvaluationConfigContextRequest request) {
    try {
      Optional<String> environmentName =
          request.getRuleScope().hasEnvironmentScope()
                  && !request.getRuleScope().getEnvironmentScope().getEnvironmentIdsList().isEmpty()
              ? Optional.of(
                  request.getRuleScope().getEnvironmentScope().getEnvironmentIdsList().get(0))
              : Optional.empty();

      List<ai.traceable.data.classification.config.service.v1.DataType> resolvedDataTypes =
          requestContext
              .call(
                  () ->
                      dataClassificationConfigServiceStub.getDataTypes(
                          GetDataTypesRequest.newBuilder()
                              .setResolveInheritedDetails(true)
                              .setOrdering(DataTypeOrdering.DATA_TYPE_ORDERING_EVALUATION_PRIORITY)
                              .setFilter(
                                  DataTypeFilter.newBuilder()
                                      .setEnabled(true)
                                      .setLegacyTypes(false))
                              .build()))
              .getDataTypesList();
      return protectionEngineDataTypeTranslator.translateDataTypes(
          resolvedDataTypes, environmentName);
    } catch (Exception e) {
      log.error("Failed to fetch data types for tenant: {}", requestContext.getTenantId(), e);
      return List.of();
    }
  }

  private void addDatatypeRulesContext(
      RequestContext requestContext,
      RuleScope requestScope,
      CustomSignatureConfigContext.Builder customSigBuilder,
      List<DataType> dataTypes) {
    try {
      List<AiAppCustomRule> datatypeRules =
          fetchDatatypeBasedCustomRules(requestContext, requestScope);
      if (datatypeRules.isEmpty()) {
        return;
      }

      DataClassificationInfo dataClassificationInfo =
          dataClassificationClient.getDataClassificationInfo(requestContext);

      CustomSignatureConfigContext datatypeContext =
          datatypeRuleToCustomSignatureConfigConverter.convert(
              datatypeRules, dataClassificationInfo);

      for (CustomSignatureRulesContext ruleContext : datatypeContext.getRuleContextsList()) {
        customSigBuilder.addRuleContexts(
            ruleContext.toBuilder().addAllDataTypes(dataTypes).build());
      }
    } catch (Exception e) {
      log.error("Error building custom signature config context from datatype-based rules", e);
    }
  }

  private List<AiAppCustomRule> fetchDatatypeBasedCustomRules(
      RequestContext requestContext, RuleScope requestScope) {
    GetAiAppRulesRequest rulesRequest =
        GetAiAppRulesRequest.newBuilder().setRuleScope(requestScope).build();
    GetAiAppRulesResponse response =
        requestContext.call(() -> aiAppConfigService.getAiAppRules(rulesRequest));

    return response.getAiAppRulesList().stream()
        .flatMap(aiAppRule -> aiAppRule.getAiAppSubRulesList().stream())
        .filter(AiAppSubRule::hasCustomRule)
        .map(AiAppSubRule::getCustomRule)
        .filter(
            customRule ->
                customRule.hasRuleData()
                    && (customRule.getRuleData().hasPiiDetectedInPromptRuleData()
                        || customRule.getRuleData().hasAiSensitiveDataProtectionRuleData())
                    && customRule.getRuleData().getEnabled())
        .filter(
            customRule ->
                customRule.getRuleData().hasAction()
                    && customRule.getRuleData().getAction().hasBlock())
        .filter(customRule -> matchesRequestScope(customRule, requestScope))
        .collect(Collectors.toList());
  }

  private boolean matchesRequestScope(AiAppCustomRule rule, RuleScope requestScope) {
    RuleScope ruleScope = rule.getRuleData().getRuleScope();

    // Tenant-scoped rules or rules without explicit scope always match any request
    if (ruleScope.hasTenantScope()
        || ruleScope.getScopeCase() == RuleScope.ScopeCase.SCOPE_NOT_SET) {
      return true;
    }

    // If request has no environment scope (tenant or unset), all rules match
    if (!requestScope.hasEnvironmentScope()) {
      return true;
    }

    // Environment-scoped rules match only if they share at least one environment with the request
    List<String> requestEnvIds = requestScope.getEnvironmentScope().getEnvironmentIdsList();
    return ruleScope.getEnvironmentScope().getEnvironmentIdsList().stream()
        .anyMatch(requestEnvIds::contains);
  }

  private Set<String> getSecRuleEvaluatedRuleIds(AiAppRules aiAppRules) {
    return aiAppRules.getThreatRulesList().stream()
        .filter(rule -> rule.hasRuleEvaluation() && rule.getRuleEvaluation().hasSecRuleEvaluation())
        .map(AiAppThreatRule::getRuleId)
        .collect(Collectors.toUnmodifiableSet());
  }

  private Set<String> getModelBasedRuleIds(AiAppRules aiAppRules) {
    return aiAppRules.getThreatRulesList().stream()
        .filter(
            rule ->
                rule.hasRuleEvaluation() && rule.getRuleEvaluation().hasModelBasedRuleEvaluation())
        .map(AiAppThreatRule::getRuleId)
        .collect(Collectors.toUnmodifiableSet());
  }

  private ModelBasedEvaluationConfig buildModelBasedEvaluationConfig(
      Set<String> allModelBasedRuleIds,
      ScopedAnomalyDetectionConfig scopedAnomalyDetectionConfig,
      RuleEvaluationPoint ruleEvaluationPoint) {
    ModelBasedEvaluationConfig.Builder builder = ModelBasedEvaluationConfig.newBuilder();
    for (AnomalyDetectionConfig detectionConfig :
        scopedAnomalyDetectionConfig.getAnomalyDetectionConfigsList()) {
      if (!detectionConfig.hasGenAiAnomalyDetectionConfig()) {
        continue;
      }
      Map<String, AnomalySubRuleConfig> subRuleConfigMap =
          detectionConfig
              .getGenAiAnomalyDetectionConfig()
              .getSubRuleConfigs()
              .getSubRuleConfigsMap();
      for (String ruleId : allModelBasedRuleIds) {
        if (!subRuleConfigMap.containsKey(ruleId)) {
          continue;
        }
        AnomalySubRuleConfig subRuleConfig = subRuleConfigMap.get(ruleId);
        if (isDisabled(detectionConfig, subRuleConfig, ruleEvaluationPoint)) {
          continue;
        }
        Value modelIdValue = subRuleConfig.getConfigParamsMap().get(MODEL_ID);
        Value matchedCategoryValue = subRuleConfig.getConfigParamsMap().get(MATCHED_CATEGORY);

        if (modelIdValue == null || modelIdValue.getStringValue().isBlank()) {
          log.debug("Rule {} has no modelId in configParams, skipping", ruleId);
          continue;
        }
        ModelBasedRuleConfig.Builder ruleConfigBuilder =
            ModelBasedRuleConfig.newBuilder().setModelId(modelIdValue.getStringValue());
        if (matchedCategoryValue != null && !matchedCategoryValue.getStringValue().isBlank()) {
          ruleConfigBuilder.setMatchedCategory(matchedCategoryValue.getStringValue());
        }
        builder.putRuleIdToConfig(ruleId, ruleConfigBuilder.build());
      }
    }
    return builder.build();
  }

  private void populateEvaluationConfigs(
      ScopedAnomalyDetectionConfig scopedAnomalyDetectionConfig,
      RuleEvaluationPoint ruleEvaluationPoint,
      Set<String> secRuleEvaluatedRuleIds,
      List<String> disabledSecRuleIds) {

    for (AnomalyDetectionConfig detectionConfig :
        scopedAnomalyDetectionConfig.getAnomalyDetectionConfigsList()) {
      if (!detectionConfig.hasGenAiAnomalyDetectionConfig()) {
        continue;
      }

      Map<String, AnomalySubRuleConfig> subRuleConfigMap =
          detectionConfig
              .getGenAiAnomalyDetectionConfig()
              .getSubRuleConfigs()
              .getSubRuleConfigsMap();

      for (Map.Entry<String, AnomalySubRuleConfig> subRuleEntry : subRuleConfigMap.entrySet()) {
        String subRuleId = subRuleEntry.getKey();
        AnomalySubRuleConfig subRuleConfig = subRuleEntry.getValue();

        if (secRuleEvaluatedRuleIds.contains(subRuleId)
            && isDisabled(detectionConfig, subRuleConfig, ruleEvaluationPoint)) {
          sanitiseSubRuleId(subRuleId).ifPresent(disabledSecRuleIds::add);
        }
      }
    }
  }

  private Optional<String> sanitiseSubRuleId(String subRuleId) {
    int underscoreIndex = subRuleId.lastIndexOf('_');
    if (underscoreIndex != -1 && underscoreIndex < subRuleId.length() - 1) {
      return Optional.of(subRuleId.substring(underscoreIndex + 1));
    }
    return Optional.empty();
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
        return isDisabled(detectionConfig, subRuleConfig)
            && !isInternal(detectionConfig, subRuleConfig);
      default:
        log.error("Unsupported rule evaluation point: {}", ruleEvaluationPoint);
        throw new IllegalArgumentException(
            "Unsupported rule evaluation point: " + ruleEvaluationPoint);
    }
  }

  private boolean isDisabled(
      AnomalyDetectionConfig threatTypeDetectionConfig, AnomalySubRuleConfig subRuleConfig) {
    return threatTypeDetectionConfig.getConfigStatus().getDisabled()
        || subRuleConfig
            .getAnomalyRuleAction()
            .equals(AnomalyRuleAction.ANOMALY_RULE_ACTION_DISABLE);
  }

  private boolean isInternal(
      AnomalyDetectionConfig threatTypeDetectionConfig, AnomalySubRuleConfig subRuleConfig) {
    return threatTypeDetectionConfig.getConfigStatus().getInternal() || subRuleConfig.getInternal();
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

  private AnomalyConfigScope convertRuleScopeToAnomalyConfigScope(RuleScope ruleScope) {
    AnomalyConfigScope.Builder builder = AnomalyConfigScope.newBuilder();
    if (ruleScope.hasTenantScope()) {
      builder.setCustomerScope(AnomalyCustomerScope.getDefaultInstance());
    } else if (ruleScope.hasEnvironmentScope()) {
      if (!ruleScope.getEnvironmentScope().getEnvironmentIdsList().isEmpty()) {
        builder.setEnvironmentScope(
            AnomalyEnvironmentScope.newBuilder()
                .setEnvironmentId(ruleScope.getEnvironmentScope().getEnvironmentIdsList().get(0))
                .build());
      }
    }
    return builder.build();
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
}
