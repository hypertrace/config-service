package ai.traceable.anomaly.config.service.modsec.protection.engine;

import static ai.traceable.anomaly.config.service.modsec.Utils.getModsecAnomalyRuleConfigMap;
import static ai.traceable.anomaly.config.service.modsec.Utils.getSubRuleConfigMap;

import ai.traceable.anomaly.config.service.detector.anomalydetection.AnomalyDetectionConfigManager;
import ai.traceable.anomaly.config.service.global.status.GlobalAnomalyConfigStatusManager;
import ai.traceable.anomaly.config.service.modsec.rules.ModsecManager;
import ai.traceable.anomaly.config.service.registry.modsec.ModsecRulesRegistry;
import ai.traceable.anomaly.config.service.v1.AnomalyConfigScope;
import ai.traceable.anomaly.config.service.v1.AnomalyRuleInfo;
import ai.traceable.anomaly.config.service.v1.AnomalySubRuleInfo;
import ai.traceable.anomaly.config.service.v1.AnomalySubRuleType;
import ai.traceable.anomaly.config.service.v1.detector.AnomalyDetectionConfig;
import ai.traceable.anomaly.config.service.v1.detector.AnomalyDetectionConfigType;
import ai.traceable.anomaly.config.service.v1.detector.AnomalySubRuleConfig;
import ai.traceable.anomaly.config.service.v1.detector.GetAnomalyDetectionConfigsFilter;
import ai.traceable.anomaly.config.service.v1.detector.ModsecurityAnomalyDetectionConfig;
import ai.traceable.anomaly.config.service.v1.detector.ScopedAnomalyDetectionConfig;
import ai.traceable.anomaly.config.service.v1.global.GlobalModsecConfig;
import ai.traceable.anomaly.config.service.v1.global.ScopedAnomalyConfigStatus;
import ai.traceable.anomaly.config.service.v1.modsec.GetWebAppEvaluationConfigContextRequest;
import ai.traceable.anomaly.config.service.v1.modsec.ModsecRuleVersion;
import ai.traceable.anomaly.config.service.v1.modsec.RuleEvaluationPoint;
import ai.traceable.protection.engine.config.webapp.v1.SecRuleProcessorConfig;
import ai.traceable.protection.engine.config.webapp.v1.WebAppEvaluationConfig;
import ai.traceable.protection.engine.config.webapp.v1.WebAppEvaluationConfigContext;
import ai.traceable.protection.engine.config.webapp.v1.WebAppEvaluationRulesContext;
import ai.traceable.protection.processing.common.v1.CustomerScope;
import ai.traceable.protection.processing.common.v1.EntityScope;
import ai.traceable.protection.processing.common.v1.EntityType;
import ai.traceable.protection.processing.common.v1.Scope;
import ai.traceable.protection.processing.common.v1.ScopeContext;
import ai.traceable.protection.processor.secrules.v1.CorazaEngineVersion;
import ai.traceable.protection.processor.secrules.v1.CorazaRuleDirectivesType;
import ai.traceable.protection.processor.secrules.v1.CorazaRuleProcessor;
import ai.traceable.protection.processor.secrules.v1.ModsecJniRuleDirectivesType;
import ai.traceable.protection.processor.secrules.v1.ModsecJniRuleProcessor;
import ai.traceable.protection.processor.secrules.v1.SecRuleProcessorDetails;
import com.google.inject.Inject;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.hypertrace.core.grpcutils.context.RequestContext;

public class WebAppEvaluationConfigContextManagerImpl
    implements WebAppEvaluationConfigContextManager {

  private static final GetAnomalyDetectionConfigsFilter ANOMALY_DETECTION_CONFIGS_FILTER =
      GetAnomalyDetectionConfigsFilter.newBuilder()
          .addAnomalyDetectionConfigTypes(
              AnomalyDetectionConfigType.ANOMALY_DETECTION_CONFIG_TYPE_MODSECURITY)
          .build();
  private static final Map<AnomalyConfigScope.ScopeCase, Integer> SCOPE_ORDER =
      Map.of(
          AnomalyConfigScope.ScopeCase.API_SCOPE,
          1,
          AnomalyConfigScope.ScopeCase.SERVICE_SCOPE,
          2,
          AnomalyConfigScope.ScopeCase.ENVIRONMENT_SCOPE,
          3,
          AnomalyConfigScope.ScopeCase.CUSTOMER_SCOPE,
          4);
  private static final List<AnomalySubRuleType> ALL_SUB_RULE_TYPES =
      List.of(
          AnomalySubRuleType.ANOMALY_SUB_RULE_TYPE_BLOCK,
          AnomalySubRuleType.ANOMALY_SUB_RULE_TYPE_SAFE,
          AnomalySubRuleType.ANOMALY_SUB_RULE_TYPE_REGULAR);
  private final ModsecManager modsecManager;
  private final ModsecRulesRegistry modsecRulesRegistry;
  private final AnomalyDetectionConfigManager anomalyDetectionConfigManager;
  private final GlobalAnomalyConfigStatusManager globalAnomalyConfigStatusManager;
  private final ModsecRuleVersion defaultModsecRuleVersion;

  @Inject
  public WebAppEvaluationConfigContextManagerImpl(
      ModsecManager modsecManager,
      ModsecRulesRegistry modsecRulesRegistry,
      AnomalyDetectionConfigManager anomalyDetectionConfigManager,
      GlobalAnomalyConfigStatusManager globalAnomalyConfigStatusManager,
      ModsecRuleVersion defaultModsecRuleVersion) {
    this.modsecManager = modsecManager;
    this.modsecRulesRegistry = modsecRulesRegistry;
    this.anomalyDetectionConfigManager = anomalyDetectionConfigManager;
    this.globalAnomalyConfigStatusManager = globalAnomalyConfigStatusManager;
    this.defaultModsecRuleVersion = defaultModsecRuleVersion;
  }

  @Override
  public WebAppEvaluationConfigContext getWebAppEvaluationConfigContext(
      RequestContext requestContext, GetWebAppEvaluationConfigContextRequest request) {

    Map<AnomalyConfigScope, ScopedAnomalyDetectionConfig> scopedAnomalyDetectionConfigMap =
        getScopedAnomalyDetectionConfigMap(requestContext);
    Map<AnomalyConfigScope, ScopedAnomalyConfigStatus> scopedAnomalyConfigStatusMap =
        getScopedAnomalyConfigStatusMap(requestContext);

    Set<AnomalyConfigScope> configScopes =
        new TreeSet<>(
            Comparator.comparing(
                scope -> SCOPE_ORDER.getOrDefault(scope.getScopeCase(), Integer.MAX_VALUE)));
    configScopes.addAll(scopedAnomalyDetectionConfigMap.keySet());
    configScopes.addAll(scopedAnomalyConfigStatusMap.keySet());

    List<WebAppEvaluationConfig> webAppEvaluationConfigs = new ArrayList<>();
    List<SecRuleProcessorConfig> secRuleProcessorConfigs = new ArrayList<>();
    List<WebAppEvaluationRulesContext> webAppEvaluationRulesContextList = new ArrayList<>();

    for (AnomalyConfigScope configScope : configScopes) {
      ScopedAnomalyDetectionConfig scopedAnomalyDetectionConfig =
          scopedAnomalyDetectionConfigMap.getOrDefault(
              configScope, ScopedAnomalyDetectionConfig.getDefaultInstance());
      ScopedAnomalyConfigStatus scopedAnomalyConfigStatus =
          scopedAnomalyConfigStatusMap.getOrDefault(
              configScope, ScopedAnomalyConfigStatus.getDefaultInstance());

      ModsecRuleVersion modsecRuleVersion = getModsecRuleVersion(scopedAnomalyDetectionConfig);

      boolean useTestRules = scopedAnomalyConfigStatus.getGlobalModsecConfig().getUseTestRules();
      List<AnomalySubRuleType> anomalySubRuleTypes =
          getAnomalySubRuleTypes(
              scopedAnomalyConfigStatus,
              request.getRuleEvaluationPoint(),
              request.getSubRuleTypesList());

      if (!scopedAnomalyConfigStatus.equals(ScopedAnomalyConfigStatus.getDefaultInstance())) {
        secRuleProcessorConfigs.add(
            getSecRuleProcessorConfig(scopedAnomalyConfigStatus, modsecRuleVersion));
      }
      if (!scopedAnomalyDetectionConfig.equals(ScopedAnomalyDetectionConfig.getDefaultInstance())) {
        Optional<WebAppEvaluationConfig> webAppEvaluationConfig =
            getWebAppEvaluationConfig(
                scopedAnomalyDetectionConfig,
                modsecRuleVersion,
                request.getRuleEvaluationPoint(),
                useTestRules);

        webAppEvaluationConfig.ifPresent(webAppEvaluationConfigs::add);

        webAppEvaluationRulesContextList.add(
            getWebAppEvaluationRulesContext(
                scopedAnomalyDetectionConfig,
                modsecRuleVersion,
                anomalySubRuleTypes,
                useTestRules));
      }
    }

    return WebAppEvaluationConfigContext.newBuilder()
        .addAllEvaluationConfigs(webAppEvaluationConfigs)
        .addAllSecRuleProcessorConfigs(secRuleProcessorConfigs)
        .addAllWebAppEvaluationRulesContexts(webAppEvaluationRulesContextList)
        .build();
  }

  private List<AnomalySubRuleType> getAnomalySubRuleTypes(
      ScopedAnomalyConfigStatus scopedAnomalyConfigStatus,
      RuleEvaluationPoint ruleEvaluationPoint,
      List<AnomalySubRuleType> subRuleTypes) {
    switch (ruleEvaluationPoint) {
      case RULE_EVALUATION_POINT_EDGE:
        if (!scopedAnomalyConfigStatus
            .getGlobalModsecConfig()
            .getBlockingAvailableForRegularRules()) {
          return filterSubRuleTypes(
              subRuleTypes,
              List.of(
                  AnomalySubRuleType.ANOMALY_SUB_RULE_TYPE_SAFE,
                  AnomalySubRuleType.ANOMALY_SUB_RULE_TYPE_BLOCK));
        }
        return filterSubRuleTypes(subRuleTypes, ALL_SUB_RULE_TYPES);
      case RULE_EVALUATION_POINT_PLATFORM:
        return filterSubRuleTypes(subRuleTypes, ALL_SUB_RULE_TYPES);
      default:
        throw new IllegalArgumentException(
            "Unsupported rule evaluation point: " + ruleEvaluationPoint);
    }
  }

  private List<AnomalySubRuleType> filterSubRuleTypes(
      List<AnomalySubRuleType> subRuleTypes, List<AnomalySubRuleType> allowedTypes) {
    return subRuleTypes.isEmpty()
        ? allowedTypes
        : subRuleTypes.stream().filter(allowedTypes::contains).collect(Collectors.toList());
  }

  private WebAppEvaluationRulesContext getWebAppEvaluationRulesContext(
      ScopedAnomalyDetectionConfig scopedAnomalyDetectionConfig,
      ModsecRuleVersion modsecRuleVersion,
      List<AnomalySubRuleType> anomalySubRuleTypes,
      boolean useTestRules) {
    ScopeContext scopeContext = getScopeContext(scopedAnomalyDetectionConfig.getConfigScope());

    return WebAppEvaluationRulesContext.newBuilder()
        .setScopeContext(scopeContext)
        .setCrsRulesBlob(
            modsecManager
                .getModsecCrsRules(anomalySubRuleTypes, modsecRuleVersion, useTestRules)
                .getAggregatedModsecBlob())
        .build();
  }

  private Optional<WebAppEvaluationConfig> getWebAppEvaluationConfig(
      ScopedAnomalyDetectionConfig scopedAnomalyDetectionConfig,
      ModsecRuleVersion modsecRuleVersion,
      RuleEvaluationPoint ruleEvaluationPoint,
      boolean useTestRules) {
    ScopeContext scopeContext = getScopeContext(scopedAnomalyDetectionConfig.getConfigScope());
    Set<String> disabledRuleIds =
        getDisabledModsecRuleIds(
            scopedAnomalyDetectionConfig, modsecRuleVersion, ruleEvaluationPoint, useTestRules);
    if (disabledRuleIds.isEmpty()) {
      return Optional.empty();
    }
    return Optional.of(
        WebAppEvaluationConfig.newBuilder()
            .setScopeContext(scopeContext)
            .addAllDisabledSecRuleIds(disabledRuleIds)
            .build());
  }

  private Set<String> getDisabledModsecRuleIds(
      ScopedAnomalyDetectionConfig scopedAnomalyDetectionConfig,
      ModsecRuleVersion modsecRuleVersion,
      RuleEvaluationPoint ruleEvaluationPoint,
      boolean useTestRules) {
    Map<String, AnomalyDetectionConfig> anomalyRuleConfigMap =
        getModsecAnomalyRuleConfigMap(scopedAnomalyDetectionConfig);
    Map<String, AnomalyRuleInfo> ruleInfoMap =
        modsecRulesRegistry.getModsecRuleInfos(modsecRuleVersion, useTestRules);

    // The disabled modsec rule ids should be ordered to ensure that
    // the blob doesn't keep changing on repeated calls
    Set<String> disabledModsecRuleIds = new TreeSet<>();

    for (Map.Entry<String, AnomalyRuleInfo> entry : ruleInfoMap.entrySet()) {
      String ruleId = entry.getKey();
      AnomalyRuleInfo anomalyRuleInfo = entry.getValue();
      AnomalyDetectionConfig detectionConfig =
          anomalyRuleConfigMap.getOrDefault(ruleId, AnomalyDetectionConfig.getDefaultInstance());

      Map<String, AnomalySubRuleConfig> subRuleConfigMap = getSubRuleConfigMap(detectionConfig);
      for (AnomalySubRuleInfo subRuleInfo : anomalyRuleInfo.getSubRuleInfosList()) {
        String subRuleId = subRuleInfo.getRuleId();
        AnomalySubRuleConfig subRuleConfig =
            subRuleConfigMap.getOrDefault(subRuleId, AnomalySubRuleConfig.getDefaultInstance());

        boolean isDisabled = isDisabled(detectionConfig, subRuleConfig, ruleEvaluationPoint);
        if (isDisabled) {
          disabledModsecRuleIds.add(subRuleId);
        }
      }
    }
    return disabledModsecRuleIds;
  }

  private boolean isDisabled(
      AnomalyDetectionConfig detectionConfig,
      AnomalySubRuleConfig subRuleConfig,
      RuleEvaluationPoint ruleEvaluationPoint) {
    switch (ruleEvaluationPoint) {
      case RULE_EVALUATION_POINT_EDGE:
        return detectionConfig.getConfigStatus().getDisabled()
            || subRuleConfig.getConfigStatus().getDisabled()
            || !subRuleConfig.getBlockingEnabled();
      case RULE_EVALUATION_POINT_PLATFORM:
        return detectionConfig.getConfigStatus().getDisabled()
            || subRuleConfig.getConfigStatus().getDisabled();
      default:
        throw new IllegalArgumentException(
            "Unsupported rule evaluation point: " + ruleEvaluationPoint);
    }
  }

  private SecRuleProcessorConfig getSecRuleProcessorConfig(
      ScopedAnomalyConfigStatus configStatus, ModsecRuleVersion modsecRuleVersion) {
    ScopeContext scopeContext = getScopeContext(configStatus.getConfigScope());
    GlobalModsecConfig globalModsecConfig = configStatus.getGlobalModsecConfig();
    CorazaEngineVersion corazaEngineVersion =
        getCorazaEngineVersion(
            globalModsecConfig.getModsecEvaluationEngineConfig().getCorazaEngineVersion());

    return SecRuleProcessorConfig.newBuilder()
        .setProcessorDetails(getSecRuleProcessorDetails(modsecRuleVersion, corazaEngineVersion))
        .setScopeContext(scopeContext)
        .build();
  }

  private SecRuleProcessorDetails getSecRuleProcessorDetails(
      ModsecRuleVersion ruleVersion, CorazaEngineVersion corazaEngineVersion) {
    switch (ruleVersion) {
      case MODSEC_RULE_VERSION_V3:
      case MODSEC_RULE_VERSION_TEST_V3:
        return SecRuleProcessorDetails.newBuilder()
            .setModsecRuleProcessor(
                ModsecJniRuleProcessor.newBuilder()
                    .setDirectivesType(
                        ModsecJniRuleDirectivesType.MODSEC_JNI_RULE_DIRECTIVES_TYPE_BASIC))
            .build();
      case MODSEC_RULE_VERSION_V3_SECARG_LIMITS:
      case MODSEC_RULE_VERSION_TEST_V3_SECARG_LIMITS:
        return SecRuleProcessorDetails.newBuilder()
            .setModsecRuleProcessor(
                ModsecJniRuleProcessor.newBuilder()
                    .setDirectivesType(
                        ModsecJniRuleDirectivesType.MODSEC_JNI_RULE_DIRECTIVES_TYPE_SECARGLIMITS))
            .build();
      case MODSEC_RULE_VERSION_V3_SECARG_LIMITS_DETECTION_ONLY_MODE:
      case MODSEC_RULE_VERSION_TEST_V3_SECARG_LIMITS_DETECTION_ONLY_MODE:
        return SecRuleProcessorDetails.newBuilder()
            .setModsecRuleProcessor(
                ModsecJniRuleProcessor.newBuilder()
                    .setDirectivesType(
                        ModsecJniRuleDirectivesType
                            .MODSEC_JNI_RULE_DIRECTIVES_TYPE_SECARGLIMITS_DETECTION_ONLY))
            .build();
      case MODSEC_RULE_VERSION_SENSITIVE_AGENT_V3:
        return SecRuleProcessorDetails.newBuilder()
            .setModsecRuleProcessor(
                ModsecJniRuleProcessor.newBuilder()
                    .setDirectivesType(
                        ModsecJniRuleDirectivesType
                            .MODSEC_JNI_RULE_DIRECTIVES_TYPE_SENSITIVE_AGENT_V3))
            .build();
      case MODSEC_RULE_VERSION_CORAZA_V3:
      case MODSEC_RULE_VERSION_TEST_CORAZA_V3:
        return SecRuleProcessorDetails.newBuilder()
            .setCorazaRuleProcessor(
                CorazaRuleProcessor.newBuilder()
                    .setCorazaEngineVersion(corazaEngineVersion)
                    .setDirectivesType(CorazaRuleDirectivesType.CORAZA_RULE_DIRECTIVES_TYPE_BASIC))
            .build();
      case MODSEC_RULE_VERSION_CORAZA_V3_DETECTION_ONLY_MODE:
      case MODSEC_RULE_VERSION_TEST_CORAZA_V3_DETECTION_ONLY_MODE:
        return SecRuleProcessorDetails.newBuilder()
            .setCorazaRuleProcessor(
                CorazaRuleProcessor.newBuilder()
                    .setCorazaEngineVersion(corazaEngineVersion)
                    .setDirectivesType(
                        CorazaRuleDirectivesType.CORAZA_RULE_DIRECTIVES_TYPE_DETECTION_ONLY))
            .build();
      case MODSEC_RULE_VERSION_SENSITIVE_AGENT_CORAZA_V3:
        return SecRuleProcessorDetails.newBuilder()
            .setCorazaRuleProcessor(
                CorazaRuleProcessor.newBuilder()
                    .setCorazaEngineVersion(corazaEngineVersion)
                    .setDirectivesType(
                        CorazaRuleDirectivesType.CORAZA_RULE_DIRECTIVES_TYPE_SENSITIVE_AGENT_V3))
            .build();
      default:
        throw new IllegalArgumentException("Unsupported ModsecRuleVersion: " + ruleVersion.name());
    }
  }

  private CorazaEngineVersion getCorazaEngineVersion(
      ai.traceable.anomaly.config.service.v1.global.CorazaEngineVersion corazaEngineVersion) {
    switch (corazaEngineVersion) {
      case CORAZA_ENGINE_VERSION_LATEST_TEST:
        return CorazaEngineVersion.CORAZA_ENGINE_VERSION_LATEST_TEST;
      case CORAZA_ENGINE_VERSION_LATEST_STABLE:
      default:
        return CorazaEngineVersion.CORAZA_ENGINE_VERSION_LATEST_STABLE;
    }
  }

  private ScopeContext getScopeContext(AnomalyConfigScope scope) {
    switch (scope.getScopeCase()) {
      case CUSTOMER_SCOPE:
        return ScopeContext.newBuilder()
            .addScopes(Scope.newBuilder().setCustomerScope(CustomerScope.getDefaultInstance()))
            .build();
      case ENVIRONMENT_SCOPE:
        return ScopeContext.newBuilder()
            .addScopes(
                Scope.newBuilder()
                    .setEntityScope(
                        EntityScope.newBuilder()
                            .setEntityType(EntityType.ENTITY_TYPE_ENVIRONMENT)
                            .addEntityIds(scope.getEnvironmentScope().getEnvironmentId())))
            .build();
      case SERVICE_SCOPE:
        return ScopeContext.newBuilder()
            .addScopes(
                Scope.newBuilder()
                    .setEntityScope(
                        EntityScope.newBuilder()
                            .setEntityType(EntityType.ENTITY_TYPE_SERVICE)
                            .addEntityIds(scope.getServiceScope().getId())))
            .build();
      case API_SCOPE:
        return ScopeContext.newBuilder()
            .addScopes(
                Scope.newBuilder()
                    .setEntityScope(
                        EntityScope.newBuilder()
                            .setEntityType(EntityType.ENTITY_TYPE_API)
                            .addEntityIds(scope.getApiScope().getId())))
            .build();
      default:
        throw new IllegalArgumentException("Unsupported scope type: " + scope.getScopeCase());
    }
  }

  private ModsecRuleVersion getModsecRuleVersion(
      ScopedAnomalyDetectionConfig scopedAnomalyDetectionConfig) {
    Optional<ModsecRuleVersion> modsecRuleVersion =
        scopedAnomalyDetectionConfig.getAnomalyDetectionConfigsList().stream()
            .filter(AnomalyDetectionConfig::hasModsecurityAnomalyDetectionConfig)
            .map(AnomalyDetectionConfig::getModsecurityAnomalyDetectionConfig)
            .filter(ModsecurityAnomalyDetectionConfig::hasModsecAllDetection)
            .map(modsecConfig -> modsecConfig.getModsecAllDetection().getModsecRuleVersion())
            .findFirst();
    if (modsecRuleVersion.isPresent()
        && modsecRuleVersion.get() != ModsecRuleVersion.MODSEC_RULE_VERSION_UNSPECIFIED) {
      return modsecRuleVersion.get();
    }
    return defaultModsecRuleVersion;
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
}
