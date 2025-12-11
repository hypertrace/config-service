package ai.traceable.anomaly.config.service.modsec.rules;

import ai.traceable.anomaly.config.service.detector.anomalydetection.AnomalyDetectionConfigManager;
import ai.traceable.anomaly.config.service.global.AnomalyGlobalConfigServiceConfig;
import ai.traceable.anomaly.config.service.global.ruleinfo.WebAppRuleInfoProvider;
import ai.traceable.anomaly.config.service.global.status.GlobalAnomalyConfigStatusManager;
import ai.traceable.anomaly.config.service.registry.modsec.ModsecRulesRegistry;
import ai.traceable.anomaly.config.service.v1.AnomalyConfigScope;
import ai.traceable.anomaly.config.service.v1.AnomalyRuleAction;
import ai.traceable.anomaly.config.service.v1.AnomalyRuleInfo;
import ai.traceable.anomaly.config.service.v1.AnomalySubRuleInfo;
import ai.traceable.anomaly.config.service.v1.AnomalySubRuleType;
import ai.traceable.anomaly.config.service.v1.RuleVersion;
import ai.traceable.anomaly.config.service.v1.detector.AnomalyDetectionConfig;
import ai.traceable.anomaly.config.service.v1.detector.AnomalyDetectionConfigType;
import ai.traceable.anomaly.config.service.v1.detector.AnomalySubRuleConfig;
import ai.traceable.anomaly.config.service.v1.detector.GetAnomalyDetectionConfigsFilter;
import ai.traceable.anomaly.config.service.v1.global.ScopedAnomalyConfigStatus;
import ai.traceable.anomaly.config.service.v1.modsec.ModsecCrsRulesTarget;
import ai.traceable.anomaly.config.service.v1.modsec.ModsecRuleVersion;
import ai.traceable.config.service.feature.caching.client.FeatureCachingClient;
import jakarta.inject.Inject;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.Function;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.hypertrace.core.grpcutils.context.RequestContext;

@Slf4j
public class ModsecManagerImpl implements ModsecManager {
  private static final List<AnomalySubRuleType> ALL_SUB_RULE_TYPES =
      List.of(
          AnomalySubRuleType.ANOMALY_SUB_RULE_TYPE_BLOCK,
          AnomalySubRuleType.ANOMALY_SUB_RULE_TYPE_SAFE,
          AnomalySubRuleType.ANOMALY_SUB_RULE_TYPE_REGULAR);
  private static final GetAnomalyDetectionConfigsFilter ANOMALY_DETECTION_CONFIGS_FILTER =
      GetAnomalyDetectionConfigsFilter.newBuilder()
          .addAnomalyDetectionConfigTypes(
              AnomalyDetectionConfigType.ANOMALY_DETECTION_CONFIG_TYPE_MODSECURITY)
          .build();

  private final ModsecRulesRegistry modsecRulesRegistry;
  private final AnomalyGlobalConfigServiceConfig config;
  private final WebAppRuleInfoProvider webAppRuleInfoProvider;
  private final FeatureCachingClient featureCachingClient;
  private final AnomalyDetectionConfigManager anomalyDetectionConfigManager;
  private final GlobalAnomalyConfigStatusManager globalAnomalyConfigStatusManager;

  @Inject
  public ModsecManagerImpl(
      ModsecRulesRegistry modsecRulesRegistry,
      AnomalyGlobalConfigServiceConfig config,
      WebAppRuleInfoProvider webAppRuleInfoProvider,
      FeatureCachingClient featureCachingClient,
      AnomalyDetectionConfigManager anomalyDetectionConfigManager,
      GlobalAnomalyConfigStatusManager globalAnomalyConfigStatusManager) {
    this.modsecRulesRegistry = modsecRulesRegistry;
    this.config = config;
    this.webAppRuleInfoProvider = webAppRuleInfoProvider;
    this.featureCachingClient = featureCachingClient;
    this.anomalyDetectionConfigManager = anomalyDetectionConfigManager;
    this.globalAnomalyConfigStatusManager = globalAnomalyConfigStatusManager;
  }

  /**
   * we are not taking useTest or experimental version in account here, as this is used in blocking
   * config service and there is no point of fetching the experimental/test rules in blocking, so we
   * are using useTest = false[deprecated flow] and version = current [ignoring the experimental
   * version if set]
   */
  @Override
  public ModsecCrsRules getModsecCrsRules(
      RequestContext requestContext,
      ModsecRuleVersion modsecRuleVersion,
      ModsecCrsRulesTarget rulesTarget,
      List<AnomalySubRuleType> subRuleTypes,
      boolean removeDisabledRules,
      AnomalyConfigScope anomalyConfigScope) {
    // if edge is enabled and protection engine web app protection is enabled for tenant
    // then don't send TA blocking modsec rules since those will get evaluated in eds via
    // protection engine
    if (featureCachingClient.isEdgeDecisionEnabledForTenant(requestContext)
        && featureCachingClient.isProtectionEngineWebAppProtectionEnabledForTenant(requestContext)
        && rulesTarget.equals(ModsecCrsRulesTarget.MODSEC_CRS_RULES_TARGET_TA_BLOCKING)) {
      log.debug(
          "Not sending TA blocking modsec rules for tenant: {}", requestContext.getTenantId());
      return new ModsecCrsRules(subRuleTypes);
    } else if (featureCachingClient.isEdgeDecisionEnabledForTenant(requestContext)
        && featureCachingClient.isProtectionEngineWebAppProtectionEnabledForTenant(requestContext)
        && !rulesTarget.equals(ModsecCrsRulesTarget.MODSEC_CRS_RULES_TARGET_PLATFORM_DETECTION)) {
      log.debug(
          "Sending actual modsec rules for tenant: {}, target: {}",
          requestContext.getTenantId(),
          rulesTarget);
    }
    ScopedAnomalyConfigStatus globalConfig =
        globalAnomalyConfigStatusManager.getScopedAnomalyConfigStatus(
            requestContext, anomalyConfigScope);
    RuleVersion currentVersion =
        globalConfig.getGlobalModsecConfig().getRuleVersionData().getCurrentVersion();
    boolean isWAAPVersioningEnabledForTenant =
        featureCachingClient.isWAAPVersioningEnabledForTenant(requestContext);
    if (rulesTarget.equals(ModsecCrsRulesTarget.MODSEC_CRS_RULES_TARGET_PLATFORM_DETECTION)) {
      // Platform is tenant-agnostic
      globalConfig = ScopedAnomalyConfigStatus.getDefaultInstance();
    }

    subRuleTypes =
        subRuleTypes.isEmpty() ? getSubRuleTypes(rulesTarget, globalConfig) : subRuleTypes;
    if (subRuleTypes.isEmpty()) {
      return new ModsecCrsRules(subRuleTypes);
    }

    Set<String> disabledModsecRuleIds;
    if (removeDisabledRules) {
      boolean checkBlockingStatus =
          rulesTarget == ModsecCrsRulesTarget.MODSEC_CRS_RULES_TARGET_TA_BLOCKING
              || (subRuleTypes.size() == 1
                  && subRuleTypes.get(0).equals(AnomalySubRuleType.ANOMALY_SUB_RULE_TYPE_BLOCK));
      disabledModsecRuleIds =
          getDisabledModsecRuleIds(
              requestContext,
              checkBlockingStatus,
              anomalyConfigScope,
              modsecRuleVersion,
              isWAAPVersioningEnabledForTenant,
              currentVersion);
    } else {
      disabledModsecRuleIds = Set.of();
    }

    ModsecCrsRules.ModsecCrsRulesBuilder builder = ModsecCrsRules.builder();
    Map<AnomalySubRuleType, String> modsecBlobsForRuleTypes =
        subRuleTypes.stream()
            .collect(
                Collectors.toUnmodifiableMap(
                    Function.identity(),
                    subRuleType -> {
                      if (isWAAPVersioningEnabledForTenant
                          && currentVersion != null
                          && !currentVersion.getVersion().isEmpty()) {
                        return webAppRuleInfoProvider.getCrsRulesBlob(
                            List.of(subRuleType),
                            modsecRuleVersion,
                            disabledModsecRuleIds,
                            currentVersion,
                            true);
                      } else {
                        return modsecRulesRegistry.getModsecCrsRulesBlob(
                            List.of(subRuleType),
                            modsecRuleVersion,
                            disabledModsecRuleIds,
                            false,
                            true);
                      }
                    }));
    builder.modsecBlobsForRuleTypes(modsecBlobsForRuleTypes);
    if (subRuleTypes.size() == 1) {
      builder.aggregatedModsecBlob(modsecBlobsForRuleTypes.get(subRuleTypes.get(0)));
    } else {
      if (isWAAPVersioningEnabledForTenant
          && currentVersion != null
          && !currentVersion.getVersion().isEmpty()) {
        builder.aggregatedModsecBlob(
            webAppRuleInfoProvider.getCrsRulesBlob(
                subRuleTypes, modsecRuleVersion, disabledModsecRuleIds, currentVersion, true));
      } else {
        builder.aggregatedModsecBlob(
            modsecRulesRegistry.getModsecCrsRulesBlob(
                subRuleTypes, modsecRuleVersion, disabledModsecRuleIds, false, true));
      }
    }

    return builder.build();
  }

  @Override
  public ModsecCrsRules getModsecCrsRules(
      List<AnomalySubRuleType> subRuleTypes,
      ModsecRuleVersion modsecRuleVersion,
      boolean useTestModsecRules,
      RuleVersion ruleVersion,
      boolean includeDirectives) {

    if (subRuleTypes.isEmpty()) {
      subRuleTypes = ALL_SUB_RULE_TYPES;
    }

    ModsecCrsRules.ModsecCrsRulesBuilder builder = ModsecCrsRules.builder();
    Map<AnomalySubRuleType, String> modsecBlobsForRuleTypes =
        subRuleTypes.stream()
            .collect(
                Collectors.toUnmodifiableMap(
                    Function.identity(),
                    subRuleType -> {
                      if (ruleVersion != null && !ruleVersion.getVersion().isEmpty()) {
                        return webAppRuleInfoProvider.getCrsRulesBlob(
                            List.of(subRuleType),
                            modsecRuleVersion,
                            Set.of(),
                            ruleVersion,
                            includeDirectives);
                      } else { // fallback to latest stable version
                        return webAppRuleInfoProvider.getCrsRulesBlob(
                            List.of(subRuleType),
                            modsecRuleVersion,
                            Set.of(),
                            config.getNewWebAppStableVersion(),
                            includeDirectives);
                      }
                    }));
    builder.modsecBlobsForRuleTypes(modsecBlobsForRuleTypes);
    if (subRuleTypes.size() == 1) {
      builder.aggregatedModsecBlob(modsecBlobsForRuleTypes.get(subRuleTypes.get(0)));
    } else {
      if (ruleVersion != null && !ruleVersion.getVersion().isEmpty()) {
        builder.aggregatedModsecBlob(
            webAppRuleInfoProvider.getCrsRulesBlob(
                subRuleTypes, modsecRuleVersion, Set.of(), ruleVersion, includeDirectives));
      } else {
        builder.aggregatedModsecBlob(
            webAppRuleInfoProvider.getCrsRulesBlob(
                subRuleTypes,
                modsecRuleVersion,
                Set.of(),
                config.getNewWebAppStableVersion(),
                includeDirectives));
      }
    }

    return builder.build();
  }

  private Set<String> getDisabledModsecRuleIds(
      RequestContext requestContext,
      boolean checkBlockingStatus,
      AnomalyConfigScope anomalyConfigScope,
      ModsecRuleVersion modsecRuleVersion,
      boolean isWAAPVersioningEnabledForTenant,
      RuleVersion currentVersion) {
    Map<String, AnomalyDetectionConfig> anomalyRuleConfigMap =
        getAnomalyRuleConfigMap(requestContext, anomalyConfigScope);
    Map<String, AnomalyRuleInfo> ruleInfoMap;
    if (isWAAPVersioningEnabledForTenant
        && currentVersion != null
        && !currentVersion.getVersion().isEmpty()) {
      List<AnomalyRuleInfo> webAppRuleInfo =
          webAppRuleInfoProvider.getWebAppRuleInfo(currentVersion);
      ruleInfoMap =
          webAppRuleInfo.stream()
              .collect(
                  Collectors.toUnmodifiableMap(AnomalyRuleInfo::getRuleId, Function.identity()));
    } else {
      ruleInfoMap = modsecRulesRegistry.getModsecRuleInfos(modsecRuleVersion, false);
    }
    // The disabled modsec rule ids should be ordered to ensure that
    // the blob doesn't keep changing on repeated calls
    Set<String> disabledModsecRuleIds = new TreeSet<>();

    for (String ruleId : ruleInfoMap.keySet()) {
      AnomalyRuleInfo anomalyRuleInfo = ruleInfoMap.get(ruleId);
      AnomalyDetectionConfig detectionConfig =
          anomalyRuleConfigMap.getOrDefault(ruleId, AnomalyDetectionConfig.getDefaultInstance());

      Map<String, AnomalySubRuleConfig> subRuleConfigMap = getSubRuleConfigMap(detectionConfig);
      for (AnomalySubRuleInfo subRuleInfo : anomalyRuleInfo.getSubRuleInfosList()) {
        String subRuleId = subRuleInfo.getRuleId();
        AnomalySubRuleConfig subRuleConfig =
            subRuleConfigMap.getOrDefault(subRuleId, AnomalySubRuleConfig.getDefaultInstance());

        boolean isDisabled =
            detectionConfig.getConfigStatus().getDisabled()
                || subRuleConfig
                    .getAnomalyRuleAction()
                    .equals(AnomalyRuleAction.ANOMALY_RULE_ACTION_DISABLE)
                || (checkBlockingStatus
                    && !(subRuleConfig
                        .getAnomalyRuleAction()
                        .equals(AnomalyRuleAction.ANOMALY_RULE_ACTION_BLOCK)));
        if (isDisabled) {
          disabledModsecRuleIds.add(subRuleId);
        }
      }
    }
    return disabledModsecRuleIds;
  }

  private Map<String, AnomalyDetectionConfig> getAnomalyRuleConfigMap(
      RequestContext requestContext, AnomalyConfigScope anomalyConfigScope) {
    return anomalyDetectionConfigManager
        .getGlobalResolvedScopedAnomalyDetectionConfig(
            requestContext, anomalyConfigScope, ANOMALY_DETECTION_CONFIGS_FILTER)
        .getAnomalyDetectionConfigsList()
        .stream()
        .filter(
            detectionConfig ->
                detectionConfig.getModsecurityAnomalyDetectionConfig().hasModsecAnomalyRule())
        .collect(
            Collectors.toUnmodifiableMap(
                detectionConfig ->
                    detectionConfig
                        .getModsecurityAnomalyDetectionConfig()
                        .getModsecAnomalyRule()
                        .getAnomalyRuleId(),
                Function.identity()));
  }

  private List<AnomalySubRuleType> getSubRuleTypes(
      ModsecCrsRulesTarget rulesTarget, ScopedAnomalyConfigStatus globalConfig) {
    switch (rulesTarget) {
      case MODSEC_CRS_RULES_TARGET_TPA_DETECTION:
        return List.of(AnomalySubRuleType.ANOMALY_SUB_RULE_TYPE_SAFE);
      case MODSEC_CRS_RULES_TARGET_TA_BLOCKING:
        if (!globalConfig.getGlobalModsecConfig().getBlockingAvailableForRegularRules()) {
          return List.of(
              AnomalySubRuleType.ANOMALY_SUB_RULE_TYPE_BLOCK,
              AnomalySubRuleType.ANOMALY_SUB_RULE_TYPE_SAFE);
        } else {
          return ALL_SUB_RULE_TYPES;
        }
      case MODSEC_CRS_RULES_TARGET_PLATFORM_DETECTION:
        return ALL_SUB_RULE_TYPES;
      default:
        log.debug("Unknown ModsecCrsRulesTarget:{} received", rulesTarget);
        return List.of();
    }
  }

  private Map<String, AnomalySubRuleConfig> getSubRuleConfigMap(
      AnomalyDetectionConfig detectionConfig) {
    return detectionConfig
        .getModsecurityAnomalyDetectionConfig()
        .getModsecAnomalyRule()
        .getSubRuleConfigsList()
        .stream()
        .collect(
            Collectors.toUnmodifiableMap(AnomalySubRuleConfig::getSubRuleId, Function.identity()));
  }
}
