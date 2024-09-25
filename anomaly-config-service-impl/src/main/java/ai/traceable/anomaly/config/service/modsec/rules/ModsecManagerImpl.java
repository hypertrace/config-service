package ai.traceable.anomaly.config.service.modsec.rules;

import ai.traceable.anomaly.config.service.detector.anomalydetection.AnomalyDetectionConfigManager;
import ai.traceable.anomaly.config.service.global.status.GlobalAnomalyConfigStatusManager;
import ai.traceable.anomaly.config.service.registry.modsec.ModsecRulesRegistry;
import ai.traceable.anomaly.config.service.v1.AnomalyConfigScope;
import ai.traceable.anomaly.config.service.v1.AnomalyConfigStatus;
import ai.traceable.anomaly.config.service.v1.AnomalyRuleInfo;
import ai.traceable.anomaly.config.service.v1.AnomalySubRuleInfo;
import ai.traceable.anomaly.config.service.v1.AnomalySubRuleType;
import ai.traceable.anomaly.config.service.v1.detector.AnomalyDetectionConfig;
import ai.traceable.anomaly.config.service.v1.detector.AnomalyDetectionConfigType;
import ai.traceable.anomaly.config.service.v1.detector.AnomalySubRuleConfig;
import ai.traceable.anomaly.config.service.v1.detector.GetAnomalyDetectionConfigsFilter;
import ai.traceable.anomaly.config.service.v1.global.ScopedAnomalyConfigStatus;
import ai.traceable.anomaly.config.service.v1.modsec.ModsecCrsRulesTarget;
import ai.traceable.anomaly.config.service.v1.modsec.ModsecRuleVersion;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.Function;
import java.util.stream.Collectors;
import javax.inject.Inject;
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
  private final AnomalyDetectionConfigManager anomalyDetectionConfigManager;
  private final GlobalAnomalyConfigStatusManager globalAnomalyConfigStatusManager;

  @Inject
  public ModsecManagerImpl(
      ModsecRulesRegistry modsecRulesRegistry,
      AnomalyDetectionConfigManager anomalyDetectionConfigManager,
      GlobalAnomalyConfigStatusManager globalAnomalyConfigStatusManager) {
    this.modsecRulesRegistry = modsecRulesRegistry;
    this.anomalyDetectionConfigManager = anomalyDetectionConfigManager;
    this.globalAnomalyConfigStatusManager = globalAnomalyConfigStatusManager;
  }

  @Override
  public ModsecCrsRules getModsecCrsRules(
      RequestContext requestContext,
      ModsecRuleVersion modsecRuleVersion,
      ModsecCrsRulesTarget rulesTarget,
      List<AnomalySubRuleType> subRuleTypes,
      boolean removeDisabledRules,
      AnomalyConfigScope anomalyConfigScope) {

    ScopedAnomalyConfigStatus globalConfig;
    if (rulesTarget.equals(ModsecCrsRulesTarget.MODSEC_CRS_RULES_TARGET_PLATFORM_DETECTION)) {
      // Platform is tenant-agnostic
      globalConfig = ScopedAnomalyConfigStatus.getDefaultInstance();
    } else {
      globalConfig =
          globalAnomalyConfigStatusManager.getScopedAnomalyConfigStatus(
              requestContext, anomalyConfigScope);
    }

    subRuleTypes =
        subRuleTypes.isEmpty() ? getSubRuleTypes(rulesTarget, globalConfig) : subRuleTypes;
    if (subRuleTypes.isEmpty()) {
      return new ModsecCrsRules(subRuleTypes);
    }

    Set<String> disabledModsecRuleIds;
    if (removeDisabledRules) {
      if (isGlobalConfigDisabled(globalConfig)) {
        return new ModsecCrsRules(subRuleTypes);
      }
      boolean checkBlockingStatus =
          rulesTarget == ModsecCrsRulesTarget.MODSEC_CRS_RULES_TARGET_TA_BLOCKING
              || (subRuleTypes.size() == 1
                  && subRuleTypes.get(0).equals(AnomalySubRuleType.ANOMALY_SUB_RULE_TYPE_BLOCK));
      disabledModsecRuleIds =
          getDisabledModsecRuleIds(
              requestContext, checkBlockingStatus, anomalyConfigScope, modsecRuleVersion);
    } else {
      disabledModsecRuleIds = Set.of();
    }

    ModsecCrsRules.ModsecCrsRulesBuilder builder = ModsecCrsRules.builder();
    Map<AnomalySubRuleType, String> modsecBlobsForRuleTypes =
        subRuleTypes.stream()
            .collect(
                Collectors.toUnmodifiableMap(
                    Function.identity(),
                    subRuleType ->
                        modsecRulesRegistry.getModsecCrsRulesBlob(
                            List.of(subRuleType), modsecRuleVersion, disabledModsecRuleIds)));
    builder.modsecBlobsForRuleTypes(modsecBlobsForRuleTypes);
    if (subRuleTypes.size() == 1) {
      builder.aggregatedModsecBlob(modsecBlobsForRuleTypes.get(subRuleTypes.get(0)));
    } else {
      builder.aggregatedModsecBlob(
          modsecRulesRegistry.getModsecCrsRulesBlob(
              subRuleTypes, modsecRuleVersion, disabledModsecRuleIds));
    }

    return builder.build();
  }

  @Override
  public ModsecCrsRules getModsecCrsRules(
      List<AnomalySubRuleType> subRuleTypes,
      ModsecRuleVersion modsecRuleVersion,
      boolean useTestModsecRules) {

    if (subRuleTypes.isEmpty()) {
      subRuleTypes = ALL_SUB_RULE_TYPES;
    }

    ModsecCrsRules.ModsecCrsRulesBuilder builder = ModsecCrsRules.builder();
    Map<AnomalySubRuleType, String> modsecBlobsForRuleTypes =
        subRuleTypes.stream()
            .collect(
                Collectors.toUnmodifiableMap(
                    Function.identity(),
                    subRuleType ->
                        modsecRulesRegistry.getModsecCrsRulesBlob(
                            List.of(subRuleType), modsecRuleVersion, Set.of())));
    builder.modsecBlobsForRuleTypes(modsecBlobsForRuleTypes);
    if (subRuleTypes.size() == 1) {
      builder.aggregatedModsecBlob(modsecBlobsForRuleTypes.get(subRuleTypes.get(0)));
    } else {
      builder.aggregatedModsecBlob(
          modsecRulesRegistry.getModsecCrsRulesBlob(subRuleTypes, modsecRuleVersion, Set.of()));
    }

    return builder.build();
  }

  private boolean isGlobalConfigDisabled(ScopedAnomalyConfigStatus globalConfig) {
    AnomalyConfigStatus configStatus = globalConfig.getConfigStatus();
    return configStatus.getDisabled() && !configStatus.getInternal();
  }

  private Set<String> getDisabledModsecRuleIds(
      RequestContext requestContext,
      boolean checkBlockingStatus,
      AnomalyConfigScope anomalyConfigScope,
      ModsecRuleVersion modsecRuleVersion) {
    Map<String, AnomalyDetectionConfig> anomalyRuleConfigMap =
        getAnomalyRuleConfigMap(requestContext, anomalyConfigScope);
    Map<String, AnomalyRuleInfo> ruleInfoMap =
        modsecRulesRegistry.getModsecRuleInfos(modsecRuleVersion);

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
                || subRuleConfig.getConfigStatus().getDisabled()
                || (checkBlockingStatus && !subRuleConfig.getBlockingEnabled());
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
        .getScopedAnomalyDetectionConfig(
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
        if (!globalConfig.getModsecGlobalConfig().getBlockingAvailableForRegularRules()) {
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
