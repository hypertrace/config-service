package ai.traceable.anomaly.config.service.modsec.rules;

import ai.traceable.anomaly.config.service.common.AnomalyConfigScopeUtils;
import ai.traceable.anomaly.config.service.detector.anomalydetection.AnomalyDetectionConfigManager;
import ai.traceable.anomaly.config.service.global.status.GlobalAnomalyConfigStatusManager;
import ai.traceable.anomaly.config.service.registry.modsec.ModsecRulesRegistry;
import ai.traceable.anomaly.config.service.v1.AnomalyConfigStatus;
import ai.traceable.anomaly.config.service.v1.AnomalyRuleInfo;
import ai.traceable.anomaly.config.service.v1.AnomalySubRuleInfo;
import ai.traceable.anomaly.config.service.v1.AnomalySubRuleType;
import ai.traceable.anomaly.config.service.v1.detector.AnomalyDetectionConfig;
import ai.traceable.anomaly.config.service.v1.detector.AnomalyDetectionConfigType;
import ai.traceable.anomaly.config.service.v1.detector.AnomalySubRuleConfig;
import ai.traceable.anomaly.config.service.v1.detector.GetAnomalyDetectionConfigsFilter;
import ai.traceable.anomaly.config.service.v1.modsec.ModsecCrsRulesData;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.Function;
import java.util.stream.Collectors;
import javax.inject.Inject;
import lombok.extern.slf4j.Slf4j;
import org.hypertrace.core.grpcutils.context.RequestContext;

@Slf4j
public class ModsecManagerImpl implements ModsecManager {
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
  public List<ModsecCrsRulesData> getModsecCrsRules(
      RequestContext requestContext,
      List<AnomalySubRuleType> requestTypes,
      boolean removeDisabledRules) {
    if (removeDisabledRules) {
      if (isGlobalConfigDisabled(requestContext)) {
        return requestTypes.stream()
            .map(requestType -> ModsecCrsRulesData.newBuilder().setSubRuleType(requestType).build())
            .collect(Collectors.toUnmodifiableList());
      }

      Set<String> configStatusDisabledModsecRuleIds = new HashSet<>();
      Set<String> blockingDisabledModsecRuleIds = new HashSet<>();
      populateDisabledModsecRuleIds(
          requestContext, configStatusDisabledModsecRuleIds, blockingDisabledModsecRuleIds);

      return requestTypes.stream()
          .map(
              requestType ->
                  getModsecCrsRulesData(
                      requestType,
                      configStatusDisabledModsecRuleIds,
                      blockingDisabledModsecRuleIds))
          .collect(Collectors.toUnmodifiableList());
    }

    return requestTypes.stream()
        .map(requestType -> getModsecCrsRulesData(requestType, Set.of(), Set.of()))
        .collect(Collectors.toUnmodifiableList());
  }

  private ModsecCrsRulesData getModsecCrsRulesData(
      AnomalySubRuleType subRuleType,
      Set<String> configStatusDisabledModsecRuleIds,
      Set<String> blockingDisabledModsecRuleIds) {
    // The disabled modsec rule ids should be ordered to ensure the blob doesn't keep changing on
    // repeated calls
    Set<String> disabledModsecRuleIds = new TreeSet<>(configStatusDisabledModsecRuleIds);

    if (subRuleType.equals(AnomalySubRuleType.ANOMALY_SUB_RULE_TYPE_BLOCK)) {
      disabledModsecRuleIds.addAll(blockingDisabledModsecRuleIds);
    }

    return ModsecCrsRulesData.newBuilder()
        .setSubRuleType(subRuleType)
        .setModsecCrsRulesBlob(
            modsecRulesRegistry.getModsecCrsRulesBlob(subRuleType, disabledModsecRuleIds))
        .build();
  }

  private boolean isGlobalConfigDisabled(RequestContext requestContext) {
    AnomalyConfigStatus configStatus =
        globalAnomalyConfigStatusManager
            .getScopedAnomalyConfigStatus(
                requestContext, AnomalyConfigScopeUtils.getDefaultCustomerConfigScope())
            .getConfigStatus();

    return configStatus.getDisabled() && !configStatus.getInternal();
  }

  private void populateDisabledModsecRuleIds(
      RequestContext requestContext,
      Set<String> configStatusDisabledModsecRuleIds,
      Set<String> blockingDisabledModsecRuleIds) {
    Map<String, AnomalyDetectionConfig> anomalyRuleConfigMap =
        getAnomalyRuleConfigMap(requestContext);
    Map<String, AnomalyRuleInfo> ruleInfoMap = modsecRulesRegistry.getModsecRuleInfos();

    for (String ruleId : ruleInfoMap.keySet()) {
      AnomalyRuleInfo anomalyRuleInfo = modsecRulesRegistry.getModsecRuleInfos().get(ruleId);
      AnomalyDetectionConfig detectionConfig = anomalyRuleConfigMap.get(ruleId);

      Map<String, AnomalySubRuleConfig> subRuleConfigMap = getSubRuleConfigMap(detectionConfig);
      for (AnomalySubRuleInfo subRuleInfo : anomalyRuleInfo.getSubRuleInfosList()) {
        String subRuleId = subRuleInfo.getRuleId();
        AnomalySubRuleConfig subRuleConfig = subRuleConfigMap.get(subRuleId);

        boolean isDisabled =
            Objects.nonNull(detectionConfig) && detectionConfig.getConfigStatus().getDisabled();

        if (Objects.nonNull(subRuleConfig)) {
          isDisabled = isDisabled || subRuleConfig.getConfigStatus().getDisabled();
        }
        if (isDisabled) {
          configStatusDisabledModsecRuleIds.add(subRuleId);
        }

        if (subRuleInfo
            .getSubRuleTypesList()
            .contains(AnomalySubRuleType.ANOMALY_SUB_RULE_TYPE_BLOCK)) {
          boolean isBlockingEnabled = false;

          if (Objects.nonNull(subRuleConfig)) {
            isBlockingEnabled = subRuleConfig.getBlockingEnabled();
          }
          if (!isBlockingEnabled) {
            blockingDisabledModsecRuleIds.add(subRuleId);
          }
        }
      }
    }
  }

  private Map<String, AnomalyDetectionConfig> getAnomalyRuleConfigMap(
      RequestContext requestContext) {
    return anomalyDetectionConfigManager
        .getScopedAnomalyDetectionConfig(
            requestContext,
            AnomalyConfigScopeUtils.getDefaultCustomerConfigScope(),
            ANOMALY_DETECTION_CONFIGS_FILTER)
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

  private Map<String, AnomalySubRuleConfig> getSubRuleConfigMap(
      AnomalyDetectionConfig detectionConfig) {
    if (Objects.isNull(detectionConfig)) {
      return Map.of();
    }
    return detectionConfig
        .getModsecurityAnomalyDetectionConfig()
        .getModsecAnomalyRule()
        .getSubRuleConfigsList()
        .stream()
        .collect(
            Collectors.toUnmodifiableMap(AnomalySubRuleConfig::getSubRuleId, Function.identity()));
  }
}
