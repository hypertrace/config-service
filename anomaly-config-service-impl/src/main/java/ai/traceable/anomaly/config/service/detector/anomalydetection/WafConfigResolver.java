package ai.traceable.anomaly.config.service.detector.anomalydetection;

import static ai.traceable.anomaly.config.service.v1.AnomalySubRuleType.ANOMALY_SUB_RULE_TYPE_BLOCK;
import static ai.traceable.anomaly.config.service.v1.AnomalySubRuleType.ANOMALY_SUB_RULE_TYPE_SAFE;

import ai.traceable.anomaly.config.service.detector.DetectorConfigServiceConfig;
import ai.traceable.anomaly.config.service.detector.anomalydetection.handler.ModsecConfigHandler;
import ai.traceable.anomaly.config.service.global.ruleinfo.RuleInfoManager;
import ai.traceable.anomaly.config.service.v1.AnomalyConfigStatusChange;
import ai.traceable.anomaly.config.service.v1.AnomalyEventFamily;
import ai.traceable.anomaly.config.service.v1.AnomalyRuleInfo;
import ai.traceable.anomaly.config.service.v1.AnomalySubRuleInfo;
import ai.traceable.anomaly.config.service.v1.RuleVersion;
import ai.traceable.anomaly.config.service.v1.detector.AnomalyDetectionConfig;
import ai.traceable.anomaly.config.service.v1.detector.AnomalySubRuleConfig;
import ai.traceable.anomaly.config.service.v1.detector.ModsecurityAnomalyDetectionConfig;
import ai.traceable.anomaly.config.service.v1.detector.ModsecurityAnomalyRuleConfig;
import ai.traceable.anomaly.config.service.v1.detector.ScopedAnomalyDetectionConfig;
import ai.traceable.anomaly.config.service.v1.global.GlobalModsecConfig;
import ai.traceable.anomaly.config.service.v1.global.ModsecDefaultConfigsType;
import ai.traceable.anomaly.config.service.v1.modsec.ModsecRuleVersion;
import jakarta.inject.Inject;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import org.hypertrace.core.grpcutils.context.RequestContext;

public class WafConfigResolver {
  private final RuleInfoManager ruleInfoManager;
  private final List<AnomalyDetectionConfig> defaultWafConfigs;
  private final AnomalyDetectionConfig defaultAllModsecDetectionConfig;
  private final ModsecConfigHandler modsecConfigHandler;

  @Inject
  public WafConfigResolver(
      RuleInfoManager ruleInfoManager,
      DetectorConfigServiceConfig config,
      ModsecConfigHandler modsecConfigHandler) {
    this.ruleInfoManager = ruleInfoManager;
    this.defaultWafConfigs = config.getDefaultWafDetectionConfigs();
    this.modsecConfigHandler = modsecConfigHandler;
    this.defaultAllModsecDetectionConfig =
        config.getDefaultWafDetectionConfigs().stream()
            .filter(
                defaultConfig ->
                    defaultConfig.getModsecurityAnomalyDetectionConfig().hasModsecAllDetection())
            .findAny()
            .orElse(AnomalyDetectionConfig.getDefaultInstance());
  }

  public List<AnomalyDetectionConfig> resolve(
      RequestContext requestContext,
      GlobalModsecConfig globalModsecConfig,
      Map<String, ScopedAnomalyDetectionConfig> configMap,
      List<String> contextsWithIncreasingPriority) {
    if (globalModsecConfig
            .getDefaultConfigsType()
            .equals(ModsecDefaultConfigsType.MODSEC_DEFAULT_CONFIGS_TYPE_DEFAULT_ENABLED)
        || globalModsecConfig
            .getDefaultConfigsType()
            .equals(ModsecDefaultConfigsType.MODSEC_DEFAULT_CONFIGS_TYPE_ALL_MONITOR)
        || globalModsecConfig
            .getDefaultConfigsType()
            .equals(ModsecDefaultConfigsType.MODSEC_DEFAULT_CONFIGS_TYPE_STRICT_MONITORING)) {
      return defaultWafConfigs;
    }
    List<AnomalyDetectionConfig> anomalyDetectionConfigList =
        new ArrayList<>(
            getModsecAnomalyRuleInfos(
                    requestContext,
                    getModsecRuleVersion(configMap, contextsWithIncreasingPriority),
                    globalModsecConfig.getRuleVersionData().getCurrentVersion(),
                    globalModsecConfig.getUseTestRules())
                .stream()
                .map(
                    ruleInfo -> {
                      List<AnomalySubRuleConfig> anomalySubRuleConfigList =
                          ruleInfo.getSubRuleInfosList().stream()
                              .map(
                                  subRuleInfo ->
                                      getSubRuleConfig(
                                          subRuleInfo, globalModsecConfig.getDefaultConfigsType()))
                              .filter(Objects::nonNull)
                              .collect(Collectors.toUnmodifiableList());
                      if (!anomalySubRuleConfigList.isEmpty()) {
                        return buildModsecAnomalyDetectionConfig(
                            ruleInfo, anomalySubRuleConfigList);
                      }
                      return null;
                    })
                .filter(Objects::nonNull)
                .collect(Collectors.toUnmodifiableList()));
    return modsecConfigHandler.merge(defaultWafConfigs, anomalyDetectionConfigList);
  }

  private ModsecRuleVersion getModsecRuleVersion(
      Map<String, ScopedAnomalyDetectionConfig> configMap,
      List<String> contextsWithIncreasingPriority) {
    ModsecRuleVersion modsecRuleVersion =
        this.defaultAllModsecDetectionConfig
            .getModsecurityAnomalyDetectionConfig()
            .getModsecAllDetection()
            .getModsecRuleVersion();
    for (String context : contextsWithIncreasingPriority) {
      if (configMap.containsKey(context)) {
        modsecRuleVersion =
            configMap.get(context).getAnomalyDetectionConfigsList().stream()
                .map(
                    config ->
                        config
                            .getModsecurityAnomalyDetectionConfig()
                            .getModsecAllDetection()
                            .getModsecRuleVersion())
                .filter(
                    ruleVersion -> ruleVersion != ModsecRuleVersion.MODSEC_RULE_VERSION_UNSPECIFIED)
                .findAny()
                .orElse(modsecRuleVersion);
      }
    }
    return modsecRuleVersion;
  }

  private AnomalySubRuleConfig getSubRuleConfig(
      AnomalySubRuleInfo subRuleInfo, ModsecDefaultConfigsType modsecDefaultConfigsType) {
    switch (modsecDefaultConfigsType) {
      case MODSEC_DEFAULT_CONFIGS_TYPE_AGGRESSIVE_DISABLED:
      case MODSEC_DEFAULT_CONFIGS_TYPE_STANDARD: // Aggressive disabled, rest monitor
      case MODSEC_DEFAULT_CONFIGS_TYPE_STANDARD_MONITORING:
        return isRuleAggressive(subRuleInfo.getSubRuleTypesList())
            ? buildDisabledAnomalySubRuleConfig(subRuleInfo)
            : null;
      case MODSEC_DEFAULT_CONFIGS_TYPE_RECOMMENDED: // Aggressive disabled, rest blocking
      case MODSEC_DEFAULT_CONFIGS_TYPE_STANDARD_BLOCKING:
        return isRuleAggressive(subRuleInfo.getSubRuleTypesList())
            ? buildDisabledAnomalySubRuleConfig(subRuleInfo)
            : buildBlockingAnomalySubRule(subRuleInfo);
      case MODSEC_DEFAULT_CONFIGS_TYPE_STRICT: // Aggressive monitor, rest blocking
      case MODSEC_DEFAULT_CONFIGS_TYPE_STRICT_BLOCKING:
        return isRuleAggressive(subRuleInfo.getSubRuleTypesList())
            ? null
            : buildBlockingAnomalySubRule(subRuleInfo);
      default:
        return null;
    }
  }

  private static AnomalyDetectionConfig buildModsecAnomalyDetectionConfig(
      AnomalyRuleInfo ruleInfo, List<AnomalySubRuleConfig> anomalySubRuleConfigList) {
    return AnomalyDetectionConfig.newBuilder()
        .setModsecurityAnomalyDetectionConfig(
            ModsecurityAnomalyDetectionConfig.newBuilder()
                .setModsecAnomalyRule(
                    ModsecurityAnomalyRuleConfig.newBuilder()
                        .setAnomalyRuleId(ruleInfo.getRuleId())
                        .addAllSubRuleConfigs(anomalySubRuleConfigList)
                        .build()))
        .build();
  }

  private List<AnomalyRuleInfo> getModsecAnomalyRuleInfos(
      RequestContext requestContext,
      ModsecRuleVersion modsecRuleVersion,
      RuleVersion currentVersion,
      boolean useTestRules) {
    return ruleInfoManager.getAnomalyRuleInfos(
        requestContext,
        List.of(AnomalyEventFamily.ANOMALY_EVENT_FAMILY_MODSEC),
        modsecRuleVersion,
        currentVersion,
        useTestRules);
  }

  private AnomalySubRuleConfig buildDisabledAnomalySubRuleConfig(AnomalySubRuleInfo subRuleInfo) {
    return AnomalySubRuleConfig.newBuilder()
        .setSubRuleId(subRuleInfo.getRuleId())
        .setConfigStatus(AnomalyConfigStatusChange.newBuilder().setDisabled(true).build())
        .build();
  }

  private static AnomalySubRuleConfig buildBlockingAnomalySubRule(AnomalySubRuleInfo subRuleInfo) {
    return AnomalySubRuleConfig.newBuilder()
        .setSubRuleId(subRuleInfo.getRuleId())
        .setBlockingEnabled(true)
        .build();
  }

  private boolean isRuleAggressive(
      List<ai.traceable.anomaly.config.service.v1.AnomalySubRuleType> subRuleTypes) {
    return !subRuleTypes.contains(ANOMALY_SUB_RULE_TYPE_BLOCK)
        && !subRuleTypes.contains(ANOMALY_SUB_RULE_TYPE_SAFE);
  }
}
