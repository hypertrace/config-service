package ai.traceable.anomaly.config.service.detector.anomalydetection;

import static ai.traceable.anomaly.config.service.v1.ApiProtectThreatRuleConfigMappingProvider.PARAMETER_ANOMALY_UUAD_SUB_RULE_ID;
import static ai.traceable.anomaly.config.service.v1.ApiProtectThreatRuleConfigMappingProvider.SCHEMA_VALIDATION_URESC_SUB_RULE_ID;

import ai.traceable.anomaly.config.service.detector.DetectorConfigServiceConfig;
import ai.traceable.anomaly.config.service.detector.anomalydetection.handler.ApiProtectionConfigHandler;
import ai.traceable.anomaly.config.service.global.ruleinfo.RuleInfoManager;
import ai.traceable.anomaly.config.service.v1.AnomalyRuleAction;
import ai.traceable.anomaly.config.service.v1.AnomalyRuleInfo;
import ai.traceable.anomaly.config.service.v1.AnomalySeverityLevel;
import ai.traceable.anomaly.config.service.v1.AnomalySubRuleInfo;
import ai.traceable.anomaly.config.service.v1.AnomalySubRuleType;
import ai.traceable.anomaly.config.service.v1.RuleVersion;
import ai.traceable.anomaly.config.service.v1.RuleVersionData;
import ai.traceable.anomaly.config.service.v1.detector.AnomalyCategoryConfig;
import ai.traceable.anomaly.config.service.v1.detector.AnomalyDetectionConfig;
import ai.traceable.anomaly.config.service.v1.detector.AnomalyEventCategory;
import ai.traceable.anomaly.config.service.v1.detector.AnomalyEventScoreCategory;
import ai.traceable.anomaly.config.service.v1.detector.AnomalySubRuleConfig;
import ai.traceable.anomaly.config.service.v1.detector.ApiProtectAnomalyDetectionConfig;
import ai.traceable.anomaly.config.service.v1.detector.ApiProtectAnomalyRuleConfig;
import ai.traceable.anomaly.config.service.v1.global.GlobalApiConfig;
import jakarta.inject.Inject;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import org.hypertrace.core.grpcutils.context.RequestContext;

public class ApiProtectionConfigResolver {
  private final RuleInfoManager ruleInfoManager;
  private final List<AnomalyDetectionConfig> defaultApiProtectionConfigs;
  private final ApiProtectionConfigHandler apiProtectionConfigHandler;
  private static final List<String> TAGGED_CATEGORY_RULE_IDS =
      List.of(PARAMETER_ANOMALY_UUAD_SUB_RULE_ID, SCHEMA_VALIDATION_URESC_SUB_RULE_ID);

  @Inject
  public ApiProtectionConfigResolver(
      RuleInfoManager ruleInfoManager,
      DetectorConfigServiceConfig config,
      ApiProtectionConfigHandler apiProtectionConfigHandler) {
    this.ruleInfoManager = ruleInfoManager;
    this.defaultApiProtectionConfigs = config.getDefaultApiProtectDetectionConfigs();
    this.apiProtectionConfigHandler = apiProtectionConfigHandler;
  }

  public List<AnomalyDetectionConfig> resolve(
      RequestContext requestContext, GlobalApiConfig globalApiConfig) {

    RuleVersionData ruleVersionData = globalApiConfig.getRuleVersionData();
    List<AnomalyDetectionConfig> anomalyDetectionConfigList =
        new ArrayList<>(
            getApiProtectionAnomalyRuleInfos(requestContext, ruleVersionData.getCurrentVersion())
                .stream()
                .map(
                    ruleInfo -> {
                      List<AnomalySubRuleConfig> anomalySubRuleConfigList =
                          ruleInfo.getSubRuleInfosList().stream()
                              .map(this::getSubRuleConfig)
                              .collect(Collectors.toUnmodifiableList());
                      if (!anomalySubRuleConfigList.isEmpty()) {
                        return buildApiProtectAnomalyDetectionConfig(
                            ruleInfo, anomalySubRuleConfigList);
                      }
                      return null;
                    })
                .filter(Objects::nonNull)
                .collect(Collectors.toUnmodifiableList()));
    return apiProtectionConfigHandler.merge(
        defaultApiProtectionConfigs, anomalyDetectionConfigList);
  }

  private AnomalySubRuleConfig getSubRuleConfig(AnomalySubRuleInfo subRuleInfo) {
    return AnomalySubRuleConfig.newBuilder()
        .setSubRuleId(subRuleInfo.getRuleId())
        .setAnomalyRuleAction(AnomalyRuleAction.ANOMALY_RULE_ACTION_MONITOR)
        .setCategoryConfig(getAnomalyCategoryConfig(subRuleInfo))
        .build();
  }

  private AnomalyCategoryConfig getAnomalyCategoryConfig(AnomalySubRuleInfo subRuleInfo) {
    return AnomalyCategoryConfig.newBuilder()
        .setEventScoreCategory(getEventScoreCategory(subRuleInfo.getSeverityLevel()))
        .setEventCategory(
            getEventCategory(subRuleInfo.getSubRuleTypesList(), subRuleInfo.getRuleId()))
        .build();
  }

  private AnomalyEventScoreCategory getEventScoreCategory(AnomalySeverityLevel severityLevel) {
    switch (severityLevel) {
      case ANOMALY_SEVERITY_LEVEL_LOW:
        return AnomalyEventScoreCategory.ANOMALY_EVENT_SCORE_CATEGORY_LOW;
      case ANOMALY_SEVERITY_LEVEL_MEDIUM:
        return AnomalyEventScoreCategory.ANOMALY_EVENT_SCORE_CATEGORY_MEDIUM;
      case ANOMALY_SEVERITY_LEVEL_HIGH:
        return AnomalyEventScoreCategory.ANOMALY_EVENT_SCORE_CATEGORY_HIGH;
      case ANOMALY_SEVERITY_LEVEL_CRITICAL:
        return AnomalyEventScoreCategory.ANOMALY_EVENT_SCORE_CATEGORY_CRITICAL;
      default:
        throw new IllegalArgumentException("Severity level " + severityLevel + " not recognized");
    }
  }

  private AnomalyEventCategory getEventCategory(
      List<AnomalySubRuleType> subRuleTypes, String subRuleId) {
    if (TAGGED_CATEGORY_RULE_IDS.contains(subRuleId)) {
      return AnomalyEventCategory.ANOMALY_EVENT_CATEGORY_TAGGED;
    }
    if (subRuleTypes.contains(AnomalySubRuleType.ANOMALY_SUB_RULE_TYPE_BLOCK)
        || subRuleTypes.contains(AnomalySubRuleType.ANOMALY_SUB_RULE_TYPE_SAFE)) {
      return AnomalyEventCategory.ANOMALY_EVENT_CATEGORY_MALICIOUS;
    } else if (subRuleTypes.contains(AnomalySubRuleType.ANOMALY_SUB_RULE_TYPE_REGULAR)) {
      return AnomalyEventCategory.ANOMALY_EVENT_CATEGORY_LATENT;
    } else {
      throw new IllegalArgumentException("Sub rule types " + subRuleTypes + " not recognized");
    }
  }

  private static AnomalyDetectionConfig buildApiProtectAnomalyDetectionConfig(
      AnomalyRuleInfo ruleInfo, List<AnomalySubRuleConfig> anomalySubRuleConfigList) {
    return AnomalyDetectionConfig.newBuilder()
        .setApiProtectAnomalyDetectionConfig(
            ApiProtectAnomalyDetectionConfig.newBuilder()
                .setApiProtectAnomalyRule(
                    ApiProtectAnomalyRuleConfig.newBuilder()
                        .setAnomalyRuleId(ruleInfo.getRuleId())
                        .addAllSubRuleConfigs(anomalySubRuleConfigList)
                        .build()))
        .build();
  }

  private List<AnomalyRuleInfo> getApiProtectionAnomalyRuleInfos(
      RequestContext requestContext, RuleVersion ruleVersion) {
    return ruleInfoManager.getAllApiProtectionAnomalyRuleInfo(requestContext, ruleVersion);
  }
}
