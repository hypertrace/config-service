package ai.traceable.anomaly.config.service.detector.anomalydetection;

import ai.traceable.anomaly.config.service.detector.DetectorConfigServiceConfig;
import ai.traceable.anomaly.config.service.detector.anomalydetection.handler.ApiProtectionConfigHandler;
import ai.traceable.anomaly.config.service.global.ruleinfo.RuleInfoManager;
import ai.traceable.anomaly.config.service.v1.AnomalyRuleAction;
import ai.traceable.anomaly.config.service.v1.AnomalyRuleInfo;
import ai.traceable.anomaly.config.service.v1.AnomalySubRuleInfo;
import ai.traceable.anomaly.config.service.v1.RuleVersion;
import ai.traceable.anomaly.config.service.v1.RuleVersionData;
import ai.traceable.anomaly.config.service.v1.detector.AnomalyDetectionConfig;
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
        .build();
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
