package ai.traceable.anomaly.config.service.detector.anomalydetection;

import static ai.traceable.anomaly.config.service.v1.AnomalySubRuleType.ANOMALY_SUB_RULE_TYPE_BLOCK;
import static ai.traceable.anomaly.config.service.v1.AnomalySubRuleType.ANOMALY_SUB_RULE_TYPE_SAFE;

import ai.traceable.anomaly.config.service.global.ruleinfo.RuleInfoManager;
import ai.traceable.anomaly.config.service.global.status.GlobalAnomalyConfigStatusManager;
import ai.traceable.anomaly.config.service.v1.AnomalyConfigScope;
import ai.traceable.anomaly.config.service.v1.AnomalyRuleInfo;
import ai.traceable.anomaly.config.service.v1.RuleVersionData;
import ai.traceable.anomaly.config.service.v1.detector.AnomalyDetectionConfigType;
import ai.traceable.anomaly.config.service.v1.detector.AnomalySubRuleConfig;
import ai.traceable.anomaly.config.service.v1.detector.GetAnomalyDetectionConfigsFilter;
import ai.traceable.anomaly.config.service.v1.detector.ModsecurityAnomalyDetectionConfig;
import ai.traceable.anomaly.config.service.v1.detector.ModsecurityAnomalyRuleConfig;
import ai.traceable.anomaly.config.service.v1.global.GlobalModsecConfig;
import ai.traceable.anomaly.config.service.v1.modsec.ModsecRuleVersion;
import io.grpc.Status;
import jakarta.inject.Inject;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import lombok.AllArgsConstructor;
import org.hypertrace.core.grpcutils.context.RequestContext;

@AllArgsConstructor(onConstructor_ = @Inject)
public class ModsecConfigValidator {

  private final RuleInfoManager ruleInfoManager;
  private final GlobalAnomalyConfigStatusManager globalAnomalyConfigStatusManager;
  private final AnomalyDetectionConfigManager anomalyDetectionConfigManager;
  private final ModsecRuleVersion defaultModsecRuleVersion;

  /**
   * @param modsecConfigs
   * @param requestContext param configScope
   * @return Status.INVALID_ARGUMENT in case of presence of configs with same ruleId or presence of
   *     subRuleConfigs with same subRuleId for a particular modsec config. Status.OK in all other
   *     cases. Or aggresive rule with blocked status
   */
  Status validate(
      List<ModsecurityAnomalyDetectionConfig> modsecConfigs,
      RequestContext requestContext,
      AnomalyConfigScope configScope) {
    GlobalModsecConfig globalModsecConfig =
        globalAnomalyConfigStatusManager
            .getScopedAnomalyConfigStatus(requestContext, configScope)
            .getGlobalModsecConfig();
    Set<String> aggressiveSubRuleIds =
        getAggressiveSubRuleIds(requestContext, configScope, globalModsecConfig);
    boolean blockingAvailableForRegularRules =
        globalModsecConfig.getBlockingAvailableForRegularRules();

    Set<String> ruleIds = new HashSet<>();
    ModsecurityAnomalyDetectionConfig modsecAllDetectionConfig =
        ModsecurityAnomalyDetectionConfig.getDefaultInstance();
    for (ModsecurityAnomalyDetectionConfig detectionConfig : modsecConfigs) {
      if (detectionConfig.hasModsecAnomalyRule()) {
        ModsecurityAnomalyRuleConfig modsecRuleConfig = detectionConfig.getModsecAnomalyRule();
        String ruleId = modsecRuleConfig.getAnomalyRuleId();

        if (ruleIds.contains(ruleId)) {
          return Status.INVALID_ARGUMENT.withDescription(
              String.format(
                  "UpdateScopedAnomalyDetectionConfigRequest should have only one modsec config with ruleId: %s",
                  ruleId));
        } else {
          Status status =
              validateSubRuleConfigs(
                  modsecRuleConfig.getSubRuleConfigsList(),
                  ruleId,
                  aggressiveSubRuleIds,
                  blockingAvailableForRegularRules);
          if (!status.isOk()) {
            return status;
          }
          ruleIds.add(ruleId);
        }
      } else if (detectionConfig.hasModsecAllDetection()) {
        if (modsecAllDetectionConfig.equals(
            ModsecurityAnomalyDetectionConfig.getDefaultInstance())) {
          modsecAllDetectionConfig = detectionConfig;
        } else {
          return Status.INVALID_ARGUMENT.withDescription(
              "UpdateScopedAnomalyDetectionConfigRequest should have only one modsecAllDetectionConfig");
        }
      }
    }
    return Status.OK;
  }

  private Status validateSubRuleConfigs(
      List<AnomalySubRuleConfig> subRuleConfigs,
      String ruleId,
      Set<String> aggressiveSubRuleIds,
      boolean blockingAvailableForRegularRules) {
    Set<String> subRuleIds = new HashSet<>();

    for (AnomalySubRuleConfig subRuleConfig : subRuleConfigs) {
      String subRuleId = subRuleConfig.getSubRuleId();
      if (!blockingAvailableForRegularRules
          && aggressiveSubRuleIds.contains(subRuleId)
          && subRuleConfig.getBlockingEnabled()) {
        return Status.INVALID_ARGUMENT.withDescription(
            String.format("%s is an aggressive rule which can't be blocked", subRuleId));
      }
      if (subRuleIds.contains(subRuleId)) {
        return Status.INVALID_ARGUMENT.withDescription(
            String.format(
                "UpdateScopedAnomalyDetectionConfigRequest should have only one subRule config with subRuleId: %s in modsec config with ruleId: %s",
                subRuleId, ruleId));
      } else {
        subRuleIds.add(subRuleId);
      }
    }

    return Status.OK;
  }

  private Set<String> getAggressiveSubRuleIds(
      RequestContext requestContext,
      AnomalyConfigScope configScope,
      GlobalModsecConfig globalModsecConfig) {
    Set<String> aggressiveSubRuleIds = new HashSet<>();
    getModsecAnomalyRuleInfos(requestContext, configScope, globalModsecConfig)
        .forEach(
            anomalyRuleInfo ->
                anomalyRuleInfo
                    .getSubRuleInfosList()
                    .forEach(
                        subRuleInfo -> {
                          if (isRuleAggressive(subRuleInfo.getSubRuleTypesList())) {
                            aggressiveSubRuleIds.add(subRuleInfo.getRuleId());
                          }
                        }));
    return aggressiveSubRuleIds;
  }

  private List<AnomalyRuleInfo> getModsecAnomalyRuleInfos(
      RequestContext requestContext,
      AnomalyConfigScope configScope,
      GlobalModsecConfig globalModsecConfig) {
    RuleVersionData ruleVersionData = globalModsecConfig.getRuleVersionData();
    ModsecRuleVersion modsecRuleVersion =
        anomalyDetectionConfigManager
            .getScopedAnomalyDetectionConfig(
                requestContext,
                configScope,
                GetAnomalyDetectionConfigsFilter.newBuilder()
                    .addAnomalyDetectionConfigTypes(
                        AnomalyDetectionConfigType.ANOMALY_DETECTION_CONFIG_TYPE_MODSECURITY)
                    .build())
            .getAnomalyDetectionConfigsList()
            .stream()
            .filter(
                config -> {
                  if (!config.getModsecurityAnomalyDetectionConfig().hasModsecAllDetection()) {
                    return false;
                  }
                  ModsecRuleVersion ruleVersion =
                      config
                          .getModsecurityAnomalyDetectionConfig()
                          .getModsecAllDetection()
                          .getModsecRuleVersion();
                  return ruleVersion != ModsecRuleVersion.MODSEC_RULE_VERSION_UNSPECIFIED
                      && ruleVersion != ModsecRuleVersion.UNRECOGNIZED;
                })
            .map(
                config ->
                    config
                        .getModsecurityAnomalyDetectionConfig()
                        .getModsecAllDetection()
                        .getModsecRuleVersion())
            .findAny()
            .orElse(defaultModsecRuleVersion);
    return ruleInfoManager.getModsecAnomalyRuleInfo(
        requestContext,
        modsecRuleVersion,
        ruleVersionData.getCurrentVersion(),
        globalModsecConfig.getUseTestRules());
  }

  private boolean isRuleAggressive(
      List<ai.traceable.anomaly.config.service.v1.AnomalySubRuleType> subRuleTypes) {
    return !subRuleTypes.contains(ANOMALY_SUB_RULE_TYPE_BLOCK)
        && !subRuleTypes.contains(ANOMALY_SUB_RULE_TYPE_SAFE);
  }
}
