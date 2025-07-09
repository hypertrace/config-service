package ai.traceable.anomaly.config.service.detector.anomalydetection.handler;

import ai.traceable.anomaly.config.service.global.version.RuleVersionManager;
import ai.traceable.anomaly.config.service.v1.AnomalyRuleAction;
import ai.traceable.anomaly.config.service.v1.RuleTestingMode;
import ai.traceable.anomaly.config.service.v1.RuleType;
import ai.traceable.anomaly.config.service.v1.RuleVersionData;
import ai.traceable.anomaly.config.service.v1.RuleVersionType;
import ai.traceable.anomaly.config.service.v1.detector.AnomalyDetectionConfig;
import ai.traceable.anomaly.config.service.v1.detector.AnomalySubRuleConfig;
import ai.traceable.anomaly.config.service.v1.detector.ModsecurityAnomalyDetectionConfig;
import ai.traceable.anomaly.config.service.v1.detector.ModsecurityAnomalyRuleConfig;
import ai.traceable.anomaly.config.service.v1.detector.ScopedAnomalyDetectionConfig;
import ai.traceable.anomaly.config.service.v1.global.RulesChangeLog;
import ai.traceable.anomaly.config.service.v1.global.ScopedAnomalyConfigStatus;
import ai.traceable.anomaly.config.service.v1.global.ThreatRuleChange;
import ai.traceable.anomaly.config.service.v1.global.ThreatRuleUpdateDetails;
import com.google.inject.Inject;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

public class GlobalTestingModeResolver {

  private final RuleVersionManager ruleVersionManager;

  @Inject
  public GlobalTestingModeResolver(RuleVersionManager ruleVersionManager) {
    this.ruleVersionManager = ruleVersionManager;
  }

  public Optional<ScopedAnomalyDetectionConfig> resolveGlobalTestingMode(
      ScopedAnomalyDetectionConfig resolvedConfig,
      Optional<ScopedAnomalyConfigStatus> globalConfigStatus) {
    if (globalConfigStatus.isPresent()) {
      ScopedAnomalyConfigStatus status = globalConfigStatus.get();
      RuleVersionData globalModsecRuleVersionData =
          status.getGlobalModsecConfig().getRuleVersionData();
      if (canApplyRuleTestingMode(globalModsecRuleVersionData)) {
        RuleTestingMode ruleTestingMode = globalModsecRuleVersionData.getRuleTestingMode();
        if (ruleTestingMode.equals(RuleTestingMode.RULE_TESTING_MODE_DISABLED)
            || ruleTestingMode.equals(RuleTestingMode.RULE_TESTING_MODE_UNSPECIFIED)) {
          return Optional.empty();
        }
        RulesChangeLog changelog =
            ruleVersionManager.getRulesChangeLog(
                RuleType.RULE_TYPE_WEB_APPLICATION,
                globalModsecRuleVersionData.getCurrentVersion(),
                globalModsecRuleVersionData.getPreviousVersion());

        Set<String> rulesToTest = getRuleIdsForTestingMode(ruleTestingMode, changelog);

        if (!rulesToTest.isEmpty()) {
          return Optional.of(overrideActionToTestForRules(resolvedConfig, rulesToTest));
        }
      }
    }
    return Optional.empty();
  }

  private Set<String> getNewRuleIds(RulesChangeLog changelog) {
    Set<String> newRuleIds = new HashSet<>();
    if (changelog.getRuleChangesCount() > 0) {
      for (ThreatRuleChange ruleChange : changelog.getRuleChangesList()) {
        if (ruleChange.hasRuleIdsAdded()) {
          newRuleIds.addAll(ruleChange.getRuleIdsAdded().getValuesList());
        }
      }
    }
    return newRuleIds;
  }

  private Set<String> getUpdatedRuleIds(RulesChangeLog changelog) {
    Set<String> updatedRuleIds = new HashSet<>();
    if (changelog.getRuleChangesCount() > 0) {
      for (ThreatRuleChange ruleChange : changelog.getRuleChangesList()) {
        if (ruleChange.hasRuleUpdated()) {
          ThreatRuleUpdateDetails ruleUpdated = ruleChange.getRuleUpdated();
          String ruleId = ruleUpdated.getRuleId();

          for (ThreatRuleUpdateDetails.ThreatRuleUpdate update : ruleUpdated.getUpdatesList()) {
            if (update.hasSignatureUpdated() && update.getSignatureUpdated()) {
              updatedRuleIds.add(ruleId);
              break;
            }
          }
        }
      }
    }
    return updatedRuleIds;
  }

  public ScopedAnomalyDetectionConfig overrideActionToTestForRules(
      ScopedAnomalyDetectionConfig resolvedConfig, Set<String> rulesIds) {
    ScopedAnomalyDetectionConfig.Builder resultBuilder = resolvedConfig.toBuilder();
    List<AnomalyDetectionConfig> updatedConfigs = new ArrayList<>();

    for (AnomalyDetectionConfig config : resolvedConfig.getAnomalyDetectionConfigsList()) {
      AnomalyDetectionConfig.Builder configBuilder = config.toBuilder();

      if (config.hasModsecurityAnomalyDetectionConfig()) {
        ModsecurityAnomalyDetectionConfig modsecConfig =
            config.getModsecurityAnomalyDetectionConfig();
        if (modsecConfig.hasModsecAnomalyRule()) {
          ModsecurityAnomalyRuleConfig ruleConfig = modsecConfig.getModsecAnomalyRule();
          ModsecurityAnomalyRuleConfig.Builder ruleBuilder = ruleConfig.toBuilder();
          List<AnomalySubRuleConfig> updatedSubRules = new ArrayList<>();

          for (AnomalySubRuleConfig subRule : ruleConfig.getSubRuleConfigsList()) {
            AnomalySubRuleConfig.Builder subRuleBuilder = subRule.toBuilder();
            if (rulesIds.contains(subRule.getSubRuleId())
                && (!subRule
                    .getAnomalyRuleAction()
                    .equals(AnomalyRuleAction.ANOMALY_RULE_ACTION_DISABLE))) {
              subRuleBuilder.setAnomalyRuleAction(AnomalyRuleAction.ANOMALY_RULE_ACTION_TESTING);
            }
            updatedSubRules.add(subRuleBuilder.build());
          }
          ruleBuilder.clearSubRuleConfigs().addAllSubRuleConfigs(updatedSubRules);
          configBuilder.setModsecurityAnomalyDetectionConfig(
              modsecConfig.toBuilder().setModsecAnomalyRule(ruleBuilder).build());
        }
      }

      updatedConfigs.add(configBuilder.build());
    }

    return resultBuilder
        .clearAnomalyDetectionConfigs()
        .addAllAnomalyDetectionConfigs(updatedConfigs)
        .build();
  }

  private Set<String> getRuleIdsForTestingMode(
      RuleTestingMode ruleTestingMode, RulesChangeLog changelog) {
    switch (ruleTestingMode) {
      case RULE_TESTING_MODE_ENABLED_FOR_NEW_AND_UPDATED_RULES:
        Set<String> allTestingRules = new HashSet<>(getNewRuleIds(changelog));
        allTestingRules.addAll(getUpdatedRuleIds(changelog));
        return allTestingRules;
      case RULE_TESTING_MODE_ENABLED_FOR_NEW_RULES:
        return getNewRuleIds(changelog);
      case RULE_TESTING_MODE_ENABLED_FOR_UPDATED_RULES:
        return getUpdatedRuleIds(changelog);
      case RULE_TESTING_MODE_DISABLED:
      case RULE_TESTING_MODE_UNSPECIFIED:
      default:
        return Collections.emptySet();
    }
  }

  private static boolean canApplyRuleTestingMode(RuleVersionData globalModsecRuleVersionData) {
    return globalModsecRuleVersionData.hasCurrentVersion()
        && globalModsecRuleVersionData.hasPreviousVersion()
        && isDateAfter(
            globalModsecRuleVersionData.getCurrentVersion().getPublishedDate(),
            globalModsecRuleVersionData.getPreviousVersion().getPublishedDate())
        && globalModsecRuleVersionData
            .getCurrentVersion()
            .getVersionType()
            .equals(RuleVersionType.RULE_VERSION_TYPE_STABLE);
  }

  private static boolean isDateAfter(String date1, String date2) {
    return date1.compareTo(date2) > 0;
  }
}
