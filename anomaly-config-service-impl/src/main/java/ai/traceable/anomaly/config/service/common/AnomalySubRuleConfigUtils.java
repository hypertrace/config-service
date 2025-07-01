package ai.traceable.anomaly.config.service.common;

import ai.traceable.anomaly.config.service.v1.AnomalyRuleAction;
import ai.traceable.anomaly.config.service.v1.detector.AnomalySubRuleConfig;
import com.google.protobuf.Message;

public class AnomalySubRuleConfigUtils {
  public static AnomalySubRuleConfig populateNewFields(AnomalySubRuleConfig subRuleConfig) {
    AnomalySubRuleConfig.Builder builder = subRuleConfig.toBuilder();

    if (!subRuleConfig.hasInternal()
        && subRuleConfig.hasConfigStatus()
        && subRuleConfig.getConfigStatus().hasInternal()) {
      builder.setInternal(subRuleConfig.getConfigStatus().getInternal());
    }

    if (subRuleConfig
        .getAnomalyRuleAction()
        .equals(AnomalyRuleAction.ANOMALY_RULE_ACTION_UNSPECIFIED)) {

      if (subRuleConfig.hasConfigStatus() && subRuleConfig.getConfigStatus().getDisabled()) {
        builder.setAnomalyRuleAction(AnomalyRuleAction.ANOMALY_RULE_ACTION_DISABLE);
      } else if (subRuleConfig.getBlockingEnabled()) {
        builder.setAnomalyRuleAction(AnomalyRuleAction.ANOMALY_RULE_ACTION_BLOCK);
      } else {
        builder.setAnomalyRuleAction(AnomalyRuleAction.ANOMALY_RULE_ACTION_MONITOR);
      }
    }
    return builder.build();
  }

  public static AnomalySubRuleConfig mergeAndPopulateNewFields(
      AnomalySubRuleConfig fallbackConfig, AnomalySubRuleConfig preferredConfig) {
    Message mergedMessage = AnomalyConfigServiceUtils.mergeConfigs(fallbackConfig, preferredConfig);
    AnomalySubRuleConfig mergedConfig = (AnomalySubRuleConfig) mergedMessage;
    return populateNewFields(mergedConfig);
  }
}
