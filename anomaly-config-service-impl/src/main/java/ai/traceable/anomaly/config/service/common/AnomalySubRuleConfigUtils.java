package ai.traceable.anomaly.config.service.common;

import ai.traceable.anomaly.config.service.v1.AnomalyConfigStatusChange;
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
    return handleMergedConfigChange(mergedConfig, preferredConfig);
  }

  /**
   * Handles merging and synchronizing old and new fields between configs
   *
   * @param mergedConfig The base config after initial merge
   * @param requestedConfig The requested changes
   * @return Final config with synchronized fields
   */
  public static AnomalySubRuleConfig handleMergedConfigChange(
      AnomalySubRuleConfig mergedConfig, AnomalySubRuleConfig requestedConfig) {

    if (!hasAnyFieldsChangeSet(requestedConfig)) {
      return populateNewFields(mergedConfig);
    }
    AnomalySubRuleConfig.Builder builder = mergedConfig.toBuilder();
    if (hasNewFieldSet(requestedConfig)) {
      handleNewFieldUpdate(builder);
    } else if (hasOldFieldsSet(requestedConfig)) {
      handleOldFieldUpdate(builder, requestedConfig, mergedConfig);
    }
    return builder.build();
  }

  private static void handleNewFieldUpdate(AnomalySubRuleConfig.Builder builder) {
    populateOldFields(builder);
  }

  private static void handleOldFieldUpdate(
      AnomalySubRuleConfig.Builder builder,
      AnomalySubRuleConfig requestedConfig,
      AnomalySubRuleConfig mergedConfig) {
    AnomalyConfigStatusChange.Builder statusBuilder =
        buildConfigStatus(requestedConfig, mergedConfig);
    builder.setConfigStatus(statusBuilder.build());
    populateNewFields(builder);
  }

  private static AnomalyConfigStatusChange.Builder buildConfigStatus(
      AnomalySubRuleConfig requestedConfig, AnomalySubRuleConfig mergedConfig) {

    AnomalyConfigStatusChange.Builder builder = AnomalyConfigStatusChange.newBuilder();
    if (requestedConfig.hasConfigStatus() && requestedConfig.getConfigStatus().hasInternal()) {
      builder.setInternal(requestedConfig.getConfigStatus().getInternal());
    } else if (mergedConfig.hasConfigStatus() && mergedConfig.getConfigStatus().hasInternal()) {
      builder.setInternal(mergedConfig.getConfigStatus().getInternal());
    }

    if (requestedConfig.hasConfigStatus() && requestedConfig.getConfigStatus().hasDisabled()) {
      builder.setDisabled(requestedConfig.getConfigStatus().getDisabled());
    } else if (mergedConfig.hasConfigStatus() && mergedConfig.getConfigStatus().hasDisabled()) {
      builder.setDisabled(mergedConfig.getConfigStatus().getDisabled());
    }

    return builder;
  }

  private static void populateOldFields(AnomalySubRuleConfig.Builder builder) {
    AnomalyConfigStatusChange.Builder anomalyConfigStatusChangeBuilder =
        AnomalyConfigStatusChange.newBuilder();
    boolean hasConfigStatusUpdate = false;
    if (builder.hasInternal()) {
      anomalyConfigStatusChangeBuilder.setInternal(builder.getInternal());
      hasConfigStatusUpdate = true;
    }
    switch (builder.getAnomalyRuleAction()) {
      case ANOMALY_RULE_ACTION_DISABLE:
        anomalyConfigStatusChangeBuilder.setDisabled(true);
        hasConfigStatusUpdate = true;
        break;
      case ANOMALY_RULE_ACTION_BLOCK:
        builder.setBlockingEnabled(true);
        break;
      case ANOMALY_RULE_ACTION_MONITOR:
      case ANOMALY_RULE_ACTION_TESTING:
      default:
        break;
    }
    if (hasConfigStatusUpdate) {
      builder.setConfigStatus(anomalyConfigStatusChangeBuilder.build());
    }
  }

  private static void populateNewFields(AnomalySubRuleConfig.Builder builder) {
    if (builder.hasConfigStatus() && builder.getConfigStatus().hasInternal()) {
      builder.setInternal(builder.getConfigStatus().getInternal());
    }
    if (builder.hasConfigStatus() && builder.getConfigStatus().getDisabled()) {
      builder.setAnomalyRuleAction(AnomalyRuleAction.ANOMALY_RULE_ACTION_DISABLE);
    } else if (builder.getBlockingEnabled()) {
      builder.setAnomalyRuleAction(AnomalyRuleAction.ANOMALY_RULE_ACTION_BLOCK);
    } else {
      builder.setAnomalyRuleAction(AnomalyRuleAction.ANOMALY_RULE_ACTION_MONITOR);
    }
  }

  private static boolean hasAnyFieldsChangeSet(AnomalySubRuleConfig config) {
    return hasNewFieldSet(config) || hasOldFieldsSet(config);
  }

  private static boolean hasNewFieldSet(AnomalySubRuleConfig config) {
    return config.hasInternal()
        || config.getAnomalyRuleAction() != AnomalyRuleAction.ANOMALY_RULE_ACTION_UNSPECIFIED;
  }

  private static boolean hasOldFieldsSet(AnomalySubRuleConfig config) {
    return (config.hasConfigStatus()
            && (config.getConfigStatus().hasInternal() || config.getConfigStatus().hasDisabled()))
        || config.hasBlockingEnabled();
  }
}
