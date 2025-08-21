package ai.traceable.anomaly.config.service.global.status;

import ai.traceable.anomaly.config.service.v1.AnomalyConfidenceLevel;
import ai.traceable.anomaly.config.service.v1.global.GlobalModsecConfigChange;
import ai.traceable.anomaly.config.service.v1.global.ModsecDefaultConfigsType;
import ai.traceable.anomaly.config.service.v1.global.ModsecGlobalConfig;
import ai.traceable.anomaly.config.service.v1.global.ScopedAnomalyConfigStatusChange;

public class GlobalAnomalyConfigStatusUtils {

  public static ScopedAnomalyConfigStatusChange handleMergedConfigChange(
      ScopedAnomalyConfigStatusChange mergedConfig,
      ScopedAnomalyConfigStatusChange requestedConfig) {

    if (!requestedConfig.hasGlobalModsecConfigChange()
        && !requestedConfig.hasModsecGlobalConfig()) {
      return mergedConfig;
    }
    ScopedAnomalyConfigStatusChange.Builder builder = mergedConfig.toBuilder();
    if (requestedConfig.hasGlobalModsecConfigChange()) {
      builder.setGlobalModsecConfigChange(
          getGlobalModsecConfigChange(mergedConfig.getModsecGlobalConfig()).toBuilder()
              .mergeFrom(mergedConfig.getGlobalModsecConfigChange())
              .build());
      builder.setModsecGlobalConfig(getModsecGlobalConfig(builder.getGlobalModsecConfigChange()));
    } else if (requestedConfig.hasModsecGlobalConfig()) {
      builder.setModsecGlobalConfig(
          getModsecGlobalConfig(mergedConfig.getGlobalModsecConfigChange()).toBuilder()
              .mergeFrom(mergedConfig.getModsecGlobalConfig())
              .build());
      builder.setGlobalModsecConfigChange(
          getGlobalModsecConfigChange(builder.getModsecGlobalConfig()));
    }

    return builder.build();
  }

  private static ModsecGlobalConfig getModsecGlobalConfig(GlobalModsecConfigChange source) {
    ModsecGlobalConfig.Builder builder = ModsecGlobalConfig.newBuilder();
    if (source.hasBlockingAvailableForRegularRules()) {
      builder.setBlockingAvailableForRegularRules(source.getBlockingAvailableForRegularRules());
    }
    if (source.hasUseTestRules()) {
      builder.setUseTestRules(source.getUseTestRules());
    }
    if (source.hasDisabled()) {
      builder.setDisabled(source.getDisabled());
    }
    if (source.hasEnabledForExitSpans()) {
      builder.setEnabledForExitSpans(source.getEnabledForExitSpans());
    }
    ModsecDefaultConfigsType configType = source.getDefaultConfigsType();
    if (configType != ModsecDefaultConfigsType.MODSEC_DEFAULT_CONFIGS_TYPE_UNSPECIFIED) {
      builder.setDefaultConfigsType(configType);
    }
    AnomalyConfidenceLevel confidenceLevel = source.getMinConfidenceLevel();
    if (confidenceLevel != AnomalyConfidenceLevel.ANOMALY_CONFIDENCE_LEVEL_UNSPECIFIED) {
      builder.setMinConfidenceLevel(confidenceLevel);
    }
    if (source.hasModsecEvaluationEngineConfig()) {
      builder.setModsecEvaluationEngineConfig(source.getModsecEvaluationEngineConfig());
    }
    return builder.build();
  }

  private static GlobalModsecConfigChange getGlobalModsecConfigChange(ModsecGlobalConfig source) {
    GlobalModsecConfigChange.Builder builder = GlobalModsecConfigChange.newBuilder();

    if (source.hasBlockingAvailableForRegularRules()) {
      builder.setBlockingAvailableForRegularRules(source.getBlockingAvailableForRegularRules());
    }
    if (source.hasUseTestRules()) {
      builder.setUseTestRules(source.getUseTestRules());
    }
    if (source.hasDisabled()) {
      builder.setDisabled(source.getDisabled());
    }
    if (source.hasEnabledForExitSpans()) {
      builder.setEnabledForExitSpans(source.getEnabledForExitSpans());
    }
    ModsecDefaultConfigsType configType = source.getDefaultConfigsType();
    if (configType != ModsecDefaultConfigsType.MODSEC_DEFAULT_CONFIGS_TYPE_UNSPECIFIED) {
      builder.setDefaultConfigsType(configType);
    }

    AnomalyConfidenceLevel confidenceLevel = source.getMinConfidenceLevel();
    if (confidenceLevel != AnomalyConfidenceLevel.ANOMALY_CONFIDENCE_LEVEL_UNSPECIFIED) {
      builder.setMinConfidenceLevel(confidenceLevel);
    }
    if (source.hasModsecEvaluationEngineConfig()) {
      builder.setModsecEvaluationEngineConfig(source.getModsecEvaluationEngineConfig());
    }
    return builder.build();
  }
}
