package ai.traceable.anomaly.config.service.global.status;

import ai.traceable.anomaly.config.service.global.AnomalyGlobalConfigServiceConfig;
import ai.traceable.anomaly.config.service.v1.AnomalyConfidenceLevel;
import ai.traceable.anomaly.config.service.v1.AnomalyConfigStatus;
import ai.traceable.anomaly.config.service.v1.AnomalyConfigStatusChange;
import ai.traceable.anomaly.config.service.v1.RuleTestingMode;
import ai.traceable.anomaly.config.service.v1.RuleVersion;
import ai.traceable.anomaly.config.service.v1.RuleVersionData;
import ai.traceable.anomaly.config.service.v1.RuleVersionDataChange;
import ai.traceable.anomaly.config.service.v1.global.ApiDefaultConfigsType;
import ai.traceable.anomaly.config.service.v1.global.GlobalGenAiConfig;
import ai.traceable.anomaly.config.service.v1.global.GlobalModsecConfig;
import ai.traceable.anomaly.config.service.v1.global.GlobalModsecConfigChange;
import ai.traceable.anomaly.config.service.v1.global.ModsecDefaultConfigsType;
import ai.traceable.anomaly.config.service.v1.global.ScopedAnomalyConfigStatus;
import ai.traceable.anomaly.config.service.v1.global.ScopedAnomalyConfigStatusChange;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Value;
import java.time.Duration;
import java.time.ZonedDateTime;
import java.util.AbstractMap.SimpleEntry;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.hypertrace.config.proto.converter.ConfigProtoConverter;

@Slf4j
public class ScopedGlobalConfigStatusChangeConverter {

  public Value convert(ScopedAnomalyConfigStatusChange config)
      throws InvalidProtocolBufferException {
    return ConfigProtoConverter.convertToValue(config);
  }

  public ScopedAnomalyConfigStatusChange convert(Value config)
      throws InvalidProtocolBufferException {
    ScopedAnomalyConfigStatusChange.Builder builder = ScopedAnomalyConfigStatusChange.newBuilder();

    if (config != null && config.getKindCase() != Value.KindCase.NULL_VALUE) {
      ConfigProtoConverter.mergeFromValue(config, builder);
    }
    return builder.build();
  }

  public ScopedAnomalyConfigStatus convertScopedConfig(
      ScopedAnomalyConfigStatusChange config,
      AnomalyGlobalConfigServiceConfig defaultConfig,
      AnomalyConfigStatus configStatus) {
    return ScopedAnomalyConfigStatus.newBuilder()
        .setConfigScope(config.getConfigScope())
        .setConfigStatus(configStatus)
        .setExcludedEventsConfig(config.getExcludedEventsConfig())
        .setMinConfidenceLevel(
            config.hasMinConfidenceLevel()
                ? config.getMinConfidenceLevel()
                : defaultConfig.getMinConfidenceLevel())
        .setEnabledForExitSpans(config.getEnabledForExitSpans())
        .setModsecGlobalConfig(
            config.toBuilder()
                .getModsecGlobalConfigBuilder()
                .setDefaultConfigsType(
                    getModsecDefaultConfigsType(
                        config,
                        defaultConfig,
                        config.getModsecGlobalConfig().getDefaultConfigsType()))
                .setMinConfidenceLevel(
                    config.getModsecGlobalConfig().getMinConfidenceLevel()
                            == AnomalyConfidenceLevel.ANOMALY_CONFIDENCE_LEVEL_UNSPECIFIED
                        ? defaultConfig.getMinConfidenceLevel()
                        : config.getModsecGlobalConfig().getMinConfidenceLevel()))
        .setGlobalModsecConfig(getGlobalModsecConfig(config, defaultConfig))
        .setApiGlobalConfig(
            config.toBuilder()
                .getApiGlobalConfigBuilder()
                .setDefaultConfigsType(
                    config.getApiGlobalConfig().getDefaultConfigsType()
                            == ApiDefaultConfigsType.API_DEFAULT_CONFIGS_TYPE_UNSPECIFIED
                        ? defaultConfig.getApiDefaultConfigsType()
                        : config.getApiGlobalConfig().getDefaultConfigsType())
                .build())
        .setGlobalGenAiConfig(
            GlobalGenAiConfig.newBuilder()
                .setDisabled(
                    config.getGlobalGenAiConfigChange().hasDisabled()
                        ? config.getGlobalGenAiConfigChange().getDisabled()
                        : defaultConfig.isGenAiDisabled()))
        .build();
  }

  private static ModsecDefaultConfigsType getModsecDefaultConfigsType(
      ScopedAnomalyConfigStatusChange config,
      AnomalyGlobalConfigServiceConfig defaultConfig,
      ModsecDefaultConfigsType configuredModsecDefaultConfigsType) {
    ModsecDefaultConfigsType modsecDefaultConfigsType =
        config.getConfigScope().hasEnvironmentScope()
            ? defaultConfig.getEnvScopeModsecDefaultConfigsType()
            : defaultConfig.getModsecDefaultConfigsType();
    return configuredModsecDefaultConfigsType
            == ModsecDefaultConfigsType.MODSEC_DEFAULT_CONFIGS_TYPE_UNSPECIFIED
        ? modsecDefaultConfigsType
        : configuredModsecDefaultConfigsType;
  }

  public ScopedAnomalyConfigStatusChange merge(
      ScopedAnomalyConfigStatusChange highPriorityConfig,
      ScopedAnomalyConfigStatusChange lowPriorityConfig) {
    return lowPriorityConfig.toBuilder().mergeFrom(highPriorityConfig).build();
  }

  public AnomalyConfigStatusChange merge(
      AnomalyConfigStatusChange highPriorityConfig, AnomalyConfigStatusChange lowPriorityConfig) {
    return lowPriorityConfig.toBuilder().mergeFrom(highPriorityConfig).build();
  }

  public AnomalyConfigStatus merge(
      AnomalyConfigStatusChange highPriorityConfig, AnomalyConfigStatus lowPriorityConfig) {
    AnomalyConfigStatus.Builder builder = lowPriorityConfig.toBuilder();
    if (highPriorityConfig.hasDisabled()) {
      builder.setDisabled(highPriorityConfig.getDisabled());
    }
    if (highPriorityConfig.hasInternal()) {
      builder.setInternal(highPriorityConfig.getInternal());
    }
    return builder.build();
  }

  private GlobalModsecConfig getGlobalModsecConfig(
      final ScopedAnomalyConfigStatusChange config,
      final AnomalyGlobalConfigServiceConfig defaultConfig) {
    GlobalModsecConfigChange globalModsecConfigChange = config.getGlobalModsecConfigChange();
    Map.Entry<RuleVersion, RuleVersion> ruleVersions =
        getRuleVersions(
            defaultConfig.getNewWebAppStableVersion(),
            defaultConfig.getOldWebAppStableVersion(),
            globalModsecConfigChange.getRuleVersionDataChange().getOverrideVersion(),
            globalModsecConfigChange.getRuleVersionDataChange().getStableVersion());
    GlobalModsecConfig.Builder builder = GlobalModsecConfig.newBuilder();

    if (isNullOrDefault(globalModsecConfigChange)) {
      builder
          .setBlockingAvailableForRegularRules(
              config.getModsecGlobalConfig().getBlockingAvailableForRegularRules())
          .setUseTestRules(config.getModsecGlobalConfig().getUseTestRules())
          .setDisabled(config.getModsecGlobalConfig().getDisabled())
          .setEnabledForExitSpans(config.getModsecGlobalConfig().getEnabledForExitSpans())
          .setModsecEvaluationEngineConfig(
              config.getModsecGlobalConfig().getModsecEvaluationEngineConfig());
      ModsecDefaultConfigsType defaultConfigsType =
          getModsecDefaultConfigsType(
              config, defaultConfig, config.getModsecGlobalConfig().getDefaultConfigsType());
      builder.setDefaultConfigsType(defaultConfigsType);
      AnomalyConfidenceLevel confidenceLevel =
          config.getModsecGlobalConfig().getMinConfidenceLevel();
      if (confidenceLevel == AnomalyConfidenceLevel.ANOMALY_CONFIDENCE_LEVEL_UNSPECIFIED) {
        builder.setMinConfidenceLevel(defaultConfig.getMinConfidenceLevel());
      } else {
        builder.setMinConfidenceLevel(config.getModsecGlobalConfig().getMinConfidenceLevel());
      }
    } else {
      builder
          .setBlockingAvailableForRegularRules(
              globalModsecConfigChange.getBlockingAvailableForRegularRules())
          .setUseTestRules(globalModsecConfigChange.getUseTestRules())
          .setDisabled(globalModsecConfigChange.getDisabled())
          .setEnabledForExitSpans(globalModsecConfigChange.getEnabledForExitSpans())
          .setModsecEvaluationEngineConfig(
              globalModsecConfigChange.getModsecEvaluationEngineConfig());
      ModsecDefaultConfigsType defaultConfigsType =
          getModsecDefaultConfigsType(
              config, defaultConfig, globalModsecConfigChange.getDefaultConfigsType());
      builder.setDefaultConfigsType(defaultConfigsType);
      if (globalModsecConfigChange.getMinConfidenceLevel()
          == AnomalyConfidenceLevel.ANOMALY_CONFIDENCE_LEVEL_UNSPECIFIED) {
        builder.setMinConfidenceLevel(defaultConfig.getMinConfidenceLevel());
      } else {
        builder.setMinConfidenceLevel(globalModsecConfigChange.getMinConfidenceLevel());
      }
    }
    RuleVersionData.Builder ruleVersionDataBuilder = RuleVersionData.newBuilder();
    if (globalModsecConfigChange.getRuleVersionDataChange().hasExperimentalVersion()) {
      ruleVersionDataBuilder.setExperimentalVersion(
          globalModsecConfigChange.getRuleVersionDataChange().getExperimentalVersion());
    }
    ruleVersionDataBuilder
        .setCurrentVersion(ruleVersions.getKey())
        .setPreviousVersion(ruleVersions.getValue())
        .setRuleTestingMode(getRuleTestingMode(globalModsecConfigChange.getRuleVersionDataChange()))
        .build();
    builder.setRuleVersionData(ruleVersionDataBuilder.build());
    return builder.build();
  }

  private static Map.Entry<RuleVersion, RuleVersion> getRuleVersions(
      final RuleVersion newStableVersion,
      final RuleVersion oldStableVersion,
      final RuleVersion overrideVersion,
      final RuleVersion currentStableVersion) {

    if (isNotNullOrDefault(overrideVersion)) {
      return new SimpleEntry<>(overrideVersion, newStableVersion);
    }

    if (newStableVersion.equals(oldStableVersion)) {
      return new SimpleEntry<>(newStableVersion, oldStableVersion);
    }

    if (isWithinTwoWeeks(newStableVersion.getPublishedDate())) {
      if (isNotNullOrDefault(currentStableVersion)
          && (currentStableVersion.equals(newStableVersion)
              || currentStableVersion.equals(oldStableVersion))) {
        return new SimpleEntry<>(
            currentStableVersion,
            currentStableVersion.equals(oldStableVersion) ? newStableVersion : oldStableVersion);
      }
      return new SimpleEntry<>(newStableVersion, oldStableVersion);
    }
    return new SimpleEntry<>(newStableVersion, newStableVersion);
  }

  private static boolean isWithinTwoWeeks(String newStableVersionDate) {
    if (newStableVersionDate.isEmpty()) {
      return false;
    }
    try {
      ZonedDateTime newStableVersionDateTime = ZonedDateTime.parse(newStableVersionDate);
      ZonedDateTime currentDateTime = ZonedDateTime.now();
      return Duration.between(newStableVersionDateTime, currentDateTime).toDays() <= 14;
    } catch (Exception e) {
      log.error("Error parsing dates for newStableVersionDate: {}", newStableVersionDate, e);
      return false;
    }
  }

  private static boolean isNullOrDefault(final GlobalModsecConfigChange globalModsecConfigChange) {
    return globalModsecConfigChange == null
        || globalModsecConfigChange.equals(GlobalModsecConfigChange.getDefaultInstance());
  }

  private static boolean isNotNullOrDefault(final RuleVersion ruleVersion) {
    return ruleVersion != null && !ruleVersion.equals(RuleVersion.getDefaultInstance());
  }

  private static RuleTestingMode getRuleTestingMode(RuleVersionDataChange ruleVersionDataChange) {
    if (ruleVersionDataChange == null
        || ruleVersionDataChange.equals(RuleVersionDataChange.getDefaultInstance())) {
      return RuleTestingMode.RULE_TESTING_MODE_ENABLED_FOR_NEW_RULES;
    }
    return ruleVersionDataChange.getRuleTestingMode()
            != RuleTestingMode.RULE_TESTING_MODE_UNSPECIFIED
        ? ruleVersionDataChange.getRuleTestingMode()
        : RuleTestingMode.RULE_TESTING_MODE_ENABLED_FOR_NEW_RULES;
  }
}
