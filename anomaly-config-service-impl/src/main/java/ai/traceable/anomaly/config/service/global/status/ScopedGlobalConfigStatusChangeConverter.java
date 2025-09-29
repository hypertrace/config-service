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
import ai.traceable.anomaly.config.service.v1.global.ApiGlobalConfig;
import ai.traceable.anomaly.config.service.v1.global.GlobalApiConfig;
import ai.traceable.anomaly.config.service.v1.global.GlobalApiConfigChange;
import ai.traceable.anomaly.config.service.v1.global.GlobalGenAiConfig;
import ai.traceable.anomaly.config.service.v1.global.GlobalModsecConfig;
import ai.traceable.anomaly.config.service.v1.global.GlobalModsecConfigChange;
import ai.traceable.anomaly.config.service.v1.global.ModsecDefaultConfigsType;
import ai.traceable.anomaly.config.service.v1.global.ModsecGlobalConfig;
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
                .setDisabled(
                    config.getModsecGlobalConfig().hasDisabled()
                        ? config.getModsecGlobalConfig().getDisabled()
                        : defaultConfig.isDisabled())
                .setDefaultConfigsType(
                    getModsecDefaultConfigsType(
                        config,
                        defaultConfig,
                        config.getModsecGlobalConfig().getDefaultConfigsType()))
                .setMinConfidenceLevel(
                    config.getModsecGlobalConfig().getMinConfidenceLevel()
                            == AnomalyConfidenceLevel.ANOMALY_CONFIDENCE_LEVEL_UNSPECIFIED
                        ? defaultConfig.getMinConfidenceLevel()
                        : config.getModsecGlobalConfig().getMinConfidenceLevel())
                .setEnabledForExitSpans(
                    config.getModsecGlobalConfig().hasEnabledForExitSpans()
                        ? config.getModsecGlobalConfig().getEnabledForExitSpans()
                        : defaultConfig.isModsecExitSpansEvalEnabled()))
        .setGlobalModsecConfig(getGlobalModsecConfig(config, defaultConfig))
        .setApiGlobalConfig(
            config.toBuilder()
                .getApiGlobalConfigBuilder()
                .setDisabled(
                    config.getApiGlobalConfig().hasDisabled()
                        ? config.getApiGlobalConfig().getDisabled()
                        : defaultConfig.isDisabled())
                .setDefaultConfigsType(
                    config.getApiGlobalConfig().getDefaultConfigsType()
                            == ApiDefaultConfigsType.API_DEFAULT_CONFIGS_TYPE_UNSPECIFIED
                        ? defaultConfig.getApiDefaultConfigsType()
                        : config.getApiGlobalConfig().getDefaultConfigsType())
                .setEnabledForExitSpans(
                    config.getApiGlobalConfig().hasEnabledForExitSpans()
                        ? config.getApiGlobalConfig().getEnabledForExitSpans()
                        : defaultConfig.isApiExitSpansEvalEnabled())
                .build())
        .setGlobalApiConfig(getGlobalApiConfig(config, defaultConfig))
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
            defaultConfig.getWebAppRuleTestingModeRetentionDays(),
            globalModsecConfigChange.getRuleVersionDataChange().getOverrideVersion(),
            globalModsecConfigChange.getRuleVersionDataChange().getStableVersion());
    GlobalModsecConfig.Builder builder = GlobalModsecConfig.newBuilder();

    if (isNullOrDefault(globalModsecConfigChange)) {
      ModsecGlobalConfig modsecGlobalConfig = config.getModsecGlobalConfig();
      builder
          .setBlockingAvailableForRegularRules(
              modsecGlobalConfig.getBlockingAvailableForRegularRules())
          .setUseTestRules(modsecGlobalConfig.getUseTestRules())
          .setDisabled(
              modsecGlobalConfig.hasDisabled()
                  ? modsecGlobalConfig.getDisabled()
                  : defaultConfig.isDisabled())
          .setEnabledForExitSpans(
              modsecGlobalConfig.hasEnabledForExitSpans()
                  ? modsecGlobalConfig.getEnabledForExitSpans()
                  : defaultConfig.isModsecExitSpansEvalEnabled())
          .setModsecEvaluationEngineConfig(modsecGlobalConfig.getModsecEvaluationEngineConfig());
      ModsecDefaultConfigsType defaultConfigsType =
          getModsecDefaultConfigsType(
              config, defaultConfig, modsecGlobalConfig.getDefaultConfigsType());
      builder.setDefaultConfigsType(defaultConfigsType);
      AnomalyConfidenceLevel confidenceLevel = modsecGlobalConfig.getMinConfidenceLevel();
      if (confidenceLevel == AnomalyConfidenceLevel.ANOMALY_CONFIDENCE_LEVEL_UNSPECIFIED) {
        builder.setMinConfidenceLevel(defaultConfig.getMinConfidenceLevel());
      } else {
        builder.setMinConfidenceLevel(modsecGlobalConfig.getMinConfidenceLevel());
      }
    } else {
      builder
          .setBlockingAvailableForRegularRules(
              globalModsecConfigChange.getBlockingAvailableForRegularRules())
          .setUseTestRules(globalModsecConfigChange.getUseTestRules())
          .setDisabled(
              globalModsecConfigChange.hasDisabled()
                  ? globalModsecConfigChange.getDisabled()
                  : defaultConfig.isDisabled())
          .setEnabledForExitSpans(
              globalModsecConfigChange.hasEnabledForExitSpans()
                  ? globalModsecConfigChange.getEnabledForExitSpans()
                  : defaultConfig.isModsecExitSpansEvalEnabled())
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

  private GlobalApiConfig getGlobalApiConfig(
      final ScopedAnomalyConfigStatusChange config,
      final AnomalyGlobalConfigServiceConfig defaultConfig) {
    GlobalApiConfigChange globalApiConfigChange = config.getGlobalApiConfigChange();
    Map.Entry<RuleVersion, RuleVersion> ruleVersions =
        getRuleVersions(
            defaultConfig.getNewApiProtectionStableVersion(),
            defaultConfig.getOldApiProtectionStableVersion(),
            -1, // we don't have testing mode for API rules so using -1 to ignore retention days
            globalApiConfigChange.getRuleVersionDataChange().getOverrideVersion(),
            globalApiConfigChange.getRuleVersionDataChange().getStableVersion());
    GlobalApiConfig.Builder builder = GlobalApiConfig.newBuilder();

    if (isNullOrDefault(globalApiConfigChange)) {
      ApiGlobalConfig apiGlobalConfig = config.getApiGlobalConfig();
      builder
          .setDisabled(
              apiGlobalConfig.hasDisabled()
                  ? apiGlobalConfig.getDisabled()
                  : defaultConfig.isDisabled())
          .setEnabledForExitSpans(
              apiGlobalConfig.hasEnabledForExitSpans()
                  ? apiGlobalConfig.getEnabledForExitSpans()
                  : defaultConfig.isModsecExitSpansEvalEnabled())
          .setDefaultConfigsType(
              apiGlobalConfig.getDefaultConfigsType()
                      == ApiDefaultConfigsType.API_DEFAULT_CONFIGS_TYPE_UNSPECIFIED
                  ? defaultConfig.getApiDefaultConfigsType()
                  : apiGlobalConfig.getDefaultConfigsType());
    } else {
      builder
          .setDisabled(
              globalApiConfigChange.hasDisabled()
                  ? globalApiConfigChange.getDisabled()
                  : defaultConfig.isDisabled())
          .setEnabledForExitSpans(
              globalApiConfigChange.hasEnabledForExitSpans()
                  ? globalApiConfigChange.getEnabledForExitSpans()
                  : defaultConfig.isModsecExitSpansEvalEnabled())
          .setDefaultConfigsType(
              globalApiConfigChange.getDefaultConfigsType()
                      == ApiDefaultConfigsType.API_DEFAULT_CONFIGS_TYPE_UNSPECIFIED
                  ? defaultConfig.getApiDefaultConfigsType()
                  : globalApiConfigChange.getDefaultConfigsType());
    }
    RuleVersionData.Builder ruleVersionDataBuilder = RuleVersionData.newBuilder();
    ruleVersionDataBuilder
        .setCurrentVersion(ruleVersions.getKey())
        .setPreviousVersion(ruleVersions.getValue())
        .build();
    builder.setRuleVersionData(ruleVersionDataBuilder.build());
    return builder.build();
  }

  private static Map.Entry<RuleVersion, RuleVersion> getRuleVersions(
      final RuleVersion newStableVersion,
      final RuleVersion oldStableVersion,
      final long ruleTestingModeRetentionDays,
      final RuleVersion overrideVersion,
      final RuleVersion currentStableVersion) {

    if (isNotNullOrDefault(overrideVersion)) {
      return new SimpleEntry<>(overrideVersion, newStableVersion);
    }

    if (newStableVersion.equals(oldStableVersion)) {
      return new SimpleEntry<>(newStableVersion, oldStableVersion);
    }

    if (isWithinRetentionDays(newStableVersion.getPublishedDate(), ruleTestingModeRetentionDays)) {
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

  private static boolean isWithinRetentionDays(
      String newStableVersionDate, long ruleTestingModeRetentionDays) {
    if (newStableVersionDate.isEmpty() || ruleTestingModeRetentionDays <= 0) {
      return false;
    }
    try {
      ZonedDateTime newStableVersionDateTime = ZonedDateTime.parse(newStableVersionDate);
      ZonedDateTime currentDateTime = ZonedDateTime.now();
      return Duration.between(newStableVersionDateTime, currentDateTime).toDays()
          <= ruleTestingModeRetentionDays;
    } catch (Exception e) {
      log.error("Error parsing dates for newStableVersionDate: {}", newStableVersionDate, e);
      return false;
    }
  }

  private static boolean isNullOrDefault(final GlobalModsecConfigChange globalModsecConfigChange) {
    return globalModsecConfigChange == null
        || globalModsecConfigChange.equals(GlobalModsecConfigChange.getDefaultInstance());
  }

  private static boolean isNullOrDefault(final GlobalApiConfigChange globalApiConfigChange) {
    return globalApiConfigChange == null
        || globalApiConfigChange.equals(GlobalApiConfigChange.getDefaultInstance());
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
