package ai.traceable.anomaly.config.service.trainer.trainingconfig;

import static ai.traceable.anomaly.config.service.common.AnomalyConfigServiceUtils.mergeConfigs;

import ai.traceable.anomaly.config.service.v1.trainer.ApiNamingTrainingConfig;
import ai.traceable.anomaly.config.service.v1.trainer.ApiNamingTrainingConfig.ConfigCase;
import ai.traceable.anomaly.config.service.v1.trainer.GetTrainingConfigsFilter;
import ai.traceable.anomaly.config.service.v1.trainer.LocalTrainingConfig;
import ai.traceable.anomaly.config.service.v1.trainer.MetadataTrainingConfig;
import ai.traceable.anomaly.config.service.v1.trainer.ScopedTrainingConfig;
import ai.traceable.anomaly.config.service.v1.trainer.SensitiveDataTrainingConfig;
import ai.traceable.anomaly.config.service.v1.trainer.SessionTrainingConfig;
import ai.traceable.anomaly.config.service.v1.trainer.TrainingConfig;
import ai.traceable.anomaly.config.service.v1.trainer.TrainingConfig.TrainingConfigCase;
import ai.traceable.anomaly.config.service.v1.trainer.TrainingConfigType;
import ai.traceable.anomaly.config.service.v1.trainer.VulnerabilityTrainingConfig;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Value;
import java.util.EnumMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.hypertrace.config.proto.converter.ConfigProtoConverter;

public class TrainingConfigHandler {
  public Value convert(ScopedTrainingConfig config) throws InvalidProtocolBufferException {
    return ConfigProtoConverter.convertToValue(config);
  }

  public ScopedTrainingConfig convert(Value config) throws InvalidProtocolBufferException {
    ScopedTrainingConfig.Builder builder = ScopedTrainingConfig.newBuilder();

    if (config != null && config.getKindCase() != Value.KindCase.NULL_VALUE) {
      ConfigProtoConverter.mergeFromValue(config, builder);
    }
    return builder.build();
  }

  public Set<TrainingConfig.TrainingConfigCase> convert(GetTrainingConfigsFilter filter) {

    Set<TrainingConfig.TrainingConfigCase> configCases = new HashSet<>();

    for (TrainingConfigType configType : filter.getTrainingConfigTypesList()) {
      switch (configType) {
        case TRAINING_CONFIG_TYPE_METADATA:
          configCases.add(TrainingConfig.TrainingConfigCase.METADATA_TRAINING_CONFIG);
          break;
        case TRAINING_CONFIG_TYPE_VULNERABILITY:
          configCases.add(TrainingConfig.TrainingConfigCase.VULNERABILITY_TRAINING_CONFIG);
          break;
        case TRAINING_CONFIG_TYPE_SESSION:
          configCases.add(TrainingConfig.TrainingConfigCase.SESSION_TRAINING_CONFIG);
          break;
        case TRAINING_CONFIG_TYPE_API_NAMING:
          configCases.add(TrainingConfig.TrainingConfigCase.API_NAMING_TRAINING_CONFIG);
          break;
        case TRAINING_CONFIG_TYPE_SENSITIVE_DATA:
          configCases.add(TrainingConfig.TrainingConfigCase.SENSITIVE_DATA_TRAINING_CONFIG);
          break;
        case TRAINING_CONFIG_TYPE_LOCAL_TRAINING:
          configCases.add(TrainingConfigCase.LOCAL_TRAINING_CONFIG);
        default:
          break;
      }
    }

    return configCases;
  }

  public ScopedTrainingConfig merge(
      ScopedTrainingConfig preferredConfig, ScopedTrainingConfig fallbackConfig) {

    if (fallbackConfig.equals(ScopedTrainingConfig.getDefaultInstance())) {
      return preferredConfig;
    }

    if (preferredConfig.equals(ScopedTrainingConfig.getDefaultInstance())) {
      return fallbackConfig;
    }

    EnumMap<MetadataTrainingConfig.ConfigCase, TrainingConfig> metadataTrainingConfigMap =
        new EnumMap<>(MetadataTrainingConfig.ConfigCase.class);

    EnumMap<VulnerabilityTrainingConfig.ConfigCase, TrainingConfig> vulnerabilityTrainingConfigMap =
        new EnumMap<>(VulnerabilityTrainingConfig.ConfigCase.class);

    EnumMap<SessionTrainingConfig.ConfigCase, TrainingConfig> sessionTrainingConfigMap =
        new EnumMap<>(SessionTrainingConfig.ConfigCase.class);

    EnumMap<ApiNamingTrainingConfig.ConfigCase, TrainingConfig> apiNamingTrainingConfigMap =
        new EnumMap<>(ApiNamingTrainingConfig.ConfigCase.class);

    EnumMap<SensitiveDataTrainingConfig.ConfigCase, TrainingConfig> sensitiveDataTrainingConfigMap =
        new EnumMap<>(SensitiveDataTrainingConfig.ConfigCase.class);

    EnumMap<LocalTrainingConfig.ConfigCase, TrainingConfig> localTrainingConfigMap =
        new EnumMap<>(LocalTrainingConfig.ConfigCase.class);

    preferredConfig
        .getTrainingConfigsList()
        .forEach(
            trainingConfig -> {
              switch (trainingConfig.getTrainingConfigCase()) {
                case METADATA_TRAINING_CONFIG:
                  metadataTrainingConfigMap.put(
                      trainingConfig.getMetadataTrainingConfig().getConfigCase(), trainingConfig);
                  break;
                case VULNERABILITY_TRAINING_CONFIG:
                  vulnerabilityTrainingConfigMap.put(
                      trainingConfig.getVulnerabilityTrainingConfig().getConfigCase(),
                      trainingConfig);
                  break;
                case SESSION_TRAINING_CONFIG:
                  sessionTrainingConfigMap.put(
                      trainingConfig.getSessionTrainingConfig().getConfigCase(), trainingConfig);
                  break;
                case API_NAMING_TRAINING_CONFIG:
                  resolveApiNamingTrainingConfig(apiNamingTrainingConfigMap, trainingConfig);
                  break;
                case SENSITIVE_DATA_TRAINING_CONFIG:
                  sensitiveDataTrainingConfigMap.put(
                      trainingConfig.getSensitiveDataTrainingConfig().getConfigCase(),
                      trainingConfig);
                  break;
                case LOCAL_TRAINING_CONFIG:
                  localTrainingConfigMap.put(
                      trainingConfig.getLocalTrainingConfig().getConfigCase(), trainingConfig);
                default:
                  break;
              }
            });

    fallbackConfig
        .getTrainingConfigsList()
        .forEach(
            trainingConfig -> {
              switch (trainingConfig.getTrainingConfigCase()) {
                case METADATA_TRAINING_CONFIG:
                  resolve(
                      metadataTrainingConfigMap,
                      trainingConfig.getMetadataTrainingConfig().getConfigCase(),
                      trainingConfig);
                  break;
                case VULNERABILITY_TRAINING_CONFIG:
                  resolve(
                      vulnerabilityTrainingConfigMap,
                      trainingConfig.getVulnerabilityTrainingConfig().getConfigCase(),
                      trainingConfig);
                  break;
                case SESSION_TRAINING_CONFIG:
                  resolve(
                      sessionTrainingConfigMap,
                      trainingConfig.getSessionTrainingConfig().getConfigCase(),
                      trainingConfig);
                  break;
                case API_NAMING_TRAINING_CONFIG:
                  resolve(
                      apiNamingTrainingConfigMap,
                      trainingConfig.getApiNamingTrainingConfig().getConfigCase(),
                      trainingConfig);
                  break;
                case SENSITIVE_DATA_TRAINING_CONFIG:
                  resolve(
                      sensitiveDataTrainingConfigMap,
                      trainingConfig.getSensitiveDataTrainingConfig().getConfigCase(),
                      trainingConfig);
                  break;
                case LOCAL_TRAINING_CONFIG:
                  resolve(
                      localTrainingConfigMap,
                      trainingConfig.getLocalTrainingConfig().getConfigCase(),
                      trainingConfig);
                default:
                  break;
              }
            });

    return ScopedTrainingConfig.newBuilder()
        .setConfigScope(preferredConfig.getConfigScope())
        .addAllTrainingConfigs(metadataTrainingConfigMap.values())
        .addAllTrainingConfigs(vulnerabilityTrainingConfigMap.values())
        .addAllTrainingConfigs(sessionTrainingConfigMap.values())
        .addAllTrainingConfigs(apiNamingTrainingConfigMap.values())
        .addAllTrainingConfigs(sensitiveDataTrainingConfigMap.values())
        .addAllTrainingConfigs(localTrainingConfigMap.values())
        .build();
  }

  private void resolveApiNamingTrainingConfig(
      EnumMap<ConfigCase, TrainingConfig> apiNamingTrainingConfigMap,
      TrainingConfig trainingConfig) {
    switch (trainingConfig.getApiNamingTrainingConfig().getConfigCase()) {
      case REJECT_FILTER_CONFIG:
      case TRIE_MODEL_TRAINING_CONFIG:
      case CUSTOM_RULES_LIST_CONFIG:
        apiNamingTrainingConfigMap.put(
            trainingConfig.getApiNamingTrainingConfig().getConfigCase(), trainingConfig);
        break;
      case URL_FILTER_CONFIG:
      default:
        break;
    }
  }

  public ScopedTrainingConfig deleteWholeTrainingConfigs(
      ScopedTrainingConfig scopedTrainingConfig,
      List<TrainingConfig> deleteTrainingConfigFilters,
      ScopedTrainingConfig.Builder deletedConfigBuilder) {
    ScopedTrainingConfig.Builder filteredConfigBuilder = ScopedTrainingConfig.newBuilder();
    filteredConfigBuilder.setConfigScope(scopedTrainingConfig.getConfigScope());
    for (TrainingConfig trainingConfig : scopedTrainingConfig.getTrainingConfigsList()) {
      if (isPresent(trainingConfig, deleteTrainingConfigFilters)) {
        deletedConfigBuilder.addTrainingConfigs(trainingConfig);
      } else {
        filteredConfigBuilder.addTrainingConfigs(trainingConfig);
      }
    }
    return filteredConfigBuilder.build();
  }

  private boolean isPresent(TrainingConfig trainingConfig, List<TrainingConfig> configFilter) {
    switch (trainingConfig.getTrainingConfigCase()) {
      case METADATA_TRAINING_CONFIG:
        return configFilter.stream()
            .filter(TrainingConfig::hasMetadataTrainingConfig)
            .map(TrainingConfig::getMetadataTrainingConfig)
            .anyMatch(
                filter ->
                    filter
                        .getConfigCase()
                        .equals(trainingConfig.getMetadataTrainingConfig().getConfigCase()));
      case API_NAMING_TRAINING_CONFIG:
        return configFilter.stream()
            .filter(TrainingConfig::hasApiNamingTrainingConfig)
            .map(TrainingConfig::getApiNamingTrainingConfig)
            .anyMatch(
                filter ->
                    filter
                        .getConfigCase()
                        .equals(trainingConfig.getApiNamingTrainingConfig().getConfigCase()));
      case VULNERABILITY_TRAINING_CONFIG:
        return configFilter.stream()
            .filter(TrainingConfig::hasVulnerabilityTrainingConfig)
            .map(TrainingConfig::getVulnerabilityTrainingConfig)
            .anyMatch(
                filter ->
                    filter
                        .getConfigCase()
                        .equals(trainingConfig.getVulnerabilityTrainingConfig().getConfigCase()));
      case SESSION_TRAINING_CONFIG:
        return configFilter.stream()
            .filter(TrainingConfig::hasSessionTrainingConfig)
            .map(TrainingConfig::getSessionTrainingConfig)
            .anyMatch(
                filter ->
                    filter
                        .getConfigCase()
                        .equals(trainingConfig.getSessionTrainingConfig().getConfigCase()));
      case LOCAL_TRAINING_CONFIG:
        return configFilter.stream()
            .filter(TrainingConfig::hasLocalTrainingConfig)
            .map(TrainingConfig::getLocalTrainingConfig)
            .anyMatch(
                filter ->
                    filter
                        .getConfigCase()
                        .equals(trainingConfig.getLocalTrainingConfig().getConfigCase()));
      default:
        return false;
    }
  }

  private <K extends Enum<K>> void resolve(
      EnumMap<K, TrainingConfig> configMap, K configCase, TrainingConfig trainingConfig) {
    if (configMap.containsKey(configCase)) {
      TrainingConfig mergedConfig =
          (TrainingConfig) mergeConfigs(trainingConfig, configMap.get(configCase));
      configMap.put(configCase, mergedConfig);
    } else {
      configMap.put(configCase, trainingConfig);
    }
  }
}
