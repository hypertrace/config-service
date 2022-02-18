package ai.traceable.anomaly.config.service.trainer.trainingconfig;

import ai.traceable.anomaly.config.service.v1.trainer.ApiNamingTrainingConfig;
import ai.traceable.anomaly.config.service.v1.trainer.GetTrainingConfigsFilter;
import ai.traceable.anomaly.config.service.v1.trainer.MetadataTrainingConfig;
import ai.traceable.anomaly.config.service.v1.trainer.ScopedTrainingConfig;
import ai.traceable.anomaly.config.service.v1.trainer.SensitiveDataTrainingConfig;
import ai.traceable.anomaly.config.service.v1.trainer.SessionTrainingConfig;
import ai.traceable.anomaly.config.service.v1.trainer.TrainingConfig;
import ai.traceable.anomaly.config.service.v1.trainer.TrainingConfigType;
import ai.traceable.anomaly.config.service.v1.trainer.VulnerabilityTrainingConfig;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Value;
import java.util.EnumMap;
import java.util.HashSet;
import java.util.Set;
import org.hypertrace.config.proto.converter.ConfigProtoConverter;

public class TrainingConfigConverter {

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
                  apiNamingTrainingConfigMap.put(
                      trainingConfig.getApiNamingTrainingConfig().getConfigCase(), trainingConfig);
                  break;
                case SENSITIVE_DATA_TRAINING_CONFIG:
                  sensitiveDataTrainingConfigMap.put(
                      trainingConfig.getSensitiveDataTrainingConfig().getConfigCase(),
                      trainingConfig);
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
        .build();
  }

  private <K extends Enum<K>> void resolve(
      EnumMap<K, TrainingConfig> configMap, K configCase, TrainingConfig trainingConfig) {
    if (configMap.containsKey(configCase)) {
      configMap.put(
          configCase, trainingConfig.toBuilder().mergeFrom(configMap.get(configCase)).build());
    } else {
      configMap.put(configCase, trainingConfig);
    }
  }
}
