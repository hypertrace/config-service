package ai.traceable.anomaly.config.service.detector.anomalydetection.converter;

import ai.traceable.anomaly.config.service.registry.apidef.ApiDefinitionRegistry;
import ai.traceable.anomaly.config.service.registry.session.SessionRulesRegistry;
import ai.traceable.anomaly.config.service.v1.detector.GetAnomalyDetectionConfigsFilter;
import ai.traceable.anomaly.config.service.v1.detector.ScopedAnomalyDetectionConfig;
import com.google.inject.Inject;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Value;
import org.hypertrace.config.proto.converter.ConfigProtoConverter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AnomalyDetectionConfigConverter {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(AnomalyDetectionConfigConverter.class);
  private final ApiDefinitionConfigConverter apiDefinitionConfigConverter;
  private final SessionDefinitionConfigConverter sessionDefinitionConfigConverter;
  private final ApiStateBasedConfigConverter apiStateBasedConfigConverter;
  private final BlockingMetadataConfigConverter blockingMetadataConfigConverter;
  private final ModsecConfigConverter modsecConfigConverter;

  @Inject
  public AnomalyDetectionConfigConverter(
      ApiDefinitionRegistry apiDefinitionRegistry, SessionRulesRegistry sessionRulesRegistry) {
    this.apiDefinitionConfigConverter = new ApiDefinitionConfigConverter(apiDefinitionRegistry);
    this.sessionDefinitionConfigConverter =
        new SessionDefinitionConfigConverter(sessionRulesRegistry);
    this.apiStateBasedConfigConverter = new ApiStateBasedConfigConverter();
    this.blockingMetadataConfigConverter = new BlockingMetadataConfigConverter();
    this.modsecConfigConverter = new ModsecConfigConverter();
  }

  public Value convert(ScopedAnomalyDetectionConfig config) throws InvalidProtocolBufferException {
    return ConfigProtoConverter.convertToValue(config);
  }

  public ScopedAnomalyDetectionConfig convert(Value config) throws InvalidProtocolBufferException {
    ScopedAnomalyDetectionConfig.Builder builder = ScopedAnomalyDetectionConfig.newBuilder();

    if (config != null && config.getKindCase() != Value.KindCase.NULL_VALUE) {
      ConfigProtoConverter.mergeFromValue(config, builder);
    }
    return builder.build();
  }

  public ScopedAnomalyDetectionConfig merge(
      ScopedAnomalyDetectionConfig preferredConfig,
      ScopedAnomalyDetectionConfig fallbackConfig,
      GetAnomalyDetectionConfigsFilter filter) {

    if (filter.getAnomalyDetectionConfigTypesList().isEmpty()) {
      return merge(preferredConfig, fallbackConfig);
    }

    ScopedAnomalyDetectionConfig.Builder builder = ScopedAnomalyDetectionConfig.newBuilder();
    builder.setConfigScope(preferredConfig.getConfigScope());

    filter
        .getAnomalyDetectionConfigTypesList()
        .forEach(
            configType -> {
              switch (configType) {
                case ANOMALY_DETECTION_CONFIG_TYPE_MODSECURITY:
                  builder.addAllAnomalyDetectionConfigs(
                      modsecConfigConverter.merge(preferredConfig, fallbackConfig));
                  break;
                case ANOMALY_DETECTION_CONFIG_TYPE_API_DEFINITION:
                  builder.addAllAnomalyDetectionConfigs(
                      apiDefinitionConfigConverter.merge(preferredConfig, fallbackConfig));
                  break;
                case ANOMALY_DETECTION_CONFIG_TYPE_API_STATE_BASED:
                  builder.addAllAnomalyDetectionConfigs(
                      apiStateBasedConfigConverter.merge(preferredConfig, fallbackConfig));
                  break;
                case ANOMALY_DETECTION_CONFIG_TYPE_SESSION_DEFINITION:
                  builder.addAllAnomalyDetectionConfigs(
                      sessionDefinitionConfigConverter.merge(preferredConfig, fallbackConfig));
                  break;
                case ANOMALY_DETECTION_CONFIG_TYPE_BLOCKING_METADATA:
                  builder.addAllAnomalyDetectionConfigs(
                      blockingMetadataConfigConverter.merge(preferredConfig, fallbackConfig));
                  break;
                default:
                  LOGGER.error(
                      "Invalid configType {} in anomalyDetectionConfigsFilter", configType);
                  break;
              }
            });

    return builder.build();
  }

  public ScopedAnomalyDetectionConfig merge(
      ScopedAnomalyDetectionConfig preferredConfig, ScopedAnomalyDetectionConfig fallbackConfig) {

    return ScopedAnomalyDetectionConfig.newBuilder()
        .setConfigScope(preferredConfig.getConfigScope())
        .addAllAnomalyDetectionConfigs(modsecConfigConverter.merge(preferredConfig, fallbackConfig))
        .addAllAnomalyDetectionConfigs(
            apiStateBasedConfigConverter.merge(preferredConfig, fallbackConfig))
        .addAllAnomalyDetectionConfigs(
            apiDefinitionConfigConverter.merge(preferredConfig, fallbackConfig))
        .addAllAnomalyDetectionConfigs(
            blockingMetadataConfigConverter.merge(preferredConfig, fallbackConfig))
        .addAllAnomalyDetectionConfigs(
            sessionDefinitionConfigConverter.merge(preferredConfig, fallbackConfig))
        .build();
  }
}
