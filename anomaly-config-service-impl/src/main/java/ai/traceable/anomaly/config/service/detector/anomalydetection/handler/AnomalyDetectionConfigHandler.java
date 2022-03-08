package ai.traceable.anomaly.config.service.detector.anomalydetection.handler;

import ai.traceable.anomaly.config.service.registry.apidef.ApiDefinitionRegistry;
import ai.traceable.anomaly.config.service.registry.session.SessionRulesRegistry;
import ai.traceable.anomaly.config.service.v1.AnomalyConfigScope;
import ai.traceable.anomaly.config.service.v1.detector.AnomalyDetectionConfig;
import ai.traceable.anomaly.config.service.v1.detector.GetAnomalyDetectionConfigsFilter;
import ai.traceable.anomaly.config.service.v1.detector.ScopedAnomalyDetectionConfig;
import com.google.inject.Inject;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Value;
import java.util.List;
import org.hypertrace.config.proto.converter.ConfigProtoConverter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AnomalyDetectionConfigHandler {

  private static final Logger LOGGER = LoggerFactory.getLogger(AnomalyDetectionConfigHandler.class);
  private final ApiDefinitionConfigHandler apiDefinitionConfigHandler;
  private final SessionDefinitionConfigHandler sessionDefinitionConfigHandler;
  private final ApiStateBasedConfigHandler apiStateBasedConfigHandler;
  private final BlockingMetadataConfigHandler blockingMetadataConfigHandler;
  private final ModsecConfigHandler modsecConfigHandler;

  @Inject
  public AnomalyDetectionConfigHandler(
      ApiDefinitionRegistry apiDefinitionRegistry, SessionRulesRegistry sessionRulesRegistry) {
    this.apiDefinitionConfigHandler = new ApiDefinitionConfigHandler(apiDefinitionRegistry);
    this.sessionDefinitionConfigHandler = new SessionDefinitionConfigHandler(sessionRulesRegistry);
    this.apiStateBasedConfigHandler = new ApiStateBasedConfigHandler();
    this.blockingMetadataConfigHandler = new BlockingMetadataConfigHandler();
    this.modsecConfigHandler = new ModsecConfigHandler();
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
                      modsecConfigHandler.merge(preferredConfig, fallbackConfig));
                  break;
                case ANOMALY_DETECTION_CONFIG_TYPE_API_DEFINITION:
                  builder.addAllAnomalyDetectionConfigs(
                      apiDefinitionConfigHandler.merge(preferredConfig, fallbackConfig));
                  break;
                case ANOMALY_DETECTION_CONFIG_TYPE_API_STATE_BASED:
                  builder.addAllAnomalyDetectionConfigs(
                      apiStateBasedConfigHandler.merge(preferredConfig, fallbackConfig));
                  break;
                case ANOMALY_DETECTION_CONFIG_TYPE_SESSION_DEFINITION:
                  builder.addAllAnomalyDetectionConfigs(
                      sessionDefinitionConfigHandler.merge(preferredConfig, fallbackConfig));
                  break;
                case ANOMALY_DETECTION_CONFIG_TYPE_BLOCKING_METADATA:
                  builder.addAllAnomalyDetectionConfigs(
                      blockingMetadataConfigHandler.merge(preferredConfig, fallbackConfig));
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
        .addAllAnomalyDetectionConfigs(modsecConfigHandler.merge(preferredConfig, fallbackConfig))
        .addAllAnomalyDetectionConfigs(
            apiStateBasedConfigHandler.merge(preferredConfig, fallbackConfig))
        .addAllAnomalyDetectionConfigs(
            apiDefinitionConfigHandler.merge(preferredConfig, fallbackConfig))
        .addAllAnomalyDetectionConfigs(
            blockingMetadataConfigHandler.merge(preferredConfig, fallbackConfig))
        .addAllAnomalyDetectionConfigs(
            sessionDefinitionConfigHandler.merge(preferredConfig, fallbackConfig))
        .build();
  }

  public ScopedAnomalyDetectionConfig deleteWholeAnomalyDetectionConfigs(
      ScopedAnomalyDetectionConfig scopedAnomalyDetectionConfig,
      List<AnomalyDetectionConfig> detectionConfigsToDelete,
      ScopedAnomalyDetectionConfig.Builder deletedConfigBuilder) {

    ScopedAnomalyDetectionConfig.Builder filteredConfigBuilder =
        ScopedAnomalyDetectionConfig.newBuilder();

    AnomalyConfigScope configScope = scopedAnomalyDetectionConfig.getConfigScope();
    filteredConfigBuilder.setConfigScope(configScope);
    List<AnomalyDetectionConfig> anomalyDetectionConfigs =
        scopedAnomalyDetectionConfig.getAnomalyDetectionConfigsList();

    anomalyDetectionConfigs =
        apiDefinitionConfigHandler.deleteWholeAnomalyDetectionConfigs(
            anomalyDetectionConfigs, detectionConfigsToDelete, deletedConfigBuilder);
    anomalyDetectionConfigs =
        apiStateBasedConfigHandler.deleteWholeAnomalyDetectionConfigs(
            anomalyDetectionConfigs, detectionConfigsToDelete, deletedConfigBuilder);
    anomalyDetectionConfigs =
        sessionDefinitionConfigHandler.deleteWholeAnomalyDetectionConfigs(
            anomalyDetectionConfigs, detectionConfigsToDelete, deletedConfigBuilder);
    anomalyDetectionConfigs =
        blockingMetadataConfigHandler.deleteWholeAnomalyDetectionConfigs(
            anomalyDetectionConfigs, detectionConfigsToDelete, deletedConfigBuilder);
    anomalyDetectionConfigs =
        modsecConfigHandler.deleteWholeAnomalyDetectionConfigs(
            anomalyDetectionConfigs, detectionConfigsToDelete, deletedConfigBuilder);

    filteredConfigBuilder.addAllAnomalyDetectionConfigs(anomalyDetectionConfigs);
    return filteredConfigBuilder.build();
  }
}
