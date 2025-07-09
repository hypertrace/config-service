package ai.traceable.anomaly.config.service.detector.anomalydetection.handler;

import ai.traceable.anomaly.config.service.registry.accounttakeover.AccountTakeoverRulesRegistry;
import ai.traceable.anomaly.config.service.registry.apidef.ApiDefinitionRegistry;
import ai.traceable.anomaly.config.service.registry.credentialstuffing.CredentialStuffingRulesRegistry;
import ai.traceable.anomaly.config.service.registry.genai.GenAiRulesRegistry;
import ai.traceable.anomaly.config.service.registry.session.SessionRulesRegistry;
import ai.traceable.anomaly.config.service.registry.volumetric.VolumetricRulesRegistry;
import ai.traceable.anomaly.config.service.v1.AnomalyConfigScope;
import ai.traceable.anomaly.config.service.v1.detector.AnomalyDetectionConfig;
import ai.traceable.anomaly.config.service.v1.detector.AnomalyDetectionConfigType;
import ai.traceable.anomaly.config.service.v1.detector.GetAnomalyDetectionConfigsFilter;
import ai.traceable.anomaly.config.service.v1.detector.ScopedAnomalyDetectionConfig;
import com.google.inject.Inject;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Value;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
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
  private final CustomRulesConfigHandler customRulesConfigHandler;

  private final VolumetricDetectionConfigHandler volumetricDetectionConfigHandler;

  private final CredentialStuffingDetectionConfigHandler credentialStuffingDetectionConfigHandler;
  private final AccountTakeoverDetectionConfigHandler accountTakeoverDetectionConfigHandler;
  private final GenAiDetectionConfigHandler genAiDetectionConfigHandler;

  @Inject
  public AnomalyDetectionConfigHandler(
      ApiDefinitionRegistry apiDefinitionRegistry,
      SessionRulesRegistry sessionRulesRegistry,
      VolumetricRulesRegistry volumetricRulesRegistry,
      CredentialStuffingRulesRegistry credentialStuffingRulesRegistry,
      AccountTakeoverRulesRegistry accountTakeoverRulesRegistry,
      GenAiRulesRegistry genAiRulesRegistry) {
    this.apiDefinitionConfigHandler = new ApiDefinitionConfigHandler(apiDefinitionRegistry);
    this.sessionDefinitionConfigHandler = new SessionDefinitionConfigHandler(sessionRulesRegistry);
    this.apiStateBasedConfigHandler = new ApiStateBasedConfigHandler();
    this.blockingMetadataConfigHandler = new BlockingMetadataConfigHandler();
    this.modsecConfigHandler = new ModsecConfigHandler();
    this.customRulesConfigHandler = new CustomRulesConfigHandler();
    this.volumetricDetectionConfigHandler =
        new VolumetricDetectionConfigHandler(volumetricRulesRegistry);
    this.credentialStuffingDetectionConfigHandler =
        new CredentialStuffingDetectionConfigHandler(credentialStuffingRulesRegistry);
    this.accountTakeoverDetectionConfigHandler =
        new AccountTakeoverDetectionConfigHandler(accountTakeoverRulesRegistry);
    this.genAiDetectionConfigHandler = new GenAiDetectionConfigHandler(genAiRulesRegistry);
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

  public Set<AnomalyDetectionConfig.AnomalyDetectionConfigCase> convert(
      GetAnomalyDetectionConfigsFilter filter) {
    Set<AnomalyDetectionConfig.AnomalyDetectionConfigCase> configCases = new HashSet<>();
    for (AnomalyDetectionConfigType configType : filter.getAnomalyDetectionConfigTypesList()) {
      switch (configType) {
        case ANOMALY_DETECTION_CONFIG_TYPE_MODSECURITY:
          configCases.add(
              AnomalyDetectionConfig.AnomalyDetectionConfigCase
                  .MODSECURITY_ANOMALY_DETECTION_CONFIG);
          break;
        case ANOMALY_DETECTION_CONFIG_TYPE_API_DEFINITION:
          configCases.add(
              AnomalyDetectionConfig.AnomalyDetectionConfigCase
                  .API_DEFINITION_METADATA_ANOMALY_DETECTION_CONFIG);
          break;
        case ANOMALY_DETECTION_CONFIG_TYPE_API_STATE_BASED:
          configCases.add(
              AnomalyDetectionConfig.AnomalyDetectionConfigCase
                  .API_STATE_BASED_ANOMALY_DETECTION_CONFIG);
          break;
        case ANOMALY_DETECTION_CONFIG_TYPE_BLOCKING_METADATA:
          configCases.add(
              AnomalyDetectionConfig.AnomalyDetectionConfigCase
                  .BLOCKING_METADATA_ANOMALY_DETECTION_CONFIG);
          break;
        case ANOMALY_DETECTION_CONFIG_TYPE_SESSION_DEFINITION:
          configCases.add(
              AnomalyDetectionConfig.AnomalyDetectionConfigCase
                  .SESSION_DEFINITION_METADATA_ANOMALY_DETECTION_CONFIG);
          break;
        case ANOMALY_DETECTION_CONFIG_TYPE_CUSTOM_RULES:
          configCases.add(
              AnomalyDetectionConfig.AnomalyDetectionConfigCase
                  .CUSTOM_RULES_ANOMALY_DETECTION_CONFIG);
          break;
        case ANOMALY_DETECTION_CONFIG_TYPE_VOLUMETRIC:
          configCases.add(
              AnomalyDetectionConfig.AnomalyDetectionConfigCase
                  .VOLUMETRIC_ANOMALY_DETECTION_CONFIG);
          break;
        case ANOMALY_DETECTION_CONFIG_TYPE_CREDENTIAL_STUFFING:
          configCases.add(
              AnomalyDetectionConfig.AnomalyDetectionConfigCase
                  .CREDENTIAL_ANOMALY_DETECTION_CONFIG);
          configCases.add(
              AnomalyDetectionConfig.AnomalyDetectionConfigCase
                  .ACCOUNT_TAKEOVER_ANOMALY_DETECTION_CONFIG);
          break;
        case ANOMALY_DETECTION_CONFIG_TYPE_GEN_AI:
          configCases.add(
              AnomalyDetectionConfig.AnomalyDetectionConfigCase.GEN_AI_ANOMALY_DETECTION_CONFIG);
          break;
        default:
          break;
      }
    }
    return configCases;
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
        .addAllAnomalyDetectionConfigs(
            customRulesConfigHandler.merge(preferredConfig, fallbackConfig))
        .addAllAnomalyDetectionConfigs(
            volumetricDetectionConfigHandler.merge(preferredConfig, fallbackConfig))
        .addAllAnomalyDetectionConfigs(
            credentialStuffingDetectionConfigHandler.merge(preferredConfig, fallbackConfig))
        .addAllAnomalyDetectionConfigs(
            accountTakeoverDetectionConfigHandler.merge(preferredConfig, fallbackConfig))
        .addAllAnomalyDetectionConfigs(
            genAiDetectionConfigHandler.merge(preferredConfig, fallbackConfig))
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
    anomalyDetectionConfigs =
        customRulesConfigHandler.deleteWholeAnomalyDetectionConfigs(
            anomalyDetectionConfigs, detectionConfigsToDelete, deletedConfigBuilder);
    anomalyDetectionConfigs =
        volumetricDetectionConfigHandler.deleteWholeAnomalyDetectionConfig(
            anomalyDetectionConfigs, detectionConfigsToDelete, deletedConfigBuilder);
    anomalyDetectionConfigs =
        credentialStuffingDetectionConfigHandler.deleteWholeAnomalyDetectionConfig(
            anomalyDetectionConfigs, detectionConfigsToDelete, deletedConfigBuilder);
    anomalyDetectionConfigs =
        accountTakeoverDetectionConfigHandler.deleteWholeAnomalyDetectionConfig(
            anomalyDetectionConfigs, detectionConfigsToDelete, deletedConfigBuilder);
    anomalyDetectionConfigs =
        genAiDetectionConfigHandler.deleteWholeAnomalyDetectionConfig(
            anomalyDetectionConfigs, detectionConfigsToDelete, deletedConfigBuilder);
    filteredConfigBuilder.addAllAnomalyDetectionConfigs(anomalyDetectionConfigs);
    return filteredConfigBuilder.build();
  }
}
