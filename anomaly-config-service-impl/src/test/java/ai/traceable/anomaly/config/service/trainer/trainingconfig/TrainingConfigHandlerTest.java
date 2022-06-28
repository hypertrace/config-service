package ai.traceable.anomaly.config.service.trainer.trainingconfig;

import static org.junit.jupiter.api.Assertions.assertEquals;

import ai.traceable.anomaly.config.service.v1.IntList;
import ai.traceable.anomaly.config.service.v1.StringList;
import ai.traceable.anomaly.config.service.v1.trainer.ApiNamingTrainingConfig;
import ai.traceable.anomaly.config.service.v1.trainer.EnumerationsTrainingConfig;
import ai.traceable.anomaly.config.service.v1.trainer.IntRangeFilterConfig;
import ai.traceable.anomaly.config.service.v1.trainer.LackOfEncryptionTrainingConfig;
import ai.traceable.anomaly.config.service.v1.trainer.MetadataTrainingConfig;
import ai.traceable.anomaly.config.service.v1.trainer.MinOccurrenceConfig;
import ai.traceable.anomaly.config.service.v1.trainer.ObjectBolaTrainingConfig;
import ai.traceable.anomaly.config.service.v1.trainer.RejectFilterConfig;
import ai.traceable.anomaly.config.service.v1.trainer.ScopedTrainingConfig;
import ai.traceable.anomaly.config.service.v1.trainer.SegmentFilterConfig;
import ai.traceable.anomaly.config.service.v1.trainer.SessionTrainingConfig;
import ai.traceable.anomaly.config.service.v1.trainer.StatusCodeFilterConfig;
import ai.traceable.anomaly.config.service.v1.trainer.ThresholdCountConfig;
import ai.traceable.anomaly.config.service.v1.trainer.TrainingConfig;
import ai.traceable.anomaly.config.service.v1.trainer.UrlFilterConfig;
import ai.traceable.anomaly.config.service.v1.trainer.UrlPathFilterConfig;
import ai.traceable.anomaly.config.service.v1.trainer.UserAgentFilterConfig;
import ai.traceable.anomaly.config.service.v1.trainer.VulnerabilityTrainingConfig;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Value;
import java.util.List;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;

class TrainingConfigHandlerTest {
  private final TrainingConfigHandler configHandler = new TrainingConfigHandler();

  @Test
  void testConvert() throws InvalidProtocolBufferException {

    ScopedTrainingConfig resultConfig;
    TrainingConfig trainingConfig;
    Value value;

    ScopedTrainingConfig config =
        ScopedTrainingConfig.newBuilder()
            .addTrainingConfigs(
                TrainingConfig.newBuilder()
                    .setMetadataTrainingConfig(
                        MetadataTrainingConfig.newBuilder()
                            .setEnum(
                                EnumerationsTrainingConfig.newBuilder()
                                    .setMaxEnumerations(500)
                                    .build())
                            .build())
                    .build())
            .addTrainingConfigs(buildUrlFilterApiNamingTrainerConfig(List.of(".random")))
            .addTrainingConfigs(buildFilterApiNamingTrainerConfig(List.of(".com", ".us", ".au")))
            .build();

    value = configHandler.convert(config);
    assertEquals(config, configHandler.convert(value));

    ScopedTrainingConfig config1 =
        ScopedTrainingConfig.newBuilder()
            .addTrainingConfigs(
                TrainingConfig.newBuilder()
                    .setMetadataTrainingConfig(
                        MetadataTrainingConfig.newBuilder()
                            .setEnum(
                                EnumerationsTrainingConfig.newBuilder()
                                    .setEnumValueMaxLength(10)
                                    .setMinOccurrenceCount(100)
                                    .setMinEnumOccurrencePercent(99.9)
                                    .build())
                            .build())
                    .build())
            .build();

    resultConfig = configHandler.merge(config, config1);
    assertEquals(
        500,
        resultConfig
            .getTrainingConfigsList()
            .get(0)
            .getMetadataTrainingConfig()
            .getEnum()
            .getMaxEnumerations());
    // Asserting default values exist
    assertEquals(
        10,
        resultConfig
            .getTrainingConfigsList()
            .get(0)
            .getMetadataTrainingConfig()
            .getEnum()
            .getEnumValueMaxLength());
    assertEquals(
        100,
        resultConfig
            .getTrainingConfigsList()
            .get(0)
            .getMetadataTrainingConfig()
            .getEnum()
            .getMinOccurrenceCount());
    assertEquals(
        99.9,
        resultConfig
            .getTrainingConfigsList()
            .get(0)
            .getMetadataTrainingConfig()
            .getEnum()
            .getMinEnumOccurrencePercent());

    TrainingConfig trainingConfig2 =
        TrainingConfig.newBuilder()
            .setVulnerabilityTrainingConfig(
                VulnerabilityTrainingConfig.newBuilder()
                    .setLackOfEncryption(
                        LackOfEncryptionTrainingConfig.newBuilder()
                            .setHttpsCallsConfig(
                                MinOccurrenceConfig.newBuilder()
                                    .setMinTotalOccurrences(1000)
                                    .build())
                            .build())
                    .build())
            .build();

    TrainingConfig trainingConfig3 =
        TrainingConfig.newBuilder()
            .setSessionTrainingConfig(
                SessionTrainingConfig.newBuilder()
                    .setObjectBola(
                        ObjectBolaTrainingConfig.newBuilder()
                            .setRequiredCountConfig(
                                ThresholdCountConfig.newBuilder().setRequiredCallsCount(50).build())
                            .build())
                    .build())
            .build();

    resultConfig =
        configHandler.merge(
            config,
            ScopedTrainingConfig.newBuilder()
                .addAllTrainingConfigs(List.of(trainingConfig2, trainingConfig3))
                .build());

    trainingConfig =
        getTrainingConfig(resultConfig, TrainingConfig.TrainingConfigCase.METADATA_TRAINING_CONFIG);
    assertEquals(500, trainingConfig.getMetadataTrainingConfig().getEnum().getMaxEnumerations());

    trainingConfig =
        getTrainingConfig(
            resultConfig, TrainingConfig.TrainingConfigCase.VULNERABILITY_TRAINING_CONFIG);
    assertEquals(
        1000,
        trainingConfig
            .getVulnerabilityTrainingConfig()
            .getLackOfEncryption()
            .getHttpsCallsConfig()
            .getMinTotalOccurrences());

    trainingConfig =
        getTrainingConfig(resultConfig, TrainingConfig.TrainingConfigCase.SESSION_TRAINING_CONFIG);
    assertEquals(
        50,
        trainingConfig
            .getSessionTrainingConfig()
            .getObjectBola()
            .getRequiredCountConfig()
            .getRequiredCallsCount());

    List<TrainingConfig> apiNamingTrainingConfig =
        resultConfig.getTrainingConfigsList().stream()
            .filter(
                conf ->
                    conf.getTrainingConfigCase()
                        .equals(TrainingConfig.TrainingConfigCase.API_NAMING_TRAINING_CONFIG))
            .collect(Collectors.toList());

    assertEquals(
        List.of(".com", ".us", ".au"),
        apiNamingTrainingConfig
            .get(0)
            .getApiNamingTrainingConfig()
            .getRejectFilterConfig()
            .getUrlPathFilterConfig()
            .getUrlPathRegexPatterns()
            .getValuesList());

    assertEquals(
        List.of(302),
        apiNamingTrainingConfig
            .get(0)
            .getApiNamingTrainingConfig()
            .getRejectFilterConfig()
            .getStatusCodeFilterConfig()
            .getRangeFilterConfigs(0)
            .getExclusions()
            .getValuesList());

    assertEquals(
        List.of("bot"),
        apiNamingTrainingConfig
            .get(0)
            .getApiNamingTrainingConfig()
            .getRejectFilterConfig()
            .getUserAgentFilterConfig()
            .getBotAgentList()
            .getValuesList());

    assertEquals(
        25,
        apiNamingTrainingConfig
            .get(0)
            .getApiNamingTrainingConfig()
            .getRejectFilterConfig()
            .getSegmentFilterConfig()
            .getUrlPartsThreshold());
  }

  private TrainingConfig getTrainingConfig(
      ScopedTrainingConfig scopedTrainingConfig, TrainingConfig.TrainingConfigCase configCase) {
    for (TrainingConfig trainingConfig : scopedTrainingConfig.getTrainingConfigsList()) {
      if (trainingConfig.getTrainingConfigCase() == configCase) {
        return trainingConfig;
      }
    }
    return null;
  }

  private TrainingConfig buildFilterApiNamingTrainerConfig(List<String> urls) {
    return TrainingConfig.newBuilder()
        .setApiNamingTrainingConfig(
            ApiNamingTrainingConfig.newBuilder()
                .setRejectFilterConfig(
                    RejectFilterConfig.newBuilder()
                        .setStatusCodeFilterConfig(
                            StatusCodeFilterConfig.newBuilder()
                                .addRangeFilterConfigs(
                                    IntRangeFilterConfig.newBuilder()
                                        .setExclusions(IntList.newBuilder().addValues(302).build())
                                        .build())
                                .build())
                        .setSegmentFilterConfig(
                            SegmentFilterConfig.newBuilder().setUrlPartsThreshold(25).build())
                        .setUserAgentFilterConfig(
                            UserAgentFilterConfig.newBuilder()
                                .setBotAgentList(StringList.newBuilder().addValues("bot").build())
                                .build())
                        .setUrlPathFilterConfig(
                            UrlPathFilterConfig.newBuilder()
                                .setUrlPathRegexPatterns(
                                    StringList.newBuilder().addAllValues(urls).build())
                                .build())
                        .build())
                .build())
        .build();
  }

  private TrainingConfig buildUrlFilterApiNamingTrainerConfig(List<String> urls) {
    return TrainingConfig.newBuilder()
        .setApiNamingTrainingConfig(
            ApiNamingTrainingConfig.newBuilder()
                .setUrlFilterConfig(
                    UrlFilterConfig.newBuilder()
                        .setUrlRejectRegexPatterns(
                            StringList.newBuilder().addAllValues(urls).build())
                        .build())
                .build())
        .build();
  }
}
