package ai.traceable.anomaly.config.service.trainer.trainingconfig;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import ai.traceable.anomaly.config.service.common.AnomalyConfigValidator;
import ai.traceable.anomaly.config.service.v1.AnomalyConfigScope;
import ai.traceable.anomaly.config.service.v1.AnomalyCustomerScope;
import ai.traceable.anomaly.config.service.v1.StringList;
import ai.traceable.anomaly.config.service.v1.trainer.ApiNamingTrainingConfig;
import ai.traceable.anomaly.config.service.v1.trainer.ContentSizeTrainingConfig;
import ai.traceable.anomaly.config.service.v1.trainer.DeviceTrainingConfig;
import ai.traceable.anomaly.config.service.v1.trainer.GetScopedTrainingConfigRequest;
import ai.traceable.anomaly.config.service.v1.trainer.MetadataTrainingConfig;
import ai.traceable.anomaly.config.service.v1.trainer.ScopedTrainingConfig;
import ai.traceable.anomaly.config.service.v1.trainer.TrainingConfig;
import ai.traceable.anomaly.config.service.v1.trainer.UpdateScopedTrainingConfigRequest;
import ai.traceable.anomaly.config.service.v1.trainer.UrlFilterConfig;
import io.grpc.Status;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class TrainingConfigValidatorTest {
  private final AnomalyConfigValidator configValidator = new AnomalyConfigValidator();
  private final AnomalyConfigScope configScope =
      AnomalyConfigScope.newBuilder()
          .setCustomerScope(AnomalyCustomerScope.getDefaultInstance())
          .build();

  private TrainingConfigValidator validator;

  @BeforeEach
  void setUp() {
    this.validator = new TrainingConfigValidator(configValidator);
  }

  @Test
  void testGetRequest() {
    Status status = validator.validate(GetScopedTrainingConfigRequest.getDefaultInstance());
    assertEquals(Status.INVALID_ARGUMENT.getCode(), status.getCode());
    assertTrue(status.getDescription().contains("valid config scope"));

    status =
        validator.validate(
            GetScopedTrainingConfigRequest.newBuilder().setConfigScope(configScope).build());
    assertEquals(Status.OK.getCode(), status.getCode());
  }

  @Test
  void testUpdateRequest() {
    Status status = validator.validate(UpdateScopedTrainingConfigRequest.getDefaultInstance());
    assertEquals(Status.INVALID_ARGUMENT.getCode(), status.getCode());
    assertTrue(status.getDescription().contains("valid config scope"));

    status =
        validator.validate(
            UpdateScopedTrainingConfigRequest.newBuilder()
                .setScopedTrainingConfig(
                    ScopedTrainingConfig.newBuilder().setConfigScope(configScope).build())
                .build());
    assertEquals(Status.OK.getCode(), status.getCode());

    TrainingConfig trainingConfig1 =
        TrainingConfig.newBuilder()
            .setMetadataTrainingConfig(
                MetadataTrainingConfig.newBuilder()
                    .setContentSize(ContentSizeTrainingConfig.newBuilder().build()))
            .build();
    TrainingConfig trainingConfig2 =
        TrainingConfig.newBuilder()
            .setMetadataTrainingConfig(
                MetadataTrainingConfig.newBuilder()
                    .setContentSize(ContentSizeTrainingConfig.newBuilder().build()))
            .build();

    status =
        validator.validate(
            UpdateScopedTrainingConfigRequest.newBuilder()
                .setScopedTrainingConfig(
                    ScopedTrainingConfig.newBuilder()
                        .setConfigScope(configScope)
                        .addAllTrainingConfigs(List.of(trainingConfig1, trainingConfig2)))
                .build());
    assertEquals(Status.INVALID_ARGUMENT.getCode(), status.getCode());
    assertTrue(status.getDescription().contains("only one training config"));

    trainingConfig2 =
        TrainingConfig.newBuilder()
            .setMetadataTrainingConfig(
                MetadataTrainingConfig.newBuilder()
                    .setDevice(DeviceTrainingConfig.newBuilder().build()))
            .build();
    status =
        validator.validate(
            UpdateScopedTrainingConfigRequest.newBuilder()
                .setScopedTrainingConfig(
                    ScopedTrainingConfig.newBuilder()
                        .setConfigScope(configScope)
                        .addAllTrainingConfigs(List.of(trainingConfig1, trainingConfig2)))
                .build());
    assertEquals(Status.OK.getCode(), status.getCode());
  }

  @Test
  void testUpdateApiNamingConfigRequest() {
    List<TrainingConfig> trainingConfigList =
        List.of(
            buildUrlFilterApiNamingTrainerConfig(List.of("a", "b")),
            buildUrlFilterApiNamingTrainerConfig(List.of("c", "d")));
    Status status =
        validator.validate(
            UpdateScopedTrainingConfigRequest.newBuilder()
                .setScopedTrainingConfig(
                    ScopedTrainingConfig.newBuilder()
                        .setConfigScope(configScope)
                        .addAllTrainingConfigs(trainingConfigList))
                .build());
    assertEquals(Status.INVALID_ARGUMENT.getCode(), status.getCode());
    assertTrue(
        status
            .getDescription()
            .contains(
                "UpdateScopedTrainingConfigRequest should have only one training config for apiNamingTrainingConfigType: "));

    trainingConfigList = List.of(buildUrlFilterApiNamingTrainerConfig(List.of("a", "b")));
    status =
        validator.validate(
            UpdateScopedTrainingConfigRequest.newBuilder()
                .setScopedTrainingConfig(
                    ScopedTrainingConfig.newBuilder()
                        .setConfigScope(configScope)
                        .addAllTrainingConfigs(trainingConfigList))
                .build());
    assertEquals(Status.OK.getCode(), status.getCode());
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
