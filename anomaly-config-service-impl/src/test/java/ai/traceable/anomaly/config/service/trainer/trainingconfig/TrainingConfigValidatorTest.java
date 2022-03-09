package ai.traceable.anomaly.config.service.trainer.trainingconfig;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import ai.traceable.anomaly.config.service.common.AnomalyConfigValidator;
import ai.traceable.anomaly.config.service.v1.AnomalyConfigScope;
import ai.traceable.anomaly.config.service.v1.AnomalyCustomerScope;
import ai.traceable.anomaly.config.service.v1.StringList;
import ai.traceable.anomaly.config.service.v1.trainer.ApiNamingTrainingConfig;
import ai.traceable.anomaly.config.service.v1.trainer.ContentSizeTrainingConfig;
import ai.traceable.anomaly.config.service.v1.trainer.DeleteAnomalyConfigOption;
import ai.traceable.anomaly.config.service.v1.trainer.DeleteScopedTrainingConfigRequest;
import ai.traceable.anomaly.config.service.v1.trainer.DeviceTrainingConfig;
import ai.traceable.anomaly.config.service.v1.trainer.GetScopedTrainingConfigRequest;
import ai.traceable.anomaly.config.service.v1.trainer.GetTrainingConfigsFilter;
import ai.traceable.anomaly.config.service.v1.trainer.GetUnresolvedScopedTrainingConfigRequest;
import ai.traceable.anomaly.config.service.v1.trainer.LackOfEncryptionTrainingConfig;
import ai.traceable.anomaly.config.service.v1.trainer.MetadataTrainingConfig;
import ai.traceable.anomaly.config.service.v1.trainer.PiiSensitiveDataTrainingConfig;
import ai.traceable.anomaly.config.service.v1.trainer.ScopedTrainingConfig;
import ai.traceable.anomaly.config.service.v1.trainer.SensitiveDataTrainingConfig;
import ai.traceable.anomaly.config.service.v1.trainer.TrainingConfig;
import ai.traceable.anomaly.config.service.v1.trainer.UpdateScopedTrainingConfigRequest;
import ai.traceable.anomaly.config.service.v1.trainer.UrlFilterConfig;
import ai.traceable.anomaly.config.service.v1.trainer.VulnerabilityTrainingConfig;
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
  void testGetUnresolvedRequest() {
    Status status =
        validator.validate(GetUnresolvedScopedTrainingConfigRequest.getDefaultInstance());
    assertEquals(Status.INVALID_ARGUMENT.getCode(), status.getCode());
    assertTrue(status.getDescription().contains("valid config scope"));

    status =
        validator.validate(
            GetUnresolvedScopedTrainingConfigRequest.newBuilder()
                .setConfigScope(configScope)
                .build());
    assertEquals(Status.INVALID_ARGUMENT.getCode(), status.getCode());
    assertTrue(status.getDescription().contains("valid config filter"));

    status =
        validator.validate(
            GetUnresolvedScopedTrainingConfigRequest.newBuilder()
                .setConfigScope(configScope)
                .setFilter(GetTrainingConfigsFilter.getDefaultInstance())
                .build());
    assertEquals(Status.OK.getCode(), status.getCode());
  }

  @Test
  void testDeleteRequest() {
    Status status = validator.validate(DeleteScopedTrainingConfigRequest.getDefaultInstance());
    assertEquals(Status.INVALID_ARGUMENT.getCode(), status.getCode());
    assertTrue(status.getDescription().contains("valid config scope"));

    ScopedTrainingConfig scopedTrainingConfig =
        ScopedTrainingConfig.newBuilder()
            .setConfigScope(configScope)
            .addTrainingConfigs(
                TrainingConfig.newBuilder()
                    .setVulnerabilityTrainingConfig(
                        VulnerabilityTrainingConfig.newBuilder()
                            .setLackOfEncryption(
                                LackOfEncryptionTrainingConfig.getDefaultInstance())))
            .build();

    status =
        validator.validate(
            DeleteScopedTrainingConfigRequest.newBuilder()
                .setScopedTrainingConfig(scopedTrainingConfig)
                .build());
    assertEquals(Status.INVALID_ARGUMENT.getCode(), status.getCode());
    assertTrue(status.getDescription().contains("valid delete option"));

    status =
        validator.validate(
            DeleteScopedTrainingConfigRequest.newBuilder()
                .setDeleteAnomalyConfigOption(
                    DeleteAnomalyConfigOption.DELETE_ANOMALY_CONFIG_OPTION_WHOLE_TRAINING_CONFIG)
                .setScopedTrainingConfig(scopedTrainingConfig)
                .build());
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

  @Test
  void testUpdateSensitiveDataRequest() {
    TrainingConfig trainingConfig =
        TrainingConfig.newBuilder()
            .setSensitiveDataTrainingConfig(
                SensitiveDataTrainingConfig.newBuilder()
                    .setPiiSensitiveData(PiiSensitiveDataTrainingConfig.getDefaultInstance()))
            .build();

    UpdateScopedTrainingConfigRequest request =
        UpdateScopedTrainingConfigRequest.newBuilder()
            .setScopedTrainingConfig(
                ScopedTrainingConfig.newBuilder()
                    .setConfigScope(configScope)
                    .addAllTrainingConfigs(List.of(trainingConfig, trainingConfig))
                    .build())
            .build();

    Status status = validator.validate(request);

    assertEquals(Status.INVALID_ARGUMENT.getCode(), status.getCode());
    assertTrue(
        status
            .getDescription()
            .contains(
                "UpdateScopedTrainingConfigRequest should have only one training config for sensitiveDataTrainingConfigType: "));

    request =
        UpdateScopedTrainingConfigRequest.newBuilder()
            .setScopedTrainingConfig(
                ScopedTrainingConfig.newBuilder()
                    .setConfigScope(configScope)
                    .addTrainingConfigs(trainingConfig)
                    .build())
            .build();

    status = validator.validate(request);

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
