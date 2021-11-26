package ai.traceable.anomaly.config.service.trainer.trainingconfig;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import ai.traceable.anomaly.config.service.common.AnomalyConfigValidator;
import ai.traceable.anomaly.config.service.v1.AnomalyConfigScope;
import ai.traceable.anomaly.config.service.v1.trainer.ContentSizeTrainingConfig;
import ai.traceable.anomaly.config.service.v1.trainer.DeviceTrainingConfig;
import ai.traceable.anomaly.config.service.v1.trainer.GetScopedTrainingConfigRequest;
import ai.traceable.anomaly.config.service.v1.trainer.MetadataTrainingConfig;
import ai.traceable.anomaly.config.service.v1.trainer.ScopedTrainingConfig;
import ai.traceable.anomaly.config.service.v1.trainer.TrainingConfig;
import ai.traceable.anomaly.config.service.v1.trainer.UpdateScopedTrainingConfigRequest;
import io.grpc.Status;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class TrainingConfigValidatorTest {
  private final AnomalyConfigValidator configValidator = mock(AnomalyConfigValidator.class);
  private final AnomalyConfigScope configScope = AnomalyConfigScope.newBuilder().build();

  private TrainingConfigValidator validator;

  @BeforeEach
  void setUp() {
    doReturn(Status.OK).when(configValidator).validate(configScope);
    this.validator = new TrainingConfigValidator(configValidator);
  }

  @Test
  void testGetRequest() {
    Status status = validator.validate(GetScopedTrainingConfigRequest.getDefaultInstance());
    assertEquals(Status.INVALID_ARGUMENT.getCode(), status.getCode());
    assertTrue(status.getDescription().contains("valid config scope"));
    verify(configValidator, times(0)).validate((AnomalyConfigScope) any());

    status =
        validator.validate(
            GetScopedTrainingConfigRequest.newBuilder().setConfigScope(configScope).build());
    assertEquals(Status.OK.getCode(), status.getCode());
    verify(configValidator, times(1)).validate((AnomalyConfigScope) any());
  }

  @Test
  void testUpdateRequest() {
    Status status = validator.validate(UpdateScopedTrainingConfigRequest.getDefaultInstance());
    assertEquals(Status.INVALID_ARGUMENT.getCode(), status.getCode());
    assertTrue(status.getDescription().contains("valid config scope"));
    verify(configValidator, times(0)).validate((AnomalyConfigScope) any());

    status =
        validator.validate(
            UpdateScopedTrainingConfigRequest.newBuilder()
                .setScopedTrainingConfig(
                    ScopedTrainingConfig.newBuilder().setConfigScope(configScope).build())
                .build());
    assertEquals(Status.OK.getCode(), status.getCode());
    verify(configValidator, times(1)).validate((AnomalyConfigScope) any());

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

    clearInvocations(configValidator);
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
    verify(configValidator, times(0)).validate((AnomalyConfigScope) any());

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
    verify(configValidator, times(1)).validate((AnomalyConfigScope) any());
  }
}
