package ai.traceable.anomaly.config.service.apidef.trainer;

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
import ai.traceable.anomaly.config.service.v1.apidef.ApiDefinitionApplierConfig;
import ai.traceable.anomaly.config.service.v1.apidef.ContentSizeMetadataApplierConfig;
import ai.traceable.anomaly.config.service.v1.apidef.GetApiDefinitionTrainerConfigsRequest;
import ai.traceable.anomaly.config.service.v1.apidef.QueryParamContainsSensitiveDataVulnerabilityApplierConfig;
import ai.traceable.anomaly.config.service.v1.apidef.UpdateApiDefinitionTrainerConfigsRequest;
import io.grpc.Status;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class ApiDefinitionTrainerConfigServiceValidatorTest {
  private final AnomalyConfigValidator configValidator = mock(AnomalyConfigValidator.class);
  private final AnomalyConfigScope configScope = AnomalyConfigScope.newBuilder().build();

  private ApiDefinitionTrainerConfigServiceValidator validator;

  @BeforeEach
  void setUp() {
    doReturn(Status.OK).when(configValidator).validate(configScope);
    this.validator = new ApiDefinitionTrainerConfigServiceValidator(configValidator);
  }

  @Test
  void testGetRequest() {
    Status status = validator.validate(GetApiDefinitionTrainerConfigsRequest.getDefaultInstance());
    assertEquals(Status.INVALID_ARGUMENT.getCode(), status.getCode());
    assertTrue(status.getDescription().contains("valid config scope"));
    verify(configValidator, times(0)).validate((AnomalyConfigScope) any());

    status =
        validator.validate(
            GetApiDefinitionTrainerConfigsRequest.newBuilder().setConfigScope(configScope).build());
    assertEquals(Status.OK.getCode(), status.getCode());
    verify(configValidator, times(1)).validate((AnomalyConfigScope) any());
  }

  @Test
  void testUpdateRequest() {
    Status status =
        validator.validate(UpdateApiDefinitionTrainerConfigsRequest.getDefaultInstance());
    assertEquals(Status.INVALID_ARGUMENT.getCode(), status.getCode());
    assertTrue(status.getDescription().contains("valid config scope"));
    verify(configValidator, times(0)).validate((AnomalyConfigScope) any());

    status =
        validator.validate(
            UpdateApiDefinitionTrainerConfigsRequest.newBuilder()
                .setConfigScope(configScope)
                .build());
    assertEquals(Status.OK.getCode(), status.getCode());
    verify(configValidator, times(1)).validate((AnomalyConfigScope) any());

    ApiDefinitionApplierConfig applierConfig1 =
        ApiDefinitionApplierConfig.newBuilder()
            .setContentSize(ContentSizeMetadataApplierConfig.newBuilder().build())
            .build();
    ApiDefinitionApplierConfig applierConfig2 =
        ApiDefinitionApplierConfig.newBuilder()
            .setContentSize(ContentSizeMetadataApplierConfig.newBuilder().build())
            .build();

    clearInvocations(configValidator);
    status =
        validator.validate(
            UpdateApiDefinitionTrainerConfigsRequest.newBuilder()
                .setConfigScope(configScope)
                .addAllApiDefinitionApplierConfigs(List.of(applierConfig1, applierConfig2))
                .build());
    assertEquals(Status.INVALID_ARGUMENT.getCode(), status.getCode());
    assertTrue(status.getDescription().contains("only one applier config"));
    verify(configValidator, times(0)).validate((AnomalyConfigScope) any());

    applierConfig2 =
        ApiDefinitionApplierConfig.newBuilder()
            .setQueryParamContainsSensitiveData(
                QueryParamContainsSensitiveDataVulnerabilityApplierConfig.newBuilder().build())
            .build();
    status =
        validator.validate(
            UpdateApiDefinitionTrainerConfigsRequest.newBuilder()
                .setConfigScope(configScope)
                .addAllApiDefinitionApplierConfigs(List.of(applierConfig1, applierConfig2))
                .build());
    assertEquals(Status.OK.getCode(), status.getCode());
    verify(configValidator, times(1)).validate((AnomalyConfigScope) any());
  }
}
