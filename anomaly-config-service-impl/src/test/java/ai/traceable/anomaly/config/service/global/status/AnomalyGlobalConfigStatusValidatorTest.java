package ai.traceable.anomaly.config.service.global.status;

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
import ai.traceable.anomaly.config.service.v1.AnomalyConfigStatusChange;
import ai.traceable.anomaly.config.service.v1.global.GetAnomalyGlobalConfigStatusRequest;
import ai.traceable.anomaly.config.service.v1.global.UpdateAnomalyGlobalConfigStatusRequest;
import io.grpc.Status;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class AnomalyGlobalConfigStatusValidatorTest {

  private final AnomalyConfigValidator configValidator = mock(AnomalyConfigValidator.class);
  private final AnomalyGlobalConfigStatusValidator configStatusValidator =
      new AnomalyGlobalConfigStatusValidator(configValidator);
  private final AnomalyConfigScope configScope = AnomalyConfigScope.newBuilder().build();
  private final AnomalyConfigStatusChange configStatusChange =
      AnomalyConfigStatusChange.newBuilder().build();

  @BeforeEach
  public void setup() {
    doReturn(Status.OK).when(configValidator).validate(configScope);
    doReturn(Status.OK).when(configValidator).validate(configStatusChange);
  }

  @Test
  public void testValidateGetStatusRequest() {
    Status status;

    status =
        configStatusValidator.validate(GetAnomalyGlobalConfigStatusRequest.getDefaultInstance());
    assertEquals(Status.INVALID_ARGUMENT.getCode(), status.getCode());
    assertTrue(status.getDescription().contains("valid config scope"));
    verify(configValidator, times(0)).validate((AnomalyConfigScope) any());

    status =
        configStatusValidator.validate(
            GetAnomalyGlobalConfigStatusRequest.newBuilder().setConfigScope(configScope).build());
    assertEquals(Status.OK.getCode(), status.getCode());
    verify(configValidator, times(1)).validate((AnomalyConfigScope) any());
  }

  @Test
  public void testValidateUpdateStatusRequest() {
    Status status;

    status =
        configStatusValidator.validate(UpdateAnomalyGlobalConfigStatusRequest.getDefaultInstance());
    assertEquals(Status.INVALID_ARGUMENT.getCode(), status.getCode());
    assertTrue(status.getDescription().contains("valid config status"));
    verify(configValidator, times(0)).validate((AnomalyConfigStatusChange) any());
    verify(configValidator, times(0)).validate((AnomalyConfigScope) any());

    status =
        configStatusValidator.validate(
            UpdateAnomalyGlobalConfigStatusRequest.newBuilder()
                .setConfigStatus(configStatusChange)
                .build());
    assertEquals(Status.INVALID_ARGUMENT.getCode(), status.getCode());
    assertTrue(status.getDescription().contains("valid config scope"));
    verify(configValidator, times(0)).validate((AnomalyConfigStatusChange) any());
    verify(configValidator, times(0)).validate((AnomalyConfigScope) any());

    status =
        configStatusValidator.validate(
            UpdateAnomalyGlobalConfigStatusRequest.newBuilder()
                .setConfigStatus(configStatusChange)
                .setConfigScope(configScope)
                .build());
    assertEquals(Status.OK.getCode(), status.getCode());
    verify(configValidator, times(1)).validate((AnomalyConfigStatusChange) any());
    verify(configValidator, times(1)).validate((AnomalyConfigScope) any());

    clearInvocations(configValidator);
    doReturn(Status.INVALID_ARGUMENT).when(configValidator).validate(configStatusChange);
    status =
        configStatusValidator.validate(
            UpdateAnomalyGlobalConfigStatusRequest.newBuilder()
                .setConfigStatus(configStatusChange)
                .setConfigScope(configScope)
                .build());
    assertEquals(Status.INVALID_ARGUMENT.getCode(), status.getCode());
    verify(configValidator, times(1)).validate((AnomalyConfigStatusChange) any());
    verify(configValidator, times(0)).validate((AnomalyConfigScope) any());
  }
}
