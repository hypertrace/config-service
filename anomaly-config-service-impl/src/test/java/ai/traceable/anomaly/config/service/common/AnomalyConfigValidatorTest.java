package ai.traceable.anomaly.config.service.common;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import ai.traceable.anomaly.config.service.v1.AnomalyApiScope;
import ai.traceable.anomaly.config.service.v1.AnomalyConfigScope;
import ai.traceable.anomaly.config.service.v1.AnomalyConfigScopeType;
import ai.traceable.anomaly.config.service.v1.AnomalyConfigStatusChange;
import ai.traceable.anomaly.config.service.v1.AnomalyServiceScope;
import io.grpc.Status;
import org.junit.jupiter.api.Test;

public class AnomalyConfigValidatorTest {

  private final AnomalyConfigValidator configValidator = new AnomalyConfigValidator();

  @Test
  public void testValidateConfigScope() {
    Status status;

    status = configValidator.validate(AnomalyConfigScope.getDefaultInstance());
    assertEquals(Status.INVALID_ARGUMENT.getCode(), status.getCode());
    assertTrue(status.getDescription().contains("valid scope type"));

    status =
        configValidator.validate(
            AnomalyConfigScope.newBuilder()
                .setScopeType(AnomalyConfigScopeType.ANOMALY_CONFIG_SCOPE_TYPE_CUSTOMER)
                .build());
    assertEquals(Status.OK.getCode(), status.getCode());

    status =
        configValidator.validate(
            AnomalyConfigScope.newBuilder()
                .setScopeType(AnomalyConfigScopeType.ANOMALY_CONFIG_SCOPE_TYPE_SERVICE)
                .build());
    assertEquals(Status.INVALID_ARGUMENT.getCode(), status.getCode());
    assertTrue(status.getDescription().contains("valid Service Scope"));

    status =
        configValidator.validate(
            AnomalyConfigScope.newBuilder()
                .setScopeType(AnomalyConfigScopeType.ANOMALY_CONFIG_SCOPE_TYPE_SERVICE)
                .setApiScope(AnomalyApiScope.newBuilder().setId("id").build())
                .build());
    assertEquals(Status.INVALID_ARGUMENT.getCode(), status.getCode());
    assertTrue(status.getDescription().contains("valid Service Scope"));

    status =
        configValidator.validate(
            AnomalyConfigScope.newBuilder()
                .setScopeType(AnomalyConfigScopeType.ANOMALY_CONFIG_SCOPE_TYPE_SERVICE)
                .setServiceScope(AnomalyServiceScope.newBuilder().setId("id").build())
                .build());
    assertEquals(Status.OK.getCode(), status.getCode());

    status =
        configValidator.validate(
            AnomalyConfigScope.newBuilder()
                .setScopeType(AnomalyConfigScopeType.ANOMALY_CONFIG_SCOPE_TYPE_API)
                .build());
    assertEquals(Status.INVALID_ARGUMENT.getCode(), status.getCode());
    assertTrue(status.getDescription().contains("valid API Scope"));

    status =
        configValidator.validate(
            AnomalyConfigScope.newBuilder()
                .setScopeType(AnomalyConfigScopeType.ANOMALY_CONFIG_SCOPE_TYPE_API)
                .setServiceScope(AnomalyServiceScope.newBuilder().setId("id").build())
                .build());
    assertEquals(Status.INVALID_ARGUMENT.getCode(), status.getCode());
    assertTrue(status.getDescription().contains("valid API Scope"));

    status =
        configValidator.validate(
            AnomalyConfigScope.newBuilder()
                .setScopeType(AnomalyConfigScopeType.ANOMALY_CONFIG_SCOPE_TYPE_API)
                .setApiScope(AnomalyApiScope.newBuilder().setId("id").build())
                .build());
    assertEquals(Status.INVALID_ARGUMENT.getCode(), status.getCode());
    assertTrue(status.getDescription().contains("valid API Scope"));

    status =
        configValidator.validate(
            AnomalyConfigScope.newBuilder()
                .setScopeType(AnomalyConfigScopeType.ANOMALY_CONFIG_SCOPE_TYPE_API)
                .setApiScope(
                    AnomalyApiScope.newBuilder()
                        .setId("api")
                        .setServiceScope(AnomalyServiceScope.newBuilder().setId("id"))
                        .build())
                .build());
    assertEquals(Status.OK.getCode(), status.getCode());
  }

  @Test
  public void testValidateConfigStatus() {
    Status status;

    status = configValidator.validate(AnomalyConfigStatusChange.getDefaultInstance());
    assertEquals(Status.INVALID_ARGUMENT.getCode(), status.getCode());

    status =
        configValidator.validate(AnomalyConfigStatusChange.newBuilder().setDisabled(true).build());
    assertEquals(Status.OK.getCode(), status.getCode());

    status =
        configValidator.validate(AnomalyConfigStatusChange.newBuilder().setInternal(true).build());
    assertEquals(Status.OK.getCode(), status.getCode());

    status =
        configValidator.validate(
            AnomalyConfigStatusChange.newBuilder().setDisabled(true).setInternal(true).build());
    assertEquals(Status.OK.getCode(), status.getCode());
  }
}
