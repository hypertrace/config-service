package ai.traceable.anomaly.config.service.common;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import ai.traceable.anomaly.config.service.v1.AnomalyApiScope;
import ai.traceable.anomaly.config.service.v1.AnomalyConfigScope;
import ai.traceable.anomaly.config.service.v1.AnomalyConfigStatusChange;
import ai.traceable.anomaly.config.service.v1.AnomalyCustomerScope;
import ai.traceable.anomaly.config.service.v1.AnomalyParamScope;
import ai.traceable.anomaly.config.service.v1.AnomalyServiceScope;
import io.grpc.Status;
import org.junit.jupiter.api.Test;

public class AnomalyConfigValidatorTest {

  private final AnomalyConfigValidator configValidator = new AnomalyConfigValidator();

  @Test
  public void testValidateConfigScope() {
    Status status;

    status =
        configValidator.validate(
            AnomalyConfigScope.newBuilder()
                .setCustomerScope(AnomalyCustomerScope.getDefaultInstance())
                .build());
    assertEquals(Status.OK.getCode(), status.getCode());

    status =
        configValidator.validate(
            AnomalyConfigScope.newBuilder()
                .setServiceScope(AnomalyServiceScope.getDefaultInstance())
                .build());
    assertEquals(Status.INVALID_ARGUMENT.getCode(), status.getCode());
    assertTrue(status.getDescription().contains("valid Service ID"));

    status =
        configValidator.validate(
            AnomalyConfigScope.newBuilder()
                .setServiceScope(AnomalyServiceScope.newBuilder().setId("id").build())
                .build());
    assertEquals(Status.OK.getCode(), status.getCode());

    status =
        configValidator.validate(
            AnomalyConfigScope.newBuilder()
                .setApiScope(AnomalyApiScope.getDefaultInstance())
                .build());
    assertEquals(Status.INVALID_ARGUMENT.getCode(), status.getCode());
    assertTrue(status.getDescription().contains("valid API and Service IDs."));

    status =
        configValidator.validate(
            AnomalyConfigScope.newBuilder()
                .setApiScope(AnomalyApiScope.newBuilder().setId("id").build())
                .build());
    assertEquals(Status.INVALID_ARGUMENT.getCode(), status.getCode());
    assertTrue(status.getDescription().contains("valid API and Service IDs."));

    status =
        configValidator.validate(
            AnomalyConfigScope.newBuilder()
                .setApiScope(
                    AnomalyApiScope.newBuilder()
                        .setId("api")
                        .setServiceScope(AnomalyServiceScope.newBuilder().setId("id"))
                        .build())
                .build());
    assertEquals(Status.OK.getCode(), status.getCode());

    status =
        configValidator.validate(
            AnomalyConfigScope.newBuilder()
                .setParamScope(AnomalyParamScope.getDefaultInstance())
                .build());
    assertEquals(Status.INVALID_ARGUMENT.getCode(), status.getCode());
    assertTrue(status.getDescription().contains("valid API and Service IDs and valid param name"));

    status =
        configValidator.validate(
            AnomalyConfigScope.newBuilder()
                .setParamScope(
                    AnomalyParamScope.newBuilder()
                        .setApiScope(AnomalyApiScope.newBuilder().build()))
                .build());
    assertEquals(Status.INVALID_ARGUMENT.getCode(), status.getCode());
    assertTrue(status.getDescription().contains("valid API and Service IDs and valid param name"));

    status =
        configValidator.validate(
            AnomalyConfigScope.newBuilder()
                .setParamScope(
                    AnomalyParamScope.newBuilder()
                        .setApiScope(AnomalyApiScope.newBuilder().setId("api").build()))
                .build());
    assertEquals(Status.INVALID_ARGUMENT.getCode(), status.getCode());
    assertTrue(status.getDescription().contains("valid API and Service IDs and valid param name"));

    status =
        configValidator.validate(
            AnomalyConfigScope.newBuilder()
                .setParamScope(
                    AnomalyParamScope.newBuilder()
                        .setApiScope(
                            AnomalyApiScope.newBuilder()
                                .setId("api")
                                .setServiceScope(AnomalyServiceScope.newBuilder().setId("id"))
                                .build()))
                .build());
    assertEquals(Status.INVALID_ARGUMENT.getCode(), status.getCode());
    assertTrue(status.getDescription().contains("valid API and Service IDs and valid param name"));

    status =
        configValidator.validate(
            AnomalyConfigScope.newBuilder()
                .setParamScope(
                    AnomalyParamScope.newBuilder()
                        .setApiScope(
                            AnomalyApiScope.newBuilder()
                                .setId("api")
                                .setServiceScope(AnomalyServiceScope.newBuilder().setId("id"))
                                .build())
                        .setParamName("param"))
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
