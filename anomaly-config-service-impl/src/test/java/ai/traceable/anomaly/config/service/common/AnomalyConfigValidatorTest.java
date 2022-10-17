package ai.traceable.anomaly.config.service.common;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import ai.traceable.anomaly.config.service.v1.AnomalyApiScope;
import ai.traceable.anomaly.config.service.v1.AnomalyConfigScope;
import ai.traceable.anomaly.config.service.v1.AnomalyConfigStatusChange;
import ai.traceable.anomaly.config.service.v1.AnomalyCustomerScope;
import ai.traceable.anomaly.config.service.v1.AnomalyEnvironmentScope;
import ai.traceable.anomaly.config.service.v1.AnomalyParamInfo;
import ai.traceable.anomaly.config.service.v1.AnomalyParamInfoScope;
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
                .setEnvironmentScope(AnomalyEnvironmentScope.getDefaultInstance())
                .build());
    assertEquals(Status.INVALID_ARGUMENT.getCode(), status.getCode());
    assertTrue(status.getDescription().contains("valid Environment ID"));

    status =
        configValidator.validate(
            AnomalyConfigScope.newBuilder()
                .setServiceScope(AnomalyServiceScope.newBuilder().setId("id").build())
                .build());
    assertEquals(Status.OK.getCode(), status.getCode());

    status =
        configValidator.validate(
            AnomalyConfigScope.newBuilder()
                .setEnvironmentScope(
                    AnomalyEnvironmentScope.newBuilder().setEnvironmentId("id").build())
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
    assertTrue(status.getDescription().contains("valid API and Service IDs"));

    status =
        configValidator.validate(
            AnomalyConfigScope.newBuilder()
                .setParamScope(
                    AnomalyParamScope.newBuilder()
                        .setApiScope(AnomalyApiScope.newBuilder().build()))
                .build());
    assertEquals(Status.INVALID_ARGUMENT.getCode(), status.getCode());
    assertTrue(status.getDescription().contains("valid API and Service IDs"));

    status =
        configValidator.validate(
            AnomalyConfigScope.newBuilder()
                .setParamScope(
                    AnomalyParamScope.newBuilder()
                        .setApiScope(AnomalyApiScope.newBuilder().setId("api").build()))
                .build());
    assertEquals(Status.INVALID_ARGUMENT.getCode(), status.getCode());
    assertTrue(status.getDescription().contains("valid API and Service IDs"));

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
    assertTrue(status.getDescription().contains("valid param name"));

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

    status =
        configValidator.validate(
            AnomalyConfigScope.newBuilder()
                .setParamScope(
                    AnomalyParamScope.newBuilder()
                        .setParamInfo(AnomalyParamInfo.getDefaultInstance())
                        .setScope(
                            AnomalyParamInfoScope.newBuilder()
                                .setApiScope(
                                    AnomalyApiScope.newBuilder()
                                        .setId("api")
                                        .setServiceScope(
                                            AnomalyServiceScope.newBuilder().setId("id"))
                                        .build())))
                .build());
    assertEquals(Status.INVALID_ARGUMENT.getCode(), status.getCode());
    assertTrue(status.getDescription().contains("ParamInfo is not set"));

    status =
        configValidator.validate(
            AnomalyConfigScope.newBuilder()
                .setParamScope(
                    AnomalyParamScope.newBuilder()
                        .setParamInfo(AnomalyParamInfo.newBuilder().setParamName("").build())
                        .setScope(
                            AnomalyParamInfoScope.newBuilder()
                                .setApiScope(
                                    AnomalyApiScope.newBuilder()
                                        .setId("api")
                                        .setServiceScope(
                                            AnomalyServiceScope.newBuilder().setId("id"))
                                        .build()))
                        .build())
                .build());
    assertEquals(Status.INVALID_ARGUMENT.getCode(), status.getCode());
    assertTrue(status.getDescription().contains("Param name should not be empty"));

    status =
        configValidator.validate(
            AnomalyConfigScope.newBuilder()
                .setParamScope(
                    AnomalyParamScope.newBuilder()
                        .setParamInfo(AnomalyParamInfo.newBuilder().setParamRegex("").build())
                        .setScope(
                            AnomalyParamInfoScope.newBuilder()
                                .setApiScope(
                                    AnomalyApiScope.newBuilder()
                                        .setId("api")
                                        .setServiceScope(
                                            AnomalyServiceScope.newBuilder().setId("id"))
                                        .build()))
                        .build())
                .build());
    assertEquals(Status.INVALID_ARGUMENT.getCode(), status.getCode());
    assertTrue(status.getDescription().contains("Param regex should not be empty"));

    status =
        configValidator.validate(
            AnomalyConfigScope.newBuilder()
                .setParamScope(
                    AnomalyParamScope.newBuilder()
                        .setParamInfo(AnomalyParamInfo.newBuilder().setParamRegex("[").build())
                        .setScope(
                            AnomalyParamInfoScope.newBuilder()
                                .setApiScope(
                                    AnomalyApiScope.newBuilder()
                                        .setId("api")
                                        .setServiceScope(
                                            AnomalyServiceScope.newBuilder().setId("id"))
                                        .build()))
                        .build())
                .build());
    assertEquals(Status.INVALID_ARGUMENT.getCode(), status.getCode());

    status =
        configValidator.validate(
            AnomalyConfigScope.newBuilder()
                .setParamScope(
                    AnomalyParamScope.newBuilder()
                        .setParamInfo(
                            AnomalyParamInfo.newBuilder().setParamName("paramName").build())
                        .setScope(AnomalyParamInfoScope.getDefaultInstance())
                        .build())
                .build());
    assertEquals(Status.INVALID_ARGUMENT.getCode(), status.getCode());
    assertTrue(status.getDescription().contains("ParamInfoScope is not set"));

    status =
        configValidator.validate(
            AnomalyConfigScope.newBuilder()
                .setParamScope(
                    AnomalyParamScope.newBuilder()
                        .setParamInfo(
                            AnomalyParamInfo.newBuilder().setParamName("paramName").build())
                        .setScope(
                            AnomalyParamInfoScope.newBuilder()
                                .setApiScope(AnomalyApiScope.newBuilder().setId("apiId")))
                        .build())
                .build());
    assertEquals(Status.INVALID_ARGUMENT.getCode(), status.getCode());
    assertTrue(status.getDescription().contains("valid API and Service IDs"));

    status =
        configValidator.validate(
            AnomalyConfigScope.newBuilder()
                .setParamScope(
                    AnomalyParamScope.newBuilder()
                        .setParamInfo(
                            AnomalyParamInfo.newBuilder().setParamName("paramName").build())
                        .setScope(
                            AnomalyParamInfoScope.newBuilder()
                                .setServiceScope(AnomalyServiceScope.newBuilder()))
                        .build())
                .build());
    assertEquals(Status.INVALID_ARGUMENT.getCode(), status.getCode());
    assertTrue(status.getDescription().contains("valid Service ID"));

    status =
        configValidator.validate(
            AnomalyConfigScope.newBuilder()
                .setParamScope(
                    AnomalyParamScope.newBuilder()
                        .setScope(
                            AnomalyParamInfoScope.newBuilder()
                                .setApiScope(
                                    AnomalyApiScope.newBuilder()
                                        .setId("api")
                                        .setServiceScope(
                                            AnomalyServiceScope.newBuilder().setId("id"))
                                        .build()))
                        .setParamInfo(AnomalyParamInfo.newBuilder().setParamName("param").build()))
                .build());
    assertEquals(Status.OK.getCode(), status.getCode());

    status = configValidator.validate(AnomalyConfigScope.getDefaultInstance(), true);

    assertEquals(Status.INVALID_ARGUMENT.getCode(), status.getCode());
    assertTrue(status.getDescription().contains("Config Scope is not set"));

    status =
        configValidator.validate(
            AnomalyConfigScope.newBuilder()
                .setCustomerScope(AnomalyCustomerScope.getDefaultInstance())
                .build(),
            true);

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
