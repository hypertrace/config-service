package ai.traceable.anomaly.config.service.override.common.validator;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;

import ai.traceable.anomaly.config.service.v1.StringList;
import ai.traceable.anomaly.config.service.v1.override.DetectionOverrideActorsConfig;
import ai.traceable.anomaly.config.service.v1.override.DetectionOverrideAssetConfig;
import ai.traceable.anomaly.config.service.v1.override.DetectionOverrideEntityScope;
import ai.traceable.anomaly.config.service.v1.override.DetectionOverrideScope;
import ai.traceable.anomaly.config.service.v1.override.DetectionOverrideScopeConfig;
import io.grpc.Status;
import java.util.List;
import org.junit.jupiter.api.Test;

public class DetectionOverrideConfigsValidatorTest {

  private final DetectionOverrideConfigsValidator configsValidator =
      new DetectionOverrideConfigsValidator(mock(DetectionOverrideConditionsValidator.class));

  @Test
  public void testValidateActorsConfig() {
    DetectionOverrideActorsConfig config = DetectionOverrideActorsConfig.getDefaultInstance();
    Status status = configsValidator.validateActorsConfig(config);
    assertEquals(Status.INVALID_ARGUMENT.getCode(), status.getCode());
    assertEquals(
        "Invalid case of actors config " + config.getConfigCase(), status.getDescription());

    config =
        DetectionOverrideActorsConfig.newBuilder()
            .setActorEntityIds(StringList.getDefaultInstance())
            .build();
    status = configsValidator.validateActorsConfig(config);
    assertEquals(Status.INVALID_ARGUMENT.getCode(), status.getCode());
    assertEquals("Actor Entity Ids should have at least one id", status.getDescription());

    config =
        DetectionOverrideActorsConfig.newBuilder()
            .setActorEntityIds(StringList.newBuilder().addAllValues(List.of("val1", "val2")))
            .build();
    assertEquals(Status.OK, configsValidator.validateActorsConfig(config));
  }

  @Test
  public void testValidateScopeConfig() {
    DetectionOverrideScopeConfig config = DetectionOverrideScopeConfig.getDefaultInstance();
    Status status = configsValidator.validateScopeConfig(config);
    assertEquals(Status.INVALID_ARGUMENT.getCode(), status.getCode());
    assertEquals("Scope config should have at least one scope", status.getDescription());

    config =
        DetectionOverrideScopeConfig.newBuilder()
            .addScopes(
                DetectionOverrideScope.newBuilder()
                    .setApiScope(DetectionOverrideEntityScope.getDefaultInstance()))
            .build();
    status = configsValidator.validateScopeConfig(config);
    assertEquals(Status.INVALID_ARGUMENT.getCode(), status.getCode());
    assertEquals("Api Ids and api labels both shouldn't be empty", status.getDescription());

    config =
        DetectionOverrideScopeConfig.newBuilder()
            .addScopes(
                DetectionOverrideScope.newBuilder()
                    .setApiScope(
                        DetectionOverrideEntityScope.newBuilder()
                            .setEntityIds(
                                StringList.newBuilder().addAllValues(List.of("id1", "id2")))))
            .build();
    assertEquals(Status.OK, configsValidator.validateScopeConfig(config));

    config =
        DetectionOverrideScopeConfig.newBuilder()
            .addScopes(
                DetectionOverrideScope.newBuilder()
                    .setServiceScope(DetectionOverrideEntityScope.getDefaultInstance()))
            .build();
    status = configsValidator.validateScopeConfig(config);
    assertEquals(Status.INVALID_ARGUMENT.getCode(), status.getCode());
    assertEquals("Service Ids and service labels both shouldn't be empty", status.getDescription());

    config =
        DetectionOverrideScopeConfig.newBuilder()
            .addScopes(
                DetectionOverrideScope.newBuilder()
                    .setServiceScope(
                        DetectionOverrideEntityScope.newBuilder()
                            .setEntityIds(
                                StringList.newBuilder().addAllValues(List.of("id1", "id2")))))
            .build();
    assertEquals(Status.OK, configsValidator.validateScopeConfig(config));
  }

  @Test
  public void testValidateAssetConfig() {
    DetectionOverrideAssetConfig config = DetectionOverrideAssetConfig.getDefaultInstance();
    Status status = configsValidator.validateAssetConfig(config);
    assertEquals(Status.INVALID_ARGUMENT.getCode(), status.getCode());
    assertEquals(
        "Asset config should have a valid scope config or a valid conditions config.",
        status.getDescription());

    config =
        DetectionOverrideAssetConfig.newBuilder()
            .setScopeConfig(
                DetectionOverrideScopeConfig.newBuilder()
                    .addScopes(
                        DetectionOverrideScope.newBuilder()
                            .setServiceScope(
                                DetectionOverrideEntityScope.newBuilder()
                                    .setEntityIds(
                                        StringList.newBuilder().addAllValues(List.of("id1"))))))
            .build();
    assertEquals(Status.OK, configsValidator.validateAssetConfig(config));
  }
}
