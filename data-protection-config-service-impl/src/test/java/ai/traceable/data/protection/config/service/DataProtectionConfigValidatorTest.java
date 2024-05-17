package ai.traceable.data.protection.config.service;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import ai.traceable.data.protection.config.service.v1.DataClassifierCondition;
import ai.traceable.data.protection.config.service.v1.DataProtectionConfig;
import ai.traceable.data.protection.config.service.v1.DataProtectionExclusion;
import ai.traceable.data.protection.config.service.v1.DataProtectionExclusionCondition;
import ai.traceable.data.protection.config.service.v1.DataProtectionScope;
import ai.traceable.data.protection.config.service.v1.DataSensitivity;
import ai.traceable.data.protection.config.service.v1.EntityCondition;
import ai.traceable.data.protection.config.service.v1.EntityType;
import ai.traceable.data.protection.config.service.v1.GetResolvedScopedDataProtectionConfigRequest;
import ai.traceable.data.protection.config.service.v1.ParameterNameCondition;
import ai.traceable.data.protection.config.service.v1.UpsertScopedDataProtectionConfigRequest;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import java.util.Objects;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;

public class DataProtectionConfigValidatorTest {
  private ConfigValidator validator;
  private final RequestContext requestContext = RequestContext.forTenantId("tenantId");

  @BeforeEach
  void setUp() {
    validator = new DataProtectionConfigValidator();
  }

  @Test
  void test_validateScope() {
    assertInvalidArgStatusContaining(
        "Scope must be non empty",
        () ->
            validator.validateGetRequest(
                requestContext,
                GetResolvedScopedDataProtectionConfigRequest.newBuilder()
                    .setScope(DataProtectionScope.newBuilder())
                    .build()));
  }

  @Test
  void test_validateDataSensitivity() {
    assertInvalidArgStatusContaining(
        "Invalid min data sensitivity",
        () ->
            validator.validateUpsertRequest(
                requestContext,
                UpsertScopedDataProtectionConfigRequest.newBuilder()
                    .setConfig(
                        DataProtectionConfig.newBuilder()
                            .addExclusions(DataProtectionExclusion.newBuilder()))
                    .build()));
  }

  @Test
  void test_DataProtectionExclusionConditionList() {
    assertInvalidArgStatusContaining(
        "Invalid data protection exclusion condition",
        () ->
            validator.validateUpsertRequest(
                requestContext,
                UpsertScopedDataProtectionConfigRequest.newBuilder()
                    .setConfig(
                        DataProtectionConfig.newBuilder()
                            .setMinDataSensitivity(DataSensitivity.DATA_SENSITIVITY_HIGH)
                            .addExclusions(
                                DataProtectionExclusion.newBuilder()
                                    .addConditions(DataProtectionExclusionCondition.newBuilder())))
                    .build()));
  }

  @Test
  void test_validateEntityCondition() {
    assertInvalidArgStatusContaining(
        "Invalid entity type condition",
        () ->
            validator.validateUpsertRequest(
                requestContext,
                UpsertScopedDataProtectionConfigRequest.newBuilder()
                    .setConfig(
                        DataProtectionConfig.newBuilder()
                            .setMinDataSensitivity(DataSensitivity.DATA_SENSITIVITY_HIGH)
                            .addExclusions(
                                DataProtectionExclusion.newBuilder()
                                    .addConditions(
                                        DataProtectionExclusionCondition.newBuilder()
                                            .setEntityCondition(EntityCondition.newBuilder()))))
                    .build()));

    assertInvalidArgStatusContaining(
        "Entity ID list cannot be empty",
        () ->
            validator.validateUpsertRequest(
                requestContext,
                UpsertScopedDataProtectionConfigRequest.newBuilder()
                    .setConfig(
                        DataProtectionConfig.newBuilder()
                            .setMinDataSensitivity(DataSensitivity.DATA_SENSITIVITY_HIGH)
                            .addExclusions(
                                DataProtectionExclusion.newBuilder()
                                    .addConditions(
                                        DataProtectionExclusionCondition.newBuilder()
                                            .setEntityCondition(
                                                EntityCondition.newBuilder()
                                                    .setEntityType(EntityType.ENTITY_TYPE_API)))))
                    .build()));
  }

  @Test
  void test_validateDataClassifierCondition() {
    assertInvalidArgStatusContaining(
        "Either dataset ID or datatype ID list should be non empty",
        () ->
            validator.validateUpsertRequest(
                requestContext,
                UpsertScopedDataProtectionConfigRequest.newBuilder()
                    .setConfig(
                        DataProtectionConfig.newBuilder()
                            .setMinDataSensitivity(DataSensitivity.DATA_SENSITIVITY_HIGH)
                            .addExclusions(
                                DataProtectionExclusion.newBuilder()
                                    .addConditions(
                                        DataProtectionExclusionCondition.newBuilder()
                                            .setDataClassifierCondition(
                                                DataClassifierCondition.newBuilder()))))
                    .build()));
  }

  @Test
  void test_validateParamNameCondition() {
    assertInvalidArgStatusContaining(
        "Either param name or param name regex list should be non empty",
        () ->
            validator.validateUpsertRequest(
                requestContext,
                UpsertScopedDataProtectionConfigRequest.newBuilder()
                    .setConfig(
                        DataProtectionConfig.newBuilder()
                            .setMinDataSensitivity(DataSensitivity.DATA_SENSITIVITY_HIGH)
                            .addExclusions(
                                DataProtectionExclusion.newBuilder()
                                    .addConditions(
                                        DataProtectionExclusionCondition.newBuilder()
                                            .setParamNameCondition(
                                                ParameterNameCondition.getDefaultInstance()))))
                    .build()));
  }

  private void assertInvalidArgStatusContaining(String text, Executable executable) {
    StatusRuntimeException exception = assertThrows(StatusRuntimeException.class, executable);
    assertEquals(Status.Code.INVALID_ARGUMENT, exception.getStatus().getCode());

    assertTrue(
        Objects.requireNonNull(exception.getStatus().getDescription()).contains(text),
        "Expected arg to contain " + text + " but was " + exception.getMessage());
  }
}
