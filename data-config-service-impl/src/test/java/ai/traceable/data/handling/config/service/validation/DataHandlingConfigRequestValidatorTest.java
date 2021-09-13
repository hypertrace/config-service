package ai.traceable.data.handling.config.service.validation;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.when;

import ai.traceable.data.handling.config.service.v1.CreateDataHandlingRuleRequest;
import ai.traceable.data.handling.config.service.v1.DataHandlingRuleAction;
import ai.traceable.data.handling.config.service.v1.DataHandlingRuleCondition;
import ai.traceable.data.handling.config.service.v1.DataHandlingRuleData;
import ai.traceable.data.handling.config.service.v1.DeleteDataHandlingRuleRequest;
import ai.traceable.data.handling.config.service.v1.GetDataHandlingRulesRequest;
import ai.traceable.data.handling.config.service.v1.RankDataHandlingRuleRequest;
import ai.traceable.data.handling.config.service.v1.UpdateDataHandlingRuleRequest;
import io.grpc.Status.Code;
import io.grpc.StatusRuntimeException;
import java.util.Objects;
import java.util.Optional;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.function.Executable;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class DataHandlingConfigRequestValidatorTest {
  private static final String TEST_TENANT_ID = "DataHandlingConfigRequestValidatorTest-tenant-id";
  private final DataHandlingConfigRequestValidator validator =
      new DataHandlingConfigRequestValidator();

  @Mock private RequestContext mockRequestContext;

  @Test
  void validatesGetRequest() {
    assertInvalidArgStatusContaining(
        "Tenant ID",
        () ->
            validator.validateOrThrow(
                mockRequestContext, GetDataHandlingRulesRequest.newBuilder().build()));

    when(mockRequestContext.getTenantId()).thenReturn(Optional.of(TEST_TENANT_ID));
    assertDoesNotThrow(
        () ->
            validator.validateOrThrow(
                mockRequestContext, GetDataHandlingRulesRequest.newBuilder().build()));
  }

  @Test
  void validatesCreateRequest() {

    assertInvalidArgStatusContaining(
        "Tenant ID",
        () ->
            validator.validateOrThrow(
                mockRequestContext, CreateDataHandlingRuleRequest.newBuilder().build()));
    when(mockRequestContext.getTenantId()).thenReturn(Optional.of(TEST_TENANT_ID));

    assertInvalidArgStatusContaining(
        "DataHandlingRuleData.name",
        () ->
            validator.validateOrThrow(
                mockRequestContext, CreateDataHandlingRuleRequest.newBuilder().build()));

    assertInvalidArgStatusContaining(
        "DataHandlingRuleData.conditions",
        () ->
            validator.validateOrThrow(
                mockRequestContext,
                CreateDataHandlingRuleRequest.newBuilder()
                    .setData(DataHandlingRuleData.newBuilder().setName("rule-name"))
                    .build()));
    assertInvalidArgStatusContaining(
        "DataHandlingRuleData.actions",
        () ->
            validator.validateOrThrow(
                mockRequestContext,
                CreateDataHandlingRuleRequest.newBuilder()
                    .setData(
                        DataHandlingRuleData.newBuilder()
                            .setName("rule-name")
                            .addConditions(DataHandlingRuleCondition.getDefaultInstance()))
                    .build()));

    assertDoesNotThrow(
        () ->
            validator.validateOrThrow(
                mockRequestContext,
                CreateDataHandlingRuleRequest.newBuilder()
                    .setData(
                        DataHandlingRuleData.newBuilder()
                            .setName("rule-name")
                            .addActions(DataHandlingRuleAction.getDefaultInstance())
                            .addConditions(DataHandlingRuleCondition.getDefaultInstance()))
                    .build()));
  }

  @Test
  void validatesUpdateRequest() {
    assertInvalidArgStatusContaining(
        "Tenant ID",
        () ->
            validator.validateOrThrow(
                mockRequestContext, UpdateDataHandlingRuleRequest.newBuilder().build()));
    when(mockRequestContext.getTenantId()).thenReturn(Optional.of(TEST_TENANT_ID));
    assertInvalidArgStatusContaining(
        "UpdateDataHandlingRuleRequest.id",
        () ->
            validator.validateOrThrow(
                mockRequestContext, UpdateDataHandlingRuleRequest.newBuilder().build()));

    assertInvalidArgStatusContaining(
        "DataHandlingRuleData.name",
        () ->
            validator.validateOrThrow(
                mockRequestContext,
                UpdateDataHandlingRuleRequest.newBuilder().setId("rule-id").build()));

    assertInvalidArgStatusContaining(
        "DataHandlingRuleData.conditions",
        () ->
            validator.validateOrThrow(
                mockRequestContext,
                UpdateDataHandlingRuleRequest.newBuilder()
                    .setId("rule-id")
                    .setData(DataHandlingRuleData.newBuilder().setName("rule-name"))
                    .build()));
    assertInvalidArgStatusContaining(
        "DataHandlingRuleData.actions",
        () ->
            validator.validateOrThrow(
                mockRequestContext,
                UpdateDataHandlingRuleRequest.newBuilder()
                    .setId("rule-id")
                    .setData(
                        DataHandlingRuleData.newBuilder()
                            .setName("rule-name")
                            .addConditions(DataHandlingRuleCondition.getDefaultInstance()))
                    .build()));

    assertDoesNotThrow(
        () ->
            validator.validateOrThrow(
                mockRequestContext,
                UpdateDataHandlingRuleRequest.newBuilder()
                    .setId("rule-id")
                    .setData(
                        DataHandlingRuleData.newBuilder()
                            .setName("rule-name")
                            .addActions(DataHandlingRuleAction.getDefaultInstance())
                            .addConditions(DataHandlingRuleCondition.getDefaultInstance()))
                    .build()));
  }

  @Test
  void validatesDeleteRequest() {
    assertInvalidArgStatusContaining(
        "Tenant ID",
        () ->
            validator.validateOrThrow(
                mockRequestContext, DeleteDataHandlingRuleRequest.newBuilder().build()));
    when(mockRequestContext.getTenantId()).thenReturn(Optional.of(TEST_TENANT_ID));
    assertInvalidArgStatusContaining(
        "DeleteDataHandlingRuleRequest.id",
        () ->
            validator.validateOrThrow(
                mockRequestContext, DeleteDataHandlingRuleRequest.newBuilder().setId("").build()));

    assertDoesNotThrow(
        () ->
            validator.validateOrThrow(
                mockRequestContext,
                DeleteDataHandlingRuleRequest.newBuilder().setId("rule-id").build()));
  }

  @Test
  void validatesRankRequest() {
    assertInvalidArgStatusContaining(
        "Tenant ID",
        () ->
            validator.validateOrThrow(
                mockRequestContext, RankDataHandlingRuleRequest.newBuilder().build()));

    when(mockRequestContext.getTenantId()).thenReturn(Optional.of(TEST_TENANT_ID));
    assertInvalidArgStatusContaining(
        "rule_id_to_update",
        () ->
            validator.validateOrThrow(
                mockRequestContext, RankDataHandlingRuleRequest.newBuilder().build()));

    assertInvalidArgStatusContaining(
        "Can't rerank a rule against itself",
        () ->
            validator.validateOrThrow(
                mockRequestContext,
                RankDataHandlingRuleRequest.newBuilder()
                    .setRuleIdToUpdate("some-id")
                    .setPrecedingRuleId("some-id")
                    .build()));
    assertDoesNotThrow(
        () ->
            validator.validateOrThrow(
                mockRequestContext,
                RankDataHandlingRuleRequest.newBuilder().setRuleIdToUpdate("some-id").build()));
  }

  private void assertInvalidArgStatusContaining(String text, Executable executable) {
    StatusRuntimeException exception = assertThrows(StatusRuntimeException.class, executable);
    assertEquals(Code.INVALID_ARGUMENT, exception.getStatus().getCode());

    assertTrue(
        Objects.requireNonNull(exception.getStatus().getDescription()).contains(text),
        "Expected arg to contain " + text + " but was " + exception.getMessage());
  }
}
