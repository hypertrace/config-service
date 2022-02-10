package ai.traceable.span.processing.config.service.validation;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.when;

import ai.traceable.span.processing.config.service.v1.CreateSpanProcessingRuleRequest;
import ai.traceable.span.processing.config.service.v1.DeleteSpanProcessingRuleRequest;
import ai.traceable.span.processing.config.service.v1.ExcludeSpanRule;
import ai.traceable.span.processing.config.service.v1.Field;
import ai.traceable.span.processing.config.service.v1.Filter;
import ai.traceable.span.processing.config.service.v1.FilterValue;
import ai.traceable.span.processing.config.service.v1.GetAllSpanProcessingRulesRequest;
import ai.traceable.span.processing.config.service.v1.RelationalFilterExpression;
import ai.traceable.span.processing.config.service.v1.RelationalOperator;
import ai.traceable.span.processing.config.service.v1.SpanProcessingRuleInfo;
import ai.traceable.span.processing.config.service.v1.UpdateExcludeSpanRule;
import ai.traceable.span.processing.config.service.v1.UpdateSpanProcessingRule;
import ai.traceable.span.processing.config.service.v1.UpdateSpanProcessingRulesRequest;
import io.grpc.Status;
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
class SpanProcessingRequestValidatorTest {
  private static final String TEST_TENANT_ID = "SpanProcessingRequestValidatorTest-tenant-id";
  private final SpanProcessingConfigRequestValidator validator =
      new SpanProcessingConfigRequestValidator();

  @Mock private RequestContext mockRequestContext;

  @Test
  void validatesGetRequest() {
    assertInvalidArgStatusContaining(
        "Tenant ID",
        () ->
            validator.validateOrThrow(
                mockRequestContext, GetAllSpanProcessingRulesRequest.newBuilder().build()));

    when(mockRequestContext.getTenantId()).thenReturn(Optional.of(TEST_TENANT_ID));
    assertDoesNotThrow(
        () ->
            validator.validateOrThrow(
                mockRequestContext, GetAllSpanProcessingRulesRequest.newBuilder().build()));
  }

  @Test
  void validatesDeleteRequest() {
    assertInvalidArgStatusContaining(
        "Tenant ID",
        () ->
            validator.validateOrThrow(
                mockRequestContext, DeleteSpanProcessingRuleRequest.newBuilder().build()));
    when(mockRequestContext.getTenantId()).thenReturn(Optional.of(TEST_TENANT_ID));

    assertInvalidArgStatusContaining(
        "DeleteSpanProcessingRuleRequest.id",
        () ->
            validator.validateOrThrow(
                mockRequestContext,
                DeleteSpanProcessingRuleRequest.newBuilder().setId("").build()));

    assertDoesNotThrow(
        () ->
            validator.validateOrThrow(
                mockRequestContext,
                DeleteSpanProcessingRuleRequest.newBuilder().setId("rule-id").build()));
  }

  @Test
  void validatesCreateRequest() {
    assertInvalidArgStatusContaining(
        "Tenant ID",
        () ->
            validator.validateOrThrow(
                mockRequestContext, CreateSpanProcessingRuleRequest.newBuilder().build()));
    when(mockRequestContext.getTenantId()).thenReturn(Optional.of(TEST_TENANT_ID));

    assertInvalidArgStatusContaining(
        "SpanProcessingRuleInfo",
        () ->
            validator.validateOrThrow(
                mockRequestContext,
                CreateSpanProcessingRuleRequest.newBuilder()
                    .setRuleInfo(SpanProcessingRuleInfo.newBuilder().build())
                    .build()));

    assertInvalidArgStatusContaining(
        "SpanProcessingRuleInfo.name",
        () ->
            validator.validateOrThrow(
                mockRequestContext,
                CreateSpanProcessingRuleRequest.newBuilder()
                    .setRuleInfo(
                        SpanProcessingRuleInfo.newBuilder()
                            .setExcludeSpanRule(ExcludeSpanRule.newBuilder().build())
                            .build())
                    .build()));

    assertDoesNotThrow(
        () ->
            validator.validateOrThrow(
                mockRequestContext,
                CreateSpanProcessingRuleRequest.newBuilder()
                    .setRuleInfo(
                        SpanProcessingRuleInfo.newBuilder()
                            .setName("name")
                            .setExcludeSpanRule(
                                ExcludeSpanRule.newBuilder()
                                    .setFilter(
                                        Filter.newBuilder()
                                            .setRelationalFilter(
                                                RelationalFilterExpression.newBuilder()
                                                    .setField(Field.FIELD_SERVICE_NAME)
                                                    .setOperator(
                                                        RelationalOperator
                                                            .RELATIONAL_OPERATOR_CONTAINS)
                                                    .setRightOperand(
                                                        FilterValue.newBuilder()
                                                            .setStringValue("a")
                                                            .build())
                                                    .build())
                                            .build())
                                    .build())
                            .build())
                    .build()));
  }

  @Test
  void validatesUpdateRequest() {
    assertInvalidArgStatusContaining(
        "Tenant ID",
        () ->
            validator.validateOrThrow(
                mockRequestContext, UpdateSpanProcessingRulesRequest.newBuilder().build()));
    when(mockRequestContext.getTenantId()).thenReturn(Optional.of(TEST_TENANT_ID));

    assertInvalidArgStatusContaining(
        "No rules specified",
        () ->
            validator.validateOrThrow(
                mockRequestContext, UpdateSpanProcessingRulesRequest.newBuilder().build()));

    assertInvalidArgStatusContaining(
        "UpdateSpanProcessingRule.id",
        () ->
            validator.validateOrThrow(
                mockRequestContext,
                UpdateSpanProcessingRulesRequest.newBuilder()
                    .addRules(UpdateSpanProcessingRule.newBuilder().setName("name").build())
                    .build()));

    assertInvalidArgStatusContaining(
        "UpdateSpanProcessingRule.name",
        () ->
            validator.validateOrThrow(
                mockRequestContext,
                UpdateSpanProcessingRulesRequest.newBuilder()
                    .addRules(UpdateSpanProcessingRule.newBuilder().setId("id").build())
                    .build()));

    assertInvalidArgStatusContaining(
        "UpdateSpanProcessingRule.id",
        () ->
            validator.validateOrThrow(
                mockRequestContext,
                UpdateSpanProcessingRulesRequest.newBuilder()
                    .addRules(
                        UpdateSpanProcessingRule.newBuilder()
                            .setExcludeSpanRule(UpdateExcludeSpanRule.newBuilder()))
                    .build()));

    assertDoesNotThrow(
        () ->
            validator.validateOrThrow(
                mockRequestContext,
                UpdateSpanProcessingRulesRequest.newBuilder()
                    .addRules(
                        UpdateSpanProcessingRule.newBuilder()
                            .setId("id")
                            .setName("name")
                            .setExcludeSpanRule(
                                UpdateExcludeSpanRule.newBuilder()
                                    .setFilter(
                                        Filter.newBuilder()
                                            .setRelationalFilter(
                                                RelationalFilterExpression.newBuilder()
                                                    .setField(Field.FIELD_SERVICE_NAME)
                                                    .setOperator(
                                                        RelationalOperator
                                                            .RELATIONAL_OPERATOR_CONTAINS)
                                                    .setRightOperand(
                                                        FilterValue.newBuilder()
                                                            .setStringValue("a")
                                                            .build())
                                                    .build())
                                            .build())
                                    .build())
                            .build())
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
