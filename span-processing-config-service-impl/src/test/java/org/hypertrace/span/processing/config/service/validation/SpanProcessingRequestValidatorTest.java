package org.hypertrace.span.processing.config.service.validation;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.when;

import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import java.util.Objects;
import java.util.Optional;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.hypertrace.span.processing.config.service.v1.CreateExcludeSpanRuleRequest;
import org.hypertrace.span.processing.config.service.v1.DeleteExcludeSpanRuleRequest;
import org.hypertrace.span.processing.config.service.v1.ExcludeSpanRuleInfo;
import org.hypertrace.span.processing.config.service.v1.Field;
import org.hypertrace.span.processing.config.service.v1.GetAllExcludeSpanRulesRequest;
import org.hypertrace.span.processing.config.service.v1.RelationalOperator;
import org.hypertrace.span.processing.config.service.v1.RelationalSpanFilterExpression;
import org.hypertrace.span.processing.config.service.v1.SpanFilter;
import org.hypertrace.span.processing.config.service.v1.SpanFilterValue;
import org.hypertrace.span.processing.config.service.v1.UpdateExcludeSpanRule;
import org.hypertrace.span.processing.config.service.v1.UpdateExcludeSpanRuleRequest;
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
                mockRequestContext, GetAllExcludeSpanRulesRequest.newBuilder().build()));

    when(mockRequestContext.getTenantId()).thenReturn(Optional.of(TEST_TENANT_ID));
    assertDoesNotThrow(
        () ->
            validator.validateOrThrow(
                mockRequestContext, GetAllExcludeSpanRulesRequest.newBuilder().build()));
  }

  @Test
  void validatesDeleteRequest() {
    assertInvalidArgStatusContaining(
        "Tenant ID",
        () ->
            validator.validateOrThrow(
                mockRequestContext, DeleteExcludeSpanRuleRequest.newBuilder().build()));
    when(mockRequestContext.getTenantId()).thenReturn(Optional.of(TEST_TENANT_ID));

    assertInvalidArgStatusContaining(
        "DeleteExcludeSpanRuleRequest.id",
        () ->
            validator.validateOrThrow(
                mockRequestContext, DeleteExcludeSpanRuleRequest.newBuilder().setId("").build()));

    assertDoesNotThrow(
        () ->
            validator.validateOrThrow(
                mockRequestContext,
                DeleteExcludeSpanRuleRequest.newBuilder().setId("rule-id").build()));
  }

  @Test
  void validatesCreateRequest() {
    assertInvalidArgStatusContaining(
        "Tenant ID",
        () ->
            validator.validateOrThrow(
                mockRequestContext, CreateExcludeSpanRuleRequest.newBuilder().build()));
    when(mockRequestContext.getTenantId()).thenReturn(Optional.of(TEST_TENANT_ID));

    assertInvalidArgStatusContaining(
        "ExcludeSpanRuleInfo.name",
        () ->
            validator.validateOrThrow(
                mockRequestContext,
                CreateExcludeSpanRuleRequest.newBuilder()
                    .setRuleInfo(ExcludeSpanRuleInfo.newBuilder().build())
                    .build()));

    assertDoesNotThrow(
        () ->
            validator.validateOrThrow(
                mockRequestContext,
                CreateExcludeSpanRuleRequest.newBuilder()
                    .setRuleInfo(
                        ExcludeSpanRuleInfo.newBuilder()
                            .setName("name")
                            .setDisabled(true)
                            .setFilter(
                                SpanFilter.newBuilder()
                                    .setRelationalSpanFilter(
                                        RelationalSpanFilterExpression.newBuilder()
                                            .setField(Field.FIELD_SERVICE_NAME)
                                            .setOperator(
                                                RelationalOperator.RELATIONAL_OPERATOR_CONTAINS)
                                            .setRightOperand(
                                                SpanFilterValue.newBuilder()
                                                    .setStringValue("a")
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
                mockRequestContext, UpdateExcludeSpanRuleRequest.newBuilder().build()));
    when(mockRequestContext.getTenantId()).thenReturn(Optional.of(TEST_TENANT_ID));

    assertInvalidArgStatusContaining(
        "UpdateExcludeSpanRule.id",
        () ->
            validator.validateOrThrow(
                mockRequestContext,
                UpdateExcludeSpanRuleRequest.newBuilder()
                    .setRule(UpdateExcludeSpanRule.newBuilder().setName("name").build())
                    .build()));

    assertDoesNotThrow(
        () ->
            validator.validateOrThrow(
                mockRequestContext,
                UpdateExcludeSpanRuleRequest.newBuilder()
                    .setRule(
                        UpdateExcludeSpanRule.newBuilder()
                            .setId("id")
                            .setName("name")
                            .setFilter(
                                SpanFilter.newBuilder()
                                    .setRelationalSpanFilter(
                                        RelationalSpanFilterExpression.newBuilder()
                                            .setField(Field.FIELD_SERVICE_NAME)
                                            .setOperator(
                                                RelationalOperator.RELATIONAL_OPERATOR_CONTAINS)
                                            .setRightOperand(
                                                SpanFilterValue.newBuilder()
                                                    .setStringValue("a")
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
