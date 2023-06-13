package org.hypertrace.span.processing.config.service.validation;

import static org.hypertrace.span.processing.config.service.v1.RuleType.RULE_TYPE_SYSTEM;
import static org.hypertrace.span.processing.config.service.v1.RuleType.RULE_TYPE_USER;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.when;

import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.hypertrace.span.processing.config.service.v1.CreateExcludeSpanRuleRequest;
import org.hypertrace.span.processing.config.service.v1.DeleteExcludeSpanRuleRequest;
import org.hypertrace.span.processing.config.service.v1.ExcludeSpanRuleInfo;
import org.hypertrace.span.processing.config.service.v1.Field;
import org.hypertrace.span.processing.config.service.v1.GetAllExcludeSpanRulesRequest;
import org.hypertrace.span.processing.config.service.v1.LogicalOperator;
import org.hypertrace.span.processing.config.service.v1.LogicalSpanFilterExpression;
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
  void validatesExcludeSpanRulesGetRequest() {
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
  void validatesExcludeSpanRuleDeleteRequest() {
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
  void validatesExcludeSpanRuleCreateRequest() {
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
                    .setRuleInfo(ExcludeSpanRuleInfo.newBuilder().setType(RULE_TYPE_USER).build())
                    .build()));

    assertInvalidArgStatusContaining(
        "Invalid rule type to create system level rule",
        () ->
            validator.validateOrThrow(
                mockRequestContext,
                CreateExcludeSpanRuleRequest.newBuilder()
                    .setRuleInfo(
                        ExcludeSpanRuleInfo.newBuilder()
                            .setName("name")
                            .setType(RULE_TYPE_SYSTEM)
                            .setFilter(buildTestFilter())
                            .build())
                    .build()));

    assertInvalidArgStatusContaining(
        "Invalid regex passed",
        () ->
            validator.validateOrThrow(
                mockRequestContext,
                CreateExcludeSpanRuleRequest.newBuilder()
                    .setRuleInfo(
                        ExcludeSpanRuleInfo.newBuilder()
                            .setName("name")
                            .setDisabled(true)
                            .setFilter(buildInvalidRegexTestFilterWithAndOperator())
                            .setType(RULE_TYPE_USER)
                            .build())
                    .build()));

    assertInvalidArgStatusContaining(
        "Invalid regex passed",
        () ->
            validator.validateOrThrow(
                mockRequestContext,
                CreateExcludeSpanRuleRequest.newBuilder()
                    .setRuleInfo(
                        ExcludeSpanRuleInfo.newBuilder()
                            .setName("name")
                            .setDisabled(true)
                            .setFilter(buildInvalidRegexTestFilterWithOrOperator())
                            .setType(RULE_TYPE_USER)
                            .build())
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
                            .setFilter(buildTestFilter())
                            .setType(RULE_TYPE_USER)
                            .build())
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
                            .setFilter(buildTestLogicalFilterWithNoOperands())
                            .setType(RULE_TYPE_USER)
                            .build())
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
                            .setFilter(buildRegexTestFilterWithOrOperator())
                            .setType(RULE_TYPE_USER)
                            .build())
                    .build()));
  }

  @Test
  void validatesExcludeSpanRuleUpdateRequest() {
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

    assertInvalidArgStatusContaining(
        "UpdateExcludeSpanRule.name",
        () ->
            validator.validateOrThrow(
                mockRequestContext,
                UpdateExcludeSpanRuleRequest.newBuilder()
                    .setRule(UpdateExcludeSpanRule.newBuilder().setId("id").build())
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
                            .setFilter(buildTestFilter())
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

  private SpanFilter buildTestFilter() {
    return SpanFilter.newBuilder()
        .setRelationalSpanFilter(
            RelationalSpanFilterExpression.newBuilder()
                .setField(Field.FIELD_SERVICE_NAME)
                .setOperator(RelationalOperator.RELATIONAL_OPERATOR_CONTAINS)
                .setRightOperand(SpanFilterValue.newBuilder().setStringValue("a").build())
                .build())
        .build();
  }

  private SpanFilter buildInvalidRegexTestFilterWithAndOperator() {
    return SpanFilter.newBuilder()
        .setLogicalSpanFilter(
            LogicalSpanFilterExpression.newBuilder()
                .setOperator(LogicalOperator.LOGICAL_OPERATOR_AND)
                .addAllOperands(
                    List.of(
                        SpanFilter.newBuilder()
                            .setRelationalSpanFilter(
                                RelationalSpanFilterExpression.newBuilder()
                                    .setField(Field.FIELD_SERVICE_NAME)
                                    .setOperator(RelationalOperator.RELATIONAL_OPERATOR_CONTAINS)
                                    .setRightOperand(
                                        SpanFilterValue.newBuilder().setStringValue("a")))
                            .build(),
                        SpanFilter.newBuilder()
                            .setRelationalSpanFilter(
                                RelationalSpanFilterExpression.newBuilder()
                                    .setField(Field.FIELD_SERVICE_NAME)
                                    .setOperator(RelationalOperator.RELATIONAL_OPERATOR_REGEX_MATCH)
                                    .setRightOperand(
                                        SpanFilterValue.newBuilder().setStringValue("[(test")))
                            .build())))
        .build();
  }

  private SpanFilter buildInvalidRegexTestFilterWithOrOperator() {
    return SpanFilter.newBuilder()
        .setLogicalSpanFilter(
            LogicalSpanFilterExpression.newBuilder()
                .setOperator(LogicalOperator.LOGICAL_OPERATOR_OR)
                .addAllOperands(
                    List.of(
                        SpanFilter.newBuilder()
                            .setRelationalSpanFilter(
                                RelationalSpanFilterExpression.newBuilder()
                                    .setField(Field.FIELD_SERVICE_NAME)
                                    .setOperator(RelationalOperator.RELATIONAL_OPERATOR_CONTAINS)
                                    .setRightOperand(
                                        SpanFilterValue.newBuilder().setStringValue("a")))
                            .build(),
                        SpanFilter.newBuilder()
                            .setRelationalSpanFilter(
                                RelationalSpanFilterExpression.newBuilder()
                                    .setField(Field.FIELD_SERVICE_NAME)
                                    .setOperator(RelationalOperator.RELATIONAL_OPERATOR_REGEX_MATCH)
                                    .setRightOperand(
                                        SpanFilterValue.newBuilder().setStringValue("[(test")))
                            .build())))
        .build();
  }

  private SpanFilter buildRegexTestFilterWithOrOperator() {
    return SpanFilter.newBuilder()
        .setLogicalSpanFilter(
            LogicalSpanFilterExpression.newBuilder()
                .setOperator(LogicalOperator.LOGICAL_OPERATOR_OR)
                .addAllOperands(
                    List.of(
                        SpanFilter.newBuilder()
                            .setRelationalSpanFilter(
                                RelationalSpanFilterExpression.newBuilder()
                                    .setField(Field.FIELD_SERVICE_NAME)
                                    .setOperator(RelationalOperator.RELATIONAL_OPERATOR_CONTAINS)
                                    .setRightOperand(
                                        SpanFilterValue.newBuilder().setStringValue("a")))
                            .build(),
                        SpanFilter.newBuilder()
                            .setRelationalSpanFilter(
                                RelationalSpanFilterExpression.newBuilder()
                                    .setField(Field.FIELD_SERVICE_NAME)
                                    .setOperator(RelationalOperator.RELATIONAL_OPERATOR_REGEX_MATCH)
                                    .setRightOperand(
                                        SpanFilterValue.newBuilder().setStringValue(".*test")))
                            .build())))
        .build();
  }

  private SpanFilter buildTestLogicalFilterWithNoOperands() {
    return SpanFilter.newBuilder()
        .setLogicalSpanFilter(
            LogicalSpanFilterExpression.newBuilder()
                .setOperator(LogicalOperator.LOGICAL_OPERATOR_OR)
                .addAllOperands(List.of()))
        .build();
  }
}
