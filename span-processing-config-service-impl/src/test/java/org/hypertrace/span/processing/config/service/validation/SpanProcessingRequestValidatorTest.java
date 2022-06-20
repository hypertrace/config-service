package org.hypertrace.span.processing.config.service.validation;

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
import org.hypertrace.span.processing.config.service.v1.ApiNamingRuleConfig;
import org.hypertrace.span.processing.config.service.v1.ApiNamingRuleInfo;
import org.hypertrace.span.processing.config.service.v1.CreateApiNamingRuleRequest;
import org.hypertrace.span.processing.config.service.v1.CreateExcludeSpanRuleRequest;
import org.hypertrace.span.processing.config.service.v1.CreateIncludeSpanRuleRequest;
import org.hypertrace.span.processing.config.service.v1.DeleteApiNamingRuleRequest;
import org.hypertrace.span.processing.config.service.v1.DeleteExcludeSpanRuleRequest;
import org.hypertrace.span.processing.config.service.v1.DeleteIncludeSpanRuleRequest;
import org.hypertrace.span.processing.config.service.v1.ExcludeSpanRuleInfo;
import org.hypertrace.span.processing.config.service.v1.Field;
import org.hypertrace.span.processing.config.service.v1.GetAllApiNamingRulesRequest;
import org.hypertrace.span.processing.config.service.v1.GetAllExcludeSpanRulesRequest;
import org.hypertrace.span.processing.config.service.v1.GetAllIncludeSpanRulesRequest;
import org.hypertrace.span.processing.config.service.v1.GetAllSamplingConfigsRequest;
import org.hypertrace.span.processing.config.service.v1.IncludeSpanRuleInfo;
import org.hypertrace.span.processing.config.service.v1.RelationalOperator;
import org.hypertrace.span.processing.config.service.v1.RelationalSpanFilterExpression;
import org.hypertrace.span.processing.config.service.v1.SegmentMatchingBasedConfig;
import org.hypertrace.span.processing.config.service.v1.SpanFilter;
import org.hypertrace.span.processing.config.service.v1.SpanFilterValue;
import org.hypertrace.span.processing.config.service.v1.UpdateApiNamingRule;
import org.hypertrace.span.processing.config.service.v1.UpdateApiNamingRuleRequest;
import org.hypertrace.span.processing.config.service.v1.UpdateExcludeSpanRule;
import org.hypertrace.span.processing.config.service.v1.UpdateExcludeSpanRuleRequest;
import org.hypertrace.span.processing.config.service.v1.UpdateIncludeSpanRule;
import org.hypertrace.span.processing.config.service.v1.UpdateIncludeSpanRuleRequest;
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
                            .setFilter(buildTestFilter())
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

  @Test
  void validatesIncludeSpanRulesGetRequest() {
    assertInvalidArgStatusContaining(
        "Tenant ID",
        () ->
            validator.validateOrThrow(
                mockRequestContext, GetAllIncludeSpanRulesRequest.newBuilder().build()));

    when(mockRequestContext.getTenantId()).thenReturn(Optional.of(TEST_TENANT_ID));
    assertDoesNotThrow(
        () ->
            validator.validateOrThrow(
                mockRequestContext, GetAllIncludeSpanRulesRequest.newBuilder().build()));
  }

  @Test
  void validatesIncludeSpanRuleDeleteRequest() {
    assertInvalidArgStatusContaining(
        "Tenant ID",
        () ->
            validator.validateOrThrow(
                mockRequestContext, DeleteIncludeSpanRuleRequest.newBuilder().build()));
    when(mockRequestContext.getTenantId()).thenReturn(Optional.of(TEST_TENANT_ID));

    assertInvalidArgStatusContaining(
        "DeleteIncludeSpanRuleRequest.id",
        () ->
            validator.validateOrThrow(
                mockRequestContext, DeleteIncludeSpanRuleRequest.newBuilder().setId("").build()));

    assertDoesNotThrow(
        () ->
            validator.validateOrThrow(
                mockRequestContext,
                DeleteIncludeSpanRuleRequest.newBuilder().setId("rule-id").build()));
  }

  @Test
  void validatesIncludeSpanRuleCreateRequest() {
    assertInvalidArgStatusContaining(
        "Tenant ID",
        () ->
            validator.validateOrThrow(
                mockRequestContext, CreateIncludeSpanRuleRequest.newBuilder().build()));
    when(mockRequestContext.getTenantId()).thenReturn(Optional.of(TEST_TENANT_ID));

    assertInvalidArgStatusContaining(
        "IncludeSpanRuleInfo.name",
        () ->
            validator.validateOrThrow(
                mockRequestContext,
                CreateIncludeSpanRuleRequest.newBuilder()
                    .setRuleInfo(IncludeSpanRuleInfo.newBuilder().build())
                    .build()));

    assertDoesNotThrow(
        () ->
            validator.validateOrThrow(
                mockRequestContext,
                CreateIncludeSpanRuleRequest.newBuilder()
                    .setRuleInfo(
                        IncludeSpanRuleInfo.newBuilder()
                            .setName("name")
                            .setDisabled(true)
                            .setFilter(buildTestFilter())
                            .build())
                    .build()));
  }

  @Test
  void validatesIncludeSpanRuleUpdateRequest() {
    assertInvalidArgStatusContaining(
        "Tenant ID",
        () ->
            validator.validateOrThrow(
                mockRequestContext, UpdateIncludeSpanRuleRequest.newBuilder().build()));
    when(mockRequestContext.getTenantId()).thenReturn(Optional.of(TEST_TENANT_ID));

    assertInvalidArgStatusContaining(
        "UpdateIncludeSpanRule.id",
        () ->
            validator.validateOrThrow(
                mockRequestContext,
                UpdateIncludeSpanRuleRequest.newBuilder()
                    .setRule(UpdateIncludeSpanRule.newBuilder().setName("name").build())
                    .build()));

    assertInvalidArgStatusContaining(
        "UpdateIncludeSpanRule.name",
        () ->
            validator.validateOrThrow(
                mockRequestContext,
                UpdateIncludeSpanRuleRequest.newBuilder()
                    .setRule(UpdateIncludeSpanRule.newBuilder().setId("id").build())
                    .build()));

    assertDoesNotThrow(
        () ->
            validator.validateOrThrow(
                mockRequestContext,
                UpdateIncludeSpanRuleRequest.newBuilder()
                    .setRule(
                        UpdateIncludeSpanRule.newBuilder()
                            .setId("id")
                            .setName("name")
                            .setFilter(buildTestFilter())
                            .build())
                    .build()));
  }

  @Test
  void validatesApiNamingRulesGetRequest() {
    assertInvalidArgStatusContaining(
        "Tenant ID",
        () ->
            validator.validateOrThrow(
                mockRequestContext, GetAllApiNamingRulesRequest.newBuilder().build()));

    when(mockRequestContext.getTenantId()).thenReturn(Optional.of(TEST_TENANT_ID));
    assertDoesNotThrow(
        () ->
            validator.validateOrThrow(
                mockRequestContext, GetAllApiNamingRulesRequest.newBuilder().build()));
  }

  @Test
  void validatesApiNamingRuleDeleteRequest() {
    assertInvalidArgStatusContaining(
        "Tenant ID",
        () ->
            validator.validateOrThrow(
                mockRequestContext, DeleteApiNamingRuleRequest.newBuilder().build()));
    when(mockRequestContext.getTenantId()).thenReturn(Optional.of(TEST_TENANT_ID));

    assertInvalidArgStatusContaining(
        "DeleteApiNamingRuleRequest.id",
        () ->
            validator.validateOrThrow(
                mockRequestContext, DeleteApiNamingRuleRequest.newBuilder().setId("").build()));

    assertDoesNotThrow(
        () ->
            validator.validateOrThrow(
                mockRequestContext,
                DeleteApiNamingRuleRequest.newBuilder().setId("rule-id").build()));
  }

  @Test
  void validatesApiNamingRuleCreateRequest() {
    assertInvalidArgStatusContaining(
        "Tenant ID",
        () ->
            validator.validateOrThrow(
                mockRequestContext, CreateApiNamingRuleRequest.newBuilder().build()));
    when(mockRequestContext.getTenantId()).thenReturn(Optional.of(TEST_TENANT_ID));

    assertInvalidArgStatusContaining(
        "ApiNamingRuleInfo.name",
        () ->
            validator.validateOrThrow(
                mockRequestContext,
                CreateApiNamingRuleRequest.newBuilder()
                    .setRuleInfo(ApiNamingRuleInfo.newBuilder().build())
                    .build()));

    assertInvalidArgStatusContaining(
        "Invalid regex count or segment matching count",
        () ->
            validator.validateOrThrow(
                mockRequestContext,
                CreateApiNamingRuleRequest.newBuilder()
                    .setRuleInfo(
                        ApiNamingRuleInfo.newBuilder()
                            .setName("name")
                            .setFilter(buildTestFilter())
                            .setRuleConfig(
                                ApiNamingRuleConfig.newBuilder()
                                    .setSegmentMatchingBasedConfig(
                                        SegmentMatchingBasedConfig.newBuilder().build())
                                    .build())
                            .build())
                    .build()));

    assertInvalidArgStatusContaining(
        "Invalid regex or value segment",
        () ->
            validator.validateOrThrow(
                mockRequestContext,
                CreateApiNamingRuleRequest.newBuilder()
                    .setRuleInfo(
                        ApiNamingRuleInfo.newBuilder()
                            .setName("name")
                            .setFilter(buildTestFilter())
                            .setRuleConfig(
                                ApiNamingRuleConfig.newBuilder()
                                    .setSegmentMatchingBasedConfig(
                                        SegmentMatchingBasedConfig.newBuilder()
                                            .addAllRegexes(List.of("regex", ""))
                                            .addAllValues(List.of("value1", "value2"))
                                            .build())
                                    .build())
                            .build())
                    .build()));

    assertDoesNotThrow(
        () ->
            validator.validateOrThrow(
                mockRequestContext,
                CreateApiNamingRuleRequest.newBuilder()
                    .setRuleInfo(
                        ApiNamingRuleInfo.newBuilder()
                            .setName("name")
                            .setDisabled(true)
                            .setRuleConfig(
                                ApiNamingRuleConfig.newBuilder()
                                    .setSegmentMatchingBasedConfig(
                                        SegmentMatchingBasedConfig.newBuilder()
                                            .addAllRegexes(List.of("regex"))
                                            .addAllValues(List.of("value"))
                                            .build())
                                    .build())
                            .setFilter(buildTestFilter())
                            .build())
                    .build()));
  }

  @Test
  void validatesApiNamingRuleUpdateRequest() {
    assertInvalidArgStatusContaining(
        "Tenant ID",
        () ->
            validator.validateOrThrow(
                mockRequestContext, UpdateApiNamingRuleRequest.newBuilder().build()));
    when(mockRequestContext.getTenantId()).thenReturn(Optional.of(TEST_TENANT_ID));

    assertInvalidArgStatusContaining(
        "UpdateApiNamingRule.id",
        () ->
            validator.validateOrThrow(
                mockRequestContext,
                UpdateApiNamingRuleRequest.newBuilder()
                    .setRule(UpdateApiNamingRule.newBuilder().setName("name").build())
                    .build()));

    assertInvalidArgStatusContaining(
        "UpdateApiNamingRule.name",
        () ->
            validator.validateOrThrow(
                mockRequestContext,
                UpdateApiNamingRuleRequest.newBuilder()
                    .setRule(UpdateApiNamingRule.newBuilder().setId("id").build())
                    .build()));

    assertInvalidArgStatusContaining(
        "Invalid regex count or segment matching count",
        () ->
            validator.validateOrThrow(
                mockRequestContext,
                UpdateApiNamingRuleRequest.newBuilder()
                    .setRule(
                        UpdateApiNamingRule.newBuilder()
                            .setId("id")
                            .setName("name")
                            .setFilter(buildTestFilter())
                            .setRuleConfig(
                                ApiNamingRuleConfig.newBuilder()
                                    .setSegmentMatchingBasedConfig(
                                        SegmentMatchingBasedConfig.newBuilder().build())
                                    .build())
                            .build())
                    .build()));

    assertInvalidArgStatusContaining(
        "Invalid regex or value segment",
        () ->
            validator.validateOrThrow(
                mockRequestContext,
                UpdateApiNamingRuleRequest.newBuilder()
                    .setRule(
                        UpdateApiNamingRule.newBuilder()
                            .setId("id")
                            .setName("name")
                            .setFilter(buildTestFilter())
                            .setRuleConfig(
                                ApiNamingRuleConfig.newBuilder()
                                    .setSegmentMatchingBasedConfig(
                                        SegmentMatchingBasedConfig.newBuilder()
                                            .addRegexes("regex")
                                            .addValues("")
                                            .build())
                                    .build())
                            .build())
                    .build()));

    assertDoesNotThrow(
        () ->
            validator.validateOrThrow(
                mockRequestContext,
                UpdateApiNamingRuleRequest.newBuilder()
                    .setRule(
                        UpdateApiNamingRule.newBuilder()
                            .setId("id")
                            .setName("name")
                            .setRuleConfig(
                                ApiNamingRuleConfig.newBuilder()
                                    .setSegmentMatchingBasedConfig(
                                        SegmentMatchingBasedConfig.newBuilder()
                                            .addAllRegexes(List.of("regex"))
                                            .addAllValues(List.of("value"))
                                            .build())
                                    .build())
                            .setFilter(buildTestFilter())
                            .build())
                    .build()));
  }

  @Test
  void validatesSamplingConfigsGetRequest() {
    assertInvalidArgStatusContaining(
        "Tenant ID",
        () ->
            validator.validateOrThrow(
                mockRequestContext, GetAllSamplingConfigsRequest.newBuilder().build()));

    when(mockRequestContext.getTenantId()).thenReturn(Optional.of(TEST_TENANT_ID));
    assertDoesNotThrow(
        () ->
            validator.validateOrThrow(
                mockRequestContext, GetAllSamplingConfigsRequest.newBuilder().build()));
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
}
