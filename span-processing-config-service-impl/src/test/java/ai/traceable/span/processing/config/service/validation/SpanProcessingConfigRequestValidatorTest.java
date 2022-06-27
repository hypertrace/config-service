package ai.traceable.span.processing.config.service.validation;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.when;

import ai.traceable.span.processing.config.service.v1.CreateProtectionSpanRuleRequest;
import ai.traceable.span.processing.config.service.v1.CreateSamplingConfigRequest;
import ai.traceable.span.processing.config.service.v1.DeleteProtectionSpanRuleRequest;
import ai.traceable.span.processing.config.service.v1.DeleteSamplingConfigRequest;
import ai.traceable.span.processing.config.service.v1.Field;
import ai.traceable.span.processing.config.service.v1.GetAllProtectionSpanRulesRequest;
import ai.traceable.span.processing.config.service.v1.GetAllSamplingConfigsRequest;
import ai.traceable.span.processing.config.service.v1.ProtectionSpanRuleInfo;
import ai.traceable.span.processing.config.service.v1.RateLimit;
import ai.traceable.span.processing.config.service.v1.RateLimitConfig;
import ai.traceable.span.processing.config.service.v1.RelationalOperator;
import ai.traceable.span.processing.config.service.v1.RelationalSpanFilterExpression;
import ai.traceable.span.processing.config.service.v1.SamplingConfigInfo;
import ai.traceable.span.processing.config.service.v1.SpanFilter;
import ai.traceable.span.processing.config.service.v1.SpanFilterValue;
import ai.traceable.span.processing.config.service.v1.UpdateProtectionSpanRule;
import ai.traceable.span.processing.config.service.v1.UpdateProtectionSpanRuleRequest;
import ai.traceable.span.processing.config.service.v1.UpdateSamplingConfig;
import ai.traceable.span.processing.config.service.v1.UpdateSamplingConfigRequest;
import ai.traceable.span.processing.config.service.v1.WindowedRateLimit;
import com.google.protobuf.Duration;
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
class SpanProcessingConfigRequestValidatorTest {
  private static final String TEST_TENANT_ID = "SpanProcessingRequestValidatorTest-tenant-id";
  private final SpanProcessingConfigRequestValidator validator =
      new SpanProcessingConfigRequestValidator();

  @Mock private RequestContext mockRequestContext;

  @Test
  void validatesProtectionSpanRulesGetRequest() {
    assertInvalidArgStatusContaining(
        "Tenant ID",
        () ->
            validator.validateOrThrow(
                mockRequestContext, GetAllProtectionSpanRulesRequest.newBuilder().build()));

    when(mockRequestContext.getTenantId()).thenReturn(Optional.of(TEST_TENANT_ID));
    assertDoesNotThrow(
        () ->
            validator.validateOrThrow(
                mockRequestContext, GetAllProtectionSpanRulesRequest.newBuilder().build()));
  }

  @Test
  void validatesProtectionSpanRuleDeleteRequest() {
    assertInvalidArgStatusContaining(
        "Tenant ID",
        () ->
            validator.validateOrThrow(
                mockRequestContext, DeleteProtectionSpanRuleRequest.newBuilder().build()));
    when(mockRequestContext.getTenantId()).thenReturn(Optional.of(TEST_TENANT_ID));

    assertInvalidArgStatusContaining(
        "DeleteProtectionSpanRuleRequest.id",
        () ->
            validator.validateOrThrow(
                mockRequestContext,
                DeleteProtectionSpanRuleRequest.newBuilder().setId("").build()));

    assertDoesNotThrow(
        () ->
            validator.validateOrThrow(
                mockRequestContext,
                DeleteProtectionSpanRuleRequest.newBuilder().setId("rule-id").build()));
  }

  @Test
  void validatesProtectionSpanRuleCreateRequest() {
    assertInvalidArgStatusContaining(
        "Tenant ID",
        () ->
            validator.validateOrThrow(
                mockRequestContext, CreateProtectionSpanRuleRequest.newBuilder().build()));
    when(mockRequestContext.getTenantId()).thenReturn(Optional.of(TEST_TENANT_ID));

    assertInvalidArgStatusContaining(
        "ProtectionSpanRuleInfo.name",
        () ->
            validator.validateOrThrow(
                mockRequestContext,
                CreateProtectionSpanRuleRequest.newBuilder()
                    .setRuleInfo(ProtectionSpanRuleInfo.newBuilder().build())
                    .build()));

    assertDoesNotThrow(
        () ->
            validator.validateOrThrow(
                mockRequestContext,
                CreateProtectionSpanRuleRequest.newBuilder()
                    .setRuleInfo(
                        ProtectionSpanRuleInfo.newBuilder()
                            .setName("name")
                            .setDisabled(true)
                            .setFilter(buildTestFilter())
                            .build())
                    .build()));
  }

  @Test
  void validatesProtectionSpanRuleUpdateRequest() {
    assertInvalidArgStatusContaining(
        "Tenant ID",
        () ->
            validator.validateOrThrow(
                mockRequestContext, UpdateProtectionSpanRuleRequest.newBuilder().build()));
    when(mockRequestContext.getTenantId()).thenReturn(Optional.of(TEST_TENANT_ID));

    assertInvalidArgStatusContaining(
        "UpdateProtectionSpanRule.id",
        () ->
            validator.validateOrThrow(
                mockRequestContext,
                UpdateProtectionSpanRuleRequest.newBuilder()
                    .setRule(UpdateProtectionSpanRule.newBuilder().setName("name").build())
                    .build()));

    assertInvalidArgStatusContaining(
        "UpdateProtectionSpanRule.name",
        () ->
            validator.validateOrThrow(
                mockRequestContext,
                UpdateProtectionSpanRuleRequest.newBuilder()
                    .setRule(UpdateProtectionSpanRule.newBuilder().setId("id").build())
                    .build()));

    assertDoesNotThrow(
        () ->
            validator.validateOrThrow(
                mockRequestContext,
                UpdateProtectionSpanRuleRequest.newBuilder()
                    .setRule(
                        UpdateProtectionSpanRule.newBuilder()
                            .setId("id")
                            .setName("name")
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

  @Test
  void validatesSamplingConfigCreateRequest() {
    assertInvalidArgStatusContaining(
        "Tenant ID",
        () ->
            validator.validateOrThrow(
                mockRequestContext, CreateSamplingConfigRequest.newBuilder().build()));
    when(mockRequestContext.getTenantId()).thenReturn(Optional.of(TEST_TENANT_ID));

    assertDoesNotThrow(
        () ->
            validator.validateOrThrow(
                mockRequestContext,
                CreateSamplingConfigRequest.newBuilder()
                    .setSamplingConfigInfo(
                        SamplingConfigInfo.newBuilder()
                            .setRateLimitConfig(buildTestRateLimitConfig())
                            .setFilter(buildTestFilter())
                            .build())
                    .build()));
  }

  @Test
  void validatesSamplingConfigUpdateRequest() {
    assertInvalidArgStatusContaining(
        "Tenant ID",
        () ->
            validator.validateOrThrow(
                mockRequestContext, UpdateSamplingConfigRequest.newBuilder().build()));
    when(mockRequestContext.getTenantId()).thenReturn(Optional.of(TEST_TENANT_ID));

    assertInvalidArgStatusContaining(
        "UpdateSamplingConfig.id",
        () ->
            validator.validateOrThrow(
                mockRequestContext,
                UpdateSamplingConfigRequest.newBuilder()
                    .setSamplingConfig(UpdateSamplingConfig.newBuilder().build())
                    .build()));

    assertDoesNotThrow(
        () ->
            validator.validateOrThrow(
                mockRequestContext,
                UpdateSamplingConfigRequest.newBuilder()
                    .setSamplingConfig(
                        UpdateSamplingConfig.newBuilder()
                            .setId("id")
                            .setRateLimitConfig(buildTestRateLimitConfig())
                            .setFilter(buildTestFilter())
                            .build())
                    .build()));
  }

  @Test
  void validatesSamplingConfigDeleteRequest() {
    assertInvalidArgStatusContaining(
        "Tenant ID",
        () ->
            validator.validateOrThrow(
                mockRequestContext, DeleteSamplingConfigRequest.newBuilder().build()));
    when(mockRequestContext.getTenantId()).thenReturn(Optional.of(TEST_TENANT_ID));

    assertInvalidArgStatusContaining(
        "DeleteSamplingConfigRequest.id",
        () ->
            validator.validateOrThrow(
                mockRequestContext, DeleteSamplingConfigRequest.newBuilder().setId("").build()));

    assertDoesNotThrow(
        () ->
            validator.validateOrThrow(
                mockRequestContext,
                DeleteSamplingConfigRequest.newBuilder().setId("sampling-config-id").build()));
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

  private RateLimitConfig buildTestRateLimitConfig() {
    return RateLimitConfig.newBuilder()
        .setTraceLimitGlobal(
            RateLimit.newBuilder()
                .setFixedWindowLimit(
                    WindowedRateLimit.newBuilder()
                        .setQuantityAllowed(100)
                        .setWindowDuration(Duration.newBuilder().setSeconds(60).build())
                        .build())
                .build())
        .setTraceLimitPerEndpoint(
            RateLimit.newBuilder()
                .setFixedWindowLimit(
                    WindowedRateLimit.newBuilder()
                        .setQuantityAllowed(100)
                        .setWindowDuration(Duration.newBuilder().setSeconds(60).build())
                        .build())
                .build())
        .setApiEndpointCacheDuration(Duration.newBuilder().setSeconds(100).setNanos(100).build())
        .build();
  }
}
