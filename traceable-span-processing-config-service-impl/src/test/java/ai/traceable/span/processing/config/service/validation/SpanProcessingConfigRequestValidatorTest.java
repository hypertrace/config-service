package ai.traceable.span.processing.config.service.validation;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.when;

import ai.traceable.span.processing.config.service.v1.ApiNamingRuleConfig;
import ai.traceable.span.processing.config.service.v1.ApiNamingRuleInfo;
import ai.traceable.span.processing.config.service.v1.ApiSpecBasedConfig;
import ai.traceable.span.processing.config.service.v1.AstScanBasedConfig;
import ai.traceable.span.processing.config.service.v1.CreateApiNamingRuleRequest;
import ai.traceable.span.processing.config.service.v1.CreateApiNamingRulesRequest;
import ai.traceable.span.processing.config.service.v1.CreateProtectionSpanRuleRequest;
import ai.traceable.span.processing.config.service.v1.CreateSamplingConfigRequest;
import ai.traceable.span.processing.config.service.v1.DefaultProtectionSpanRuleEvaluationStatus;
import ai.traceable.span.processing.config.service.v1.DeleteApiNamingRuleRequest;
import ai.traceable.span.processing.config.service.v1.DeleteApiNamingRulesRequest;
import ai.traceable.span.processing.config.service.v1.DeleteProtectionSpanRuleRequest;
import ai.traceable.span.processing.config.service.v1.DeleteSamplingConfigRequest;
import ai.traceable.span.processing.config.service.v1.Field;
import ai.traceable.span.processing.config.service.v1.GetAllApiNamingRulesRequest;
import ai.traceable.span.processing.config.service.v1.GetAllProtectionSpanRulesRequest;
import ai.traceable.span.processing.config.service.v1.GetAllResolvedProtectionSpanRulesRequest;
import ai.traceable.span.processing.config.service.v1.GetAllResolvedSamplingConfigsRequest;
import ai.traceable.span.processing.config.service.v1.GetAllSamplingConfigsRequest;
import ai.traceable.span.processing.config.service.v1.GetDefaultProtectionSpanRuleEvaluationStatusRequest;
import ai.traceable.span.processing.config.service.v1.ProtectionSpanRuleInfo;
import ai.traceable.span.processing.config.service.v1.RateLimit;
import ai.traceable.span.processing.config.service.v1.RateLimitConfig;
import ai.traceable.span.processing.config.service.v1.RelationalOperator;
import ai.traceable.span.processing.config.service.v1.RelationalSpanFilterExpression;
import ai.traceable.span.processing.config.service.v1.SamplingConfigInfo;
import ai.traceable.span.processing.config.service.v1.SegmentMatchingBasedConfig;
import ai.traceable.span.processing.config.service.v1.SpanFilter;
import ai.traceable.span.processing.config.service.v1.SpanFilterValue;
import ai.traceable.span.processing.config.service.v1.UpdateApiNamingRule;
import ai.traceable.span.processing.config.service.v1.UpdateApiNamingRuleRequest;
import ai.traceable.span.processing.config.service.v1.UpdateApiNamingRulesRequest;
import ai.traceable.span.processing.config.service.v1.UpdateDefaultProtectionSpanRuleEvaluationStatusRequest;
import ai.traceable.span.processing.config.service.v1.UpdateProtectionSpanRule;
import ai.traceable.span.processing.config.service.v1.UpdateProtectionSpanRuleRequest;
import ai.traceable.span.processing.config.service.v1.UpdateSamplingConfig;
import ai.traceable.span.processing.config.service.v1.UpdateSamplingConfigRequest;
import ai.traceable.span.processing.config.service.v1.WindowedRateLimit;
import com.google.protobuf.Duration;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import java.util.List;
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
  void validatesResolvedProtectionSpanRulesGetRequest() {
    assertInvalidArgStatusContaining(
        "Tenant ID",
        () ->
            validator.validateOrThrow(
                mockRequestContext, GetAllResolvedProtectionSpanRulesRequest.newBuilder().build()));

    when(mockRequestContext.getTenantId()).thenReturn(Optional.of(TEST_TENANT_ID));
    assertDoesNotThrow(
        () ->
            validator.validateOrThrow(
                mockRequestContext, GetAllResolvedProtectionSpanRulesRequest.newBuilder().build()));
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
  void validatesGetDefaultProtectionSpanRuleEvaluationStatusRequest() {
    assertInvalidArgStatusContaining(
        "Tenant ID",
        () ->
            validator.validateOrThrow(
                mockRequestContext,
                GetDefaultProtectionSpanRuleEvaluationStatusRequest.newBuilder().build()));

    when(mockRequestContext.getTenantId()).thenReturn(Optional.of(TEST_TENANT_ID));
    assertDoesNotThrow(
        () ->
            validator.validateOrThrow(
                mockRequestContext,
                GetDefaultProtectionSpanRuleEvaluationStatusRequest.newBuilder().build()));
  }

  @Test
  void validatesUpdateDefaultProtectionSpanRuleEvaluationStatusRequest() {
    assertInvalidArgStatusContaining(
        "Tenant ID",
        () ->
            validator.validateOrThrow(
                mockRequestContext,
                UpdateDefaultProtectionSpanRuleEvaluationStatusRequest.newBuilder().build()));

    when(mockRequestContext.getTenantId()).thenReturn(Optional.of(TEST_TENANT_ID));
    assertDoesNotThrow(
        () ->
            validator.validateOrThrow(
                mockRequestContext,
                UpdateDefaultProtectionSpanRuleEvaluationStatusRequest.newBuilder()
                    .setStatus(
                        DefaultProtectionSpanRuleEvaluationStatus.newBuilder()
                            .setEnabled(false)
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
  void validatesResolvedSamplingConfigsGetRequest() {
    assertInvalidArgStatusContaining(
        "Tenant ID",
        () ->
            validator.validateOrThrow(
                mockRequestContext, GetAllResolvedSamplingConfigsRequest.newBuilder().build()));

    when(mockRequestContext.getTenantId()).thenReturn(Optional.of(TEST_TENANT_ID));
    assertDoesNotThrow(
        () ->
            validator.validateOrThrow(
                mockRequestContext, GetAllResolvedSamplingConfigsRequest.newBuilder().build()));
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
  void validatesApiNamingRulesDeleteRequest() {
    assertInvalidArgStatusContaining(
        "Tenant ID",
        () ->
            validator.validateOrThrow(
                mockRequestContext, DeleteApiNamingRulesRequest.newBuilder().build()));
    when(mockRequestContext.getTenantId()).thenReturn(Optional.of(TEST_TENANT_ID));

    assertInvalidArgStatusContaining(
        "Invalid id in request",
        () ->
            validator.validateOrThrow(
                mockRequestContext,
                DeleteApiNamingRulesRequest.newBuilder().addAllIds(List.of("", "id")).build()));

    assertDoesNotThrow(
        () ->
            validator.validateOrThrow(
                mockRequestContext,
                DeleteApiNamingRulesRequest.newBuilder().addAllIds(List.of("rule-id")).build()));
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
        "Invalid regexes",
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
                                            .addAllRegexes(List.of("regex", "[^]+$"))
                                            .addAllValues(List.of("value1", "value2"))
                                            .build())
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
  void validatesApiNamingRulesCreateRequest() {
    assertInvalidArgStatusContaining(
        "Tenant ID",
        () ->
            validator.validateOrThrow(
                mockRequestContext, CreateApiNamingRulesRequest.newBuilder().build()));
    when(mockRequestContext.getTenantId()).thenReturn(Optional.of(TEST_TENANT_ID));

    assertInvalidArgStatusContaining(
        "ApiNamingRuleInfo.name",
        () ->
            validator.validateOrThrow(
                mockRequestContext,
                CreateApiNamingRulesRequest.newBuilder()
                    .addAllRulesInfo(List.of(ApiNamingRuleInfo.newBuilder().build()))
                    .build()));

    assertInvalidArgStatusContaining(
        "Invalid regexes",
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
                                    .setApiSpecBasedConfig(
                                        ApiSpecBasedConfig.newBuilder()
                                            .setApiSpecId("id")
                                            .addAllRegexes(List.of("regex", "[^]+$"))
                                            .addAllValues(List.of("value1", "value2"))
                                            .build())
                                    .build())
                            .build())
                    .build()));

    assertInvalidArgStatusContaining(
        "Invalid regexes",
        () ->
            validator.validateOrThrow(
                mockRequestContext,
                CreateApiNamingRuleRequest.newBuilder()
                    .setRuleInfo(
                        ApiNamingRuleInfo.newBuilder()
                            .setName("name")
                            .setFilter(buildTestFilter())
                            .setRuleConfig(
                                buildAstScanBasedConfig(
                                    "scanId",
                                    "apiSpecId",
                                    List.of("regex", "[^]+$"),
                                    List.of("value1", "value2")))
                            .build())
                    .build()));

    assertInvalidArgStatusContaining(
        "Invalid scanId",
        () ->
            validator.validateOrThrow(
                mockRequestContext,
                CreateApiNamingRuleRequest.newBuilder()
                    .setRuleInfo(
                        ApiNamingRuleInfo.newBuilder()
                            .setName("name")
                            .setFilter(buildTestFilter())
                            .setRuleConfig(
                                buildAstScanBasedConfig(
                                    "",
                                    "apiSpecId",
                                    List.of("regex", "[^]+$"),
                                    List.of("value1", "value2")))
                            .build())
                    .build()));

    assertInvalidArgStatusContaining(
        "Invalid specId",
        () ->
            validator.validateOrThrow(
                mockRequestContext,
                CreateApiNamingRuleRequest.newBuilder()
                    .setRuleInfo(
                        ApiNamingRuleInfo.newBuilder()
                            .setName("name")
                            .setFilter(buildTestFilter())
                            .setRuleConfig(
                                buildAstScanBasedConfig(
                                    "scanId",
                                    "",
                                    List.of("regex", "[^]+$"),
                                    List.of("value1", "value2")))
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
                                    .setApiSpecBasedConfig(
                                        ApiSpecBasedConfig.newBuilder()
                                            .setApiSpecId("id")
                                            .addAllRegexes(List.of("regex", ""))
                                            .addAllValues(List.of("value1", "value2"))
                                            .build())
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
                                buildAstScanBasedConfig(
                                    "scanId",
                                    "apiSpecId",
                                    List.of("regex", ""),
                                    List.of("value1", "value2")))
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
                                    .setApiSpecBasedConfig(
                                        ApiSpecBasedConfig.newBuilder()
                                            .setApiSpecId("id")
                                            .addAllRegexes(List.of("regex"))
                                            .addAllValues(List.of("value"))
                                            .build())
                                    .build())
                            .setFilter(buildTestFilter())
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
                                buildAstScanBasedConfig(
                                    "scanId", "apiSpecId", List.of("regex"), List.of("value")))
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

    assertInvalidArgStatusContaining(
        "Invalid regexes",
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
                                            .addRegexes("[^]+")
                                            .addValues("*")
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
  void validatesApiNamingRulesUpdateRequest() {
    assertInvalidArgStatusContaining(
        "Tenant ID",
        () ->
            validator.validateOrThrow(
                mockRequestContext, UpdateApiNamingRulesRequest.newBuilder().build()));
    when(mockRequestContext.getTenantId()).thenReturn(Optional.of(TEST_TENANT_ID));

    assertInvalidArgStatusContaining(
        "UpdateApiNamingRule.id",
        () ->
            validator.validateOrThrow(
                mockRequestContext,
                UpdateApiNamingRulesRequest.newBuilder()
                    .addRules(UpdateApiNamingRule.newBuilder().setName("name").build())
                    .build()));

    assertInvalidArgStatusContaining(
        "UpdateApiNamingRule.name",
        () ->
            validator.validateOrThrow(
                mockRequestContext,
                UpdateApiNamingRulesRequest.newBuilder()
                    .addRules(UpdateApiNamingRule.newBuilder().setId("id").build())
                    .build()));

    assertInvalidArgStatusContaining(
        "Invalid regex count or segment matching count",
        () ->
            validator.validateOrThrow(
                mockRequestContext,
                UpdateApiNamingRulesRequest.newBuilder()
                    .addRules(
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
                UpdateApiNamingRulesRequest.newBuilder()
                    .addRules(
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

    assertInvalidArgStatusContaining(
        "Invalid regexes",
        () ->
            validator.validateOrThrow(
                mockRequestContext,
                UpdateApiNamingRulesRequest.newBuilder()
                    .addRules(
                        UpdateApiNamingRule.newBuilder()
                            .setId("id")
                            .setName("name")
                            .setFilter(buildTestFilter())
                            .setRuleConfig(
                                ApiNamingRuleConfig.newBuilder()
                                    .setSegmentMatchingBasedConfig(
                                        SegmentMatchingBasedConfig.newBuilder()
                                            .addRegexes("[^]+")
                                            .addValues("*")
                                            .build())
                                    .build())
                            .build())
                    .build()));

    assertDoesNotThrow(
        () ->
            validator.validateOrThrow(
                mockRequestContext,
                UpdateApiNamingRulesRequest.newBuilder()
                    .addRules(
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

  private ApiNamingRuleConfig buildAstScanBasedConfig(
      String scanId, String apiSpecId, List<String> regexes, List<String> values) {
    return ApiNamingRuleConfig.newBuilder()
        .setAstScanBasedConfig(
            AstScanBasedConfig.newBuilder()
                .setScanId(scanId)
                .setApiSpecId(apiSpecId)
                .addAllRegexes(regexes)
                .addAllValues(values)
                .build())
        .build();
  }
}
