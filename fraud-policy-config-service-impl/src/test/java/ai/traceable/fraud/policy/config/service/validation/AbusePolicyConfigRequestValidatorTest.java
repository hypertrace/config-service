package ai.traceable.fraud.policy.config.service.validation;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import ai.traceable.fraud.policy.config.service.v1.AbuseActionConfig;
import ai.traceable.fraud.policy.config.service.v1.AbuseActionType;
import ai.traceable.fraud.policy.config.service.v1.AbuseAggregationConfig;
import ai.traceable.fraud.policy.config.service.v1.AbuseAggregationFunction;
import ai.traceable.fraud.policy.config.service.v1.AbuseApiIds;
import ai.traceable.fraud.policy.config.service.v1.AbuseApiLabels;
import ai.traceable.fraud.policy.config.service.v1.AbuseApiScope;
import ai.traceable.fraud.policy.config.service.v1.AbuseEnvironmentScope;
import ai.traceable.fraud.policy.config.service.v1.AbuseEvaluationSchedule;
import ai.traceable.fraud.policy.config.service.v1.AbuseGroupByConfig;
import ai.traceable.fraud.policy.config.service.v1.AbusePolicyData;
import ai.traceable.fraud.policy.config.service.v1.AbusePolicyScope;
import ai.traceable.fraud.policy.config.service.v1.AbusePredefinedTemplateConfig;
import ai.traceable.fraud.policy.config.service.v1.AbusePredefinedTemplateType;
import ai.traceable.fraud.policy.config.service.v1.AbuseRiskSeverity;
import ai.traceable.fraud.policy.config.service.v1.AbuseSimpleAggregationTemplateConfig;
import ai.traceable.fraud.policy.config.service.v1.AbuseThresholdConfig;
import ai.traceable.fraud.policy.config.service.v1.AbuseThresholdOperator;
import ai.traceable.fraud.policy.config.service.v1.AbuseTimeWindow;
import ai.traceable.fraud.policy.config.service.v1.CreateAbusePolicyRequest;
import ai.traceable.fraud.policy.config.service.v1.UpdateAbusePolicyRequest;
import com.google.protobuf.Duration;
import io.grpc.StatusRuntimeException;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class AbusePolicyConfigRequestValidatorTest {

  private AbusePolicyConfigRequestValidator validator;
  private RequestContext requestContext;

  @BeforeEach
  void setUp() {
    validator = new AbusePolicyConfigRequestValidator();
    requestContext = RequestContext.forTenantId("test-tenant");
  }

  @Test
  void testValidateRequestContext_Success() {
    assertDoesNotThrow(() -> validator.validateRequestContext(requestContext));
  }

  @Test
  void testValidateCreateRequest_Success() {
    CreateAbusePolicyRequest request =
        CreateAbusePolicyRequest.newBuilder().setData(createValidPolicyData()).build();

    assertDoesNotThrow(() -> validator.validateCreateRequest(request, requestContext));
  }

  @Test
  void testValidateCreateRequest_MissingData() {
    CreateAbusePolicyRequest request = CreateAbusePolicyRequest.newBuilder().build();

    StatusRuntimeException exception =
        assertThrows(
            StatusRuntimeException.class,
            () -> validator.validateCreateRequest(request, requestContext));
    assertEquals("INVALID_ARGUMENT: Policy data is required", exception.getMessage());
  }

  @Test
  void testValidateUpdateRequest_Success() {
    UpdateAbusePolicyRequest request =
        UpdateAbusePolicyRequest.newBuilder()
            .setPolicyId("policy-123")
            .setData(createValidPolicyData())
            .build();

    assertDoesNotThrow(() -> validator.validateUpdateRequest(request, requestContext));
  }

  @Test
  void testValidateUpdateRequest_MissingPolicyId() {
    UpdateAbusePolicyRequest request =
        UpdateAbusePolicyRequest.newBuilder().setData(createValidPolicyData()).build();

    StatusRuntimeException exception =
        assertThrows(
            StatusRuntimeException.class,
            () -> validator.validateUpdateRequest(request, requestContext));
    assertEquals("INVALID_ARGUMENT: Policy ID is required for update", exception.getMessage());
  }

  @Test
  void testValidatePolicyData_MissingName() {
    AbusePolicyData data = createValidPolicyData().toBuilder().setName("").build();
    CreateAbusePolicyRequest request = CreateAbusePolicyRequest.newBuilder().setData(data).build();

    StatusRuntimeException exception =
        assertThrows(
            StatusRuntimeException.class,
            () -> validator.validateCreateRequest(request, requestContext));
    assertEquals("INVALID_ARGUMENT: Policy name is required", exception.getMessage());
  }

  @Test
  void testValidatePolicyData_MissingScope() {
    AbusePolicyData data = createValidPolicyData().toBuilder().clearScope().build();
    CreateAbusePolicyRequest request = CreateAbusePolicyRequest.newBuilder().setData(data).build();

    StatusRuntimeException exception =
        assertThrows(
            StatusRuntimeException.class,
            () -> validator.validateCreateRequest(request, requestContext));
    assertEquals("INVALID_ARGUMENT: Policy scope is required", exception.getMessage());
  }

  @Test
  void testValidatePolicyData_EmptyScope() {
    AbusePolicyData data =
        createValidPolicyData().toBuilder().setScope(AbusePolicyScope.newBuilder().build()).build();
    CreateAbusePolicyRequest request = CreateAbusePolicyRequest.newBuilder().setData(data).build();

    StatusRuntimeException exception =
        assertThrows(
            StatusRuntimeException.class,
            () -> validator.validateCreateRequest(request, requestContext));
    assertEquals(
        "INVALID_ARGUMENT: At least one scope (environment_scope or api_scope) must be defined",
        exception.getMessage());
  }

  @Test
  void testValidatePolicyData_EnvironmentScopeOnly() {
    AbusePolicyData data =
        createValidPolicyData().toBuilder()
            .setScope(
                AbusePolicyScope.newBuilder()
                    .setEnvironmentScope(
                        AbuseEnvironmentScope.newBuilder().addEnvironmentIds("env1").build())
                    .build())
            .build();
    CreateAbusePolicyRequest request = CreateAbusePolicyRequest.newBuilder().setData(data).build();

    assertDoesNotThrow(() -> validator.validateCreateRequest(request, requestContext));
  }

  @Test
  void testValidatePolicyData_ApiScopeOnly() {
    AbusePolicyData data =
        createValidPolicyData().toBuilder()
            .setScope(
                AbusePolicyScope.newBuilder()
                    .setApiScope(
                        AbuseApiScope.newBuilder()
                            .setApiIds(AbuseApiIds.newBuilder().addIds("api1").build())
                            .build())
                    .build())
            .build();
    CreateAbusePolicyRequest request = CreateAbusePolicyRequest.newBuilder().setData(data).build();

    assertDoesNotThrow(() -> validator.validateCreateRequest(request, requestContext));
  }

  @Test
  void testValidatePolicyData_EmptyApiIdsList() {
    AbusePolicyData data =
        createValidPolicyData().toBuilder()
            .setScope(
                AbusePolicyScope.newBuilder()
                    .setApiScope(
                        AbuseApiScope.newBuilder()
                            .setApiIds(AbuseApiIds.newBuilder().build())
                            .build())
                    .build())
            .build();
    CreateAbusePolicyRequest request = CreateAbusePolicyRequest.newBuilder().setData(data).build();

    StatusRuntimeException exception =
        assertThrows(
            StatusRuntimeException.class,
            () -> validator.validateCreateRequest(request, requestContext));
    assertEquals(
        "INVALID_ARGUMENT: API IDs list cannot be empty when specified", exception.getMessage());
  }

  @Test
  void testValidatePolicyData_EmptyApiLabelsList() {
    AbusePolicyData data =
        createValidPolicyData().toBuilder()
            .setScope(
                AbusePolicyScope.newBuilder()
                    .setApiScope(
                        AbuseApiScope.newBuilder()
                            .setApiLabels(AbuseApiLabels.newBuilder().build())
                            .build())
                    .build())
            .build();
    CreateAbusePolicyRequest request = CreateAbusePolicyRequest.newBuilder().setData(data).build();

    StatusRuntimeException exception =
        assertThrows(
            StatusRuntimeException.class,
            () -> validator.validateCreateRequest(request, requestContext));
    assertEquals(
        "INVALID_ARGUMENT: API labels list cannot be empty when specified", exception.getMessage());
  }

  @Test
  void testValidatePolicyData_UnspecifiedSeverity() {
    AbusePolicyData data =
        createValidPolicyData().toBuilder()
            .setSeverity(AbuseRiskSeverity.ABUSE_RISK_SEVERITY_UNSPECIFIED)
            .build();
    CreateAbusePolicyRequest request = CreateAbusePolicyRequest.newBuilder().setData(data).build();

    StatusRuntimeException exception =
        assertThrows(
            StatusRuntimeException.class,
            () -> validator.validateCreateRequest(request, requestContext));
    assertEquals("INVALID_ARGUMENT: Policy severity must be specified", exception.getMessage());
  }

  @Test
  void testValidatePolicyData_UnspecifiedActionType() {
    AbusePolicyData data =
        createValidPolicyData().toBuilder()
            .setAction(
                AbuseActionConfig.newBuilder()
                    .setActionType(AbuseActionType.ABUSE_ACTION_TYPE_UNSPECIFIED)
                    .build())
            .build();
    CreateAbusePolicyRequest request = CreateAbusePolicyRequest.newBuilder().setData(data).build();

    StatusRuntimeException exception =
        assertThrows(
            StatusRuntimeException.class,
            () -> validator.validateCreateRequest(request, requestContext));
    assertEquals("INVALID_ARGUMENT: Action type must be specified", exception.getMessage());
  }

  @Test
  void testValidatePolicyData_MissingTemplateConfig() {
    AbusePolicyData data =
        createValidPolicyData().toBuilder()
            .clearSimpleAggregationTemplate()
            .clearPredefinedTemplate()
            .build();
    CreateAbusePolicyRequest request = CreateAbusePolicyRequest.newBuilder().setData(data).build();

    StatusRuntimeException exception =
        assertThrows(
            StatusRuntimeException.class,
            () -> validator.validateCreateRequest(request, requestContext));
    assertEquals(
        "INVALID_ARGUMENT: Either simple_aggregation_template or predefined_template must be specified",
        exception.getMessage());
  }

  @Test
  void testValidatePolicyData_MissingMessageFormat() {
    AbusePolicyData data = createValidPolicyData().toBuilder().setMessageFormat("").build();
    CreateAbusePolicyRequest request = CreateAbusePolicyRequest.newBuilder().setData(data).build();

    StatusRuntimeException exception =
        assertThrows(
            StatusRuntimeException.class,
            () -> validator.validateCreateRequest(request, requestContext));
    assertEquals("INVALID_ARGUMENT: Message format is required", exception.getMessage());
  }

  @Test
  void testValidateSimpleAggregationTemplate_UnspecifiedFunction() {
    AbuseSimpleAggregationTemplateConfig template =
        createValidSimpleAggregationTemplate().toBuilder()
            .setAggregation(
                AbuseAggregationConfig.newBuilder()
                    .setFunction(AbuseAggregationFunction.ABUSE_AGGREGATION_FUNCTION_UNSPECIFIED)
                    .setDerivedEntityId("entity")
                    .build())
            .build();
    AbusePolicyData data =
        createValidPolicyData().toBuilder().setSimpleAggregationTemplate(template).build();
    CreateAbusePolicyRequest request = CreateAbusePolicyRequest.newBuilder().setData(data).build();

    StatusRuntimeException exception =
        assertThrows(
            StatusRuntimeException.class,
            () -> validator.validateCreateRequest(request, requestContext));
    assertEquals(
        "INVALID_ARGUMENT: Aggregation function must be specified", exception.getMessage());
  }

  @Test
  void testValidateSimpleAggregationTemplate_MissingThreshold() {
    AbuseSimpleAggregationTemplateConfig template =
        createValidSimpleAggregationTemplate().toBuilder().clearThreshold().build();
    AbusePolicyData data =
        createValidPolicyData().toBuilder().setSimpleAggregationTemplate(template).build();
    CreateAbusePolicyRequest request = CreateAbusePolicyRequest.newBuilder().setData(data).build();

    StatusRuntimeException exception =
        assertThrows(
            StatusRuntimeException.class,
            () -> validator.validateCreateRequest(request, requestContext));
    assertEquals("INVALID_ARGUMENT: Threshold configuration is required", exception.getMessage());
  }

  @Test
  void testValidateSimpleAggregationTemplate_MissingTimeWindow() {
    AbuseSimpleAggregationTemplateConfig template =
        createValidSimpleAggregationTemplate().toBuilder().clearTimeWindow().build();
    AbusePolicyData data =
        createValidPolicyData().toBuilder().setSimpleAggregationTemplate(template).build();
    CreateAbusePolicyRequest request = CreateAbusePolicyRequest.newBuilder().setData(data).build();

    StatusRuntimeException exception =
        assertThrows(
            StatusRuntimeException.class,
            () -> validator.validateCreateRequest(request, requestContext));
    assertEquals("INVALID_ARGUMENT: Time window configuration is required", exception.getMessage());
  }

  @Test
  void testValidateSimpleAggregationTemplate_InvalidLookbackDuration() {
    AbuseSimpleAggregationTemplateConfig template =
        createValidSimpleAggregationTemplate().toBuilder()
            .setTimeWindow(
                AbuseTimeWindow.newBuilder()
                    .setLookbackDuration(Duration.newBuilder().setSeconds(0).build())
                    .build())
            .build();
    AbusePolicyData data =
        createValidPolicyData().toBuilder().setSimpleAggregationTemplate(template).build();
    CreateAbusePolicyRequest request = CreateAbusePolicyRequest.newBuilder().setData(data).build();

    StatusRuntimeException exception =
        assertThrows(
            StatusRuntimeException.class,
            () -> validator.validateCreateRequest(request, requestContext));
    assertEquals("INVALID_ARGUMENT: Lookback duration must be positive", exception.getMessage());
  }

  @Test
  void testValidatePredefinedTemplate_UnspecifiedType() {
    AbusePredefinedTemplateConfig template =
        AbusePredefinedTemplateConfig.newBuilder()
            .setTemplateType(AbusePredefinedTemplateType.ABUSE_PREDEFINED_TEMPLATE_TYPE_UNSPECIFIED)
            .putParameters(
                "key", com.google.protobuf.Value.newBuilder().setStringValue("value").build())
            .build();
    AbusePolicyData data =
        createValidPolicyData().toBuilder()
            .clearSimpleAggregationTemplate()
            .setPredefinedTemplate(template)
            .build();
    CreateAbusePolicyRequest request = CreateAbusePolicyRequest.newBuilder().setData(data).build();

    StatusRuntimeException exception =
        assertThrows(
            StatusRuntimeException.class,
            () -> validator.validateCreateRequest(request, requestContext));
    assertEquals(
        "INVALID_ARGUMENT: Predefined template type must be specified", exception.getMessage());
  }

  @Test
  void testValidateEvaluationSchedule_MissingScheduleType() {
    AbuseEvaluationSchedule schedule = AbuseEvaluationSchedule.newBuilder().build();
    AbusePolicyData data =
        createValidPolicyData().toBuilder().setEvaluationSchedule(schedule).build();
    CreateAbusePolicyRequest request = CreateAbusePolicyRequest.newBuilder().setData(data).build();

    StatusRuntimeException exception =
        assertThrows(
            StatusRuntimeException.class,
            () -> validator.validateCreateRequest(request, requestContext));
    assertEquals(
        "INVALID_ARGUMENT: Evaluation schedule must specify either cron_expression or interval",
        exception.getMessage());
  }

  @Test
  void testValidateEvaluationSchedule_InvalidCronExpression() {
    AbuseEvaluationSchedule schedule =
        AbuseEvaluationSchedule.newBuilder().setCronExpression("* *").build();
    AbusePolicyData data =
        createValidPolicyData().toBuilder().setEvaluationSchedule(schedule).build();
    CreateAbusePolicyRequest request = CreateAbusePolicyRequest.newBuilder().setData(data).build();

    StatusRuntimeException exception =
        assertThrows(
            StatusRuntimeException.class,
            () -> validator.validateCreateRequest(request, requestContext));
    assertTrue(
        exception.getMessage().contains("INVALID_ARGUMENT: Invalid cron expression"),
        "Expected error message to contain 'Invalid cron expression', but got: "
            + exception.getMessage());
  }

  @Test
  void testValidateEvaluationSchedule_ValidCronExpression() {
    AbuseEvaluationSchedule schedule =
        AbuseEvaluationSchedule.newBuilder().setCronExpression("0 */10 * * * ?").build();
    AbusePolicyData data =
        createValidPolicyData().toBuilder().setEvaluationSchedule(schedule).build();
    CreateAbusePolicyRequest request = CreateAbusePolicyRequest.newBuilder().setData(data).build();

    assertDoesNotThrow(() -> validator.validateCreateRequest(request, requestContext));
  }

  @Test
  void testValidateEvaluationSchedule_ValidInterval() {
    AbuseEvaluationSchedule schedule =
        AbuseEvaluationSchedule.newBuilder()
            .setInterval(Duration.newBuilder().setSeconds(600).build())
            .build();
    AbusePolicyData data =
        createValidPolicyData().toBuilder().setEvaluationSchedule(schedule).build();
    CreateAbusePolicyRequest request = CreateAbusePolicyRequest.newBuilder().setData(data).build();

    assertDoesNotThrow(() -> validator.validateCreateRequest(request, requestContext));
  }

  @Test
  void testValidateEvaluationSchedule_InvalidTimeRange() {
    AbuseEvaluationSchedule schedule =
        AbuseEvaluationSchedule.newBuilder()
            .setInterval(Duration.newBuilder().setSeconds(600).build())
            .setScheduleStartTime(1640086400)
            .setScheduleEndTime(1640000000)
            .build();
    AbusePolicyData data =
        createValidPolicyData().toBuilder().setEvaluationSchedule(schedule).build();
    CreateAbusePolicyRequest request = CreateAbusePolicyRequest.newBuilder().setData(data).build();

    StatusRuntimeException exception =
        assertThrows(
            StatusRuntimeException.class,
            () -> validator.validateCreateRequest(request, requestContext));
    assertEquals(
        "INVALID_ARGUMENT: Schedule end time must be after start time", exception.getMessage());
  }

  private AbusePolicyData createValidPolicyData() {
    return AbusePolicyData.newBuilder()
        .setName("Test Policy")
        .setScope(
            AbusePolicyScope.newBuilder()
                .setEnvironmentScope(
                    AbuseEnvironmentScope.newBuilder().addEnvironmentIds("env1").build())
                .setApiScope(
                    AbuseApiScope.newBuilder()
                        .setApiIds(AbuseApiIds.newBuilder().addIds("api1").build())
                        .build())
                .build())
        .setEnabled(true)
        .setSeverity(AbuseRiskSeverity.ABUSE_RISK_SEVERITY_HIGH)
        .setAction(
            AbuseActionConfig.newBuilder()
                .setActionType(AbuseActionType.ABUSE_ACTION_TYPE_ALERT)
                .build())
        .setSimpleAggregationTemplate(createValidSimpleAggregationTemplate())
        .setMessageFormat("Abuse detected: {message}")
        .build();
  }

  private AbuseSimpleAggregationTemplateConfig createValidSimpleAggregationTemplate() {
    return AbuseSimpleAggregationTemplateConfig.newBuilder()
        .setAggregation(
            AbuseAggregationConfig.newBuilder()
                .setFunction(AbuseAggregationFunction.ABUSE_AGGREGATION_FUNCTION_COUNT)
                .setDerivedEntityId("request_count")
                .build())
        .setGroupBy(AbuseGroupByConfig.newBuilder().setDerivedEntityId("user_id").build())
        .setThreshold(
            AbuseThresholdConfig.newBuilder()
                .setOperator(AbuseThresholdOperator.ABUSE_THRESHOLD_OPERATOR_GREATER_THAN)
                .setValue(100)
                .build())
        .setTimeWindow(
            AbuseTimeWindow.newBuilder()
                .setLookbackDuration(Duration.newBuilder().setSeconds(3600).build())
                .build())
        .build();
  }
}
