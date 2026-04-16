package ai.traceable.fraud.policy.config.service.validation;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

import ai.traceable.edge.decision.config.service.v1.EdgeDecisionType;
import ai.traceable.fraud.datamodel.entity.derivation.config.service.v1.EntityDerivationConfig;
import ai.traceable.fraud.datamodel.entity.derivation.config.service.v1.EntityDerivationConfigData;
import ai.traceable.fraud.datamodel.entity.derivation.config.service.v1.EntityDerivationConfigServiceGrpc.EntityDerivationConfigServiceBlockingStub;
import ai.traceable.fraud.datamodel.entity.derivation.config.service.v1.GetEntityDerivationConfigsResponse;
import ai.traceable.fraud.datamodel.event.kind.v1.AggregationFunctionType;
import ai.traceable.fraud.datamodel.event.kind.v1.ComplexDataModelEventKind;
import ai.traceable.fraud.datamodel.event.kind.v1.FraudDataModelEventKindRegistry;
import ai.traceable.fraud.datamodel.event.kind.v1.OperatorType;
import ai.traceable.fraud.policy.config.service.v1.AbuseActionConfig;
import ai.traceable.fraud.policy.config.service.v1.AbuseActionType;
import ai.traceable.fraud.policy.config.service.v1.AbuseAggregationConfig;
import ai.traceable.fraud.policy.config.service.v1.AbuseApiIds;
import ai.traceable.fraud.policy.config.service.v1.AbuseApiScope;
import ai.traceable.fraud.policy.config.service.v1.AbuseEnvironmentScope;
import ai.traceable.fraud.policy.config.service.v1.AbuseEvaluationSchedule;
import ai.traceable.fraud.policy.config.service.v1.AbuseGroupByConfig;
import ai.traceable.fraud.policy.config.service.v1.AbusePolicyData;
import ai.traceable.fraud.policy.config.service.v1.AbusePolicyDetectionFilter;
import ai.traceable.fraud.policy.config.service.v1.AbusePolicyLiteralValues;
import ai.traceable.fraud.policy.config.service.v1.AbusePolicyRelationalFilter;
import ai.traceable.fraud.policy.config.service.v1.AbusePolicyScope;
import ai.traceable.fraud.policy.config.service.v1.AbusePredefinedBrowserBypassPolicy;
import ai.traceable.fraud.policy.config.service.v1.AbusePredefinedDistributedAttackPolicy;
import ai.traceable.fraud.policy.config.service.v1.AbusePredefinedTemplateConfig;
import ai.traceable.fraud.policy.config.service.v1.AbusePredefinedTemplateType;
import ai.traceable.fraud.policy.config.service.v1.AbuseRiskSeverity;
import ai.traceable.fraud.policy.config.service.v1.AbuseSimpleAggregationTemplateConfig;
import ai.traceable.fraud.policy.config.service.v1.AbuseThresholdConfig;
import ai.traceable.fraud.policy.config.service.v1.AbuseThresholdOperator;
import ai.traceable.fraud.policy.config.service.v1.AbuseTimeWindow;
import ai.traceable.fraud.policy.config.service.v1.BrowserBypassPolicyGenerationConfig;
import ai.traceable.fraud.policy.config.service.v1.BrowserBypassTrainingConfig;
import ai.traceable.fraud.policy.config.service.v1.BrowserBypassTriageConfig;
import ai.traceable.fraud.policy.config.service.v1.CreateAbusePolicyRequest;
import ai.traceable.fraud.policy.config.service.v1.DistributedAttackThresholds;
import ai.traceable.fraud.policy.config.service.v1.UpdateAbusePolicyRequest;
import com.google.protobuf.Duration;
import io.grpc.StatusRuntimeException;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class AbusePolicyConfigRequestValidatorTest {

  @Mock private FraudDataModelEventKindRegistry fraudDataModelEventKindRegistry;
  @Mock private EntityDerivationConfigServiceBlockingStub entityDerivationConfigServiceStub;

  private AbusePolicyConfigRequestValidator validator;
  private RequestContext requestContext;

  @BeforeEach
  void setUp() {
    validator =
        new AbusePolicyConfigRequestValidator(
            fraudDataModelEventKindRegistry, entityDerivationConfigServiceStub);
    requestContext = RequestContext.forTenantId("test-tenant");

    ComplexDataModelEventKind stringKind =
        ComplexDataModelEventKind.newBuilder().setKindId("system_event_kind_string").build();
    GetEntityDerivationConfigsResponse response =
        GetEntityDerivationConfigsResponse.newBuilder()
            .addEntityDerivationConfigs(
                EntityDerivationConfig.newBuilder()
                    .setId("test-entity")
                    .setData(EntityDerivationConfigData.newBuilder().setEventKind(stringKind)))
            .build();
    when(entityDerivationConfigServiceStub.getEntityDerivationConfigs(any())).thenReturn(response);
    when(fraudDataModelEventKindRegistry.isOperatorCompatibleWithKind(any(), any()))
        .thenReturn(true);
    when(fraudDataModelEventKindRegistry.isAggregationFunctionCompatibleWithKind(any(), any()))
        .thenReturn(true);
    when(fraudDataModelEventKindRegistry.isLiteralValueCompatible(any(), any())).thenReturn(true);
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
        "INVALID_ARGUMENT: environment_scope is required (empty list means all environments)",
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

    StatusRuntimeException exception =
        assertThrows(
            StatusRuntimeException.class,
            () -> validator.validateCreateRequest(request, requestContext));
    assertEquals(
        "INVALID_ARGUMENT: api_scope is required (empty api_ids/api_labels means all APIs)",
        exception.getMessage());
  }

  @Test
  void testValidatePolicyData_AllEnvironmentsAndAllApis() {
    AbusePolicyData data =
        createValidPolicyData().toBuilder()
            .setScope(
                AbusePolicyScope.newBuilder()
                    .setEnvironmentScope(AbuseEnvironmentScope.newBuilder().build())
                    .setApiScope(
                        AbuseApiScope.newBuilder()
                            .setApiIds(AbuseApiIds.newBuilder().build())
                            .build())
                    .build())
            .build();
    CreateAbusePolicyRequest request = CreateAbusePolicyRequest.newBuilder().setData(data).build();

    // Empty environment_ids and empty api_ids means all environments and all APIs - allowed
    assertDoesNotThrow(() -> validator.validateCreateRequest(request, requestContext));
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
        "INVALID_ARGUMENT: One of simple_aggregation_template, predefined_template,"
            + " abuse_predefined_browser_bypass_policy, or"
            + " abuse_predefined_distributed_attack_policy must be specified",
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
                    .setAggregationFunction(
                        AggregationFunctionType.AGGREGATION_FUNCTION_TYPE_UNSPECIFIED)
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
  void testValidatePredefinedTemplate_ValidBrowserBypassPolicy() {
    AbusePolicyData data = createValidPolicyDataWithPredefinedBrowserBypass();
    CreateAbusePolicyRequest request = CreateAbusePolicyRequest.newBuilder().setData(data).build();

    assertDoesNotThrow(() -> validator.validateCreateRequest(request, requestContext));
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

  @Test
  void testValidateFilter_RelationalFilter_MissingDerivedEntityId() {
    AbuseSimpleAggregationTemplateConfig template =
        createValidSimpleAggregationTemplate().toBuilder()
            .addFilters(
                AbusePolicyDetectionFilter.newBuilder()
                    .setRelationalFilter(
                        AbusePolicyRelationalFilter.newBuilder()
                            .setOperator(OperatorType.OPERATOR_TYPE_STRING_EQUALS)
                            .build())
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
        "INVALID_ARGUMENT: Relational filter derived entity ID is required",
        exception.getMessage());
  }

  @Test
  void testValidateFilter_RelationalFilter_MissingOperatorId() {
    AbuseSimpleAggregationTemplateConfig template =
        createValidSimpleAggregationTemplate().toBuilder()
            .addFilters(
                AbusePolicyDetectionFilter.newBuilder()
                    .setRelationalFilter(
                        AbusePolicyRelationalFilter.newBuilder()
                            .setDerivedEntityId("some_entity")
                            .build())
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
        "INVALID_ARGUMENT: Relational filter operator is required", exception.getMessage());
  }

  @Test
  void testValidateFilter_ValidRelationalFilter() {
    AbuseSimpleAggregationTemplateConfig template =
        createValidSimpleAggregationTemplate().toBuilder()
            .addFilters(
                AbusePolicyDetectionFilter.newBuilder()
                    .setRelationalFilter(
                        AbusePolicyRelationalFilter.newBuilder()
                            .setDerivedEntityId("some_entity")
                            .setOperator(OperatorType.OPERATOR_TYPE_STRING_EQUALS)
                            .setLiteralValues(
                                AbusePolicyLiteralValues.newBuilder()
                                    .addValues(
                                        com.google.protobuf.Value.newBuilder()
                                            .setStringValue("test")
                                            .build())
                                    .build())
                            .build())
                    .build())
            .build();
    AbusePolicyData data =
        createValidPolicyData().toBuilder().setSimpleAggregationTemplate(template).build();
    CreateAbusePolicyRequest request = CreateAbusePolicyRequest.newBuilder().setData(data).build();

    assertDoesNotThrow(() -> validator.validateCreateRequest(request, requestContext));
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

  private AbusePolicyData createValidPolicyDataWithPredefinedBrowserBypass() {
    return createValidPolicyData().toBuilder()
        .clearSimpleAggregationTemplate()
        .setAbusePredefinedBrowserBypassPolicy(
            AbusePredefinedBrowserBypassPolicy.newBuilder()
                .setTrainingConfig(
                    BrowserBypassTrainingConfig.newBuilder()
                        .addCorrelationKey("session_id")
                        .setWindowSize(Duration.newBuilder().setSeconds(300).build())
                        .build())
                .setPolicyGenerationConfig(
                    BrowserBypassPolicyGenerationConfig.newBuilder()
                        .setMinOccurrenceRateForCorrelatedApis(0.1d)
                        .build())
                .setTriageConfig(
                    BrowserBypassTriageConfig.newBuilder()
                        .setAction(EdgeDecisionType.EDGE_DECISION_TYPE_BLOCK)
                        .build())
                .build())
        .build();
  }

  @Test
  void testValidateDistributedAttackPolicy_Valid() {
    AbusePolicyData data = createValidPolicyDataWithDistributedAttack();
    CreateAbusePolicyRequest request = CreateAbusePolicyRequest.newBuilder().setData(data).build();

    assertDoesNotThrow(() -> validator.validateCreateRequest(request, requestContext));
  }

  @Test
  void testValidateDistributedAttackPolicy_UnspecifiedAction() {
    AbusePolicyData data =
        createValidPolicyData().toBuilder()
            .clearSimpleAggregationTemplate()
            .setAbusePredefinedDistributedAttackPolicy(
                AbusePredefinedDistributedAttackPolicy.newBuilder()
                    .setThresholds(createValidDistributedAttackThresholds())
                    .build())
            .build();
    CreateAbusePolicyRequest request = CreateAbusePolicyRequest.newBuilder().setData(data).build();

    StatusRuntimeException exception =
        assertThrows(
            StatusRuntimeException.class,
            () -> validator.validateCreateRequest(request, requestContext));
    assertEquals(
        "INVALID_ARGUMENT: Distributed attack policy action must be specified",
        exception.getMessage());
  }

  @Test
  void testValidateDistributedAttackPolicy_MissingThresholds() {
    AbusePolicyData data =
        createValidPolicyData().toBuilder()
            .clearSimpleAggregationTemplate()
            .setAbusePredefinedDistributedAttackPolicy(
                AbusePredefinedDistributedAttackPolicy.newBuilder()
                    .setAction(EdgeDecisionType.EDGE_DECISION_TYPE_ALERT)
                    .build())
            .build();
    CreateAbusePolicyRequest request = CreateAbusePolicyRequest.newBuilder().setData(data).build();

    StatusRuntimeException exception =
        assertThrows(
            StatusRuntimeException.class,
            () -> validator.validateCreateRequest(request, requestContext));
    assertEquals(
        "INVALID_ARGUMENT: Distributed attack policy requires thresholds", exception.getMessage());
  }

  @Test
  void testValidateDistributedAttackPolicy_ZeroThreshold() {
    AbusePolicyData data =
        createValidPolicyData().toBuilder()
            .clearSimpleAggregationTemplate()
            .setAbusePredefinedDistributedAttackPolicy(
                AbusePredefinedDistributedAttackPolicy.newBuilder()
                    .setAction(EdgeDecisionType.EDGE_DECISION_TYPE_ALERT)
                    .setThresholds(
                        createValidDistributedAttackThresholds().toBuilder()
                            .setDailyBaselineThreshold(0)
                            .build())
                    .build())
            .build();
    CreateAbusePolicyRequest request = CreateAbusePolicyRequest.newBuilder().setData(data).build();

    StatusRuntimeException exception =
        assertThrows(
            StatusRuntimeException.class,
            () -> validator.validateCreateRequest(request, requestContext));
    assertEquals(
        "INVALID_ARGUMENT: thresholds.daily_baseline_threshold must be a positive finite number",
        exception.getMessage());
  }

  @Test
  void testValidateDistributedAttackPolicy_NaNThreshold() {
    AbusePolicyData data =
        createValidPolicyData().toBuilder()
            .clearSimpleAggregationTemplate()
            .setAbusePredefinedDistributedAttackPolicy(
                AbusePredefinedDistributedAttackPolicy.newBuilder()
                    .setAction(EdgeDecisionType.EDGE_DECISION_TYPE_ALERT)
                    .setThresholds(
                        createValidDistributedAttackThresholds().toBuilder()
                            .setWeeklyBaselineDeviationThreshold(Double.NaN)
                            .build())
                    .build())
            .build();
    CreateAbusePolicyRequest request = CreateAbusePolicyRequest.newBuilder().setData(data).build();

    StatusRuntimeException exception =
        assertThrows(
            StatusRuntimeException.class,
            () -> validator.validateCreateRequest(request, requestContext));
    assertEquals(
        "INVALID_ARGUMENT: thresholds.weekly_baseline_deviation_threshold must be a positive finite number",
        exception.getMessage());
  }

  @Test
  void testValidateDistributedAttackPolicy_InfinityThreshold() {
    AbusePolicyData data =
        createValidPolicyData().toBuilder()
            .clearSimpleAggregationTemplate()
            .setAbusePredefinedDistributedAttackPolicy(
                AbusePredefinedDistributedAttackPolicy.newBuilder()
                    .setAction(EdgeDecisionType.EDGE_DECISION_TYPE_ALERT)
                    .setThresholds(
                        createValidDistributedAttackThresholds().toBuilder()
                            .setIpCountDeviationThreshold(Double.POSITIVE_INFINITY)
                            .build())
                    .build())
            .build();
    CreateAbusePolicyRequest request = CreateAbusePolicyRequest.newBuilder().setData(data).build();

    StatusRuntimeException exception =
        assertThrows(
            StatusRuntimeException.class,
            () -> validator.validateCreateRequest(request, requestContext));
    assertEquals(
        "INVALID_ARGUMENT: thresholds.ip_count_deviation_threshold must be a positive finite number",
        exception.getMessage());
  }

  private AbusePolicyData createValidPolicyDataWithDistributedAttack() {
    return createValidPolicyData().toBuilder()
        .clearSimpleAggregationTemplate()
        .setAbusePredefinedDistributedAttackPolicy(
            AbusePredefinedDistributedAttackPolicy.newBuilder()
                .setAction(EdgeDecisionType.EDGE_DECISION_TYPE_ALERT)
                .setThresholds(createValidDistributedAttackThresholds())
                .build())
        .build();
  }

  private DistributedAttackThresholds createValidDistributedAttackThresholds() {
    return DistributedAttackThresholds.newBuilder()
        .setDailyBaselineThreshold(2.0)
        .setWeeklyBaselineDeviationThreshold(2.0)
        .setDailySeasonalityBaselineDeviationThreshold(2.0)
        .setIpCountDeviationThreshold(3.0)
        .setRequestDensityPerIpDeviationThreshold(1.5)
        .setRequestDensityPerUaDeviationThreshold(2.0)
        .setAnomalyScoreThreshold(70)
        .build();
  }

  private AbuseSimpleAggregationTemplateConfig createValidSimpleAggregationTemplate() {
    return AbuseSimpleAggregationTemplateConfig.newBuilder()
        .setAggregation(
            AbuseAggregationConfig.newBuilder()
                .setAggregationFunction(AggregationFunctionType.AGGREGATION_FUNCTION_TYPE_COUNT)
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
