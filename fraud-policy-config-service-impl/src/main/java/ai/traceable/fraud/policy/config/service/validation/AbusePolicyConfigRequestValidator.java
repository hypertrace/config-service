package ai.traceable.fraud.policy.config.service.validation;

import static org.hypertrace.config.validation.GrpcValidatorUtils.validateRequestContextOrThrow;

import ai.traceable.edge.decision.config.service.v1.EdgeDecisionType;
import ai.traceable.fraud.datamodel.entity.derivation.config.service.v1.EntityDerivationConfig;
import ai.traceable.fraud.datamodel.entity.derivation.config.service.v1.EntityDerivationConfigServiceGrpc.EntityDerivationConfigServiceBlockingStub;
import ai.traceable.fraud.datamodel.entity.derivation.config.service.v1.EventDerivationConfigDetails;
import ai.traceable.fraud.datamodel.entity.derivation.config.service.v1.ExtractionLocationType;
import ai.traceable.fraud.datamodel.entity.derivation.config.service.v1.GetEntityDerivationConfigsRequest;
import ai.traceable.fraud.datamodel.entity.derivation.config.service.v1.GetEntityDerivationConfigsResponse;
import ai.traceable.fraud.datamodel.event.kind.v1.AggregationFunctionType;
import ai.traceable.fraud.datamodel.event.kind.v1.ComplexDataModelEventKind;
import ai.traceable.fraud.datamodel.event.kind.v1.FraudDataModelEventKindRegistry;
import ai.traceable.fraud.datamodel.event.kind.v1.OperatorType;
import ai.traceable.fraud.policy.config.service.v1.AbusePolicyData;
import ai.traceable.fraud.policy.config.service.v1.AbusePolicyLiteralValues;
import ai.traceable.fraud.policy.config.service.v1.AbusePredefinedBrowserBypassPolicy;
import ai.traceable.fraud.policy.config.service.v1.AbusePredefinedDistributedAttackPolicy;
import ai.traceable.fraud.policy.config.service.v1.AbusePredefinedTemplateConfig;
import ai.traceable.fraud.policy.config.service.v1.AbusePredefinedTemplateType;
import ai.traceable.fraud.policy.config.service.v1.BrowserBypassPolicyGenerationConfig;
import ai.traceable.fraud.policy.config.service.v1.BrowserBypassTrainingConfig;
import ai.traceable.fraud.policy.config.service.v1.BrowserBypassTriageConfig;
import ai.traceable.fraud.policy.config.service.v1.CreateAbusePolicyRequest;
import ai.traceable.fraud.policy.config.service.v1.DistributedAttackThresholds;
import ai.traceable.fraud.policy.config.service.v1.UpdateAbusePolicyRequest;
import com.cronutils.model.CronType;
import com.cronutils.model.definition.CronDefinitionBuilder;
import com.cronutils.parser.CronParser;
import com.google.protobuf.Duration;
import com.google.protobuf.Value;
import io.grpc.Status;
import jakarta.inject.Inject;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.hypertrace.core.grpcutils.context.RequestContext;

public class AbusePolicyConfigRequestValidator {

  private final FraudDataModelEventKindRegistry fraudDataModelEventKindRegistry;
  private final EntityDerivationConfigServiceBlockingStub entityDerivationConfigServiceStub;

  @Inject
  public AbusePolicyConfigRequestValidator(
      FraudDataModelEventKindRegistry fraudDataModelEventKindRegistry,
      EntityDerivationConfigServiceBlockingStub entityDerivationConfigServiceStub) {
    this.fraudDataModelEventKindRegistry = fraudDataModelEventKindRegistry;
    this.entityDerivationConfigServiceStub = entityDerivationConfigServiceStub;
  }

  public void validateRequestContext(RequestContext requestContext) {
    validateRequestContextOrThrow(requestContext);
    if (requestContext.getTenantId().isEmpty()) {
      throw Status.INVALID_ARGUMENT
          .withDescription("Missing expected Tenant ID")
          .asRuntimeException(requestContext.buildTrailers());
    }
  }

  public void validateCreateRequest(
      CreateAbusePolicyRequest request, RequestContext requestContext) {
    validateRequestContext(requestContext);

    if (!request.hasData()) {
      throw Status.INVALID_ARGUMENT
          .withDescription("Policy data is required")
          .asRuntimeException(requestContext.buildTrailers());
    }

    validateAbusePolicyData(request.getData(), requestContext);
  }

  public void validateUpdateRequest(
      UpdateAbusePolicyRequest request, RequestContext requestContext) {
    validateRequestContext(requestContext);

    if (request.getPolicyId().isEmpty()) {
      throw Status.INVALID_ARGUMENT
          .withDescription("Policy ID is required for update")
          .asRuntimeException(requestContext.buildTrailers());
    }

    if (!request.hasData()) {
      throw Status.INVALID_ARGUMENT
          .withDescription("Policy data is required")
          .asRuntimeException(requestContext.buildTrailers());
    }

    validateAbusePolicyData(request.getData(), requestContext);
  }

  private void validateAbusePolicyData(AbusePolicyData data, RequestContext requestContext) {
    validatePolicyName(data, requestContext);
    validatePolicyScope(data, requestContext);
    validatePolicySeverity(data, requestContext);
    validatePolicyAction(data, requestContext);
    validatePolicyTemplate(data, requestContext);
    validatePolicyMessageFormat(data, requestContext);
    validatePolicyEvaluationSchedule(data, requestContext);
    validateBlockActionEntityExtractions(data, requestContext);
  }

  private void validatePolicyName(AbusePolicyData data, RequestContext requestContext) {
    if (data.getName().isEmpty()) {
      throw Status.INVALID_ARGUMENT
          .withDescription("Policy name is required")
          .asRuntimeException(requestContext.buildTrailers());
    }
  }

  private void validatePolicyScope(AbusePolicyData data, RequestContext requestContext) {
    if (!data.hasScope()) {
      throw Status.INVALID_ARGUMENT
          .withDescription("Policy scope is required")
          .asRuntimeException(requestContext.buildTrailers());
    }

    // Both environment_scope and api_scope are mandatory
    if (!data.getScope().hasEnvironmentScope()) {
      throw Status.INVALID_ARGUMENT
          .withDescription("environment_scope is required (empty list means all environments)")
          .asRuntimeException(requestContext.buildTrailers());
    }

    if (!data.getScope().hasApiScope()) {
      throw Status.INVALID_ARGUMENT
          .withDescription("api_scope is required (empty api_ids/api_labels means all APIs)")
          .asRuntimeException(requestContext.buildTrailers());
    }
  }

  private void validatePolicySeverity(AbusePolicyData data, RequestContext requestContext) {
    if (data.getSeverity()
        == ai.traceable.fraud.policy.config.service.v1.AbuseRiskSeverity
            .ABUSE_RISK_SEVERITY_UNSPECIFIED) {
      throw Status.INVALID_ARGUMENT
          .withDescription("Policy severity must be specified")
          .asRuntimeException(requestContext.buildTrailers());
    }
  }

  private void validatePolicyAction(AbusePolicyData data, RequestContext requestContext) {
    if (data.getAction().getActionType()
        == ai.traceable.fraud.policy.config.service.v1.AbuseActionType
            .ABUSE_ACTION_TYPE_UNSPECIFIED) {
      throw Status.INVALID_ARGUMENT
          .withDescription("Action type must be specified")
          .asRuntimeException(requestContext.buildTrailers());
    }
  }

  private void validatePolicyTemplate(AbusePolicyData data, RequestContext requestContext) {
    if (!data.hasSimpleAggregationTemplate()
        && !data.hasPredefinedTemplate()
        && !data.hasAbusePredefinedBrowserBypassPolicy()
        && !data.hasAbusePredefinedDistributedAttackPolicy()) {
      throw Status.INVALID_ARGUMENT
          .withDescription(
              "One of simple_aggregation_template, predefined_template,"
                  + " abuse_predefined_browser_bypass_policy, or"
                  + " abuse_predefined_distributed_attack_policy must be specified")
          .asRuntimeException(requestContext.buildTrailers());
    }

    if (data.hasSimpleAggregationTemplate()) {
      validateSimpleAggregationTemplate(data.getSimpleAggregationTemplate(), requestContext);
    }

    if (data.hasPredefinedTemplate()) {
      validatePredefinedTemplate(data.getPredefinedTemplate(), requestContext);
    }

    if (data.hasAbusePredefinedBrowserBypassPolicy()) {
      validateAbusePredefinedBrowserBypassPolicy(
          data.getAbusePredefinedBrowserBypassPolicy(), requestContext);
    }

    if (data.hasAbusePredefinedDistributedAttackPolicy()) {
      validateAbusePredefinedDistributedAttackPolicy(
          data.getAbusePredefinedDistributedAttackPolicy(), requestContext);
    }
  }

  private void validatePolicyMessageFormat(AbusePolicyData data, RequestContext requestContext) {
    if (data.getMessageFormat().isEmpty()) {
      throw Status.INVALID_ARGUMENT
          .withDescription("Message format is required")
          .asRuntimeException(requestContext.buildTrailers());
    }
  }

  private void validatePolicyEvaluationSchedule(
      AbusePolicyData data, RequestContext requestContext) {
    if (data.hasEvaluationSchedule()) {
      validateEvaluationSchedule(data.getEvaluationSchedule(), requestContext);
    }
  }

  private void validateSimpleAggregationTemplate(
      ai.traceable.fraud.policy.config.service.v1.AbuseSimpleAggregationTemplateConfig template,
      RequestContext requestContext) {

    // Validate aggregation function type
    AggregationFunctionType aggregationFunction =
        template.getAggregation().getAggregationFunction();
    if (aggregationFunction == AggregationFunctionType.AGGREGATION_FUNCTION_TYPE_UNSPECIFIED) {
      throw Status.INVALID_ARGUMENT
          .withDescription("Aggregation function must be specified")
          .asRuntimeException(requestContext.buildTrailers());
    }

    String aggregationEntityId = template.getAggregation().getDerivedEntityId();
    if (aggregationEntityId.isEmpty()) {
      throw Status.INVALID_ARGUMENT
          .withDescription("Derived entity ID is required for aggregation")
          .asRuntimeException(requestContext.buildTrailers());
    }

    // Validate aggregation entity exists and aggregation function is compatible
    ComplexDataModelEventKind aggregationEntityKind =
        validateDerivedEntityExists(aggregationEntityId, requestContext);
    validateAggregationCompatibility(
        aggregationEntityId, aggregationFunction, aggregationEntityKind, requestContext);

    // Validate group by if present
    if (template.hasGroupBy() && template.getGroupBy().getDerivedEntityId().isEmpty()) {
      throw Status.INVALID_ARGUMENT
          .withDescription("Derived entity ID is required for group by")
          .asRuntimeException(requestContext.buildTrailers());
    }

    // Validate group by entity exists if present
    if (template.hasGroupBy() && !template.getGroupBy().getDerivedEntityId().isEmpty()) {
      validateDerivedEntityExists(template.getGroupBy().getDerivedEntityId(), requestContext);
    }

    // Aggregation and group by must not use the same derived entity
    if (template.hasGroupBy()
        && !template.getGroupBy().getDerivedEntityId().isEmpty()
        && !aggregationEntityId.isEmpty()
        && aggregationEntityId.equals(template.getGroupBy().getDerivedEntityId())) {
      throw Status.INVALID_ARGUMENT
          .withDescription("Aggregation and group by must not use the same derived entity ID")
          .asRuntimeException(requestContext.buildTrailers());
    }

    // Validate threshold (required)
    if (!template.hasThreshold()) {
      throw Status.INVALID_ARGUMENT
          .withDescription("Threshold configuration is required")
          .asRuntimeException(requestContext.buildTrailers());
    }

    if (template.getThreshold().getOperator()
        == ai.traceable.fraud.policy.config.service.v1.AbuseThresholdOperator
            .ABUSE_THRESHOLD_OPERATOR_UNSPECIFIED) {
      throw Status.INVALID_ARGUMENT
          .withDescription("Threshold operator must be specified")
          .asRuntimeException(requestContext.buildTrailers());
    }

    if (template.getThreshold().getValue() < 0) {
      throw Status.INVALID_ARGUMENT
          .withDescription("Threshold value must be non-negative")
          .asRuntimeException(requestContext.buildTrailers());
    }

    // Validate time window (required)
    if (!template.hasTimeWindow()) {
      throw Status.INVALID_ARGUMENT
          .withDescription("Time window configuration is required")
          .asRuntimeException(requestContext.buildTrailers());
    }

    if (!template.getTimeWindow().hasLookbackDuration()) {
      throw Status.INVALID_ARGUMENT
          .withDescription("Lookback duration is required in time window")
          .asRuntimeException(requestContext.buildTrailers());
    }

    long lookbackSeconds = template.getTimeWindow().getLookbackDuration().getSeconds();
    if (lookbackSeconds <= 0) {
      throw Status.INVALID_ARGUMENT
          .withDescription("Lookback duration must be positive")
          .asRuntimeException(requestContext.buildTrailers());
    }

    long maxLookbackSeconds = 86400; // 24 hours
    if (lookbackSeconds > maxLookbackSeconds) {
      throw Status.INVALID_ARGUMENT
          .withDescription(
              String.format(
                  "Lookback duration exceeds maximum allowed value of %d seconds (24 hours)",
                  maxLookbackSeconds))
          .asRuntimeException(requestContext.buildTrailers());
    }

    // Validate filters if present
    for (ai.traceable.fraud.policy.config.service.v1.AbusePolicyDetectionFilter filter :
        template.getFiltersList()) {
      validateDetectionFilter(filter, requestContext);
    }
  }

  private void validateDetectionFilter(
      ai.traceable.fraud.policy.config.service.v1.AbusePolicyDetectionFilter filter,
      RequestContext requestContext) {
    if (filter.hasRelationalFilter()) {
      validateRelationalFilter(filter.getRelationalFilter(), requestContext);
    } else if (filter.hasLogicalFilter()) {
      ai.traceable.fraud.policy.config.service.v1.AbusePolicyLogicalFilter logicalFilter =
          filter.getLogicalFilter();
      if (logicalFilter.getOperator()
          == ai.traceable.fraud.policy.config.service.v1.AbusePolicyLogicalOperator
              .ABUSE_POLICY_LOGICAL_OPERATOR_UNSPECIFIED) {
        throw Status.INVALID_ARGUMENT
            .withDescription("Logical filter operator must be specified")
            .asRuntimeException(requestContext.buildTrailers());
      }
      for (ai.traceable.fraud.policy.config.service.v1.AbusePolicyDetectionFilter operand :
          logicalFilter.getOperandsList()) {
        validateDetectionFilter(operand, requestContext);
      }
    } else {
      throw Status.INVALID_ARGUMENT
          .withDescription("Detection filter must have either a relational or logical filter")
          .asRuntimeException(requestContext.buildTrailers());
    }
  }

  private void validateRelationalFilter(
      ai.traceable.fraud.policy.config.service.v1.AbusePolicyRelationalFilter filter,
      RequestContext requestContext) {
    String derivedEntityId = filter.getDerivedEntityId();
    if (derivedEntityId.isEmpty()) {
      throw Status.INVALID_ARGUMENT
          .withDescription("Relational filter derived entity ID is required")
          .asRuntimeException(requestContext.buildTrailers());
    }
    if (filter.getOperator() == OperatorType.OPERATOR_TYPE_UNSPECIFIED) {
      throw Status.INVALID_ARGUMENT
          .withDescription("Relational filter operator is required")
          .asRuntimeException(requestContext.buildTrailers());
    }

    ComplexDataModelEventKind entityKind =
        validateDerivedEntityExists(derivedEntityId, requestContext);

    validateOperatorCompatibility(
        derivedEntityId, filter.getOperator(), entityKind, requestContext);

    if (filter.hasLiteralValues()) {
      validateLiteralValues(derivedEntityId, filter.getLiteralValues(), entityKind, requestContext);
    }
  }

  private ComplexDataModelEventKind validateDerivedEntityExists(
      String derivedEntityId, RequestContext requestContext) {
    GetEntityDerivationConfigsRequest request =
        GetEntityDerivationConfigsRequest.newBuilder().addIds(derivedEntityId).build();

    GetEntityDerivationConfigsResponse response =
        requestContext.call(
            () -> entityDerivationConfigServiceStub.getEntityDerivationConfigs(request));

    List<EntityDerivationConfig> configs = response.getEntityDerivationConfigsList();
    if (configs.isEmpty()) {
      throw Status.INVALID_ARGUMENT
          .withDescription("Derived entity not found: " + derivedEntityId)
          .asRuntimeException(requestContext.buildTrailers());
    }

    return configs.get(0).getData().getEventKind();
  }

  private void validateOperatorCompatibility(
      String derivedEntityId,
      OperatorType operator,
      ComplexDataModelEventKind entityKind,
      RequestContext requestContext) {
    if (!fraudDataModelEventKindRegistry.isOperatorCompatibleWithKind(operator, entityKind)) {
      throw Status.INVALID_ARGUMENT
          .withDescription(
              String.format(
                  "Operator %s not compatible with entity type '%s' (entity: %s)",
                  operator, formatKind(entityKind), derivedEntityId))
          .asRuntimeException(requestContext.buildTrailers());
    }
  }

  private void validateLiteralValues(
      String derivedEntityId,
      AbusePolicyLiteralValues literalValues,
      ComplexDataModelEventKind entityKind,
      RequestContext requestContext) {
    for (Value value : literalValues.getValuesList()) {
      if (!fraudDataModelEventKindRegistry.isLiteralValueCompatible(entityKind, value)) {
        throw Status.INVALID_ARGUMENT
            .withDescription(
                String.format(
                    "Literal value type mismatch for entity '%s': expected %s compatible value",
                    derivedEntityId, formatKind(entityKind)))
            .asRuntimeException(requestContext.buildTrailers());
      }
    }
  }

  private void validateAggregationCompatibility(
      String derivedEntityId,
      AggregationFunctionType functionType,
      ComplexDataModelEventKind entityKind,
      RequestContext requestContext) {
    if (!fraudDataModelEventKindRegistry.isAggregationFunctionCompatibleWithKind(
        functionType, entityKind)) {
      throw Status.INVALID_ARGUMENT
          .withDescription(
              String.format(
                  "Aggregation function %s not compatible with entity type '%s' (entity: %s)",
                  functionType, formatKind(entityKind), derivedEntityId))
          .asRuntimeException(requestContext.buildTrailers());
    }
  }

  private String formatKind(ComplexDataModelEventKind kind) {
    if (kind.hasKindId()) {
      return kind.getKindId();
    }
    if (kind.hasArrayOf()) {
      return "array<" + formatKind(kind.getArrayOf()) + ">";
    }
    return kind.toString();
  }

  /**
   * Validates that BLOCK action policies do not reference entity derivation configs with
   * monitor-only extraction locations (response headers/body/cookies, span attributes), since this
   * data is not available at the edge when the block decision is made.
   */
  private void validateBlockActionEntityExtractions(
      AbusePolicyData data, RequestContext requestContext) {
    if (data.getAction().getActionType()
        != ai.traceable.fraud.policy.config.service.v1.AbuseActionType.ABUSE_ACTION_TYPE_BLOCK) {
      return;
    }

    Set<String> entityIds = collectAllDerivedEntityIds(data);
    if (entityIds.isEmpty()) {
      return;
    }

    GetEntityDerivationConfigsResponse response =
        requestContext.call(
            () ->
                entityDerivationConfigServiceStub.getEntityDerivationConfigs(
                    GetEntityDerivationConfigsRequest.newBuilder().addAllIds(entityIds).build()));

    for (EntityDerivationConfig config : response.getEntityDerivationConfigsList()) {
      if (!config.getData().hasSpanProjection()) {
        continue;
      }
      for (EventDerivationConfigDetails details :
          config.getData().getSpanProjection().getEventDerivationConfigsList()) {
        if (details.hasSpanExtraction()
            && isMonitorOnlyLocationType(
                details.getSpanExtraction().getLocation().getLocationType())) {
          throw Status.INVALID_ARGUMENT
              .withDescription(
                  String.format(
                      "Block action policies cannot use entities with monitor-only extraction"
                          + " (entity: %s, location: %s). This data is not available at the"
                          + " edge for blocking decisions.",
                      config.getId(), details.getSpanExtraction().getLocation().getLocationType()))
              .asRuntimeException(requestContext.buildTrailers());
        }
      }
    }
  }

  private static Set<String> collectAllDerivedEntityIds(AbusePolicyData data) {
    Set<String> ids = new HashSet<>();
    if (data.hasSimpleAggregationTemplate()) {
      ai.traceable.fraud.policy.config.service.v1.AbuseSimpleAggregationTemplateConfig template =
          data.getSimpleAggregationTemplate();
      if (!template.getAggregation().getDerivedEntityId().isEmpty()) {
        ids.add(template.getAggregation().getDerivedEntityId());
      }
      if (template.hasGroupBy() && !template.getGroupBy().getDerivedEntityId().isEmpty()) {
        ids.add(template.getGroupBy().getDerivedEntityId());
      }
      for (ai.traceable.fraud.policy.config.service.v1.AbusePolicyDetectionFilter filter :
          template.getFiltersList()) {
        collectDerivedEntityIdsFromFilter(filter, ids);
      }
    }
    return ids;
  }

  private static void collectDerivedEntityIdsFromFilter(
      ai.traceable.fraud.policy.config.service.v1.AbusePolicyDetectionFilter filter,
      Set<String> ids) {
    if (filter.hasRelationalFilter()) {
      String id = filter.getRelationalFilter().getDerivedEntityId();
      if (!id.isEmpty()) {
        ids.add(id);
      }
    } else if (filter.hasLogicalFilter()) {
      for (ai.traceable.fraud.policy.config.service.v1.AbusePolicyDetectionFilter operand :
          filter.getLogicalFilter().getOperandsList()) {
        collectDerivedEntityIdsFromFilter(operand, ids);
      }
    }
  }

  private static boolean isMonitorOnlyLocationType(ExtractionLocationType locationType) {
    return locationType == ExtractionLocationType.EXTRACTION_LOCATION_TYPE_RESPONSE_HEADER
        || locationType == ExtractionLocationType.EXTRACTION_LOCATION_TYPE_RESPONSE_BODY
        || locationType == ExtractionLocationType.EXTRACTION_LOCATION_TYPE_RESPONSE_COOKIE
        || locationType == ExtractionLocationType.EXTRACTION_LOCATION_TYPE_SPAN_ATTRIBUTE;
  }

  private void validatePredefinedTemplate(
      AbusePredefinedTemplateConfig template, RequestContext requestContext) {
    if (template.getTemplateType()
        == AbusePredefinedTemplateType.ABUSE_PREDEFINED_TEMPLATE_TYPE_UNSPECIFIED) {
      throw Status.INVALID_ARGUMENT
          .withDescription("Predefined template type must be specified")
          .asRuntimeException(requestContext.buildTrailers());
    }

    if (template.getParametersCount() == 0) {
      throw Status.INVALID_ARGUMENT
          .withDescription("Parameters are required for predefined template")
          .asRuntimeException(requestContext.buildTrailers());
    }
  }

  private void validateAbusePredefinedBrowserBypassPolicy(
      AbusePredefinedBrowserBypassPolicy policy, RequestContext requestContext) {
    if (!policy.hasTrainingConfig()) {
      throw Status.INVALID_ARGUMENT
          .withDescription("Browser bypass policy requires training_config")
          .asRuntimeException(requestContext.buildTrailers());
    }
    validateBrowserBypassTrainingConfig(policy.getTrainingConfig(), requestContext);

    if (!policy.hasPolicyGenerationConfig()) {
      throw Status.INVALID_ARGUMENT
          .withDescription("Browser bypass policy requires policy_generation_config")
          .asRuntimeException(requestContext.buildTrailers());
    }
    validateBrowserBypassPolicyGenerationConfig(policy.getPolicyGenerationConfig(), requestContext);

    if (!policy.hasTriageConfig()) {
      throw Status.INVALID_ARGUMENT
          .withDescription("Browser bypass policy requires triage_config")
          .asRuntimeException(requestContext.buildTrailers());
    }
    validateBrowserBypassTriageConfig(policy.getTriageConfig(), requestContext);
  }

  private void validateBrowserBypassTrainingConfig(
      BrowserBypassTrainingConfig config, RequestContext requestContext) {
    if (config.getCorrelationKeyCount() == 0) {
      throw Status.INVALID_ARGUMENT
          .withDescription(
              "training_config requires at least one correlation_key for browser bypass")
          .asRuntimeException(requestContext.buildTrailers());
    }
    boolean hasNonBlankKey =
        config.getCorrelationKeyList().stream().anyMatch(key -> !key.isBlank());
    if (!hasNonBlankKey) {
      throw Status.INVALID_ARGUMENT
          .withDescription("training_config correlation_key entries must not be blank")
          .asRuntimeException(requestContext.buildTrailers());
    }

    if (!config.hasWindowSize()) {
      throw Status.INVALID_ARGUMENT
          .withDescription("training_config requires window_size")
          .asRuntimeException(requestContext.buildTrailers());
    }
    validatePositiveDuration(config.getWindowSize(), "training_config.window_size", requestContext);
  }

  private void validateBrowserBypassPolicyGenerationConfig(
      BrowserBypassPolicyGenerationConfig config, RequestContext requestContext) {
    double rate = config.getMinOccurrenceRateForCorrelatedApis();
    if (Double.isNaN(rate) || rate < 0.0d || rate > 1.0d) {
      throw Status.INVALID_ARGUMENT
          .withDescription(
              "policy_generation_config.min_occurrence_rate_for_correlated_apis must be between 0 and 1")
          .asRuntimeException(requestContext.buildTrailers());
    }
  }

  private void validateBrowserBypassTriageConfig(
      BrowserBypassTriageConfig config, RequestContext requestContext) {
    EdgeDecisionType action = config.getAction();
    if (action == EdgeDecisionType.EDGE_DECISION_TYPE_UNSPECIFIED
        || action == EdgeDecisionType.UNRECOGNIZED) {
      throw Status.INVALID_ARGUMENT
          .withDescription("triage_config.action must be specified")
          .asRuntimeException(requestContext.buildTrailers());
    }
  }

  private void validateAbusePredefinedDistributedAttackPolicy(
      AbusePredefinedDistributedAttackPolicy policy, RequestContext requestContext) {
    EdgeDecisionType action = policy.getAction();
    if (action == EdgeDecisionType.EDGE_DECISION_TYPE_UNSPECIFIED
        || action == EdgeDecisionType.UNRECOGNIZED) {
      throw Status.INVALID_ARGUMENT
          .withDescription("Distributed attack policy action must be specified")
          .asRuntimeException(requestContext.buildTrailers());
    }

    if (!policy.hasThresholds()) {
      throw Status.INVALID_ARGUMENT
          .withDescription("Distributed attack policy requires thresholds")
          .asRuntimeException(requestContext.buildTrailers());
    }

    validateDistributedAttackThresholds(policy.getThresholds(), requestContext);
  }

  private void validateDistributedAttackThresholds(
      DistributedAttackThresholds thresholds, RequestContext requestContext) {
    validatePositiveThreshold(
        thresholds.getDailyBaselineThreshold(),
        "thresholds.daily_baseline_threshold",
        requestContext);
    validatePositiveThreshold(
        thresholds.getWeeklyBaselineDeviationThreshold(),
        "thresholds.weekly_baseline_deviation_threshold",
        requestContext);
    validatePositiveThreshold(
        thresholds.getDailySeasonalityBaselineDeviationThreshold(),
        "thresholds.daily_seasonality_baseline_deviation_threshold",
        requestContext);
    validatePositiveThreshold(
        thresholds.getIpCountDeviationThreshold(),
        "thresholds.ip_count_deviation_threshold",
        requestContext);
    validatePositiveThreshold(
        thresholds.getRequestDensityPerIpDeviationThreshold(),
        "thresholds.request_density_per_ip_deviation_threshold",
        requestContext);
    validatePositiveThreshold(
        thresholds.getRequestDensityPerUaDeviationThreshold(),
        "thresholds.request_density_per_ua_deviation_threshold",
        requestContext);
    validatePositiveThreshold(
        thresholds.getAnomalyScoreThreshold(),
        "thresholds.anomaly_score_threshold",
        requestContext);
  }

  private void validatePositiveThreshold(
      double value, String fieldName, RequestContext requestContext) {
    if (Double.isNaN(value) || Double.isInfinite(value) || value <= 0) {
      throw Status.INVALID_ARGUMENT
          .withDescription(fieldName + " must be a positive finite number")
          .asRuntimeException(requestContext.buildTrailers());
    }
  }

  private void validatePositiveDuration(
      Duration duration, String fieldName, RequestContext requestContext) {
    if (duration.getSeconds() < 0 || duration.getNanos() < 0) {
      throw Status.INVALID_ARGUMENT
          .withDescription(fieldName + " must not be negative")
          .asRuntimeException(requestContext.buildTrailers());
    }
    if (duration.getSeconds() == 0 && duration.getNanos() == 0) {
      throw Status.INVALID_ARGUMENT
          .withDescription(fieldName + " must be positive")
          .asRuntimeException(requestContext.buildTrailers());
    }
  }

  private void validateEvaluationSchedule(
      ai.traceable.fraud.policy.config.service.v1.AbuseEvaluationSchedule schedule,
      RequestContext requestContext) {

    // Validate that at least one schedule type is specified (cron_expression or interval)
    boolean hasCronExpression =
        schedule.hasCronExpression() && !schedule.getCronExpression().isEmpty();
    boolean hasInterval = schedule.hasInterval();

    if (!hasCronExpression && !hasInterval) {
      throw Status.INVALID_ARGUMENT
          .withDescription("Evaluation schedule must specify either cron_expression or interval")
          .asRuntimeException(requestContext.buildTrailers());
    }

    // Validate cron expression if present
    if (hasCronExpression) {
      validateCronExpression(schedule.getCronExpression(), requestContext);
    }

    // Validate interval if present
    if (hasInterval && schedule.getInterval().getSeconds() <= 0) {
      throw Status.INVALID_ARGUMENT
          .withDescription("Evaluation interval must be positive")
          .asRuntimeException(requestContext.buildTrailers());
    }

    // Validate schedule times if present
    if (schedule.hasScheduleStartTime()
        && schedule.hasScheduleEndTime()
        && schedule.getScheduleEndTime() <= schedule.getScheduleStartTime()) {
      throw Status.INVALID_ARGUMENT
          .withDescription("Schedule end time must be after start time")
          .asRuntimeException(requestContext.buildTrailers());
    }
  }

  /**
   * Validates cron expression format using cron-utils library. Supports standard cron formats: - 5
   * fields: minute hour day month weekday (Unix) - 6 fields: second minute hour day month weekday
   * (Quartz) - 7 fields: second minute hour day month weekday year (Quartz with year)
   */
  private void validateCronExpression(String cronExpression, RequestContext requestContext) {
    String trimmedCron = cronExpression.trim();

    if (trimmedCron.isEmpty()) {
      throw Status.INVALID_ARGUMENT
          .withDescription("Cron expression cannot be empty")
          .asRuntimeException(requestContext.buildTrailers());
    }

    // Try parsing with different cron formats
    boolean isValid = false;
    String errorMessage = null;

    // Try Quartz format first (supports seconds and year: 6-7 fields)
    try {
      CronParser quartzParser =
          new CronParser(CronDefinitionBuilder.instanceDefinitionFor(CronType.QUARTZ));
      quartzParser.parse(trimmedCron).validate();
      isValid = true;
    } catch (IllegalArgumentException e) {
      errorMessage = e.getMessage();
    }

    // If Quartz fails, try Unix format (5 fields)
    if (!isValid) {
      try {
        CronParser unixParser =
            new CronParser(CronDefinitionBuilder.instanceDefinitionFor(CronType.UNIX));
        unixParser.parse(trimmedCron).validate();
        isValid = true;
      } catch (IllegalArgumentException e) {
        // Keep the more descriptive error message
        if (errorMessage == null) {
          errorMessage = e.getMessage();
        }
      }
    }

    if (!isValid) {
      throw Status.INVALID_ARGUMENT
          .withDescription("Invalid cron expression: " + errorMessage)
          .asRuntimeException(requestContext.buildTrailers());
    }
  }
}
