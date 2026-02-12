package ai.traceable.fraud.policy.config.service.validation;

import static org.hypertrace.config.validation.GrpcValidatorUtils.validateRequestContextOrThrow;

import ai.traceable.fraud.policy.config.service.v1.AbusePolicyData;
import ai.traceable.fraud.policy.config.service.v1.CreateAbusePolicyRequest;
import ai.traceable.fraud.policy.config.service.v1.UpdateAbusePolicyRequest;
import com.cronutils.model.CronType;
import com.cronutils.model.definition.CronDefinitionBuilder;
import com.cronutils.parser.CronParser;
import io.grpc.Status;
import org.hypertrace.core.grpcutils.context.RequestContext;

public class AbusePolicyConfigRequestValidator {

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

    // At least one scope type must be defined (environment or API)
    boolean hasEnvironmentScope =
        data.getScope().hasEnvironmentScope()
            && data.getScope().getEnvironmentScope().getEnvironmentIdsCount() > 0;
    boolean hasApiScope =
        data.getScope().hasApiScope()
            && (data.getScope().getApiScope().hasApiIds()
                || data.getScope().getApiScope().hasApiLabels());

    if (!hasEnvironmentScope && !hasApiScope) {
      throw Status.INVALID_ARGUMENT
          .withDescription("At least one scope (environment_scope or api_scope) must be defined")
          .asRuntimeException(requestContext.buildTrailers());
    }

    // Validate API scope if present
    if (data.getScope().hasApiScope()) {
      validateApiScope(data.getScope().getApiScope(), requestContext);
    }
  }

  private void validateApiScope(
      ai.traceable.fraud.policy.config.service.v1.AbuseApiScope apiScope,
      RequestContext requestContext) {
    // Validate api_ids is not empty if present
    if (apiScope.hasApiIds() && apiScope.getApiIds().getIdsCount() == 0) {
      throw Status.INVALID_ARGUMENT
          .withDescription("API IDs list cannot be empty when specified")
          .asRuntimeException(requestContext.buildTrailers());
    }

    // Validate api_labels is not empty if present
    if (apiScope.hasApiLabels() && apiScope.getApiLabels().getLabelsCount() == 0) {
      throw Status.INVALID_ARGUMENT
          .withDescription("API labels list cannot be empty when specified")
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
    if (!data.hasSimpleAggregationTemplate() && !data.hasPredefinedTemplate()) {
      throw Status.INVALID_ARGUMENT
          .withDescription(
              "Either simple_aggregation_template or predefined_template must be specified")
          .asRuntimeException(requestContext.buildTrailers());
    }

    // Validate simple aggregation template if present
    if (data.hasSimpleAggregationTemplate()) {
      validateSimpleAggregationTemplate(data.getSimpleAggregationTemplate(), requestContext);
    }

    // Validate predefined template if present
    if (data.hasPredefinedTemplate()) {
      validatePredefinedTemplate(data.getPredefinedTemplate(), requestContext);
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

    // Validate aggregation function
    if (template.getAggregation().getFunction()
        == ai.traceable.fraud.policy.config.service.v1.AbuseAggregationFunction
            .ABUSE_AGGREGATION_FUNCTION_UNSPECIFIED) {
      throw Status.INVALID_ARGUMENT
          .withDescription("Aggregation function must be specified")
          .asRuntimeException(requestContext.buildTrailers());
    }

    if (template.getAggregation().getDerivedEntityId().isEmpty()) {
      throw Status.INVALID_ARGUMENT
          .withDescription("Derived entity ID is required for aggregation")
          .asRuntimeException(requestContext.buildTrailers());
    }

    // Validate group by if present
    if (template.hasGroupBy() && template.getGroupBy().getDerivedEntityId().isEmpty()) {
      throw Status.INVALID_ARGUMENT
          .withDescription("Derived entity ID is required for group by")
          .asRuntimeException(requestContext.buildTrailers());
    }

    // Aggregation and group by must not use the same derived entity
    if (template.hasGroupBy()
        && !template.getGroupBy().getDerivedEntityId().isEmpty()
        && !template.getAggregation().getDerivedEntityId().isEmpty()
        && template
            .getAggregation()
            .getDerivedEntityId()
            .equals(template.getGroupBy().getDerivedEntityId())) {
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

    if (template.getTimeWindow().getLookbackDuration().getSeconds() <= 0) {
      throw Status.INVALID_ARGUMENT
          .withDescription("Lookback duration must be positive")
          .asRuntimeException(requestContext.buildTrailers());
    }
  }

  private void validatePredefinedTemplate(
      ai.traceable.fraud.policy.config.service.v1.AbusePredefinedTemplateConfig template,
      RequestContext requestContext) {

    if (template.getTemplateType()
        == ai.traceable.fraud.policy.config.service.v1.AbusePredefinedTemplateType
            .ABUSE_PREDEFINED_TEMPLATE_TYPE_UNSPECIFIED) {
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
