package ai.traceable.span.processing.config.service.validation;

import static org.hypertrace.config.validation.GrpcValidatorUtils.printMessage;
import static org.hypertrace.config.validation.GrpcValidatorUtils.validateNonDefaultPresenceOrThrow;
import static org.hypertrace.config.validation.GrpcValidatorUtils.validateRequestContextOrThrow;

import ai.traceable.span.processing.config.service.v1.CreateSpanProcessingRuleRequest;
import ai.traceable.span.processing.config.service.v1.DeleteSpanProcessingRuleRequest;
import ai.traceable.span.processing.config.service.v1.Filter;
import ai.traceable.span.processing.config.service.v1.GetAllSpanProcessingRulesRequest;
import ai.traceable.span.processing.config.service.v1.SpanProcessingRuleInfo;
import ai.traceable.span.processing.config.service.v1.UpdateSpanProcessingRule;
import ai.traceable.span.processing.config.service.v1.UpdateSpanProcessingRulesRequest;
import io.grpc.Status;
import org.hypertrace.core.grpcutils.context.RequestContext;

public class SpanProcessingConfigRequestValidator {

  public void validateOrThrow(
      RequestContext requestContext, GetAllSpanProcessingRulesRequest request) {
    validateRequestContextOrThrow(requestContext);
  }

  public void validateOrThrow(
      RequestContext requestContext, CreateSpanProcessingRuleRequest request) {
    validateRequestContextOrThrow(requestContext);
    this.validateData(request.getRuleInfo());
  }

  public void validateOrThrow(
      RequestContext requestContext, UpdateSpanProcessingRulesRequest request) {
    validateRequestContextOrThrow(requestContext);
    if (request.getRulesList().isEmpty()) {
      throw Status.INVALID_ARGUMENT.withDescription("No rules specified").asRuntimeException();
    }
    for (UpdateSpanProcessingRule rule : request.getRulesList()) {
      this.validateUpdateRule(rule);
    }
  }

  public void validateOrThrow(
      RequestContext requestContext, DeleteSpanProcessingRuleRequest request) {
    validateRequestContextOrThrow(requestContext);
    validateNonDefaultPresenceOrThrow(request, DeleteSpanProcessingRuleRequest.ID_FIELD_NUMBER);
  }

  private void validateData(SpanProcessingRuleInfo spanProcessingRuleInfo) {
    validateNonDefaultPresenceOrThrow(
        spanProcessingRuleInfo, SpanProcessingRuleInfo.NAME_FIELD_NUMBER);
    this.validateRule(spanProcessingRuleInfo);
  }

  private void validateRule(SpanProcessingRuleInfo spanProcessingRuleInfo) {
    switch (spanProcessingRuleInfo.getRuleCase()) {
      case EXCLUDE_SPAN_RULE:
        this.validateFilter(spanProcessingRuleInfo.getExcludeSpanRule().getFilter());
        break;
      default:
        throw Status.INVALID_ARGUMENT
            .withDescription("Unexpected rule info case: " + printMessage(spanProcessingRuleInfo))
            .asRuntimeException();
    }
  }

  private void validateUpdateRule(UpdateSpanProcessingRule updateSpanProcessingRule) {
    validateNonDefaultPresenceOrThrow(
        updateSpanProcessingRule, UpdateSpanProcessingRule.ID_FIELD_NUMBER);
    validateNonDefaultPresenceOrThrow(
        updateSpanProcessingRule, UpdateSpanProcessingRule.NAME_FIELD_NUMBER);
    switch (updateSpanProcessingRule.getRuleCase()) {
      case EXCLUDE_SPAN_RULE:
        this.validateFilter(updateSpanProcessingRule.getExcludeSpanRule().getFilter());
        break;
      default:
        throw Status.INVALID_ARGUMENT
            .withDescription(
                "Unexpected updated span processing rule case: "
                    + printMessage(updateSpanProcessingRule))
            .asRuntimeException();
    }
  }

  private void validateFilter(Filter filter) {
    switch (filter.getFilterExpressionCase()) {
      case LOGICAL_FILTER:
      case RELATIONAL_FILTER:
        break;
      default:
        throw Status.INVALID_ARGUMENT
            .withDescription("Unexpected filter case: " + printMessage(filter))
            .asRuntimeException();
    }
  }
}
