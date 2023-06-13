package org.hypertrace.span.processing.config.service.validation;

import static org.hypertrace.config.validation.GrpcValidatorUtils.printMessage;
import static org.hypertrace.config.validation.GrpcValidatorUtils.validateNonDefaultPresenceOrThrow;
import static org.hypertrace.config.validation.GrpcValidatorUtils.validateRequestContextOrThrow;
import static org.hypertrace.span.processing.config.service.v1.RelationalOperator.RELATIONAL_OPERATOR_REGEX_MATCH;
import static org.hypertrace.span.processing.config.service.v1.RuleType.RULE_TYPE_SYSTEM;
import static org.hypertrace.span.processing.config.service.v1.SpanFilterValue.ValueCase.STRING_VALUE;

import com.google.re2j.Pattern;
import com.google.re2j.PatternSyntaxException;
import io.grpc.Status;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.hypertrace.span.processing.config.service.v1.CreateExcludeSpanRuleRequest;
import org.hypertrace.span.processing.config.service.v1.DeleteExcludeSpanRuleRequest;
import org.hypertrace.span.processing.config.service.v1.ExcludeSpanRuleInfo;
import org.hypertrace.span.processing.config.service.v1.GetAllExcludeSpanRulesRequest;
import org.hypertrace.span.processing.config.service.v1.LogicalOperator;
import org.hypertrace.span.processing.config.service.v1.SpanFilter;
import org.hypertrace.span.processing.config.service.v1.SpanFilterValue;
import org.hypertrace.span.processing.config.service.v1.UpdateExcludeSpanRule;
import org.hypertrace.span.processing.config.service.v1.UpdateExcludeSpanRuleRequest;

public class SpanProcessingConfigRequestValidator {

  public void validateOrThrow(
      RequestContext requestContext, GetAllExcludeSpanRulesRequest request) {
    validateRequestContextOrThrow(requestContext);
  }

  public void validateOrThrow(RequestContext requestContext, CreateExcludeSpanRuleRequest request) {
    validateRequestContextOrThrow(requestContext);
    this.validateData(request.getRuleInfo());
    if (RULE_TYPE_SYSTEM.equals(request.getRuleInfo().getType())) {
      throw Status.INVALID_ARGUMENT
          .withDescription(
              String.format(
                  "Invalid rule type to create system level rule : %s", request.getRuleInfo()))
          .asRuntimeException();
    }
  }

  public void validateOrThrow(RequestContext requestContext, UpdateExcludeSpanRuleRequest request) {
    validateRequestContextOrThrow(requestContext);
    this.validateUpdateRule(request.getRule());
  }

  public void validateOrThrow(RequestContext requestContext, DeleteExcludeSpanRuleRequest request) {
    validateRequestContextOrThrow(requestContext);
    validateNonDefaultPresenceOrThrow(request, DeleteExcludeSpanRuleRequest.ID_FIELD_NUMBER);
  }

  private void validateData(ExcludeSpanRuleInfo excludeSpanRuleInfo) {
    validateNonDefaultPresenceOrThrow(excludeSpanRuleInfo, ExcludeSpanRuleInfo.NAME_FIELD_NUMBER);
    this.validateSpanFilter(excludeSpanRuleInfo.getFilter());
  }

  private void validateUpdateRule(UpdateExcludeSpanRule updateExcludeSpanRule) {
    validateNonDefaultPresenceOrThrow(updateExcludeSpanRule, UpdateExcludeSpanRule.ID_FIELD_NUMBER);
    validateNonDefaultPresenceOrThrow(
        updateExcludeSpanRule, UpdateExcludeSpanRule.NAME_FIELD_NUMBER);
    this.validateSpanFilter(updateExcludeSpanRule.getFilter());
  }

  private void validateSpanFilter(SpanFilter filter) {
    switch (filter.getSpanFilterExpressionCase()) {
      case LOGICAL_SPAN_FILTER:
        if (filter
            .getLogicalSpanFilter()
            .getOperator()
            .equals(LogicalOperator.LOGICAL_OPERATOR_AND)) {
          filter.getLogicalSpanFilter().getOperandsList().stream()
              .filter(SpanFilter::hasRelationalSpanFilter)
              .forEach(this::validateRelationalSpanFilter);
        } else {
          if (filter.getLogicalSpanFilter().getOperandsCount() == 0) {
            return;
          }
          filter.getLogicalSpanFilter().getOperandsList().stream()
              .filter(SpanFilter::hasRelationalSpanFilter)
              .forEach(this::validateRelationalSpanFilter);
        }
        break;
      case RELATIONAL_SPAN_FILTER:
        validateRelationalSpanFilter(filter);
        break;
      default:
        throw Status.INVALID_ARGUMENT
            .withDescription("Unexpected filter case: " + printMessage(filter))
            .asRuntimeException();
    }
  }

  private void validateRelationalSpanFilter(SpanFilter filter) {
    SpanFilterValue rhs = filter.getRelationalSpanFilter().getRightOperand();
    if (rhs.getValueCase().equals(STRING_VALUE)
        && filter.getRelationalSpanFilter().getOperator().equals(RELATIONAL_OPERATOR_REGEX_MATCH)) {
      validateRegex(rhs.getStringValue());
    }
  }

  private void validateRegex(String regex) {
    try {
      Pattern.compile(regex);
    } catch (PatternSyntaxException e) {
      throw Status.INVALID_ARGUMENT
          .withDescription("Invalid regex passed: " + regex)
          .asRuntimeException();
    }
  }
}
