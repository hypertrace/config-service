package org.hypertrace.span.processing.config.service.validation;

import static org.hypertrace.config.validation.GrpcValidatorUtils.printMessage;
import static org.hypertrace.config.validation.GrpcValidatorUtils.validateNonDefaultPresenceOrThrow;
import static org.hypertrace.config.validation.GrpcValidatorUtils.validateRequestContextOrThrow;

import com.google.re2j.Pattern;
import io.grpc.Status;
import java.util.List;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.hypertrace.span.processing.config.service.v1.ApiNamingRuleConfig;
import org.hypertrace.span.processing.config.service.v1.ApiNamingRuleInfo;
import org.hypertrace.span.processing.config.service.v1.CreateApiNamingRuleRequest;
import org.hypertrace.span.processing.config.service.v1.CreateExcludeSpanRuleRequest;
import org.hypertrace.span.processing.config.service.v1.CreateIncludeSpanRuleRequest;
import org.hypertrace.span.processing.config.service.v1.CreateSamplingConfigRequest;
import org.hypertrace.span.processing.config.service.v1.DeleteApiNamingRuleRequest;
import org.hypertrace.span.processing.config.service.v1.DeleteExcludeSpanRuleRequest;
import org.hypertrace.span.processing.config.service.v1.DeleteIncludeSpanRuleRequest;
import org.hypertrace.span.processing.config.service.v1.DeleteSamplingConfigRequest;
import org.hypertrace.span.processing.config.service.v1.ExcludeSpanRuleInfo;
import org.hypertrace.span.processing.config.service.v1.GetAllApiNamingRulesRequest;
import org.hypertrace.span.processing.config.service.v1.GetAllExcludeSpanRulesRequest;
import org.hypertrace.span.processing.config.service.v1.GetAllIncludeSpanRulesRequest;
import org.hypertrace.span.processing.config.service.v1.GetAllSamplingConfigsRequest;
import org.hypertrace.span.processing.config.service.v1.IncludeSpanRuleInfo;
import org.hypertrace.span.processing.config.service.v1.RateLimit;
import org.hypertrace.span.processing.config.service.v1.RateLimitConfig;
import org.hypertrace.span.processing.config.service.v1.SamplingConfigInfo;
import org.hypertrace.span.processing.config.service.v1.SegmentMatchingBasedConfig;
import org.hypertrace.span.processing.config.service.v1.SpanFilter;
import org.hypertrace.span.processing.config.service.v1.UpdateApiNamingRule;
import org.hypertrace.span.processing.config.service.v1.UpdateApiNamingRuleRequest;
import org.hypertrace.span.processing.config.service.v1.UpdateExcludeSpanRule;
import org.hypertrace.span.processing.config.service.v1.UpdateExcludeSpanRuleRequest;
import org.hypertrace.span.processing.config.service.v1.UpdateIncludeSpanRule;
import org.hypertrace.span.processing.config.service.v1.UpdateIncludeSpanRuleRequest;
import org.hypertrace.span.processing.config.service.v1.UpdateSamplingConfig;
import org.hypertrace.span.processing.config.service.v1.UpdateSamplingConfigRequest;

public class SpanProcessingConfigRequestValidator {

  public void validateOrThrow(
      RequestContext requestContext, GetAllExcludeSpanRulesRequest request) {
    validateRequestContextOrThrow(requestContext);
  }

  public void validateOrThrow(RequestContext requestContext, CreateExcludeSpanRuleRequest request) {
    validateRequestContextOrThrow(requestContext);
    this.validateData(request.getRuleInfo());
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

  public void validateOrThrow(
      RequestContext requestContext, GetAllIncludeSpanRulesRequest request) {
    validateRequestContextOrThrow(requestContext);
  }

  public void validateOrThrow(RequestContext requestContext, CreateIncludeSpanRuleRequest request) {
    validateRequestContextOrThrow(requestContext);
    this.validateData(request.getRuleInfo());
  }

  public void validateOrThrow(RequestContext requestContext, UpdateIncludeSpanRuleRequest request) {
    validateRequestContextOrThrow(requestContext);
    this.validateUpdateRule(request.getRule());
  }

  public void validateOrThrow(RequestContext requestContext, DeleteIncludeSpanRuleRequest request) {
    validateRequestContextOrThrow(requestContext);
    validateNonDefaultPresenceOrThrow(request, DeleteIncludeSpanRuleRequest.ID_FIELD_NUMBER);
  }

  private void validateData(IncludeSpanRuleInfo includeSpanRuleInfo) {
    validateNonDefaultPresenceOrThrow(includeSpanRuleInfo, IncludeSpanRuleInfo.NAME_FIELD_NUMBER);
    this.validateSpanFilter(includeSpanRuleInfo.getFilter());
  }

  private void validateUpdateRule(UpdateIncludeSpanRule updateIncludeSpanRule) {
    validateNonDefaultPresenceOrThrow(updateIncludeSpanRule, UpdateIncludeSpanRule.ID_FIELD_NUMBER);
    validateNonDefaultPresenceOrThrow(
        updateIncludeSpanRule, UpdateIncludeSpanRule.NAME_FIELD_NUMBER);
    this.validateSpanFilter(updateIncludeSpanRule.getFilter());
  }

  public void validateOrThrow(RequestContext requestContext, GetAllApiNamingRulesRequest request) {
    validateRequestContextOrThrow(requestContext);
  }

  public void validateOrThrow(RequestContext requestContext, CreateApiNamingRuleRequest request) {
    validateRequestContextOrThrow(requestContext);
    this.validateData(request.getRuleInfo());
  }

  public void validateOrThrow(RequestContext requestContext, UpdateApiNamingRuleRequest request) {
    validateRequestContextOrThrow(requestContext);
    this.validateUpdateRule(request.getRule());
  }

  public void validateOrThrow(RequestContext requestContext, DeleteApiNamingRuleRequest request) {
    validateRequestContextOrThrow(requestContext);
    validateNonDefaultPresenceOrThrow(request, DeleteExcludeSpanRuleRequest.ID_FIELD_NUMBER);
  }

  private void validateData(ApiNamingRuleInfo apiNamingRuleInfo) {
    validateNonDefaultPresenceOrThrow(apiNamingRuleInfo, ApiNamingRuleInfo.NAME_FIELD_NUMBER);
    this.validateConfig(apiNamingRuleInfo.getRuleConfig());
  }

  private void validateUpdateRule(UpdateApiNamingRule updateApiNamingRule) {
    validateNonDefaultPresenceOrThrow(updateApiNamingRule, UpdateApiNamingRule.ID_FIELD_NUMBER);
    validateNonDefaultPresenceOrThrow(updateApiNamingRule, UpdateApiNamingRule.NAME_FIELD_NUMBER);
    this.validateConfig(updateApiNamingRule.getRuleConfig());
  }

  private void validateConfig(ApiNamingRuleConfig ruleConfig) {
    switch (ruleConfig.getRuleConfigCase()) {
      case SEGMENT_MATCHING_BASED_CONFIG:
        SegmentMatchingBasedConfig segmentMatchingBasedConfig =
            ruleConfig.getSegmentMatchingBasedConfig();
        if (segmentMatchingBasedConfig.getRegexesCount() == 0
            || segmentMatchingBasedConfig.getRegexesCount()
                != segmentMatchingBasedConfig.getValuesCount()) {
          throw Status.INVALID_ARGUMENT
              .withDescription(
                  String.format(
                      "Invalid regex count or segment matching count : %s",
                      segmentMatchingBasedConfig))
              .asRuntimeException();
        }
        if (segmentMatchingBasedConfig.getRegexesList().stream().anyMatch(String::isEmpty)
            || segmentMatchingBasedConfig.getValuesList().stream().anyMatch(String::isEmpty)) {
          throw Status.INVALID_ARGUMENT
              .withDescription(
                  String.format(
                      "Invalid regex or value segment : %s. Regex/value segment must not be empty",
                      segmentMatchingBasedConfig))
              .asRuntimeException();
        }
        validateRegex(segmentMatchingBasedConfig.getRegexesList());
        break;
      default:
        throw Status.INVALID_ARGUMENT
            .withDescription("Unexpected rule config case: " + printMessage(ruleConfig))
            .asRuntimeException();
    }
  }

  public void validateOrThrow(RequestContext requestContext, GetAllSamplingConfigsRequest request) {
    validateRequestContextOrThrow(requestContext);
  }

  public void validateOrThrow(RequestContext requestContext, CreateSamplingConfigRequest request) {
    validateRequestContextOrThrow(requestContext);
    this.validateData(request.getSamplingConfigInfo());
  }

  public void validateOrThrow(RequestContext requestContext, UpdateSamplingConfigRequest request) {
    validateRequestContextOrThrow(requestContext);
    this.validateUpdateSamplingConfig(request.getSamplingConfig());
  }

  public void validateOrThrow(RequestContext requestContext, DeleteSamplingConfigRequest request) {
    validateRequestContextOrThrow(requestContext);
    validateNonDefaultPresenceOrThrow(request, DeleteSamplingConfigRequest.ID_FIELD_NUMBER);
  }

  private void validateData(SamplingConfigInfo samplingConfigInfo) {
    this.validateRateLimitConfig(samplingConfigInfo.getRateLimitConfig());
    this.validateSpanFilter(samplingConfigInfo.getFilter());
  }

  private void validateUpdateSamplingConfig(UpdateSamplingConfig updateSamplingConfig) {
    validateNonDefaultPresenceOrThrow(updateSamplingConfig, UpdateSamplingConfig.ID_FIELD_NUMBER);
    this.validateSpanFilter(updateSamplingConfig.getFilter());
    this.validateRateLimitConfig(updateSamplingConfig.getRateLimitConfig());
  }

  private void validateRateLimitConfig(RateLimitConfig rateLimitConfig) {
    this.validateRateLimit(rateLimitConfig.getTraceLimitGlobal());
    this.validateRateLimit(rateLimitConfig.getTraceLimitPerEndpoint());
  }

  private void validateRateLimit(RateLimit rateLimit) {
    switch (rateLimit.getLimitCase()) {
      case FIXED_WINDOW_LIMIT:
        break;
      default:
        throw Status.INVALID_ARGUMENT
            .withDescription("Unexpected rate limit case: " + printMessage(rateLimit))
            .asRuntimeException();
    }
  }

  private void validateSpanFilter(SpanFilter filter) {
    switch (filter.getSpanFilterExpressionCase()) {
      case LOGICAL_SPAN_FILTER:
      case RELATIONAL_SPAN_FILTER:
        break;
      default:
        throw Status.INVALID_ARGUMENT
            .withDescription("Unexpected filter case: " + printMessage(filter))
            .asRuntimeException();
    }
  }

  private void validateRegex(List<String> regexes) {
    try {
      Pattern pattern = Pattern.compile(String.join("/", regexes));
    } catch (Exception e) {
      throw Status.INVALID_ARGUMENT
          .withDescription(String.format("Invalid regexes : %s.", regexes))
          .asRuntimeException();
    }
  }
}
