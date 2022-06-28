package ai.traceable.span.processing.config.service.validation;

import static org.hypertrace.config.validation.GrpcValidatorUtils.printMessage;
import static org.hypertrace.config.validation.GrpcValidatorUtils.validateNonDefaultPresenceOrThrow;
import static org.hypertrace.config.validation.GrpcValidatorUtils.validateRequestContextOrThrow;

import ai.traceable.span.processing.config.service.v1.CreateProtectionSpanRuleRequest;
import ai.traceable.span.processing.config.service.v1.CreateSamplingConfigRequest;
import ai.traceable.span.processing.config.service.v1.DeleteProtectionSpanRuleRequest;
import ai.traceable.span.processing.config.service.v1.DeleteSamplingConfigRequest;
import ai.traceable.span.processing.config.service.v1.GetAllProtectionSpanRulesRequest;
import ai.traceable.span.processing.config.service.v1.GetAllResolvedSamplingConfigsRequest;
import ai.traceable.span.processing.config.service.v1.GetAllSamplingConfigsRequest;
import ai.traceable.span.processing.config.service.v1.ProtectionSpanRuleInfo;
import ai.traceable.span.processing.config.service.v1.RateLimit;
import ai.traceable.span.processing.config.service.v1.RateLimitConfig;
import ai.traceable.span.processing.config.service.v1.SamplingConfigInfo;
import ai.traceable.span.processing.config.service.v1.SpanFilter;
import ai.traceable.span.processing.config.service.v1.UpdateProtectionSpanRule;
import ai.traceable.span.processing.config.service.v1.UpdateProtectionSpanRuleRequest;
import ai.traceable.span.processing.config.service.v1.UpdateSamplingConfig;
import ai.traceable.span.processing.config.service.v1.UpdateSamplingConfigRequest;
import io.grpc.Status;
import org.hypertrace.core.grpcutils.context.RequestContext;

public class SpanProcessingConfigRequestValidator {
  public void validateOrThrow(
      RequestContext requestContext, GetAllProtectionSpanRulesRequest request) {
    validateRequestContextOrThrow(requestContext);
  }

  public void validateOrThrow(
      RequestContext requestContext, CreateProtectionSpanRuleRequest request) {
    validateRequestContextOrThrow(requestContext);
    this.validateData(request.getRuleInfo());
  }

  public void validateOrThrow(
      RequestContext requestContext, UpdateProtectionSpanRuleRequest request) {
    validateRequestContextOrThrow(requestContext);
    this.validateUpdateRule(request.getRule());
  }

  public void validateOrThrow(
      RequestContext requestContext, DeleteProtectionSpanRuleRequest request) {
    validateRequestContextOrThrow(requestContext);
    validateNonDefaultPresenceOrThrow(request, DeleteProtectionSpanRuleRequest.ID_FIELD_NUMBER);
  }

  private void validateData(ProtectionSpanRuleInfo protectionSpanRuleInfo) {
    validateNonDefaultPresenceOrThrow(
        protectionSpanRuleInfo, ProtectionSpanRuleInfo.NAME_FIELD_NUMBER);
    if (protectionSpanRuleInfo.hasFilter()) {
      this.validateSpanFilter(protectionSpanRuleInfo.getFilter());
    }
  }

  private void validateUpdateRule(UpdateProtectionSpanRule updateProtectionSpanRule) {
    validateNonDefaultPresenceOrThrow(
        updateProtectionSpanRule, UpdateProtectionSpanRule.ID_FIELD_NUMBER);
    validateNonDefaultPresenceOrThrow(
        updateProtectionSpanRule, UpdateProtectionSpanRule.NAME_FIELD_NUMBER);
    this.validateSpanFilter(updateProtectionSpanRule.getFilter());
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

  public void validateOrThrow(
      RequestContext requestContext, GetAllResolvedSamplingConfigsRequest request) {
    validateRequestContextOrThrow(requestContext);
  }

  private void validateData(SamplingConfigInfo samplingConfigInfo) {
    this.validateRateLimitConfig(samplingConfigInfo.getRateLimitConfig());
    if (samplingConfigInfo.hasFilter()) {
      this.validateSpanFilter(samplingConfigInfo.getFilter());
    }
  }

  private void validateUpdateSamplingConfig(UpdateSamplingConfig updateSamplingConfig) {
    validateNonDefaultPresenceOrThrow(updateSamplingConfig, UpdateSamplingConfig.ID_FIELD_NUMBER);
    if (updateSamplingConfig.hasFilter()) {
      this.validateSpanFilter(updateSamplingConfig.getFilter());
    }
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
}
