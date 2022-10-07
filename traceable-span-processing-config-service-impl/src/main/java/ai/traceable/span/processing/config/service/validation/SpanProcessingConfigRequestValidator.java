package ai.traceable.span.processing.config.service.validation;

import static org.hypertrace.config.validation.GrpcValidatorUtils.printMessage;
import static org.hypertrace.config.validation.GrpcValidatorUtils.validateNonDefaultPresenceOrThrow;
import static org.hypertrace.config.validation.GrpcValidatorUtils.validateRequestContextOrThrow;

import ai.traceable.span.processing.config.service.v1.ApiNamingRuleConfig;
import ai.traceable.span.processing.config.service.v1.ApiNamingRuleInfo;
import ai.traceable.span.processing.config.service.v1.ApiSpecBasedConfig;
import ai.traceable.span.processing.config.service.v1.CreateApiNamingRuleRequest;
import ai.traceable.span.processing.config.service.v1.CreateApiNamingRulesRequest;
import ai.traceable.span.processing.config.service.v1.CreateProtectionSpanRuleRequest;
import ai.traceable.span.processing.config.service.v1.CreateSamplingConfigRequest;
import ai.traceable.span.processing.config.service.v1.DeleteApiNamingRuleRequest;
import ai.traceable.span.processing.config.service.v1.DeleteApiNamingRulesRequest;
import ai.traceable.span.processing.config.service.v1.DeleteProtectionSpanRuleRequest;
import ai.traceable.span.processing.config.service.v1.DeleteSamplingConfigRequest;
import ai.traceable.span.processing.config.service.v1.GetAllApiNamingRulesRequest;
import ai.traceable.span.processing.config.service.v1.GetAllProtectionSpanRulesRequest;
import ai.traceable.span.processing.config.service.v1.GetAllResolvedProtectionSpanRulesRequest;
import ai.traceable.span.processing.config.service.v1.GetAllResolvedSamplingConfigsRequest;
import ai.traceable.span.processing.config.service.v1.GetAllSamplingConfigsRequest;
import ai.traceable.span.processing.config.service.v1.GetDefaultProtectionSpanRuleEvaluationStatusRequest;
import ai.traceable.span.processing.config.service.v1.ProtectionSpanRuleInfo;
import ai.traceable.span.processing.config.service.v1.RateLimit;
import ai.traceable.span.processing.config.service.v1.RateLimitConfig;
import ai.traceable.span.processing.config.service.v1.SamplingConfigInfo;
import ai.traceable.span.processing.config.service.v1.SegmentMatchingBasedConfig;
import ai.traceable.span.processing.config.service.v1.SpanFilter;
import ai.traceable.span.processing.config.service.v1.UpdateApiNamingRule;
import ai.traceable.span.processing.config.service.v1.UpdateApiNamingRuleRequest;
import ai.traceable.span.processing.config.service.v1.UpdateApiNamingRulesRequest;
import ai.traceable.span.processing.config.service.v1.UpdateDefaultProtectionSpanRuleEvaluationStatusRequest;
import ai.traceable.span.processing.config.service.v1.UpdateProtectionSpanRule;
import ai.traceable.span.processing.config.service.v1.UpdateProtectionSpanRuleRequest;
import ai.traceable.span.processing.config.service.v1.UpdateSamplingConfig;
import ai.traceable.span.processing.config.service.v1.UpdateSamplingConfigRequest;
import com.google.common.base.Strings;
import io.grpc.Status;
import java.util.List;
import java.util.regex.Pattern;
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

  public void validateOrThrow(
      RequestContext requestContext, GetAllResolvedProtectionSpanRulesRequest request) {
    validateRequestContextOrThrow(requestContext);
  }

  public void validateOrThrow(
      RequestContext requestContext, GetDefaultProtectionSpanRuleEvaluationStatusRequest request) {
    validateRequestContextOrThrow(requestContext);
  }

  public void validateOrThrow(
      RequestContext requestContext,
      UpdateDefaultProtectionSpanRuleEvaluationStatusRequest request) {
    validateRequestContextOrThrow(requestContext);
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

  public void validateOrThrow(RequestContext requestContext, GetAllApiNamingRulesRequest request) {
    validateRequestContextOrThrow(requestContext);
  }

  public void validateOrThrow(RequestContext requestContext, CreateApiNamingRuleRequest request) {
    validateRequestContextOrThrow(requestContext);
    this.validateData(request.getRuleInfo());
  }

  public void validateOrThrow(RequestContext requestContext, CreateApiNamingRulesRequest request) {
    validateRequestContextOrThrow(requestContext);
    for (ApiNamingRuleInfo apiNamingRuleInfo : request.getRulesInfoList()) {
      this.validateData(apiNamingRuleInfo);
    }
  }

  public void validateOrThrow(RequestContext requestContext, UpdateApiNamingRuleRequest request) {
    validateRequestContextOrThrow(requestContext);
    this.validateUpdateRule(request.getRule());
  }

  public void validateOrThrow(RequestContext requestContext, UpdateApiNamingRulesRequest request) {
    validateRequestContextOrThrow(requestContext);
    for (UpdateApiNamingRule updateApiNamingRule : request.getRulesList()) {
      this.validateUpdateRule(updateApiNamingRule);
    }
  }

  public void validateOrThrow(RequestContext requestContext, DeleteApiNamingRuleRequest request) {
    validateRequestContextOrThrow(requestContext);
    validateNonDefaultPresenceOrThrow(request, DeleteApiNamingRuleRequest.ID_FIELD_NUMBER);
  }

  public void validateOrThrow(RequestContext requestContext, DeleteApiNamingRulesRequest request) {
    validateRequestContextOrThrow(requestContext);
    for (String id : request.getIdsList()) {
      if (id.isEmpty() || id.isBlank()) {
        throw Status.INVALID_ARGUMENT
            .withDescription(String.format("Invalid id in request: %s", id))
            .asRuntimeException();
      }
    }
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
      case API_SPEC_BASED_CONFIG:
        // TODO: Add validations for specId list after migration of upstream services
        ApiSpecBasedConfig apiSpecBasedConfig = ruleConfig.getApiSpecBasedConfig();
        if (Strings.isNullOrEmpty(apiSpecBasedConfig.getApiSpecId())
            && apiSpecBasedConfig.getApiSpecIdsCount() == 0) {
          throw Status.INVALID_ARGUMENT
              .withDescription(String.format("Invalid specIds : %s", apiSpecBasedConfig))
              .asRuntimeException();
        }
        if (apiSpecBasedConfig.getRegexesCount() == 0
            || apiSpecBasedConfig.getRegexesCount() != apiSpecBasedConfig.getValuesCount()) {
          throw Status.INVALID_ARGUMENT
              .withDescription(
                  String.format(
                      "Invalid regex count or segment matching count : %s", apiSpecBasedConfig))
              .asRuntimeException();
        }
        if (apiSpecBasedConfig.getRegexesList().stream().anyMatch(String::isEmpty)
            || apiSpecBasedConfig.getValuesList().stream().anyMatch(String::isEmpty)) {
          throw Status.INVALID_ARGUMENT
              .withDescription(
                  String.format(
                      "Invalid regex or value segment : %s. Regex/value segment must not be empty",
                      apiSpecBasedConfig))
              .asRuntimeException();
        }
        validateRegex(apiSpecBasedConfig.getRegexesList());
        break;
      default:
        throw Status.INVALID_ARGUMENT
            .withDescription("Unexpected rule config case: " + printMessage(ruleConfig))
            .asRuntimeException();
    }
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

  private void validateRegex(List<String> regexes) {
    try {
      Pattern.compile(String.join("/", regexes));
    } catch (Exception e) {
      throw Status.INVALID_ARGUMENT
          .withDescription(String.format("Invalid regexes : %s.", regexes))
          .asRuntimeException();
    }
  }
}
