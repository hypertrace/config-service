package ai.traceable.span.processing.config.service.validation;

import static ai.traceable.span.processing.config.service.v1.RelationalOperator.RELATIONAL_OPERATOR_REGEX_MATCH;
import static com.google.common.base.Strings.isNullOrEmpty;
import static org.hypertrace.config.validation.GrpcValidatorUtils.printMessage;
import static org.hypertrace.config.validation.GrpcValidatorUtils.validateNonDefaultPresenceOrThrow;
import static org.hypertrace.config.validation.GrpcValidatorUtils.validateRequestContextOrThrow;

import ai.traceable.config.utils.RegexValidator;
import ai.traceable.span.processing.config.service.v1.ApiNamingRuleConfig;
import ai.traceable.span.processing.config.service.v1.ApiNamingRuleInfo;
import ai.traceable.span.processing.config.service.v1.ApiSpecBasedConfig;
import ai.traceable.span.processing.config.service.v1.AstScanBasedConfig;
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
import ai.traceable.span.processing.config.service.v1.GetApiNamingRulesRequest;
import ai.traceable.span.processing.config.service.v1.GetDefaultProtectionSpanRuleEvaluationStatusRequest;
import ai.traceable.span.processing.config.service.v1.LogicalSpanFilterExpression;
import ai.traceable.span.processing.config.service.v1.ProtectionSpanRuleInfo;
import ai.traceable.span.processing.config.service.v1.RateLimit;
import ai.traceable.span.processing.config.service.v1.RateLimitConfig;
import ai.traceable.span.processing.config.service.v1.RateLimitStrategy;
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
import io.grpc.Status;
import java.util.List;
import java.util.function.Predicate;
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

  public void validateOrThrow(RequestContext requestContext, GetApiNamingRulesRequest request) {
    validateRequestContextOrThrow(requestContext);
  }

  public void validateOrThrow(RequestContext requestContext, CreateApiNamingRuleRequest request) {
    validateRequestContextOrThrow(requestContext);
    this.validateData(request.getRuleInfo());
  }

  public void validateOrThrow(RequestContext requestContext, CreateApiNamingRulesRequest request) {
    validateRequestContextOrThrow(requestContext);
    if (request.getRulesInfoCount() == 0) {
      throw Status.INVALID_ARGUMENT
          .withDescription(
              String.format(
                  "Invalid request. At least 1 naming rule is to be provided in request: %s",
                  request))
          .asRuntimeException();
    }
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
    if (request.getRulesCount() == 0) {
      throw Status.INVALID_ARGUMENT
          .withDescription(
              String.format(
                  "Invalid request. At least 1 naming rule is to be provided in request: %s",
                  request))
          .asRuntimeException();
    }
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
    if (request.getIdsCount() == 0) {
      throw Status.INVALID_ARGUMENT
          .withDescription(
              String.format(
                  "Invalid request. At least 1 naming rule id is to be provided in request: %s",
                  request))
          .asRuntimeException();
    }
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
        if (isNullOrEmpty(apiSpecBasedConfig.getApiSpecId())
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
      case AST_SCAN_BASED_CONFIG:
        AstScanBasedConfig astScanBasedConfig = ruleConfig.getAstScanBasedConfig();
        if (isNullOrEmpty(astScanBasedConfig.getScanId())) {
          throw Status.INVALID_ARGUMENT
              .withDescription(String.format("Invalid scanId : %s", astScanBasedConfig))
              .asRuntimeException();
        }
        if (isNullOrEmpty(astScanBasedConfig.getApiSpecId())) {
          throw Status.INVALID_ARGUMENT
              .withDescription(String.format("Invalid specId : %s", astScanBasedConfig))
              .asRuntimeException();
        }
        if (astScanBasedConfig.getRegexesCount() == 0
            || astScanBasedConfig.getRegexesCount() != astScanBasedConfig.getValuesCount()) {
          throw Status.INVALID_ARGUMENT
              .withDescription(
                  String.format(
                      "Invalid regex count or segment matching count : %s", astScanBasedConfig))
              .asRuntimeException();
        }
        if (astScanBasedConfig.getRegexesList().stream().anyMatch(String::isEmpty)
            || astScanBasedConfig.getValuesList().stream().anyMatch(String::isEmpty)) {
          throw Status.INVALID_ARGUMENT
              .withDescription(
                  String.format(
                      "Invalid regex or value segment : %s. Regex/value segment must not be empty",
                      astScanBasedConfig))
              .asRuntimeException();
        }
        validateRegex(astScanBasedConfig.getRegexesList());
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
    this.validateRateLimitStrategy(rateLimitConfig.getRateLimitStrategy());
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

  private void validateRateLimitStrategy(RateLimitStrategy rateLimitStrategy) {
    switch (rateLimitStrategy) {
      case RATE_LIMIT_STRATEGY_DROP:
      case RATE_LIMIT_STRATEGY_BARESPAN:
      case RATE_LIMIT_STRATEGY_DO_NOT_PERSIST:
        break;
      default:
        throw Status.INVALID_ARGUMENT
            .withDescription("Unexpected rate limit strategy: " + rateLimitStrategy)
            .asRuntimeException();
    }
  }

  private void validateSpanFilter(SpanFilter filter) {
    switch (filter.getSpanFilterExpressionCase()) {
      case LOGICAL_SPAN_FILTER:
        validateLogicalSpanFilter(filter);
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

  private void validateLogicalSpanFilter(SpanFilter filter) {
    validateNonDefaultPresenceOrThrow(
        filter.getLogicalSpanFilter(), LogicalSpanFilterExpression.OPERATOR_FIELD_NUMBER);
    validateNonDefaultPresenceOrThrow(
        filter.getLogicalSpanFilter(), LogicalSpanFilterExpression.OPERANDS_FIELD_NUMBER);
    filter.getLogicalSpanFilter().getOperandsList().forEach(this::validateSpanFilter);
  }

  private void validateRelationalSpanFilter(SpanFilter filter) {
    validateNonDefaultPresenceOrThrow(
        filter.getRelationalSpanFilter(), RelationalSpanFilterExpression.OPERATOR_FIELD_NUMBER);

    final SpanFilterValue rhs = filter.getRelationalSpanFilter().getRightOperand();
    if (filter.getRelationalSpanFilter().getOperator().equals(RELATIONAL_OPERATOR_REGEX_MATCH)) {
      validateNonDefaultPresenceOrThrow(rhs, SpanFilterValue.STRING_VALUE_FIELD_NUMBER);
      final Status status = RegexValidator.validate(rhs.getStringValue());
      if (!status.isOk()) {
        throw status.asRuntimeException();
      }
    }
  }

  private void validateRegex(List<String> regexes) {
    Status status =
        regexes.stream()
            .map(RegexValidator::validate)
            .filter(Predicate.not(Status::isOk))
            .findFirst()
            .orElse(Status.OK);
    if (!status.isOk()) {
      throw Status.INVALID_ARGUMENT
          .withDescription(String.format("Invalid regexes : %s.", regexes))
          .asRuntimeException();
    }
  }
}
