package ai.traceable.detection.exclusion.config.service.v1.rules;

import static org.hypertrace.config.validation.GrpcValidatorUtils.validateNonDefaultPresenceOrThrow;
import static org.hypertrace.config.validation.GrpcValidatorUtils.validateRequestContextOrThrow;

import ai.traceable.detection.exclusion.config.service.v1.CreateDetectionExclusionRuleRequest;
import ai.traceable.detection.exclusion.config.service.v1.DeleteDetectionExclusionRuleRequest;
import ai.traceable.detection.exclusion.config.service.v1.DetectionExclusionRule;
import ai.traceable.detection.exclusion.config.service.v1.DetectionExclusionRuleInfo;
import ai.traceable.detection.exclusion.config.service.v1.DetectionExclusionRuleScope;
import ai.traceable.detection.exclusion.config.service.v1.EnvironmentScope;
import ai.traceable.detection.exclusion.config.service.v1.GetDetectionExclusionRulesRequest;
import ai.traceable.detection.exclusion.config.service.v1.RuleSource;
import ai.traceable.detection.exclusion.config.service.v1.UpdateDetectionExclusionRuleRequest;
import com.google.common.annotations.VisibleForTesting;
import io.grpc.Status;
import java.util.List;
import java.util.Optional;
import javax.inject.Inject;
import org.hypertrace.core.grpcutils.context.RequestContext;

public class DetectionExclusionRulesValidator implements RulesValidator {

  private final DetectionExclusionConditionValidator conditionValidator;

  @Inject
  public DetectionExclusionRulesValidator(DetectionExclusionConditionValidator conditionValidator) {
    this.conditionValidator = conditionValidator;
  }

  @Override
  public void validateOrThrow(
      RequestContext requestContext, GetDetectionExclusionRulesRequest request) {
    validateRequestContextOrThrow(requestContext);
  }

  @Override
  public void validateOrThrow(
      RequestContext requestContext,
      UpdateDetectionExclusionRuleRequest request,
      List<DetectionExclusionRule> existingRules) {
    validateRequestContextOrThrow(requestContext);
    DetectionExclusionRule rule = request.getRule();
    String ruleName = rule.getRuleInfo().getName();
    if (!rule.getRuleInfo()
        .getRuleStatus()
        .getRuleCreationSource()
        .equals(RuleSource.RULE_SOURCE_UNSPECIFIED)) {
      throw Status.INVALID_ARGUMENT
          .withDescription(
              String.format(
                  "Update request does not allow to update rule creation source for rule with id: %s",
                  rule.getId()))
          .asRuntimeException();
    }
    Optional<DetectionExclusionRule> existingRuleWithSameName =
        getRuleForName(ruleName, existingRules);
    if (existingRuleWithSameName.isPresent()
        && !existingRuleWithSameName.get().getId().equals(rule.getId())) {
      throw Status.INVALID_ARGUMENT
          .withDescription(String.format("Rule with name %s already exists", ruleName))
          .asRuntimeException();
    }
    validateNonDefaultPresenceOrThrow(rule, DetectionExclusionRule.ID_FIELD_NUMBER);
    validateRuleInfo(rule.getRuleInfo());
    validateRuleScope(rule.getRuleScope());
  }

  @Override
  public void validateOrThrow(
      RequestContext requestContext,
      CreateDetectionExclusionRuleRequest request,
      List<DetectionExclusionRule> existingRules) {
    validateRequestContextOrThrow(requestContext);
    String ruleName = request.getRuleInfo().getName();
    Optional<DetectionExclusionRule> existingRuleWithSameName =
        getRuleForName(ruleName, existingRules);
    if (existingRuleWithSameName.isPresent()) {
      throw Status.INVALID_ARGUMENT
          .withDescription(String.format("Rule with name %s already exists", ruleName))
          .asRuntimeException();
    }
    validateRuleInfo(request.getRuleInfo());
    validateRuleScope(request.getRuleScope());
  }

  @Override
  public void validateOrThrow(
      RequestContext requestContext, DeleteDetectionExclusionRuleRequest request) {
    validateRequestContextOrThrow(requestContext);
    validateNonDefaultPresenceOrThrow(request, DeleteDetectionExclusionRuleRequest.ID_FIELD_NUMBER);
  }

  @VisibleForTesting
  public void validateRuleInfo(DetectionExclusionRuleInfo ruleInfo) {
    validateNonDefaultPresenceOrThrow(ruleInfo, DetectionExclusionRuleInfo.NAME_FIELD_NUMBER);
    if (ruleInfo.getConditionsList().isEmpty()) {
      throw Status.INVALID_ARGUMENT
          .withDescription("DetectionExclusionRule has no specified conditions")
          .asRuntimeException();
    }
    ruleInfo.getConditionsList().forEach(conditionValidator::validateRuleCondition);
  }

  private void validateRuleScope(DetectionExclusionRuleScope scope) {
    if (scope.hasEnvironmentScope()) {
      validateNonDefaultPresenceOrThrow(
          scope.getEnvironmentScope(), EnvironmentScope.ENVIRONMENT_IDS_FIELD_NUMBER);
      if (scope.getEnvironmentScope().getEnvironmentIdsList().stream().anyMatch(String::isEmpty)) {
        throw Status.INVALID_ARGUMENT
            .withDescription("Environment id should not be an empty string")
            .asRuntimeException();
      }
    }
  }

  private Optional<DetectionExclusionRule> getRuleForName(
      String ruleName, List<DetectionExclusionRule> detectionExclusionRules) {
    return detectionExclusionRules.stream()
        .filter(rule -> rule.getRuleInfo().getName().equals(ruleName))
        .findFirst();
  }
}
