package ai.traceable.ratelimiting.service.v2.rules;

import static ai.traceable.ratelimiting.config.service.v2.RuleStatus.RuleSource.RULE_SOURCE_UNSPECIFIED;
import static org.hypertrace.config.validation.GrpcValidatorUtils.printMessage;
import static org.hypertrace.config.validation.GrpcValidatorUtils.validateNonDefaultPresenceOrThrow;
import static org.hypertrace.config.validation.GrpcValidatorUtils.validateRequestContextOrThrow;

import ai.traceable.ratelimiting.config.service.v2.Action;
import ai.traceable.ratelimiting.config.service.v2.ApiAggregateType;
import ai.traceable.ratelimiting.config.service.v2.Category;
import ai.traceable.ratelimiting.config.service.v2.Condition;
import ai.traceable.ratelimiting.config.service.v2.CreateRateLimitingRuleRequest;
import ai.traceable.ratelimiting.config.service.v2.DeleteRateLimitingRuleRequest;
import ai.traceable.ratelimiting.config.service.v2.EnvironmentScope;
import ai.traceable.ratelimiting.config.service.v2.GetRateLimitingRulesRequest;
import ai.traceable.ratelimiting.config.service.v2.RateLimitingRule;
import ai.traceable.ratelimiting.config.service.v2.RateLimitingRuleData;
import ai.traceable.ratelimiting.config.service.v2.ResourceAccessThresholdConfig;
import ai.traceable.ratelimiting.config.service.v2.RuleConfigScope;
import ai.traceable.ratelimiting.config.service.v2.ThresholdActionConfig;
import ai.traceable.ratelimiting.config.service.v2.UpdateRateLimitingRuleRequest;
import ai.traceable.ratelimiting.config.service.v2.UserAggregateType;
import java.util.List;
import java.util.Optional;
import org.hypertrace.core.grpcutils.context.RequestContext;

public class RateLimitingRulesValidator implements RulesValidator {
  private static final ValidatorUtils validatorUtils = new ValidatorUtils();
  private static final TransactionActionConfigValidator transactionActionConfigValidator =
      new TransactionActionConfigValidator();

  @Override
  public void validateOrThrow(RequestContext requestContext, GetRateLimitingRulesRequest request) {
    validateRequestContextOrThrow(requestContext);
  }

  @Override
  public void validateOrThrow(
      RequestContext requestContext,
      UpdateRateLimitingRuleRequest request,
      List<RateLimitingRule> existingRules) {
    validateRequestContextOrThrow(requestContext);
    validateNonDefaultPresenceOrThrow(request, UpdateRateLimitingRuleRequest.RULE_ID_FIELD_NUMBER);
    RateLimitingRuleData requestData = request.getData();
    if (!requestData.getRuleStatus().getRuleCreationSource().equals(RULE_SOURCE_UNSPECIFIED)) {
      validatorUtils.throwInvalidArgumentException(
          String.format(
              "Update request does not allow to update rule creation source for rule with id: %s",
              request.getRuleId()));
    }
    Optional<RateLimitingRule> rule =
        getRuleOfSameNameAndCategory(
            requestData.getCategory(), requestData.getName(), existingRules);
    if (rule.isPresent() && !rule.get().getId().equals(request.getRuleId())) {
      validatorUtils.throwInvalidArgumentException(
          String.format(
              "Rate limiting rule with name (%s) already exists for category : %s",
              requestData.getName(), requestData.getCategory()));
    }
    validateRateLimitingRuleData(requestData);
  }

  @Override
  public void validateOrThrow(
      RequestContext requestContext, DeleteRateLimitingRuleRequest request) {
    validateRequestContextOrThrow(requestContext);
    validateNonDefaultPresenceOrThrow(request, DeleteRateLimitingRuleRequest.RULE_ID_FIELD_NUMBER);
  }

  @Override
  public void validateOrThrow(
      RequestContext requestContext,
      CreateRateLimitingRuleRequest request,
      List<RateLimitingRule> existingRules) {
    validateRequestContextOrThrow(requestContext);
    RateLimitingRuleData requestData = request.getData();
    Optional<RateLimitingRule> rule =
        getRuleOfSameNameAndCategory(
            requestData.getCategory(), requestData.getName(), existingRules);
    if (rule.isPresent()) {
      validatorUtils.throwInvalidArgumentException(
          String.format(
              "Rate limiting rule with name (%s) already exists for category : %s",
              requestData.getName(), requestData.getCategory()));
    }
    validateRateLimitingRuleData(requestData);
  }

  private Optional<RateLimitingRule> getRuleOfSameNameAndCategory(
      Category category, String name, List<RateLimitingRule> existingRules) {
    Optional<RateLimitingRule> sameNameRuleOfSameCategory =
        existingRules.stream()
            .filter(
                rule ->
                    rule.getData().getCategory().equals(category)
                        && rule.getData().getName().equals(name))
            .findFirst();
    return sameNameRuleOfSameCategory;
  }

  private void validateRateLimitingRuleData(RateLimitingRuleData data) {
    validateNonDefaultPresenceOrThrow(data, RateLimitingRuleData.CATEGORY_FIELD_NUMBER);
    validateNonDefaultPresenceOrThrow(data, RateLimitingRuleData.NAME_FIELD_NUMBER);
    validateRuleConfigScope(data.getRuleConfigScope());
    if (data.hasTransactionActionConfig()) {
      transactionActionConfigValidator.validateRateLimitingRuleData(data);
    } else {
      validateNonDefaultPresenceOrThrow(
          data, RateLimitingRuleData.THRESHOLD_ACTION_CONFIGS_FIELD_NUMBER);
      data.getThresholdActionConfigsList().forEach(this::validateThresholdActionConfig);
      validateCondition(data.getCondition());
    }
  }

  private void validateRuleConfigScope(RuleConfigScope scope) {
    if (scope.hasEnvironmentScope()) {
      validateNonDefaultPresenceOrThrow(
          scope.getEnvironmentScope(), EnvironmentScope.ENVIRONMENT_IDS_FIELD_NUMBER);
      if (scope.getEnvironmentScope().getEnvironmentIdsList().stream().anyMatch(String::isEmpty)) {
        validatorUtils.throwInvalidArgumentException("Environment id should not be empty string.");
      }
    }
  }

  private void validateCondition(Condition condition) {
    switch (condition.getConditionCase()) {
      case LEAF_CONDITION:
        validatorUtils.validateLeafCondition(condition.getLeafCondition());
        break;
      case COMPOSITE_CONDITION:
        validatorUtils.validateCompositeCondition(condition.getCompositeCondition());
        break;
      default:
        validatorUtils.throwInvalidArgumentException(
            String.format(
                "Invalid Case in %s:%n %s",
                validatorUtils.getName(condition), printMessage(condition)));
    }
  }

  private void validateThresholdActionConfig(ThresholdActionConfig thresholdActionConfig) {
    validateNonDefaultPresenceOrThrow(
        thresholdActionConfig,
        ThresholdActionConfig.RESOURCE_ACCESS_THRESHOLD_CONFIGS_FIELD_NUMBER);
    thresholdActionConfig
        .getResourceAccessThresholdConfigsList()
        .forEach(this::validateResourceAccessThresholdConfig);
    validateNonDefaultPresenceOrThrow(
        thresholdActionConfig, ThresholdActionConfig.ACTIONS_FIELD_NUMBER);

    boolean userAggregationAcrossAllPresent =
        thresholdActionConfig.getResourceAccessThresholdConfigsList().stream()
            .map(ResourceAccessThresholdConfig::getUserAggregateType)
            .anyMatch(
                userAggregateType ->
                    userAggregateType.equals(UserAggregateType.USER_AGGREGATE_TYPE_ACROSS_USERS));

    thresholdActionConfig
        .getActionsList()
        .forEach(action -> validateAction(action, userAggregationAcrossAllPresent));
  }

  private void validateResourceAccessThresholdConfig(
      ResourceAccessThresholdConfig resourceAccessThresholdConfig) {
    validateNonDefaultPresenceOrThrow(
        resourceAccessThresholdConfig,
        ResourceAccessThresholdConfig.USER_AGGREGATE_TYPE_FIELD_NUMBER);
    validateNonDefaultPresenceOrThrow(
        resourceAccessThresholdConfig,
        ResourceAccessThresholdConfig.API_AGGREGATE_TYPE_FIELD_NUMBER);
    switch (resourceAccessThresholdConfig.getThresholdConfigCase()) {
      case ROLLING_WINDOW_THRESHOLD_CONFIG:
        validateRollingWindowThresholdConfig(
            resourceAccessThresholdConfig.getRollingWindowThresholdConfig());
        break;
      case VALUE_BASED_THRESHOLD_CONFIG:
        validateValueBasedThresholdConfig(
            resourceAccessThresholdConfig.getValueBasedThresholdConfig());
        break;
      case DYNAMIC_THRESHOLD_CONFIG:
        validateDynamicThresholdConfig(resourceAccessThresholdConfig.getDynamicThresholdConfig());
        if (!resourceAccessThresholdConfig
                .getUserAggregateType()
                .equals(UserAggregateType.USER_AGGREGATE_TYPE_PER_USER)
            || !resourceAccessThresholdConfig
                .getApiAggregateType()
                .equals(ApiAggregateType.API_AGGREGATE_TYPE_PER_ENDPOINT)) {
          validatorUtils.throwInvalidArgumentException(
              "Dynamic threshold supports aggregation only on per user and per api endpoint");
        }
        break;
      default:
        validatorUtils.throwInvalidArgumentException(
            String.format(
                "Invalid Case in %s:%n %s",
                validatorUtils.getName(resourceAccessThresholdConfig),
                printMessage(resourceAccessThresholdConfig)));
    }
  }

  private void validateRollingWindowThresholdConfig(
      ResourceAccessThresholdConfig.RollingWindowThresholdConfig rollingWindowThresholdConfig) {
    if (rollingWindowThresholdConfig.getCountAllowed() < 0) {
      validatorUtils.throwInvalidArgumentException(
          "Count allowed should be greater than or equal to 0 for rolling threshold config");
    }
    validateNonDefaultPresenceOrThrow(
        rollingWindowThresholdConfig,
        ResourceAccessThresholdConfig.RollingWindowThresholdConfig.DURATION_ISO_FIELD_NUMBER);
  }

  private void validateValueBasedThresholdConfig(
      ResourceAccessThresholdConfig.ValueBasedThresholdConfig valueBasedThresholdConfig) {
    validateNonDefaultPresenceOrThrow(
        valueBasedThresholdConfig,
        ResourceAccessThresholdConfig.ValueBasedThresholdConfig.UNIQUE_VALUES_ALLOWED_FIELD_NUMBER);
    validateNonDefaultPresenceOrThrow(
        valueBasedThresholdConfig,
        ResourceAccessThresholdConfig.ValueBasedThresholdConfig.DURATION_ISO_FIELD_NUMBER);
    validateNonDefaultPresenceOrThrow(
        valueBasedThresholdConfig,
        ResourceAccessThresholdConfig.ValueBasedThresholdConfig.VALUE_TYPE_FIELD_NUMBER);
  }

  private void validateDynamicThresholdConfig(
      ResourceAccessThresholdConfig.DynamicThresholdConfig dynamicThresholdConfig) {
    validateNonDefaultPresenceOrThrow(
        dynamicThresholdConfig,
        ResourceAccessThresholdConfig.DynamicThresholdConfig
            .PERCENT_EXCEEDING_MEAN_ALLOWED_FIELD_NUMBER);
    if (!dynamicThresholdConfig.hasMeanCalculationDuration()) {
      validatorUtils.throwInvalidArgumentException(
          "Dynamic threshold config does not have valid mean calculation duration");
    }
    if (!dynamicThresholdConfig.hasDuration()) {
      validatorUtils.throwInvalidArgumentException(
          "Dynamic threshold config does not have valid duration");
    }
  }

  private void validateAction(Action action, boolean aggregateAcrossAllUsersPresent) {
    switch (action.getActionCase()) {
      case ALERT:
        break;
      case BLOCK:
        if (aggregateAcrossAllUsersPresent) {
          validatorUtils.throwInvalidArgumentException(
              "Block action unsupported on aggregation across users");
        }
        break;
      default:
        validatorUtils.throwInvalidArgumentException(
            String.format(
                "Invalid Case in %s:%n %s", validatorUtils.getName(action), printMessage(action)));
    }
  }
}
