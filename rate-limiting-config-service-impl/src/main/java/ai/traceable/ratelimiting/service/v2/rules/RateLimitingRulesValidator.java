package ai.traceable.ratelimiting.service.v2.rules;

import static ai.traceable.ratelimiting.config.service.v2.Action.MatchCategory.MATCH_CATEGORY_REQUEST;
import static ai.traceable.ratelimiting.config.service.v2.RuleStatus.RuleSource.RULE_SOURCE_DEFAULT;
import static ai.traceable.ratelimiting.config.service.v2.RuleStatus.RuleSource.RULE_SOURCE_UNSPECIFIED;
import static ai.traceable.ratelimiting.service.v2.rules.shared.RateLimitingRulesEdgeDecisionValidator.checkConversionToEdgeDecisionRules;
import static org.hypertrace.config.validation.GrpcValidatorUtils.printMessage;
import static org.hypertrace.config.validation.GrpcValidatorUtils.validateNonDefaultPresenceOrThrow;
import static org.hypertrace.config.validation.GrpcValidatorUtils.validateRequestContextOrThrow;

import ai.traceable.ratelimiting.config.service.v2.Action;
import ai.traceable.ratelimiting.config.service.v2.Action.BodyModification;
import ai.traceable.ratelimiting.config.service.v2.Action.HeaderInjection;
import ai.traceable.ratelimiting.config.service.v2.ApiAggregateType;
import ai.traceable.ratelimiting.config.service.v2.Category;
import ai.traceable.ratelimiting.config.service.v2.Condition;
import ai.traceable.ratelimiting.config.service.v2.CreateRateLimitingRuleRequest;
import ai.traceable.ratelimiting.config.service.v2.DataLocation;
import ai.traceable.ratelimiting.config.service.v2.DeleteRateLimitingRuleRequest;
import ai.traceable.ratelimiting.config.service.v2.EnvironmentScope;
import ai.traceable.ratelimiting.config.service.v2.GetRateLimitingEdgeDecisionRulesRequest;
import ai.traceable.ratelimiting.config.service.v2.GetRateLimitingModsecRulesFilter;
import ai.traceable.ratelimiting.config.service.v2.GetRateLimitingModsecRulesFilter.RuleAction;
import ai.traceable.ratelimiting.config.service.v2.GetRateLimitingRuleModsecRulesRequest;
import ai.traceable.ratelimiting.config.service.v2.GetRateLimitingRulesFilter;
import ai.traceable.ratelimiting.config.service.v2.GetRateLimitingRulesRequest;
import ai.traceable.ratelimiting.config.service.v2.RateLimitingRule;
import ai.traceable.ratelimiting.config.service.v2.RateLimitingRuleData;
import ai.traceable.ratelimiting.config.service.v2.ResourceAccessThresholdConfig;
import ai.traceable.ratelimiting.config.service.v2.RuleConfigScope;
import ai.traceable.ratelimiting.config.service.v2.RuleStatus;
import ai.traceable.ratelimiting.config.service.v2.ThresholdActionConfig;
import ai.traceable.ratelimiting.config.service.v2.UpdateRateLimitingRuleRequest;
import ai.traceable.ratelimiting.config.service.v2.UserAggregateType;
import io.grpc.Status;
import java.util.List;
import java.util.Optional;
import org.hypertrace.core.grpcutils.context.RequestContext;

public class RateLimitingRulesValidator implements RulesValidator {
  private static final Integer CUSTOM_LABELS_LIMIT = 5;
  private static final ValidatorUtils validatorUtils = new ValidatorUtils();
  private static final TransactionActionConfigValidator transactionActionConfigValidator =
      new TransactionActionConfigValidator();

  @Override
  public void validateOrThrow(RequestContext requestContext, GetRateLimitingRulesRequest request) {
    validateRequestContextOrThrow(requestContext);
    validateFilter(request.getRulesFilter());
  }

  @Override
  public void validateOrThrow(
      RequestContext requestContext, GetRateLimitingEdgeDecisionRulesRequest request) {
    validateRequestContextOrThrow(requestContext);
    validateFilter(request.getRulesFilter());
  }

  @Override
  public void validateOrThrow(
      RequestContext requestContext, GetRateLimitingRuleModsecRulesRequest request) {
    validateRequestContextOrThrow(requestContext);
    validateModsecFilter(request.getRulesFilter());
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
      validatorUtils.throwAlreadyExistsException(
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
      validatorUtils.throwAlreadyExistsException(
          String.format(
              "Rate limiting rule with name (%s) already exists for category : %s",
              requestData.getName(), requestData.getCategory()));
    }
    validateRateLimitingRuleData(requestData);
    validateRuleCreationSource(requestData.getRuleStatus().getRuleCreationSource());
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

  private void validateRuleCreationSource(RuleStatus.RuleSource ruleSource) {
    if (ruleSource.equals(RULE_SOURCE_DEFAULT)) {
      throw Status.INVALID_ARGUMENT
          .withDescription("Rule source cannot be set to default while creating a rule")
          .asRuntimeException();
    }
  }

  private void validateRateLimitingRuleData(RateLimitingRuleData data) {
    if (data.getLabelsMap().size() > CUSTOM_LABELS_LIMIT) {
      validatorUtils.throwInvalidArgumentException("Custom labels limit exceeded");
    }
    validateNonDefaultPresenceOrThrow(data, RateLimitingRuleData.CATEGORY_FIELD_NUMBER);
    validateNonDefaultPresenceOrThrow(data, RateLimitingRuleData.NAME_FIELD_NUMBER);
    validateRuleConfigScope(data.getRuleConfigScope());
    if (data.hasTransactionActionConfig()) {
      transactionActionConfigValidator.validateRateLimitingRuleData(data);
    } else {
      validateNonDefaultPresenceOrThrow(
          data, RateLimitingRuleData.THRESHOLD_ACTION_CONFIGS_FIELD_NUMBER);
      boolean isSelectedDataTypesSensitiveParamsEvaluationValid =
          isSelectedDataTypesSensitiveParamsEvaluationValid(
              data.getCondition(), data.getCategory());
      data.getThresholdActionConfigsList()
          .forEach(
              thresholdActionConfig ->
                  this.validateThresholdActionConfig(
                      thresholdActionConfig, isSelectedDataTypesSensitiveParamsEvaluationValid));
      validateCondition(data.getCondition());
      checkConversionToEdgeDecisionRules(data);
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

  private void validateFilter(GetRateLimitingRulesFilter filter) {
    if (filter.getCategoriesList().contains(Category.CATEGORY_UNSPECIFIED)) {
      throw Status.INVALID_ARGUMENT
          .withDescription("Cannot filter for UNSPECIFIED category")
          .asRuntimeException();
    }
    if (filter.hasFilterEdgeDecisionRules() && !filter.getFilterEdgeDecisionRules()) {
      throw Status.INVALID_ARGUMENT
          .withDescription(
              "Need to use get edge decision rules api and not call this api with filterEdgeDecisionRules set to false")
          .asRuntimeException();
    }
  }

  private void validateModsecFilter(GetRateLimitingModsecRulesFilter filter) {
    validateFilter(filter.getRulesFilter());
    if (filter.getRuleActionsList().contains(RuleAction.RULE_ACTION_UNSPECIFIED)) {
      throw Status.INVALID_ARGUMENT
          .withDescription("Cannot filter for UNSPECIFIED rule actions")
          .asRuntimeException();
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

  private void validateThresholdActionConfig(
      ThresholdActionConfig thresholdActionConfig,
      boolean isSelectedDataTypesSensitiveParamsEvaluationValid) {
    validateNonDefaultPresenceOrThrow(
        thresholdActionConfig,
        ThresholdActionConfig.RESOURCE_ACCESS_THRESHOLD_CONFIGS_FIELD_NUMBER);
    thresholdActionConfig
        .getResourceAccessThresholdConfigsList()
        .forEach(
            resourceAccessThresholdConfig ->
                validateResourceAccessThresholdConfig(
                    resourceAccessThresholdConfig,
                    isSelectedDataTypesSensitiveParamsEvaluationValid));
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
      ResourceAccessThresholdConfig resourceAccessThresholdConfig,
      boolean isSelectedDataTypesSensitiveParamsEvaluationValid) {
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
            resourceAccessThresholdConfig.getValueBasedThresholdConfig(),
            isSelectedDataTypesSensitiveParamsEvaluationValid);
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
      ResourceAccessThresholdConfig.ValueBasedThresholdConfig valueBasedThresholdConfig,
      boolean isSelectedDataTypesSensitiveParamsEvaluationValid) {
    validateNonDefaultPresenceOrThrow(
        valueBasedThresholdConfig,
        ResourceAccessThresholdConfig.ValueBasedThresholdConfig.UNIQUE_VALUES_ALLOWED_FIELD_NUMBER);
    validateNonDefaultPresenceOrThrow(
        valueBasedThresholdConfig,
        ResourceAccessThresholdConfig.ValueBasedThresholdConfig.DURATION_ISO_FIELD_NUMBER);
    validateNonDefaultPresenceOrThrow(
        valueBasedThresholdConfig,
        ResourceAccessThresholdConfig.ValueBasedThresholdConfig.VALUE_TYPE_FIELD_NUMBER);
    if (!valueBasedThresholdConfig
            .getValueType()
            .equals(ResourceAccessThresholdConfig.ValueType.VALUE_TYPE_SENSITIVE_PARAMS)
        && !valueBasedThresholdConfig
            .getSensitiveParamsEvaluation()
            .equals(
                ResourceAccessThresholdConfig.SensitiveParamsEvaluation
                    .SENSITIVE_PARAMS_EVALUATION_UNSPECIFIED)) {
      validatorUtils.throwInvalidArgumentException(
          String.format(
              "Sensitive params evaluation not applicable for value type : %s",
              valueBasedThresholdConfig.getValueType()));
    }
    if (!isSelectedDataTypesSensitiveParamsEvaluationValid
        && valueBasedThresholdConfig
            .getSensitiveParamsEvaluation()
            .equals(
                ResourceAccessThresholdConfig.SensitiveParamsEvaluation
                    .SENSITIVE_PARAMS_EVALUATION_SELECTED_DATA_TYPES)) {
      validatorUtils.throwInvalidArgumentException(
          "Selected data types sensitive params evaluation not valid for the rule");
    }
  }

  private boolean isSelectedDataTypesSensitiveParamsEvaluationValid(
      Condition condition, Category category) {
    switch (category) {
      case CATEGORY_ENUMERATION:
        return isSelectedDataTypesSensitiveParamsEvaluationValid(
            condition, DataLocation.DATA_LOCATION_REQUEST);
      case CATEGORY_DATA_EXFILTRATION:
        return isSelectedDataTypesSensitiveParamsEvaluationValid(
            condition, DataLocation.DATA_LOCATION_RESPONSE);
      default:
        return false;
    }
  }

  private boolean isSelectedDataTypesSensitiveParamsEvaluationValid(
      Condition condition, DataLocation dataLocation) {
    switch (condition.getConditionCase()) {
      case COMPOSITE_CONDITION:
        return condition.getCompositeCondition().getChildrenList().stream()
            .anyMatch(
                childCondition ->
                    isSelectedDataTypesSensitiveParamsEvaluationValid(
                        childCondition, dataLocation));
      case LEAF_CONDITION:
        if (condition.getLeafCondition().hasDatatypeCondition()) {
          DataLocation location =
              condition.getLeafCondition().getDatatypeCondition().getDataLocation();
          return location.equals(dataLocation)
              || location.equals(DataLocation.DATA_LOCATION_UNSPECIFIED);
        }
        return false;
      default:
        return false;
    }
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
      case MARK_FOR_TESTING:
        validateMarkForTestingAction(action.getMarkForTesting());
        break;
      case ALERT:
        validateAlertAction(action.getAlert());
        break;
      case BLOCK:
        validateBlockAction(aggregateAcrossAllUsersPresent, action.getBlock());
        break;
      default:
        validatorUtils.throwInvalidArgumentException(
            String.format(
                "Invalid Case in %s:%n %s", validatorUtils.getName(action), printMessage(action)));
    }
  }

  private void validateBlockAction(boolean aggregateAcrossAllUsersPresent, Action.Block block) {
    if (aggregateAcrossAllUsersPresent && !block.hasUseThresholdDuration()) {
      validatorUtils.throwInvalidArgumentException(
          "Block action unsupported on aggregation across users");
    }
    if (block.hasDurationIso() && block.getUseThresholdDuration()) {
      validatorUtils.throwInvalidArgumentException(
          "Either duration or use threshold duration can be configured. Both of them cannot be configured together");
    }
    validateNonDefaultPresenceOrThrow(block, Action.Block.EVENT_SEVERITY_FIELD_NUMBER);
  }

  private void validateAlertAction(Action.Alert alert) {
    validateNonDefaultPresenceOrThrow(alert, Action.Alert.EVENT_SEVERITY_FIELD_NUMBER);
    if (alert.hasAgentRuleEffect()) {
      validategentRuleEffect(alert.getAgentRuleEffect());
    }
  }

  private void validateMarkForTestingAction(Action.MarkForTesting markForTesting) {
    validateNonDefaultPresenceOrThrow(
        markForTesting, Action.MarkForTesting.EVENT_SEVERITY_FIELD_NUMBER);
    if (markForTesting.hasAgentRuleEffect()) {
      validategentRuleEffect(markForTesting.getAgentRuleEffect());
    }
  }

  private void validategentRuleEffect(Action.AgentRuleEffect agentRuleEffect) {
    if (agentRuleEffect.getAgentModificationsList().isEmpty()) {
      validatorUtils.throwInvalidArgumentException(
          "Agent rule effect should have at least one modification.");
    }

    agentRuleEffect
        .getAgentModificationsList()
        .forEach(
            agentModification -> {
              // Check which oneof field is set and validate accordingly
              if (agentModification.hasHeaderInjection()) {
                HeaderInjection headerInjection = agentModification.getHeaderInjection();
                validateNonDefaultPresenceOrThrow(
                    headerInjection, HeaderInjection.HEADER_CATEGORY_FIELD_NUMBER);
                validateNonDefaultPresenceOrThrow(
                    headerInjection, HeaderInjection.HEADER_NAME_FIELD_NUMBER);
                validateNonDefaultPresenceOrThrow(
                    headerInjection.getValue(), Action.FieldValue.STATIC_VALUE_FIELD_NUMBER);
              } else if (agentModification.hasStatusCodeModification()) {
                // Do nothing
              } else if (agentModification.hasBodyModification()) {
                BodyModification bodyModification = agentModification.getBodyModification();
                validatorUtils.throwInvalidArgumentExceptionIf(
                    agentModification
                        .getBodyModification()
                        .getLocationCategory()
                        .equals(MATCH_CATEGORY_REQUEST),
                    "Request body modification not supported yet.");
                validateNonDefaultPresenceOrThrow(
                    bodyModification.getBodyValue(), Action.FieldValue.STATIC_VALUE_FIELD_NUMBER);
              } else {
                validatorUtils.throwInvalidArgumentException(
                    "Agent modification must specify a valid modification type.");
              }
            });
  }
}
