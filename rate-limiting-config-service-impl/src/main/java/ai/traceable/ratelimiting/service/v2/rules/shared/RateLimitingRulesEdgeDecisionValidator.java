package ai.traceable.ratelimiting.service.v2.rules.shared;

import static ai.traceable.ratelimiting.config.service.v2.CompositeCondition.LogicalOperator.LOGICAL_OPERATOR_AND;

import ai.traceable.ratelimiting.config.service.v2.Action;
import ai.traceable.ratelimiting.config.service.v2.ApiAggregateType;
import ai.traceable.ratelimiting.config.service.v2.Category;
import ai.traceable.ratelimiting.config.service.v2.CompositeCondition;
import ai.traceable.ratelimiting.config.service.v2.Condition;
import ai.traceable.ratelimiting.config.service.v2.KeyValueCondition;
import ai.traceable.ratelimiting.config.service.v2.LeafCondition;
import ai.traceable.ratelimiting.config.service.v2.RateLimitingRuleData;
import ai.traceable.ratelimiting.config.service.v2.ResourceAccessThresholdConfig;
import ai.traceable.ratelimiting.config.service.v2.ResourceAccessThresholdConfig.ValueType;
import ai.traceable.ratelimiting.config.service.v2.ThresholdActionConfig;
import ai.traceable.ratelimiting.service.v2.rules.ValidatorUtils;

public class RateLimitingRulesEdgeDecisionValidator {

  private static final ValidatorUtils validatorUtils = new ValidatorUtils();

  public static void checkConversionToEdgeDecisionRules(RateLimitingRuleData data) {
    if (data.getCategory().equals(Category.CATEGORY_RATE_LIMITING)) {
      final boolean isCompatibleCondition = isCompatibleCondition(data.getCondition());
      final boolean hasScopeConditionWithSpecificEntityIds =
          hasScopeConditionWithSpecificEntityIds(data.getCondition());
      data.getThresholdActionConfigsList()
          .forEach(
              thresholdActionConfig ->
                  RateLimitingRulesEdgeDecisionValidator.isCompatibleThresholdActionConfig(
                      thresholdActionConfig,
                      isCompatibleCondition,
                      hasScopeConditionWithSpecificEntityIds));
    } else if (data.getCategory().equals(Category.CATEGORY_ENUMERATION)) {
      final boolean isCompatibleCondition = isCompatibleCondition(data.getCondition());
      final boolean hasScopeConditionWithSpecificEntityIds =
          hasScopeConditionWithSpecificEntityIds(data.getCondition());
      data.getThresholdActionConfigsList()
          .forEach(
              thresholdActionConfig ->
                  RateLimitingRulesEdgeDecisionValidator
                      .isCompatibleThresholdActionConfigForEnumerationRule(
                          thresholdActionConfig,
                          isCompatibleCondition,
                          hasScopeConditionWithSpecificEntityIds));
    }
  }

  private static boolean isCompatibleCondition(Condition condition) {
    switch (condition.getConditionCase()) {
      case LEAF_CONDITION:
        final LeafCondition leafCondition = condition.getLeafCondition();
        return !(leafCondition.hasDatatypeCondition()
            || leafCondition.hasRequestScannerTypeCondition()
            || (leafCondition.hasKeyValueCondition()
                && isIncompatibleKeyValueType(leafCondition.getKeyValueCondition().getType())));
      case COMPOSITE_CONDITION:
        return condition.getCompositeCondition().getChildrenList().stream()
            .allMatch(RateLimitingRulesEdgeDecisionValidator::isCompatibleCondition);
      default:
        return false;
    }
  }

  private static boolean hasScopeConditionWithSpecificEntityIds(Condition condition) {
    switch (condition.getConditionCase()) {
      case LEAF_CONDITION:
        LeafCondition leafCondition = condition.getLeafCondition();
        return leafCondition.hasScopeCondition()
            && (leafCondition.getScopeCondition().hasEntityScope()
                || leafCondition.getScopeCondition().hasLabelScope());
      case COMPOSITE_CONDITION:
        CompositeCondition compositeCondition = condition.getCompositeCondition();
        return compositeCondition.getOperator().equals(LOGICAL_OPERATOR_AND)
            && compositeCondition.getChildrenList().stream()
                .anyMatch(
                    RateLimitingRulesEdgeDecisionValidator::hasScopeConditionWithSpecificEntityIds);
      default:
        return false;
    }
  }

  private static boolean isIncompatibleKeyValueType(KeyValueCondition.Type type) {
    switch (type) {
      case TYPE_RESPONSE_BODY:
      case TYPE_RESPONSE_HEADER:
      case TYPE_RESPONSE_COOKIE:
      case TYPE_RESPONSE_BODY_PARAMETER:
      case TYPE_TAG:
      case TYPE_RESPONSE_BODY_SIZE:
      case TYPE_RESPONSE_HEADERS_COUNT:
      case TYPE_RESPONSE_COOKIES_COUNT:
        return true;
      default:
        return false;
    }
  }

  private static void isCompatibleThresholdActionConfig(
      ThresholdActionConfig thresholdActionConfig,
      boolean isCompatibleEdgeDecisionCondition,
      boolean hasScopeConditionWithSpecificEntityIds) {
    final boolean isCompatibleThresholdActionConfig =
        isCompatibleEdgeDecisionCondition
            && thresholdActionConfig.getResourceAccessThresholdConfigsList().stream()
                .allMatch(ResourceAccessThresholdConfig::hasRollingWindowThresholdConfig);
    final boolean perEndPointAggregation =
        thresholdActionConfig.getResourceAccessThresholdConfigsList().stream()
            .anyMatch(
                resourceAccessThresholdConfig ->
                    resourceAccessThresholdConfig
                        .getApiAggregateType()
                        .equals(ApiAggregateType.API_AGGREGATE_TYPE_PER_ENDPOINT));
    thresholdActionConfig
        .getActionsList()
        .forEach(
            action ->
                isCompatibleAction(
                    action,
                    isCompatibleThresholdActionConfig,
                    perEndPointAggregation,
                    hasScopeConditionWithSpecificEntityIds));
  }

  private static void isCompatibleThresholdActionConfigForEnumerationRule(
      ThresholdActionConfig thresholdActionConfig,
      boolean isCompatibleEdgeDecisionCondition,
      boolean hasScopeConditionWithSpecificEntityIds) {
    boolean isCompatibleResourceAccessThresholdConfig =
        isCompatibleEdgeDecisionCondition
            && thresholdActionConfig.getResourceAccessThresholdConfigsList().stream()
                .allMatch(
                    resourceAccessThresholdConfig ->
                        isCompatibleResourceAccessThresholdConfigForEnumerationRule(
                            resourceAccessThresholdConfig, hasScopeConditionWithSpecificEntityIds));
    thresholdActionConfig
        .getActionsList()
        .forEach(
            action ->
                isCompatibleActionForEnumerationRule(
                    action, isCompatibleResourceAccessThresholdConfig));
  }

  private static boolean isCompatibleResourceAccessThresholdConfigForEnumerationRule(
      ResourceAccessThresholdConfig resourceAccessThresholdConfig,
      boolean hasScopeConditionWithSpecificEntityIds) {
    ValueType valueType =
        resourceAccessThresholdConfig.getValueBasedThresholdConfig().getValueType();
    if (valueType == ValueType.VALUE_TYPE_SENSITIVE_PARAMS) {
      return false;
    }
    if (valueType == ValueType.VALUE_TYPE_PATH_PARAMS) {
      return hasScopeConditionWithSpecificEntityIds
          && resourceAccessThresholdConfig.getApiAggregateType()
              != ApiAggregateType.API_AGGREGATE_TYPE_ACROSS_ENDPOINTS;
    }
    return true;
  }

  private static void isCompatibleAction(
      Action action,
      boolean isCompatibleThresholdActionConfig,
      boolean perEndPointAggregation,
      boolean hasScopeConditionWithSpecificEntityIds) {
    switch (action.getActionCase()) {
      case ALERT:
        if (action.getAlert().hasAgentRuleEffect()) {
          if (!isCompatibleThresholdActionConfig) {
            validatorUtils.throwInvalidArgumentException(
                "Alert action is incompatible with current rule conditions.");
          }
          if (perEndPointAggregation && !hasScopeConditionWithSpecificEntityIds) {
            validatorUtils.throwInvalidArgumentException(
                "Alert action with per endpoint will work only with scope set of specific endpoints or endpoint labels");
          }
        }
      case MARK_FOR_TESTING:
        if (action.getMarkForTesting().hasAgentRuleEffect()) {
          if (!isCompatibleThresholdActionConfig) {
            validatorUtils.throwInvalidArgumentException(
                "Mark for testing action is incompatible with current rule conditions.");
          }
          if (perEndPointAggregation && !hasScopeConditionWithSpecificEntityIds) {
            validatorUtils.throwInvalidArgumentException(
                "Mark for testing action with per endpoint will work only with scope set of specific endpoints or endpoint labels");
          }
        }
        break;
      case BLOCK:
        Action.Block block = action.getBlock();
        if (block.getUseThresholdDuration()) {
          if (!isCompatibleThresholdActionConfig) {
            validatorUtils.throwInvalidArgumentException(
                "Block action with use threshold config is incompatible with current rule conditions.");
          }
          if (perEndPointAggregation && !hasScopeConditionWithSpecificEntityIds) {
            validatorUtils.throwInvalidArgumentException(
                "Block action with per endpoint will work only with scope set to specific endpoints or endpoint labels");
          }
        }
        break;
      default:
        // Nothing to do here
    }
  }

  private static void isCompatibleActionForEnumerationRule(
      Action action, boolean isCompatibleResourceAccessThresholdConfig) {
    switch (action.getActionCase()) {
      case ALERT:
        if (action.getAlert().hasAgentRuleEffect() && !isCompatibleResourceAccessThresholdConfig) {
          validatorUtils.throwInvalidArgumentException(
              "Alert action is incompatible with current rule conditions.");
        }
      case MARK_FOR_TESTING:
        if (action.getMarkForTesting().hasAgentRuleEffect()
            && !isCompatibleResourceAccessThresholdConfig) {
          validatorUtils.throwInvalidArgumentException(
              "Mark for testing action is incompatible with current rule conditions.");
        }
        break;
      case BLOCK:
        Action.Block block = action.getBlock();
        if (block.getUseThresholdDuration() && !isCompatibleResourceAccessThresholdConfig) {
          validatorUtils.throwInvalidArgumentException(
              "Block action with use threshold config is incompatible with current rule conditions.");
        }
        break;
      default:
        // Nothing to do here
    }
  }
}
