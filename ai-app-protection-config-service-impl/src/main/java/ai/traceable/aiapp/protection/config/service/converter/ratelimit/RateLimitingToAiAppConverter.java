package ai.traceable.aiapp.protection.config.service.converter.ratelimit;

import static ai.traceable.aiapp.protection.config.service.converter.AiAppConverterConstants.AI_RATE_LIMITING_THREAT_TYPE_ID;
import static ai.traceable.aiapp.protection.config.service.converter.AiAppConverterConstants.AI_SENSITIVE_DATA_PROTECTION_THREAT_TYPE_ID;
import static ai.traceable.aiapp.protection.config.service.converter.AiAppConverterConstants.GENAI_MODELS_ATTRIBUTE_KEY;
import static ai.traceable.aiapp.protection.config.service.converter.AiAppConverterConstants.GENAI_PROVIDERS_ATTRIBUTE_KEY;
import static ai.traceable.aiapp.protection.config.service.converter.AiAppConverterConstants.PII_DETECTED_IN_PROMPT_THREAT_TYPE_ID;
import static ai.traceable.aiapp.protection.config.service.converter.AiAppConverterConstants.THREAT_TYPE_ID_LABEL_KEY;

import ai.traceable.aiapp.protection.config.service.v1.Action;
import ai.traceable.aiapp.protection.config.service.v1.AiAppCustomRule;
import ai.traceable.aiapp.protection.config.service.v1.AiAppCustomRuleData;
import ai.traceable.aiapp.protection.config.service.v1.AiRateLimitingRuleData;
import ai.traceable.aiapp.protection.config.service.v1.AiSensitiveDataProtectionRuleData;
import ai.traceable.aiapp.protection.config.service.v1.ApiAggregateType;
import ai.traceable.aiapp.protection.config.service.v1.DatatypeCondition;
import ai.traceable.aiapp.protection.config.service.v1.MatchOperator;
import ai.traceable.aiapp.protection.config.service.v1.MatchOperatorCondition;
import ai.traceable.aiapp.protection.config.service.v1.PiiDetectedInPromptRuleData;
import ai.traceable.aiapp.protection.config.service.v1.ResourceAccessThresholdConfig;
import ai.traceable.aiapp.protection.config.service.v1.RuleScope;
import ai.traceable.aiapp.protection.config.service.v1.RuleStatusDetails;
import ai.traceable.aiapp.protection.config.service.v1.ScopeCondition;
import ai.traceable.aiapp.protection.config.service.v1.SeverityLevel;
import ai.traceable.aiapp.protection.config.service.v1.TenantScope;
import ai.traceable.aiapp.protection.config.service.v1.UserAggregateType;
import ai.traceable.ratelimiting.config.service.v2.CompositeCondition;
import ai.traceable.ratelimiting.config.service.v2.Condition;
import ai.traceable.ratelimiting.config.service.v2.DataLocation;
import ai.traceable.ratelimiting.config.service.v2.KeyValueCondition;
import ai.traceable.ratelimiting.config.service.v2.LeafCondition;
import ai.traceable.ratelimiting.config.service.v2.RateLimitingRule;
import ai.traceable.ratelimiting.config.service.v2.RateLimitingRuleData;
import ai.traceable.ratelimiting.config.service.v2.RuleConfigScope;
import ai.traceable.ratelimiting.config.service.v2.ThresholdActionConfig;
import lombok.extern.slf4j.Slf4j;

/**
 * Converter class for converting rate limiting rules back to AI app custom rules. Uses the
 * threatTypeId label to determine the original rule type.
 */
@Slf4j
public class RateLimitingToAiAppConverter {

  /**
   * Converts a rate limiting rule back to AI app custom rule using threatTypeId label.
   *
   * @param rateLimitingRule the rate limiting rule to convert
   * @return the converted AI app custom rule
   * @throws IllegalArgumentException if the threatTypeId label is missing or unsupported
   */
  public AiAppCustomRule convertFromRateLimitingRule(RateLimitingRule rateLimitingRule) {
    RateLimitingRuleData ruleData = rateLimitingRule.getData();

    // Extract threatTypeId from labels
    String threatTypeId = ruleData.getLabelsMap().get(THREAT_TYPE_ID_LABEL_KEY);
    if (threatTypeId == null || threatTypeId.isEmpty()) {
      throw new IllegalArgumentException(
          "Missing threatTypeId label in rate limiting rule: " + rateLimitingRule.getId());
    }

    // Build base AI app custom rule data
    AiAppCustomRuleData.Builder aiAppRuleDataBuilder =
        AiAppCustomRuleData.newBuilder()
            .setRuleName(ruleData.getName())
            .setDescription(ruleData.getDescription())
            .setEnabled(ruleData.getEnabled())
            .putAllEventLabels(ruleData.getLabelsMap())
            .setRuleStatusDetails(convertRuleStatusDetails(ruleData))
            .setRuleScope(convertRuleScope(ruleData.getRuleConfigScope()));

    // Convert action from transaction action config or threshold action config
    if (ruleData.hasTransactionActionConfig()) {
      aiAppRuleDataBuilder.setAction(
          convertAction(ruleData.getTransactionActionConfig().getAction()));
    } else if (ruleData.getThresholdActionConfigsCount() > 0
        && ruleData.getThresholdActionConfigs(0).getActionsCount() > 0) {
      // For AI rate limiting rules, action is in ThresholdActionConfig
      aiAppRuleDataBuilder.setAction(
          convertAction(ruleData.getThresholdActionConfigs(0).getActions(0)));
    }

    // Set rule-specific data based on threatTypeId using rule-specific converters
    switch (threatTypeId) {
      case PII_DETECTED_IN_PROMPT_THREAT_TYPE_ID:
        aiAppRuleDataBuilder.setPiiDetectedInPromptRuleData(
            convertToPiiDetectedInPromptRuleData(ruleData));
        break;
      case AI_RATE_LIMITING_THREAT_TYPE_ID:
        aiAppRuleDataBuilder.setAiRateLimitingRuleData(convertToAiRateLimitingRuleData(ruleData));
        break;
      case AI_SENSITIVE_DATA_PROTECTION_THREAT_TYPE_ID:
        aiAppRuleDataBuilder.setAiSensitiveDataProtectionRuleData(
            convertToAiSensitiveDataProtectionRuleData(ruleData));
        break;
      default:
        throw new IllegalArgumentException(
            "Unsupported threatTypeId for rate limiting rule conversion: " + threatTypeId);
    }

    return AiAppCustomRule.newBuilder()
        .setRuleId(rateLimitingRule.getId())
        .setRuleData(aiAppRuleDataBuilder.build())
        .build();
  }

  /** Converts rate limiting rule data to PII detected in prompt rule data. */
  private PiiDetectedInPromptRuleData convertToPiiDetectedInPromptRuleData(
      RateLimitingRuleData ruleData) {
    PiiDetectedInPromptRuleData.Builder builder = PiiDetectedInPromptRuleData.newBuilder();

    if (ruleData.hasCondition()) {
      // Extract datatype and scope conditions from the rate limiting condition
      extractConditionsFromRateLimitingCondition(ruleData.getCondition(), builder);
    }

    return builder.build();
  }

  /** Converts rate limiting rule data to AI rate limiting rule data. */
  private AiRateLimitingRuleData convertToAiRateLimitingRuleData(RateLimitingRuleData ruleData) {
    AiRateLimitingRuleData.Builder builder = AiRateLimitingRuleData.newBuilder();

    // Extract threshold config from threshold action configs
    builder.setThresholdConfig(convertThresholdConfig(ruleData));

    // Extract AI model types and vendors conditions from the rate limiting condition
    if (ruleData.hasCondition()) {
      extractConditionsFromRateLimitingCondition(ruleData.getCondition(), builder);
    }

    return builder.build();
  }

  /** Converts rate limiting rule data to AI sensitive data protection rule data. */
  private AiSensitiveDataProtectionRuleData convertToAiSensitiveDataProtectionRuleData(
      RateLimitingRuleData ruleData) {
    AiSensitiveDataProtectionRuleData.Builder builder =
        AiSensitiveDataProtectionRuleData.newBuilder();

    if (ruleData.hasCondition()) {
      extractSensitiveDataConditions(ruleData.getCondition(), builder);
    }

    return builder.build();
  }

  /**
   * Recursively traverses the condition tree (handling AND-composite outer wrapper and OR-composite
   * inner datatype wrapper) and routes each leaf to the AI sensitive data protection builder. Both
   * AND and OR wrappers' children are processed identically — the leaf type drives whether each
   * child becomes a datatype condition or a scope condition.
   */
  private void extractSensitiveDataConditions(
      Condition condition, AiSensitiveDataProtectionRuleData.Builder builder) {
    if (condition.hasLeafCondition()) {
      extractSensitiveDataLeaf(condition.getLeafCondition(), builder);
    } else if (condition.hasCompositeCondition()) {
      CompositeCondition composite = condition.getCompositeCondition();
      for (Condition child : composite.getChildrenList()) {
        extractSensitiveDataConditions(child, builder);
      }
    }
  }

  /**
   * Routes a leaf condition to either the datatype-conditions list or scope-conditions list on the
   * AI sensitive data protection rule data builder.
   */
  private void extractSensitiveDataLeaf(
      LeafCondition leafCondition, AiSensitiveDataProtectionRuleData.Builder builder) {
    if (leafCondition.hasDatatypeCondition()) {
      builder.addDatatypeConditions(
          convertRateLimitingDatatypeCondition(leafCondition.getDatatypeCondition()));
    } else if (leafCondition.hasScopeCondition()) {
      ScopeCondition scopeCondition =
          convertRateLimitingScopeConditionToAiApp(leafCondition.getScopeCondition());
      if (scopeCondition != null) {
        builder.addScopeConditions(scopeCondition);
      }
    }
  }

  /**
   * Converts a rate limiting DatatypeCondition into an AI app DatatypeCondition. The rate-limiting
   * {@link DataLocation} chooses between the request-body and response-body custom-location oneof
   * on the AI app side.
   */
  private DatatypeCondition convertRateLimitingDatatypeCondition(
      ai.traceable.ratelimiting.config.service.v2.DatatypeCondition rateLimitingDatatypeCondition) {
    DatatypeCondition.Builder datatypeBuilder =
        DatatypeCondition.newBuilder()
            .addAllDatasetIds(rateLimitingDatatypeCondition.getDatasetIdsList())
            .addAllDatatypeIds(rateLimitingDatatypeCondition.getDatatypeIdsList());

    if (rateLimitingDatatypeCondition
        .getDatatypeMatching()
        .getRegexBasedMatching()
        .hasCustomMatchingLocation()) {
      KeyValueCondition customLocation =
          rateLimitingDatatypeCondition
              .getDatatypeMatching()
              .getRegexBasedMatching()
              .getCustomMatchingLocation();

      MatchOperatorCondition customLocationCondition =
          reverseKeyValueConditionToMatchOperator(customLocation);

      if (rateLimitingDatatypeCondition.getDataLocation() == DataLocation.DATA_LOCATION_RESPONSE) {
        datatypeBuilder.setResponseBodyCustomLocationCondition(customLocationCondition);
      } else {
        datatypeBuilder.setRequestBodyCustomLocationCondition(customLocationCondition);
      }
    }

    return datatypeBuilder.build();
  }

  /**
   * Extracts datatype and scope conditions from rate limiting condition for PII detection rules.
   * Reverses the logic from convertPiiDetectedInPromptRule in AiAppToRateLimitingConverter.
   */
  private void extractConditionsFromRateLimitingCondition(
      Condition condition, PiiDetectedInPromptRuleData.Builder builder) {

    if (condition.hasLeafCondition()) {
      extractFromLeafCondition(condition.getLeafCondition(), builder);
    } else if (condition.hasCompositeCondition()) {
      // Handle composite conditions (AND/OR logic) - extract from all children
      CompositeCondition composite = condition.getCompositeCondition();
      for (Condition childCondition : composite.getChildrenList()) {
        extractConditionsFromRateLimitingCondition(childCondition, builder);
      }
    }
  }

  /**
   * Extracts AI model types and vendors conditions from rate limiting condition for AI rate
   * limiting rules. Reverses the logic from convertAiRateLimitingRule in
   * AiAppToRateLimitingConverter.
   */
  private void extractConditionsFromRateLimitingCondition(
      Condition condition, AiRateLimitingRuleData.Builder builder) {

    if (condition.hasLeafCondition()) {
      extractAiConditionsFromLeafCondition(condition.getLeafCondition(), builder);
    } else if (condition.hasCompositeCondition()) {
      // Handle composite conditions (AND/OR logic) - extract from all children
      CompositeCondition composite = condition.getCompositeCondition();
      for (Condition childCondition : composite.getChildrenList()) {
        extractConditionsFromRateLimitingCondition(childCondition, builder);
      }
    }
  }

  /**
   * Extracts conditions from a leaf condition for PII detection rules. Reverses the logic from the
   * forward converter.
   */
  private void extractFromLeafCondition(
      LeafCondition leafCondition, PiiDetectedInPromptRuleData.Builder builder) {

    if (leafCondition.hasDatatypeCondition()) {
      // Reverse the datatype condition conversion
      ai.traceable.ratelimiting.config.service.v2.DatatypeCondition rateLimitingDatatypeCondition =
          leafCondition.getDatatypeCondition();

      DatatypeCondition.Builder datatypeBuilder =
          DatatypeCondition.newBuilder()
              .addAllDatasetIds(rateLimitingDatatypeCondition.getDatasetIdsList())
              .addAllDatatypeIds(rateLimitingDatatypeCondition.getDatatypeIdsList());

      // Handle custom location condition if present (reverse of
      // convertCustomLocationMatchingCondition)
      if (rateLimitingDatatypeCondition
          .getDatatypeMatching()
          .getRegexBasedMatching()
          .hasCustomMatchingLocation()) {

        KeyValueCondition customLocation =
            rateLimitingDatatypeCondition
                .getDatatypeMatching()
                .getRegexBasedMatching()
                .getCustomMatchingLocation();

        MatchOperatorCondition customLocationCondition =
            reverseKeyValueConditionToMatchOperator(customLocation);
        datatypeBuilder.setRequestBodyCustomLocationCondition(customLocationCondition);
      }

      builder.setDatatypeCondition(datatypeBuilder.build());

    } else if (leafCondition.hasScopeCondition()) {
      // Use common method to convert scope condition
      ScopeCondition scopeCondition =
          convertRateLimitingScopeConditionToAiApp(leafCondition.getScopeCondition());
      if (scopeCondition != null) {
        builder.addScopeConditions(scopeCondition);
      }
    }
  }

  /**
   * Extracts AI conditions from a leaf condition for AI rate limiting rules. Reverses the logic
   * from convertSpanAttributeCondition in AiAppToRateLimitingConverter.
   */
  private void extractAiConditionsFromLeafCondition(
      LeafCondition leafCondition, AiRateLimitingRuleData.Builder builder) {

    if (leafCondition
        .getKeyValueCondition()
        .getStaticValueCondition()
        .getKeyCondition()
        .getKeyType()
        .equals(KeyValueCondition.Type.TYPE_TAG)) {

      KeyValueCondition.StaticValueCondition staticValueCondition =
          leafCondition.getKeyValueCondition().getStaticValueCondition();
      KeyValueCondition.KeyCondition keyCondition = staticValueCondition.getKeyCondition();

      // Extract attribute name from key condition (reverse of convertSpanAttributeCondition)
      String attributeName =
          keyCondition.getKeyMatchOperatorCondition().getValue().getStringValue();

      // Convert the value match operator condition back to AI app MatchOperatorCondition
      MatchOperatorCondition matchCondition =
          reverseRateLimitingMatchOperatorToAiApp(
              staticValueCondition.getValueMatchOperatorCondition());

      // Map based on attribute name (reverse of the forward converter logic)
      if (GENAI_MODELS_ATTRIBUTE_KEY.equals(attributeName)) {
        builder.setAiModelTypesCondition(matchCondition);
      } else if (GENAI_PROVIDERS_ATTRIBUTE_KEY.equals(attributeName)) {
        builder.setAiVendorsCondition(matchCondition);
      }
    } else if (leafCondition.hasScopeCondition()) {
      // Use common method to convert scope condition for AI rate limiting rules
      ScopeCondition scopeCondition =
          convertRateLimitingScopeConditionToAiApp(leafCondition.getScopeCondition());
      if (scopeCondition != null) {
        builder.addScopeConditions(scopeCondition);
      }
    }
  }

  /**
   * Common method to convert rate limiting scope condition to AI app scope condition. Used by both
   * PII detection and AI rate limiting rule conversions.
   */
  private ScopeCondition convertRateLimitingScopeConditionToAiApp(
      ai.traceable.ratelimiting.config.service.v2.ScopeCondition rateLimitingScopeCondition) {

    if (rateLimitingScopeCondition
        .getEntityScope()
        .getEntityType()
        .equals(
            ai.traceable.ratelimiting.config.service.v2.ScopeCondition.EntityType
                .ENTITY_TYPE_API)) {
      ai.traceable.aiapp.protection.config.service.v1.ScopeCondition.EntityScope.Builder
          entityScopeBuilder =
              ai.traceable.aiapp.protection.config.service.v1.ScopeCondition.EntityScope
                  .newBuilder()
                  .setEntityType(
                      ai.traceable.aiapp.protection.config.service.v1.ScopeCondition.EntityType
                          .ENTITY_TYPE_API)
                  .addAllEntityIds(rateLimitingScopeCondition.getEntityScope().getEntityIdsList());

      return ScopeCondition.newBuilder().setEntityScope(entityScopeBuilder.build()).build();

    } else if (rateLimitingScopeCondition.hasUrlScope()) {
      // Handle URL scope condition - reverse of URL scope conversion
      ai.traceable.aiapp.protection.config.service.v1.ScopeCondition.UrlScope.Builder
          urlScopeBuilder =
              ai.traceable.aiapp.protection.config.service.v1.ScopeCondition.UrlScope.newBuilder()
                  .addAllUrlRegexes(rateLimitingScopeCondition.getUrlScope().getUrlRegexesList());

      return ScopeCondition.newBuilder().setUrlScope(urlScopeBuilder.build()).build();
    }

    return null; // Return null if no supported scope condition found
  }

  /** Converts rate limiting rule status to AI app rule status details. */
  private RuleStatusDetails convertRuleStatusDetails(RateLimitingRuleData ruleData) {
    RuleStatusDetails.Builder builder = RuleStatusDetails.newBuilder();

    switch (ruleData.getRuleStatus().getRuleCreationSource()) {
      case RULE_SOURCE_DEFAULT:
        builder.setRuleCreationSource(RuleStatusDetails.RuleSource.RULE_SOURCE_DEFAULT);
        break;
      case RULE_SOURCE_CUSTOMER:
        builder.setRuleCreationSource(RuleStatusDetails.RuleSource.RULE_SOURCE_CUSTOMER);
        break;
      case RULE_SOURCE_TRACEABLE:
        builder.setRuleCreationSource(RuleStatusDetails.RuleSource.RULE_SOURCE_TRACEABLE);
        break;
      default:
        break;
    }

    // Set internal and hidden flags from rule status
    builder.setInternal(ruleData.getRuleStatus().getInternal());
    builder.setHidden(ruleData.getRuleStatus().getHidden());

    return builder.build();
  }

  /** Converts rate limiting rule config scope to AI app rule scope. */
  private RuleScope convertRuleScope(RuleConfigScope ruleConfigScope) {
    RuleScope.Builder builder = RuleScope.newBuilder();

    if (ruleConfigScope.hasEnvironmentScope()) {
      // Convert environment scope back to AI app environment scope
      ai.traceable.aiapp.protection.config.service.v1.EnvironmentScope.Builder envScopeBuilder =
          ai.traceable.aiapp.protection.config.service.v1.EnvironmentScope.newBuilder()
              .addAllEnvironmentIds(ruleConfigScope.getEnvironmentScope().getEnvironmentIdsList());

      builder.setEnvironmentScope(envScopeBuilder.build());
    } else {
      builder.setTenantScope(TenantScope.getDefaultInstance());
    }

    return builder.build();
  }

  /** Converts rate limiting action to AI app action. */
  private Action convertAction(
      ai.traceable.ratelimiting.config.service.v2.Action rateLimitingAction) {
    Action.Builder builder = Action.newBuilder();

    if (rateLimitingAction.hasAlert()) {
      // Convert rate limiting alert event severity to AI app alert severity
      SeverityLevel severityLevel;
      switch (rateLimitingAction.getAlert().getEventSeverity()) {
        case EVENT_SEVERITY_LOW:
          severityLevel = SeverityLevel.SEVERITY_LEVEL_LOW;
          break;
        case EVENT_SEVERITY_MEDIUM:
          severityLevel = SeverityLevel.SEVERITY_LEVEL_MEDIUM;
          break;
        case EVENT_SEVERITY_HIGH:
          severityLevel = SeverityLevel.SEVERITY_LEVEL_HIGH;
          break;
        case EVENT_SEVERITY_CRITICAL:
          severityLevel = SeverityLevel.SEVERITY_LEVEL_CRITICAL;
          break;
        default:
          severityLevel = SeverityLevel.SEVERITY_LEVEL_UNSPECIFIED;
          break;
      }

      Action.Alert alert = Action.Alert.newBuilder().setSeverityLevel(severityLevel).build();
      builder.setAlert(alert);

    } else if (rateLimitingAction.hasMarkForTesting()) {
      // Convert mark for testing action to AI app mark for testing action
      Action.MarkForTesting markForTesting = Action.MarkForTesting.newBuilder().build();
      builder.setMarkForTesting(markForTesting);
    }

    return builder.build();
  }

  /** Converts rate limiting rule data to threshold config for AI rate limiting rules. */
  private ResourceAccessThresholdConfig convertThresholdConfig(RateLimitingRuleData ruleData) {
    ResourceAccessThresholdConfig.Builder builder = ResourceAccessThresholdConfig.newBuilder();

    // Extract threshold information from ThresholdActionConfig
    // Reverse the logic from convertThresholdConfig in AiAppToRateLimitingConverter

    if (!ruleData.getThresholdActionConfigsList().isEmpty()
        && !ruleData
            .getThresholdActionConfigs(0)
            .getResourceAccessThresholdConfigsList()
            .isEmpty()) {

      // Get the first threshold action config and resource access threshold config
      ThresholdActionConfig thresholdActionConfig = ruleData.getThresholdActionConfigs(0);
      ai.traceable.ratelimiting.config.service.v2.ResourceAccessThresholdConfig
          rateLimitingThreshold = thresholdActionConfig.getResourceAccessThresholdConfigs(0);

      // Extract aggregation types from rate limiting threshold config
      switch (rateLimitingThreshold.getUserAggregateType()) {
        case USER_AGGREGATE_TYPE_PER_USER:
          builder.setUserAggregateType(UserAggregateType.USER_AGGREGATE_TYPE_PER_USER);
          break;
        case USER_AGGREGATE_TYPE_ACROSS_USERS:
          builder.setUserAggregateType(UserAggregateType.USER_AGGREGATE_TYPE_ACROSS_USERS);
          break;
        default:
          break;
      }

      switch (rateLimitingThreshold.getApiAggregateType()) {
        case API_AGGREGATE_TYPE_PER_ENDPOINT:
          builder.setApiAggregateType(ApiAggregateType.API_AGGREGATE_TYPE_PER_ENDPOINT);
          break;
        case API_AGGREGATE_TYPE_ACROSS_ENDPOINTS:
          builder.setApiAggregateType(ApiAggregateType.API_AGGREGATE_TYPE_ACROSS_ENDPOINTS);
          break;
        default:
          break;
      }

      // Extract rolling window threshold config
      if (rateLimitingThreshold.hasRollingWindowThresholdConfig()) {
        ai.traceable.ratelimiting.config.service.v2.ResourceAccessThresholdConfig
                .RollingWindowThresholdConfig
            rateLimitingRollingWindow = rateLimitingThreshold.getRollingWindowThresholdConfig();

        ResourceAccessThresholdConfig.RollingWindowThresholdConfig rollingWindowConfig =
            ResourceAccessThresholdConfig.RollingWindowThresholdConfig.newBuilder()
                .setCountAllowed(rateLimitingRollingWindow.getCountAllowed())
                .setDurationIso(rateLimitingRollingWindow.getDurationIso())
                .build();

        builder.setRollingWindowThresholdConfig(rollingWindowConfig);
      }
    }

    return builder.build();
  }

  /**
   * Reverses KeyValueCondition back to MatchOperatorCondition for custom location conditions.
   * Reverses the logic from convertCustomLocationMatchingCondition in AiAppToRateLimitingConverter.
   */
  private MatchOperatorCondition reverseKeyValueConditionToMatchOperator(
      KeyValueCondition keyValueCondition) {
    MatchOperatorCondition.Builder builder = MatchOperatorCondition.newBuilder();

    if (keyValueCondition
        .getStaticValueCondition()
        .getKeyCondition()
        .hasKeyMatchOperatorCondition()) {

      KeyValueCondition.StaticValueCondition staticValueCondition =
          keyValueCondition.getStaticValueCondition();
      KeyValueCondition.KeyCondition keyCondition = staticValueCondition.getKeyCondition();

      // Extract the match operator and value from the key condition (reverse of
      // convertCustomLocationMatchingCondition)
      KeyValueCondition.MatchOperatorCondition rateLimitingMatchCondition =
          keyCondition.getKeyMatchOperatorCondition();

      // Convert match operator back to AI app match operator
      builder.setOperator(
          reverseRateLimitingMatchOperatorToAiAppOperator(
              rateLimitingMatchCondition.getOperator()));

      // Extract the value
      if (rateLimitingMatchCondition.hasValue()) {
        builder.setValue(rateLimitingMatchCondition.getValue());
      }
    }

    return builder.build();
  }

  /**
   * Reverses rate limiting MatchOperatorCondition back to AI app MatchOperatorCondition. Reverses
   * the logic from convertSpanAttributeCondition in AiAppToRateLimitingConverter.
   */
  private MatchOperatorCondition reverseRateLimitingMatchOperatorToAiApp(
      KeyValueCondition.MatchOperatorCondition rateLimitingMatchCondition) {
    MatchOperatorCondition.Builder builder = MatchOperatorCondition.newBuilder();

    // Convert match operator back to AI app match operator
    builder.setOperator(
        reverseRateLimitingMatchOperatorToAiAppOperator(rateLimitingMatchCondition.getOperator()));

    // Extract the value
    if (rateLimitingMatchCondition.hasValue()) {
      builder.setValue(rateLimitingMatchCondition.getValue());
    }

    return builder.build();
  }

  /**
   * Reverses rate limiting match operator to AI app match operator. Reverses the logic from
   * convertMatchOperator in AiAppToRateLimitingConverter.
   */
  private MatchOperator reverseRateLimitingMatchOperatorToAiAppOperator(
      KeyValueCondition.MatchOperator rateLimitingOperator) {
    switch (rateLimitingOperator) {
      case MATCH_OPERATOR_EQUALS:
        return MatchOperator.MATCH_OPERATOR_EQUALS;
      case MATCH_OPERATOR_NOT_EQUAL:
        return MatchOperator.MATCH_OPERATOR_NOT_EQUAL;
      case MATCH_OPERATOR_CONTAINS:
        return MatchOperator.MATCH_OPERATOR_CONTAINS;
      case MATCH_OPERATOR_NOT_CONTAIN:
        return MatchOperator.MATCH_OPERATOR_NOT_CONTAIN;
      case MATCH_OPERATOR_MATCHES_REGEX:
        return MatchOperator.MATCH_OPERATOR_MATCHES_REGEX;
      case MATCH_OPERATOR_NOT_MATCH_REGEX:
        return MatchOperator.MATCH_OPERATOR_NOT_MATCH_REGEX;
      case MATCH_OPERATOR_GREATER_THAN:
        return MatchOperator.MATCH_OPERATOR_GREATER_THAN;
      case MATCH_OPERATOR_LESS_THAN:
        return MatchOperator.MATCH_OPERATOR_LESS_THAN;
      default:
        return MatchOperator.MATCH_OPERATOR_UNSPECIFIED;
    }
  }
}
