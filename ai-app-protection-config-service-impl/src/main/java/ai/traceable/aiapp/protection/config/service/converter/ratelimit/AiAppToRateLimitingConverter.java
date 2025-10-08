package ai.traceable.aiapp.protection.config.service.converter.ratelimit;

import static ai.traceable.aiapp.protection.config.service.converter.AiAppConverterConstants.AI_RATE_LIMITING_THREAT_TYPE_ID;
import static ai.traceable.aiapp.protection.config.service.converter.AiAppConverterConstants.GENAI_MODELS_ATTRIBUTE_KEY;
import static ai.traceable.aiapp.protection.config.service.converter.AiAppConverterConstants.GENAI_PROVIDERS_ATTRIBUTE_KEY;
import static ai.traceable.aiapp.protection.config.service.converter.AiAppConverterConstants.PII_DETECTED_IN_PROMPT_THREAT_TYPE_ID;
import static ai.traceable.aiapp.protection.config.service.converter.AiAppConverterConstants.THREAT_TYPE_ID_LABEL_KEY;

import ai.traceable.aiapp.protection.config.service.v1.Action;
import ai.traceable.aiapp.protection.config.service.v1.AiAppCustomRuleData;
import ai.traceable.aiapp.protection.config.service.v1.AiRateLimitingRuleData;
import ai.traceable.aiapp.protection.config.service.v1.ApiAggregateType;
import ai.traceable.aiapp.protection.config.service.v1.MatchOperatorCondition;
import ai.traceable.aiapp.protection.config.service.v1.PiiDetectedInPromptRuleData;
import ai.traceable.aiapp.protection.config.service.v1.ResourceAccessThresholdConfig;
import ai.traceable.aiapp.protection.config.service.v1.SeverityLevel;
import ai.traceable.aiapp.protection.config.service.v1.UserAggregateType;
import ai.traceable.ratelimiting.config.service.v2.Category;
import ai.traceable.ratelimiting.config.service.v2.CompositeCondition;
import ai.traceable.ratelimiting.config.service.v2.Condition;
import ai.traceable.ratelimiting.config.service.v2.CreateRateLimitingRuleRequest;
import ai.traceable.ratelimiting.config.service.v2.KeyValueCondition;
import ai.traceable.ratelimiting.config.service.v2.LeafCondition;
import ai.traceable.ratelimiting.config.service.v2.RateLimitingRuleData;
import ai.traceable.ratelimiting.config.service.v2.RuleConfigScope;
import ai.traceable.ratelimiting.config.service.v2.RuleEvaluationPoint;
import ai.traceable.ratelimiting.config.service.v2.RuleStatus;
import ai.traceable.ratelimiting.config.service.v2.ThresholdActionConfig;
import ai.traceable.ratelimiting.config.service.v2.TransactionActionConfig;
import java.util.ArrayList;
import java.util.List;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class AiAppToRateLimitingConverter {

  /** Converts AI app custom rule data to a CreateRateLimitingRuleRequest for create operations */
  public CreateRateLimitingRuleRequest convertToCreateRateLimitingRuleRequest(
      AiAppCustomRuleData aiAppRuleData) {
    RateLimitingRuleData rateLimitingRuleData = buildRateLimitingRuleData(aiAppRuleData, true);
    return CreateRateLimitingRuleRequest.newBuilder().setData(rateLimitingRuleData).build();
  }

  /** Converts AI app custom rule data to a RateLimitingRule for upsert operations */
  public RateLimitingRuleData convertToRateLimitingRule(AiAppCustomRuleData aiAppRuleData) {
    return buildRateLimitingRuleData(aiAppRuleData, false);
  }

  /** Builds the common RateLimitingRuleData from AiAppCustomRuleData */
  private RateLimitingRuleData buildRateLimitingRuleData(
      AiAppCustomRuleData aiAppRuleData, boolean isCreateRequest) {
    RateLimitingRuleData.Builder rateLimitingRuleDataBuilder =
        RateLimitingRuleData.newBuilder()
            .setCategory(Category.CATEGORY_AI_APP_PROTECTION)
            .setName(aiAppRuleData.getRuleName())
            .setDescription(aiAppRuleData.getDescription())
            .setEnabled(aiAppRuleData.getEnabled())
            .setRuleStatus(convertRuleStatus(aiAppRuleData, isCreateRequest))
            .putAllLabels(aiAppRuleData.getEventLabelsMap())
            .addRuleEvaluationPoints(RuleEvaluationPoint.RULE_EVALUATION_POINT_PLATFORM);

    String threatTypeId = getThreatTypeIdLabel(aiAppRuleData);
    rateLimitingRuleDataBuilder.putLabels(THREAT_TYPE_ID_LABEL_KEY, threatTypeId);

    // Convert rule scope if present
    if (aiAppRuleData.hasRuleScope()) {
      rateLimitingRuleDataBuilder.setRuleConfigScope(
          convertRuleScope(aiAppRuleData.getRuleScope()));
    }

    // Handle different rule data types
    if (aiAppRuleData.hasPiiDetectedInPromptRuleData()) {
      convertPiiDetectedInPromptRule(aiAppRuleData, rateLimitingRuleDataBuilder);
    } else if (aiAppRuleData.hasAiRateLimitingRuleData()) {
      convertAiRateLimitingRule(aiAppRuleData, rateLimitingRuleDataBuilder);
    }

    return rateLimitingRuleDataBuilder.build();
  }

  private RuleStatus convertRuleStatus(AiAppCustomRuleData aiAppRuleData, boolean isCreateRequest) {
    RuleStatus.Builder builder =
        RuleStatus.newBuilder()
            .setInternal(aiAppRuleData.getRuleStatusDetails().getInternal())
            .setHidden(aiAppRuleData.getRuleStatusDetails().getHidden());

    if (isCreateRequest) {
      switch (aiAppRuleData.getRuleStatusDetails().getRuleCreationSource()) {
        case RULE_SOURCE_TRACEABLE:
          builder.setRuleCreationSource(RuleStatus.RuleSource.RULE_SOURCE_TRACEABLE);
          break;
        case RULE_SOURCE_CUSTOMER:
        default:
          builder.setRuleCreationSource(RuleStatus.RuleSource.RULE_SOURCE_CUSTOMER);
          break;
      }
    }

    return builder.build();
  }

  private RuleConfigScope convertRuleScope(
      ai.traceable.aiapp.protection.config.service.v1.RuleScope ruleScope) {
    RuleConfigScope.Builder builder = RuleConfigScope.newBuilder();

    if (ruleScope.hasEnvironmentScope()) {
      ai.traceable.ratelimiting.config.service.v2.EnvironmentScope.Builder envScopeBuilder =
          ai.traceable.ratelimiting.config.service.v2.EnvironmentScope.newBuilder();

      // Map environment IDs from AI app protection rules
      envScopeBuilder.addAllEnvironmentIds(ruleScope.getEnvironmentScope().getEnvironmentIdsList());
      builder.setEnvironmentScope(envScopeBuilder.build());
    }

    return builder.build();
  }

  private void convertPiiDetectedInPromptRule(
      AiAppCustomRuleData aiAppRuleData, RateLimitingRuleData.Builder builder) {
    PiiDetectedInPromptRuleData piiRuleData = aiAppRuleData.getPiiDetectedInPromptRuleData();

    // Collect all conditions that need to be combined
    List<Condition> conditions = new ArrayList<>();

    // Convert datatype condition
    ai.traceable.ratelimiting.config.service.v2.DatatypeCondition.Builder datatypeBuilder =
        ai.traceable.ratelimiting.config.service.v2.DatatypeCondition.newBuilder()
            .addAllDatasetIds(piiRuleData.getDatatypeCondition().getDatasetIdsList())
            .addAllDatatypeIds(piiRuleData.getDatatypeCondition().getDatatypeIdsList())
            .setDataLocation(
                ai.traceable.ratelimiting.config.service.v2.DataLocation
                    .DATA_LOCATION_REQUEST); // PII in prompt is request data

    // Handle custom location condition if present
    if (piiRuleData.getDatatypeCondition().hasRequestBodyCustomLocationCondition()) {
      MatchOperatorCondition customLocationCondition =
          piiRuleData.getDatatypeCondition().getRequestBodyCustomLocationCondition();

      // Convert custom location condition to KeyValueCondition
      KeyValueCondition customMatchingLocation =
          convertCustomLocationMatchingCondition(customLocationCondition);

      // Create regex-based matching with custom location
      ai.traceable.ratelimiting.config.service.v2.DatatypeCondition.RegexBasedMatching
          regexMatching =
              ai.traceable.ratelimiting.config.service.v2.DatatypeCondition.RegexBasedMatching
                  .newBuilder()
                  .setCustomMatchingLocation(customMatchingLocation)
                  .build();

      // Create datatype matching with regex-based matching
      ai.traceable.ratelimiting.config.service.v2.DatatypeCondition.DatatypeMatching
          datatypeMatching =
              ai.traceable.ratelimiting.config.service.v2.DatatypeCondition.DatatypeMatching
                  .newBuilder()
                  .setRegexBasedMatching(regexMatching)
                  .build();

      datatypeBuilder.setDatatypeMatching(datatypeMatching);
    }

    // Add datatype condition
    LeafCondition datatypeLeafCondition =
        LeafCondition.newBuilder().setDatatypeCondition(datatypeBuilder.build()).build();
    conditions.add(Condition.newBuilder().setLeafCondition(datatypeLeafCondition).build());

    // Convert scope conditions if present (now using plural scope_conditions field)
    for (ai.traceable.aiapp.protection.config.service.v1.ScopeCondition scopeCondition :
        piiRuleData.getScopeConditionsList()) {
      Condition convertedScopeCondition = convertScopeCondition(scopeCondition);
      conditions.add(convertedScopeCondition);
    }

    // Combine conditions using AND logic if multiple conditions exist
    Condition condition;
    if (conditions.size() == 1) {
      condition = conditions.get(0);
    } else {
      // Create composite condition with AND logic
      CompositeCondition.Builder compositeBuilder =
          CompositeCondition.newBuilder()
              .setOperator(CompositeCondition.LogicalOperator.LOGICAL_OPERATOR_AND)
              .addAllChildren(conditions);
      condition = Condition.newBuilder().setCompositeCondition(compositeBuilder.build()).build();
    }

    builder.setCondition(condition);

    TransactionActionConfig transactionActionConfig =
        TransactionActionConfig.newBuilder()
            .setAction(convertAction(aiAppRuleData.getAction()))
            .build();
    builder.setTransactionActionConfig(transactionActionConfig);
  }

  /** Converts AI app ScopeCondition to rate limiting ScopeCondition */
  private Condition convertScopeCondition(
      ai.traceable.aiapp.protection.config.service.v1.ScopeCondition aiAppScopeCondition) {

    // Convert AI app scope condition to rate limiting ScopeCondition
    ai.traceable.ratelimiting.config.service.v2.ScopeCondition.Builder rateLimitingScopeBuilder =
        ai.traceable.ratelimiting.config.service.v2.ScopeCondition.newBuilder();

    if (aiAppScopeCondition.hasEntityScope()) {
      ai.traceable.aiapp.protection.config.service.v1.ScopeCondition.EntityScope aiAppEntityScope =
          aiAppScopeCondition.getEntityScope();

      // Create EntityScope for rate limiting service
      ai.traceable.ratelimiting.config.service.v2.ScopeCondition.EntityScope.Builder
          entityScopeBuilder =
              ai.traceable.ratelimiting.config.service.v2.ScopeCondition.EntityScope.newBuilder();

      if (aiAppEntityScope.getEntityType()
          == ai.traceable.aiapp.protection.config.service.v1.ScopeCondition.EntityType
              .ENTITY_TYPE_API) {

        entityScopeBuilder
            .setEntityType(
                ai.traceable.ratelimiting.config.service.v2.ScopeCondition.EntityType
                    .ENTITY_TYPE_API)
            .addAllEntityIds(aiAppEntityScope.getEntityIdsList());
      }

      rateLimitingScopeBuilder.setEntityScope(entityScopeBuilder.build());
    } else if (aiAppScopeCondition.hasUrlScope()) {
      // Handle URL scope condition - use proper UrlScope in rate limiting proto
      ai.traceable.aiapp.protection.config.service.v1.ScopeCondition.UrlScope aiAppUrlScope =
          aiAppScopeCondition.getUrlScope();

      // Create UrlScope for rate limiting service
      ai.traceable.ratelimiting.config.service.v2.ScopeCondition.UrlScope.Builder urlScopeBuilder =
          ai.traceable.ratelimiting.config.service.v2.ScopeCondition.UrlScope.newBuilder()
              .addAllUrlRegexes(aiAppUrlScope.getUrlRegexesList());

      rateLimitingScopeBuilder.setUrlScope(urlScopeBuilder.build());
    }

    // Create LeafCondition with ScopeCondition
    LeafCondition leafCondition =
        LeafCondition.newBuilder().setScopeCondition(rateLimitingScopeBuilder.build()).build();

    return Condition.newBuilder().setLeafCondition(leafCondition).build();
  }

  private void convertAiRateLimitingRule(
      AiAppCustomRuleData aiAppCustomRuleData, RateLimitingRuleData.Builder builder) {

    AiRateLimitingRuleData aiRateLimitingData = aiAppCustomRuleData.getAiRateLimitingRuleData();

    // Convert AI model types and vendors conditions to rate limiting conditions
    List<Condition> conditions = new ArrayList<>();

    // Convert AI model types condition
    if (aiRateLimitingData.hasAiModelTypesCondition()) {
      Condition modelTypesCondition =
          convertSpanAttributeCondition(
              aiRateLimitingData.getAiModelTypesCondition(), GENAI_MODELS_ATTRIBUTE_KEY);
      conditions.add(modelTypesCondition);
    }

    // Convert AI vendors condition
    if (aiRateLimitingData.hasAiVendorsCondition()) {
      Condition vendorsCondition =
          convertSpanAttributeCondition(
              aiRateLimitingData.getAiVendorsCondition(), GENAI_PROVIDERS_ATTRIBUTE_KEY);
      conditions.add(vendorsCondition);
    }

    // Convert scope conditions if present (now using plural scope_conditions field)
    for (ai.traceable.aiapp.protection.config.service.v1.ScopeCondition scopeCondition :
        aiRateLimitingData.getScopeConditionsList()) {
      Condition convertedScopeCondition = convertScopeCondition(scopeCondition);
      conditions.add(convertedScopeCondition);
    }

    // Combine conditions using AND logic if multiple conditions exist
    if (!conditions.isEmpty()) {
      if (conditions.size() == 1) {
        builder.setCondition(conditions.get(0));
      } else {
        // Create composite condition with AND logic
        CompositeCondition.Builder compositeBuilder =
            CompositeCondition.newBuilder()
                .setOperator(CompositeCondition.LogicalOperator.LOGICAL_OPERATOR_AND)
                .addAllChildren(conditions);

        Condition compositeCondition =
            Condition.newBuilder().setCompositeCondition(compositeBuilder.build()).build();
        builder.setCondition(compositeCondition);
      }
    }

    // Convert threshold configuration
    if (aiRateLimitingData.hasThresholdConfig()) {
      ResourceAccessThresholdConfig aiThresholdConfig = aiRateLimitingData.getThresholdConfig();

      ai.traceable.ratelimiting.config.service.v2.ResourceAccessThresholdConfig.Builder
          rlThresholdBuilder =
              ai.traceable.ratelimiting.config.service.v2.ResourceAccessThresholdConfig.newBuilder()
                  .setUserAggregateType(
                      convertUserAggregateType(aiThresholdConfig.getUserAggregateType()))
                  .setApiAggregateType(
                      convertApiAggregateType(aiThresholdConfig.getApiAggregateType()));

      // Convert rolling window threshold config
      if (aiThresholdConfig.hasRollingWindowThresholdConfig()) {
        ResourceAccessThresholdConfig.RollingWindowThresholdConfig aiRollingWindow =
            aiThresholdConfig.getRollingWindowThresholdConfig();

        ai.traceable.ratelimiting.config.service.v2.ResourceAccessThresholdConfig
                .RollingWindowThresholdConfig.Builder
            rlRollingWindowBuilder =
                ai.traceable.ratelimiting.config.service.v2.ResourceAccessThresholdConfig
                    .RollingWindowThresholdConfig.newBuilder()
                    .setCountAllowed(aiRollingWindow.getCountAllowed())
                    .setDurationIso(aiRollingWindow.getDurationIso());

        rlThresholdBuilder.setRollingWindowThresholdConfig(rlRollingWindowBuilder.build());
      }

      ThresholdActionConfig thresholdActionConfig =
          ThresholdActionConfig.newBuilder()
              .addActions(convertAction(aiAppCustomRuleData.getAction()))
              .addResourceAccessThresholdConfigs(rlThresholdBuilder.build())
              .build();

      builder.addThresholdActionConfigs(thresholdActionConfig);
    }
  }

  private ai.traceable.ratelimiting.config.service.v2.UserAggregateType convertUserAggregateType(
      UserAggregateType userAggregateType) {
    switch (userAggregateType) {
      case USER_AGGREGATE_TYPE_PER_USER:
        return ai.traceable.ratelimiting.config.service.v2.UserAggregateType
            .USER_AGGREGATE_TYPE_PER_USER;
      case USER_AGGREGATE_TYPE_ACROSS_USERS:
        return ai.traceable.ratelimiting.config.service.v2.UserAggregateType
            .USER_AGGREGATE_TYPE_ACROSS_USERS;
      default:
        return ai.traceable.ratelimiting.config.service.v2.UserAggregateType
            .USER_AGGREGATE_TYPE_UNSPECIFIED;
    }
  }

  private ai.traceable.ratelimiting.config.service.v2.ApiAggregateType convertApiAggregateType(
      ApiAggregateType apiAggregateType) {
    switch (apiAggregateType) {
      case API_AGGREGATE_TYPE_PER_ENDPOINT:
        return ai.traceable.ratelimiting.config.service.v2.ApiAggregateType
            .API_AGGREGATE_TYPE_PER_ENDPOINT;
      case API_AGGREGATE_TYPE_ACROSS_ENDPOINTS:
        return ai.traceable.ratelimiting.config.service.v2.ApiAggregateType
            .API_AGGREGATE_TYPE_ACROSS_ENDPOINTS;
      default:
        return ai.traceable.ratelimiting.config.service.v2.ApiAggregateType
            .API_AGGREGATE_TYPE_UNSPECIFIED;
    }
  }

  /** Converts AI app action to rate limiting action - reusable utility method */
  private ai.traceable.ratelimiting.config.service.v2.Action convertAction(Action action) {
    ai.traceable.ratelimiting.config.service.v2.Action.Builder actionBuilder =
        ai.traceable.ratelimiting.config.service.v2.Action.newBuilder();

    if (action.hasAlert()) {
      ai.traceable.ratelimiting.config.service.v2.Action.Alert.Builder alertBuilder =
          ai.traceable.ratelimiting.config.service.v2.Action.Alert.newBuilder()
              .setEventSeverity(convertSeverityLevel(action.getAlert().getSeverityLevel()));
      actionBuilder.setAlert(alertBuilder.build());
    } else if (action.hasMarkForTesting()) {
      ai.traceable.ratelimiting.config.service.v2.Action.MarkForTesting.Builder testingBuilder =
          ai.traceable.ratelimiting.config.service.v2.Action.MarkForTesting.newBuilder();
      actionBuilder.setMarkForTesting(testingBuilder.build());
    }

    return actionBuilder.build();
  }

  /** Converts AI app match operator to rate limiting match operator */
  private KeyValueCondition.MatchOperator convertMatchOperator(
      ai.traceable.aiapp.protection.config.service.v1.MatchOperator aiAppOperator) {
    switch (aiAppOperator) {
      case MATCH_OPERATOR_EQUALS:
        return KeyValueCondition.MatchOperator.MATCH_OPERATOR_EQUALS;
      case MATCH_OPERATOR_NOT_EQUAL:
        return KeyValueCondition.MatchOperator.MATCH_OPERATOR_NOT_EQUAL;
      case MATCH_OPERATOR_CONTAINS:
        return KeyValueCondition.MatchOperator.MATCH_OPERATOR_CONTAINS;
      case MATCH_OPERATOR_NOT_CONTAIN:
        return KeyValueCondition.MatchOperator.MATCH_OPERATOR_NOT_CONTAIN;
      case MATCH_OPERATOR_MATCHES_REGEX:
        return KeyValueCondition.MatchOperator.MATCH_OPERATOR_MATCHES_REGEX;
      case MATCH_OPERATOR_NOT_MATCH_REGEX:
        return KeyValueCondition.MatchOperator.MATCH_OPERATOR_NOT_MATCH_REGEX;
      case MATCH_OPERATOR_GREATER_THAN:
        return KeyValueCondition.MatchOperator.MATCH_OPERATOR_GREATER_THAN;
      case MATCH_OPERATOR_LESS_THAN:
        return KeyValueCondition.MatchOperator.MATCH_OPERATOR_LESS_THAN;
      default:
        return KeyValueCondition.MatchOperator.MATCH_OPERATOR_UNSPECIFIED;
    }
  }

  /** Converts MatchOperatorCondition to rate limiting Condition */
  private Condition convertSpanAttributeCondition(
      MatchOperatorCondition matchOperatorCondition, String attributeName) {

    // Create key condition for the attribute name
    KeyValueCondition.KeyCondition.Builder keyConditionBuilder =
        KeyValueCondition.KeyCondition.newBuilder()
            .setKeyType(KeyValueCondition.Type.TYPE_TAG)
            .setKeyMatchOperatorCondition(
                KeyValueCondition.MatchOperatorCondition.newBuilder()
                    .setOperator(KeyValueCondition.MatchOperator.MATCH_OPERATOR_EQUALS)
                    .setValue(
                        com.google.protobuf.Value.newBuilder()
                            .setStringValue(attributeName)
                            .build())
                    .build());

    // Create value match operator condition using the reusable converter
    KeyValueCondition.MatchOperatorCondition.Builder valueConditionBuilder =
        KeyValueCondition.MatchOperatorCondition.newBuilder()
            .setValue(matchOperatorCondition.getValue())
            .setOperator(convertMatchOperator(matchOperatorCondition.getOperator()));

    // Create value condition
    KeyValueCondition.StaticValueCondition staticValueCondition =
        KeyValueCondition.StaticValueCondition.newBuilder()
            .setKeyCondition(keyConditionBuilder.build())
            .setValueMatchOperatorCondition(valueConditionBuilder.build())
            .build();

    // Create key-value condition
    KeyValueCondition keyValueCondition =
        KeyValueCondition.newBuilder().setStaticValueCondition(staticValueCondition).build();

    LeafCondition leafCondition =
        LeafCondition.newBuilder().setKeyValueCondition(keyValueCondition).build();

    return Condition.newBuilder().setLeafCondition(leafCondition).build();
  }

  private KeyValueCondition convertCustomLocationMatchingCondition(
      MatchOperatorCondition matchOperatorCondition) {

    // Create key match operator condition using the reusable converter
    KeyValueCondition.MatchOperatorCondition.Builder keyConditionBuilder =
        KeyValueCondition.MatchOperatorCondition.newBuilder()
            .setOperator(convertMatchOperator(matchOperatorCondition.getOperator()))
            .setValue(
                com.google.protobuf.Value.newBuilder()
                    .setStringValue(matchOperatorCondition.getValue().getStringValue())
                    .build());

    // Create value condition
    KeyValueCondition.StaticValueCondition staticValueCondition =
        KeyValueCondition.StaticValueCondition.newBuilder()
            .setKeyCondition(
                KeyValueCondition.KeyCondition.newBuilder()
                    .setKeyType(KeyValueCondition.Type.TYPE_REQUEST_BODY_PARAMETER)
                    .setKeyMatchOperatorCondition(keyConditionBuilder))
            .build();

    // Create and return key-value condition
    return KeyValueCondition.newBuilder().setStaticValueCondition(staticValueCondition).build();
  }

  private ai.traceable.ratelimiting.config.service.v2.Action.EventSeverity convertSeverityLevel(
      SeverityLevel severityLevel) {
    switch (severityLevel) {
      case SEVERITY_LEVEL_LOW:
        return ai.traceable.ratelimiting.config.service.v2.Action.EventSeverity.EVENT_SEVERITY_LOW;
      case SEVERITY_LEVEL_MEDIUM:
        return ai.traceable.ratelimiting.config.service.v2.Action.EventSeverity
            .EVENT_SEVERITY_MEDIUM;
      case SEVERITY_LEVEL_HIGH:
        return ai.traceable.ratelimiting.config.service.v2.Action.EventSeverity.EVENT_SEVERITY_HIGH;
      case SEVERITY_LEVEL_CRITICAL:
        return ai.traceable.ratelimiting.config.service.v2.Action.EventSeverity
            .EVENT_SEVERITY_CRITICAL;
      default:
        return ai.traceable.ratelimiting.config.service.v2.Action.EventSeverity
            .EVENT_SEVERITY_UNSPECIFIED;
    }
  }

  private String getThreatTypeIdLabel(AiAppCustomRuleData aiAppRuleData) {
    if (aiAppRuleData.hasPiiDetectedInPromptRuleData()) {
      return PII_DETECTED_IN_PROMPT_THREAT_TYPE_ID;
    } else if (aiAppRuleData.hasAiRateLimitingRuleData()) {
      return AI_RATE_LIMITING_THREAT_TYPE_ID;
    }
    throw new IllegalArgumentException(
        "Unsupported rule type for rate limiting conversion: " + aiAppRuleData.getRuleDataCase());
  }
}
