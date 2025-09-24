package ai.traceable.aiapp.protection.config.service.converter.ratelimit;

import static ai.traceable.aiapp.protection.config.service.converter.AiAppConverterConstants.AI_RATE_LIMITING_THREAT_TYPE_ID;
import static ai.traceable.aiapp.protection.config.service.converter.AiAppConverterConstants.GENAI_MODELS_ATTRIBUTE_KEY;
import static ai.traceable.aiapp.protection.config.service.converter.AiAppConverterConstants.GENAI_PROVIDERS_ATTRIBUTE_KEY;
import static ai.traceable.aiapp.protection.config.service.converter.AiAppConverterConstants.PII_DETECTED_IN_PROMPT_THREAT_TYPE_ID;
import static ai.traceable.aiapp.protection.config.service.converter.AiAppConverterConstants.THREAT_TYPE_ID_LABEL_KEY;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import ai.traceable.aiapp.protection.config.service.v1.AiAppCustomRule;
import ai.traceable.aiapp.protection.config.service.v1.AiAppCustomRuleData;
import ai.traceable.aiapp.protection.config.service.v1.AiRateLimitingRuleData;
import ai.traceable.aiapp.protection.config.service.v1.MatchOperator;
import ai.traceable.aiapp.protection.config.service.v1.MatchOperatorCondition;
import ai.traceable.aiapp.protection.config.service.v1.PiiDetectedInPromptRuleData;
import ai.traceable.aiapp.protection.config.service.v1.RuleStatusDetails;
import ai.traceable.aiapp.protection.config.service.v1.SeverityLevel;
import ai.traceable.ratelimiting.config.service.v2.Category;
import ai.traceable.ratelimiting.config.service.v2.CompositeCondition;
import ai.traceable.ratelimiting.config.service.v2.Condition;
import ai.traceable.ratelimiting.config.service.v2.DatatypeCondition;
import ai.traceable.ratelimiting.config.service.v2.KeyValueCondition;
import ai.traceable.ratelimiting.config.service.v2.LeafCondition;
import ai.traceable.ratelimiting.config.service.v2.RateLimitingRule;
import ai.traceable.ratelimiting.config.service.v2.RateLimitingRuleData;
import ai.traceable.ratelimiting.config.service.v2.ThresholdActionConfig;
import ai.traceable.ratelimiting.config.service.v2.TransactionActionConfig;
import com.google.protobuf.Value;
import org.junit.jupiter.api.Test;

class RateLimitingToAiAppConverterTest {

  @Test
  void testConvertPiiDetectedInPromptRuleFromRateLimiting() {
    // Create simplified test rate limiting rule for PII detection
    RateLimitingRule rateLimitingRule =
        RateLimitingRule.newBuilder()
            .setId("rule-123")
            .setData(
                RateLimitingRuleData.newBuilder()
                    .setName("Test PII Rule")
                    .setDescription("Test PII detection rule")
                    .setEnabled(true)
                    .setCategory(Category.CATEGORY_AI_APP_PROTECTION)
                    .putLabels(THREAT_TYPE_ID_LABEL_KEY, PII_DETECTED_IN_PROMPT_THREAT_TYPE_ID)
                    .setCondition(
                        Condition.newBuilder()
                            .setCompositeCondition(
                                CompositeCondition.newBuilder()
                                    .setOperator(
                                        CompositeCondition.LogicalOperator.LOGICAL_OPERATOR_AND)
                                    .addChildren(
                                        Condition.newBuilder()
                                            .setLeafCondition(
                                                LeafCondition.newBuilder()
                                                    .setDatatypeCondition(
                                                        ai.traceable.ratelimiting.config.service.v2
                                                            .DatatypeCondition.newBuilder()
                                                            .addDatasetIds("dataset-1")
                                                            .addDatatypeIds("datatype-1")
                                                            .setDataLocation(
                                                                ai.traceable.ratelimiting.config
                                                                    .service.v2.DataLocation
                                                                    .DATA_LOCATION_REQUEST)
                                                            .setDatatypeMatching(
                                                                DatatypeCondition.DatatypeMatching
                                                                    .newBuilder()
                                                                    .setRegexBasedMatching(
                                                                        DatatypeCondition
                                                                            .RegexBasedMatching
                                                                            .newBuilder()
                                                                            .setCustomMatchingLocation(
                                                                                KeyValueCondition
                                                                                    .newBuilder()
                                                                                    .setStaticValueCondition(
                                                                                        KeyValueCondition
                                                                                            .StaticValueCondition
                                                                                            .newBuilder()
                                                                                            .setKeyCondition(
                                                                                                KeyValueCondition
                                                                                                    .KeyCondition
                                                                                                    .newBuilder()
                                                                                                    .setKeyType(
                                                                                                        KeyValueCondition
                                                                                                            .Type
                                                                                                            .TYPE_REQUEST_BODY_PARAMETER)
                                                                                                    .setKeyMatchOperatorCondition(
                                                                                                        KeyValueCondition
                                                                                                            .MatchOperatorCondition
                                                                                                            .newBuilder()
                                                                                                            .setOperator(
                                                                                                                KeyValueCondition
                                                                                                                    .MatchOperator
                                                                                                                    .MATCH_OPERATOR_EQUALS)
                                                                                                            .setValue(
                                                                                                                Value
                                                                                                                    .newBuilder()
                                                                                                                    .setStringValue(
                                                                                                                        "param")))))))))))
                                    .addChildren(
                                        Condition.newBuilder()
                                            .setLeafCondition(
                                                LeafCondition.newBuilder()
                                                    .setScopeCondition(
                                                        ai.traceable.ratelimiting.config.service.v2
                                                            .ScopeCondition.newBuilder()
                                                            .setEntityScope(
                                                                ai.traceable.ratelimiting.config
                                                                    .service.v2.ScopeCondition
                                                                    .EntityScope.newBuilder()
                                                                    .setEntityType(
                                                                        ai.traceable.ratelimiting
                                                                            .config.service.v2
                                                                            .ScopeCondition
                                                                            .EntityType
                                                                            .ENTITY_TYPE_API)
                                                                    .addEntityIds("api-1")))))))
                    .setTransactionActionConfig(
                        TransactionActionConfig.newBuilder()
                            .setAction(
                                ai.traceable.ratelimiting.config.service.v2.Action.newBuilder()
                                    .setAlert(
                                        ai.traceable.ratelimiting.config.service.v2.Action.Alert
                                            .newBuilder()
                                            .setEventSeverity(
                                                ai.traceable.ratelimiting.config.service.v2.Action
                                                    .EventSeverity.EVENT_SEVERITY_HIGH))))
                    .setRuleStatus(
                        ai.traceable.ratelimiting.config.service.v2.RuleStatus.newBuilder()
                            .setRuleCreationSource(
                                ai.traceable.ratelimiting.config.service.v2.RuleStatus.RuleSource
                                    .RULE_SOURCE_CUSTOMER)
                            .setInternal(false)
                            .setHidden(false)))
            .build();

    // Convert back to AI app custom rule
    AiAppCustomRule aiAppRule =
        RateLimitingToAiAppConverter.convertFromRateLimitingRule(rateLimitingRule);

    // Verify basic conversion
    assertNotNull(aiAppRule);
    assertEquals("rule-123", aiAppRule.getRuleId());

    AiAppCustomRuleData aiAppRuleData = aiAppRule.getRuleData();
    assertEquals("Test PII Rule", aiAppRuleData.getRuleName());
    assertEquals("Test PII detection rule", aiAppRuleData.getDescription());
    assertTrue(aiAppRuleData.getEnabled());

    // Verify it's a PII detection rule with basic structure
    assertTrue(aiAppRuleData.hasPiiDetectedInPromptRuleData());
    PiiDetectedInPromptRuleData piiRuleData = aiAppRuleData.getPiiDetectedInPromptRuleData();
    assertTrue(piiRuleData.hasDatatypeCondition());
    assertEquals(1, piiRuleData.getDatatypeCondition().getDatasetIdsCount());
    assertEquals("dataset-1", piiRuleData.getDatatypeCondition().getDatasetIds(0));
    MatchOperatorCondition customLocationCondition =
        piiRuleData.getDatatypeCondition().getRequestBodyCustomLocationCondition();
    assertEquals(MatchOperator.MATCH_OPERATOR_EQUALS, customLocationCondition.getOperator());
    assertEquals("param", customLocationCondition.getValue().getStringValue());

    // Verify action conversion
    assertTrue(aiAppRuleData.hasAction());
    assertTrue(aiAppRuleData.getAction().hasAlert());
    assertEquals(
        SeverityLevel.SEVERITY_LEVEL_HIGH, aiAppRuleData.getAction().getAlert().getSeverityLevel());

    // Verify rule status
    assertTrue(aiAppRuleData.hasRuleStatusDetails());
    assertEquals(
        RuleStatusDetails.RuleSource.RULE_SOURCE_CUSTOMER,
        aiAppRuleData.getRuleStatusDetails().getRuleCreationSource());
  }

  @Test
  void testConvertAiRateLimitingRuleFromRateLimiting() {
    // Create simplified test rate limiting rule for AI rate limiting
    RateLimitingRule rateLimitingRule =
        RateLimitingRule.newBuilder()
            .setId("rule-456")
            .setData(
                RateLimitingRuleData.newBuilder()
                    .setName("Test AI Rate Limiting Rule")
                    .setDescription("Test AI rate limiting rule")
                    .setEnabled(true)
                    .setCategory(Category.CATEGORY_AI_APP_PROTECTION)
                    .putLabels(THREAT_TYPE_ID_LABEL_KEY, AI_RATE_LIMITING_THREAT_TYPE_ID)
                    .setCondition(
                        Condition.newBuilder()
                            .setCompositeCondition(
                                CompositeCondition.newBuilder()
                                    .setOperator(
                                        CompositeCondition.LogicalOperator.LOGICAL_OPERATOR_AND)
                                    .addChildren(
                                        Condition.newBuilder()
                                            .setLeafCondition(
                                                LeafCondition.newBuilder()
                                                    .setKeyValueCondition(
                                                        KeyValueCondition.newBuilder()
                                                            .setStaticValueCondition(
                                                                KeyValueCondition
                                                                    .StaticValueCondition
                                                                    .newBuilder()
                                                                    .setKeyCondition(
                                                                        KeyValueCondition
                                                                            .KeyCondition
                                                                            .newBuilder()
                                                                            .setKeyType(
                                                                                KeyValueCondition
                                                                                    .Type.TYPE_TAG)
                                                                            .setKeyMatchOperatorCondition(
                                                                                KeyValueCondition
                                                                                    .MatchOperatorCondition
                                                                                    .newBuilder()
                                                                                    .setOperator(
                                                                                        KeyValueCondition
                                                                                            .MatchOperator
                                                                                            .MATCH_OPERATOR_EQUALS)
                                                                                    .setValue(
                                                                                        Value
                                                                                            .newBuilder()
                                                                                            .setStringValue(
                                                                                                GENAI_MODELS_ATTRIBUTE_KEY))))
                                                                    .setValueMatchOperatorCondition(
                                                                        KeyValueCondition
                                                                            .MatchOperatorCondition
                                                                            .newBuilder()
                                                                            .setOperator(
                                                                                KeyValueCondition
                                                                                    .MatchOperator
                                                                                    .MATCH_OPERATOR_EQUALS)
                                                                            .setValue(
                                                                                Value.newBuilder()
                                                                                    .setStringValue(
                                                                                        "gpt-4")))))))
                                    .addChildren(
                                        Condition.newBuilder()
                                            .setLeafCondition(
                                                LeafCondition.newBuilder()
                                                    .setScopeCondition(
                                                        ai.traceable.ratelimiting.config.service.v2
                                                            .ScopeCondition.newBuilder()
                                                            .setEntityScope(
                                                                ai.traceable.ratelimiting.config
                                                                    .service.v2.ScopeCondition
                                                                    .EntityScope.newBuilder()
                                                                    .setEntityType(
                                                                        ai.traceable.ratelimiting
                                                                            .config.service.v2
                                                                            .ScopeCondition
                                                                            .EntityType
                                                                            .ENTITY_TYPE_API)
                                                                    .addEntityIds("api-123")))))))
                    .addThresholdActionConfigs(
                        ThresholdActionConfig.newBuilder()
                            .addActions(
                                ai.traceable.ratelimiting.config.service.v2.Action.newBuilder()
                                    .setMarkForTesting(
                                        ai.traceable.ratelimiting.config.service.v2.Action
                                            .MarkForTesting.newBuilder()))
                            .addResourceAccessThresholdConfigs(
                                ai.traceable.ratelimiting.config.service.v2
                                    .ResourceAccessThresholdConfig.newBuilder()
                                    .setUserAggregateType(
                                        ai.traceable.ratelimiting.config.service.v2
                                            .UserAggregateType.USER_AGGREGATE_TYPE_PER_USER)
                                    .setApiAggregateType(
                                        ai.traceable.ratelimiting.config.service.v2.ApiAggregateType
                                            .API_AGGREGATE_TYPE_PER_ENDPOINT)
                                    .setRollingWindowThresholdConfig(
                                        ai.traceable.ratelimiting.config.service.v2
                                            .ResourceAccessThresholdConfig
                                            .RollingWindowThresholdConfig.newBuilder()
                                            .setCountAllowed(100)
                                            .setDurationIso("PT1H"))))
                    .setRuleStatus(
                        ai.traceable.ratelimiting.config.service.v2.RuleStatus.newBuilder()
                            .setRuleCreationSource(
                                ai.traceable.ratelimiting.config.service.v2.RuleStatus.RuleSource
                                    .RULE_SOURCE_TRACEABLE)
                            .setInternal(true)
                            .setHidden(false)))
            .build();

    // Convert back to AI app custom rule
    AiAppCustomRule aiAppRule =
        RateLimitingToAiAppConverter.convertFromRateLimitingRule(rateLimitingRule);

    // Verify basic conversion
    assertNotNull(aiAppRule);
    assertEquals("rule-456", aiAppRule.getRuleId());

    AiAppCustomRuleData aiAppRuleData = aiAppRule.getRuleData();
    assertEquals("Test AI Rate Limiting Rule", aiAppRuleData.getRuleName());
    assertEquals("Test AI rate limiting rule", aiAppRuleData.getDescription());
    assertTrue(aiAppRuleData.getEnabled());

    // Verify it's an AI rate limiting rule with comprehensive structure
    assertTrue(aiAppRuleData.hasAiRateLimitingRuleData());
    AiRateLimitingRuleData aiRateLimitingData = aiAppRuleData.getAiRateLimitingRuleData();

    // Verify AI model types condition
    assertTrue(aiRateLimitingData.hasAiModelTypesCondition());
    assertEquals(
        MatchOperator.MATCH_OPERATOR_EQUALS,
        aiRateLimitingData.getAiModelTypesCondition().getOperator());
    assertEquals(
        "gpt-4", aiRateLimitingData.getAiModelTypesCondition().getValue().getStringValue());

    // Verify scope conditions are properly extracted
    assertEquals(1, aiRateLimitingData.getScopeConditionsCount());
    ai.traceable.aiapp.protection.config.service.v1.ScopeCondition scopeCondition =
        aiRateLimitingData.getScopeConditions(0);
    assertTrue(scopeCondition.hasEntityScope());

    ai.traceable.aiapp.protection.config.service.v1.ScopeCondition.EntityScope entityScope =
        scopeCondition.getEntityScope();
    assertEquals(
        ai.traceable.aiapp.protection.config.service.v1.ScopeCondition.EntityType.ENTITY_TYPE_API,
        entityScope.getEntityType());
    assertEquals(1, entityScope.getEntityIdsCount());
    assertEquals("api-123", entityScope.getEntityIds(0));

    // Verify threshold configuration
    assertTrue(aiRateLimitingData.hasThresholdConfig());
    ai.traceable.aiapp.protection.config.service.v1.ResourceAccessThresholdConfig
        aiAppThresholdConfig = aiRateLimitingData.getThresholdConfig();
    assertEquals(
        ai.traceable.aiapp.protection.config.service.v1.UserAggregateType
            .USER_AGGREGATE_TYPE_PER_USER,
        aiAppThresholdConfig.getUserAggregateType());
    assertEquals(100, aiAppThresholdConfig.getRollingWindowThresholdConfig().getCountAllowed());

    // Verify action conversion
    assertTrue(aiAppRuleData.hasAction());
    assertTrue(aiAppRuleData.getAction().hasMarkForTesting());

    // Verify rule status
    assertTrue(aiAppRuleData.hasRuleStatusDetails());
    assertEquals(
        RuleStatusDetails.RuleSource.RULE_SOURCE_TRACEABLE,
        aiAppRuleData.getRuleStatusDetails().getRuleCreationSource());
  }

  @Test
  void testConvertAiRateLimitingRuleWithUrlScopeFromRateLimiting() {
    // Create test rate limiting rule for AI rate limiting with URL scope condition
    RateLimitingRule rateLimitingRule =
        RateLimitingRule.newBuilder()
            .setId("rule-789")
            .setData(
                RateLimitingRuleData.newBuilder()
                    .setName("Test AI Rate Limiting Rule with URL Scope")
                    .setDescription("Test AI rate limiting rule with URL scope")
                    .setEnabled(true)
                    .setCategory(Category.CATEGORY_AI_APP_PROTECTION)
                    .putLabels(THREAT_TYPE_ID_LABEL_KEY, AI_RATE_LIMITING_THREAT_TYPE_ID)
                    .setCondition(
                        Condition.newBuilder()
                            .setCompositeCondition(
                                CompositeCondition.newBuilder()
                                    .setOperator(
                                        CompositeCondition.LogicalOperator.LOGICAL_OPERATOR_AND)
                                    .addChildren(
                                        Condition.newBuilder()
                                            .setLeafCondition(
                                                LeafCondition.newBuilder()
                                                    .setKeyValueCondition(
                                                        KeyValueCondition.newBuilder()
                                                            .setStaticValueCondition(
                                                                KeyValueCondition
                                                                    .StaticValueCondition
                                                                    .newBuilder()
                                                                    .setKeyCondition(
                                                                        KeyValueCondition
                                                                            .KeyCondition
                                                                            .newBuilder()
                                                                            .setKeyType(
                                                                                KeyValueCondition
                                                                                    .Type.TYPE_TAG)
                                                                            .setKeyMatchOperatorCondition(
                                                                                KeyValueCondition
                                                                                    .MatchOperatorCondition
                                                                                    .newBuilder()
                                                                                    .setOperator(
                                                                                        KeyValueCondition
                                                                                            .MatchOperator
                                                                                            .MATCH_OPERATOR_EQUALS)
                                                                                    .setValue(
                                                                                        Value
                                                                                            .newBuilder()
                                                                                            .setStringValue(
                                                                                                GENAI_PROVIDERS_ATTRIBUTE_KEY))))
                                                                    .setValueMatchOperatorCondition(
                                                                        KeyValueCondition
                                                                            .MatchOperatorCondition
                                                                            .newBuilder()
                                                                            .setOperator(
                                                                                KeyValueCondition
                                                                                    .MatchOperator
                                                                                    .MATCH_OPERATOR_CONTAINS)
                                                                            .setValue(
                                                                                Value.newBuilder()
                                                                                    .setStringValue(
                                                                                        "openai")))))))
                                    .addChildren(
                                        Condition.newBuilder()
                                            .setLeafCondition(
                                                LeafCondition.newBuilder()
                                                    .setScopeCondition(
                                                        ai.traceable.ratelimiting.config.service.v2
                                                            .ScopeCondition.newBuilder()
                                                            .setUrlScope(
                                                                ai.traceable.ratelimiting.config
                                                                    .service.v2.ScopeCondition
                                                                    .UrlScope.newBuilder()
                                                                    .addUrlRegexes("/api/v1/.*")
                                                                    .addUrlRegexes(
                                                                        "/api/v2/.*")))))))
                    .addThresholdActionConfigs(
                        ThresholdActionConfig.newBuilder()
                            .addActions(
                                ai.traceable.ratelimiting.config.service.v2.Action.newBuilder()
                                    .setAlert(
                                        ai.traceable.ratelimiting.config.service.v2.Action.Alert
                                            .newBuilder()
                                            .setEventSeverity(
                                                ai.traceable.ratelimiting.config.service.v2.Action
                                                    .EventSeverity.EVENT_SEVERITY_MEDIUM)))
                            .addResourceAccessThresholdConfigs(
                                ai.traceable.ratelimiting.config.service.v2
                                    .ResourceAccessThresholdConfig.newBuilder()
                                    .setUserAggregateType(
                                        ai.traceable.ratelimiting.config.service.v2
                                            .UserAggregateType.USER_AGGREGATE_TYPE_ACROSS_USERS)
                                    .setApiAggregateType(
                                        ai.traceable.ratelimiting.config.service.v2.ApiAggregateType
                                            .API_AGGREGATE_TYPE_ACROSS_ENDPOINTS)
                                    .setRollingWindowThresholdConfig(
                                        ai.traceable.ratelimiting.config.service.v2
                                            .ResourceAccessThresholdConfig
                                            .RollingWindowThresholdConfig.newBuilder()
                                            .setCountAllowed(50)
                                            .setDurationIso("PT30M"))))
                    .setRuleStatus(
                        ai.traceable.ratelimiting.config.service.v2.RuleStatus.newBuilder()
                            .setRuleCreationSource(
                                ai.traceable.ratelimiting.config.service.v2.RuleStatus.RuleSource
                                    .RULE_SOURCE_CUSTOMER)
                            .setInternal(false)
                            .setHidden(true)))
            .build();

    // Convert back to AI app custom rule
    AiAppCustomRule aiAppRule =
        RateLimitingToAiAppConverter.convertFromRateLimitingRule(rateLimitingRule);

    // Verify basic conversion
    assertNotNull(aiAppRule);
    assertEquals("rule-789", aiAppRule.getRuleId());

    AiAppCustomRuleData aiAppRuleData = aiAppRule.getRuleData();
    assertEquals("Test AI Rate Limiting Rule with URL Scope", aiAppRuleData.getRuleName());
    assertEquals("Test AI rate limiting rule with URL scope", aiAppRuleData.getDescription());
    assertTrue(aiAppRuleData.getEnabled());

    // Verify it's an AI rate limiting rule with comprehensive structure
    assertTrue(aiAppRuleData.hasAiRateLimitingRuleData());
    AiRateLimitingRuleData aiRateLimitingData = aiAppRuleData.getAiRateLimitingRuleData();

    // Verify AI vendors condition
    assertTrue(aiRateLimitingData.hasAiVendorsCondition());
    assertEquals(
        MatchOperator.MATCH_OPERATOR_CONTAINS,
        aiRateLimitingData.getAiVendorsCondition().getOperator());
    assertEquals("openai", aiRateLimitingData.getAiVendorsCondition().getValue().getStringValue());

    // Verify URL scope conditions are properly extracted
    assertEquals(1, aiRateLimitingData.getScopeConditionsCount());
    ai.traceable.aiapp.protection.config.service.v1.ScopeCondition scopeCondition =
        aiRateLimitingData.getScopeConditions(0);
    assertTrue(scopeCondition.hasUrlScope());

    ai.traceable.aiapp.protection.config.service.v1.ScopeCondition.UrlScope urlScope =
        scopeCondition.getUrlScope();
    assertEquals(2, urlScope.getUrlRegexesCount());
    assertEquals("/api/v1/.*", urlScope.getUrlRegexes(0));
    assertEquals("/api/v2/.*", urlScope.getUrlRegexes(1));

    // Verify threshold configuration
    assertTrue(aiRateLimitingData.hasThresholdConfig());
    ai.traceable.aiapp.protection.config.service.v1.ResourceAccessThresholdConfig
        aiAppThresholdConfig = aiRateLimitingData.getThresholdConfig();
    assertEquals(
        ai.traceable.aiapp.protection.config.service.v1.UserAggregateType
            .USER_AGGREGATE_TYPE_ACROSS_USERS,
        aiAppThresholdConfig.getUserAggregateType());
    assertEquals(
        ai.traceable.aiapp.protection.config.service.v1.ApiAggregateType
            .API_AGGREGATE_TYPE_ACROSS_ENDPOINTS,
        aiAppThresholdConfig.getApiAggregateType());
    assertEquals(50, aiAppThresholdConfig.getRollingWindowThresholdConfig().getCountAllowed());
    assertEquals("PT30M", aiAppThresholdConfig.getRollingWindowThresholdConfig().getDurationIso());

    // Verify action conversion
    assertTrue(aiAppRuleData.hasAction());
    assertTrue(aiAppRuleData.getAction().hasAlert());
    assertEquals(
        SeverityLevel.SEVERITY_LEVEL_MEDIUM,
        aiAppRuleData.getAction().getAlert().getSeverityLevel());

    // Verify rule status
    assertTrue(aiAppRuleData.hasRuleStatusDetails());
    assertEquals(
        RuleStatusDetails.RuleSource.RULE_SOURCE_CUSTOMER,
        aiAppRuleData.getRuleStatusDetails().getRuleCreationSource());
    assertFalse(aiAppRuleData.getRuleStatusDetails().getInternal());
    assertTrue(aiAppRuleData.getRuleStatusDetails().getHidden());
  }
}
