package ai.traceable.aiapp.protection.config.service.converter.ratelimit;

import static ai.traceable.aiapp.protection.config.service.converter.AiAppConverterConstants.AI_RATE_LIMITING_THREAT_TYPE_ID;
import static ai.traceable.aiapp.protection.config.service.converter.AiAppConverterConstants.AI_SENSITIVE_DATA_PROTECTION_THREAT_TYPE_ID;
import static ai.traceable.aiapp.protection.config.service.converter.AiAppConverterConstants.GENAI_MODELS_ATTRIBUTE_KEY;
import static ai.traceable.aiapp.protection.config.service.converter.AiAppConverterConstants.GENAI_PROVIDERS_ATTRIBUTE_KEY;
import static ai.traceable.aiapp.protection.config.service.converter.AiAppConverterConstants.PII_DETECTED_IN_PROMPT_THREAT_TYPE_ID;
import static ai.traceable.aiapp.protection.config.service.converter.AiAppConverterConstants.THREAT_TYPE_ID_LABEL_KEY;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import ai.traceable.aiapp.protection.config.service.v1.AiAppCustomRuleData;
import ai.traceable.aiapp.protection.config.service.v1.AiRateLimitingRuleData;
import ai.traceable.aiapp.protection.config.service.v1.AiSensitiveDataProtectionRuleData;
import ai.traceable.aiapp.protection.config.service.v1.DatatypeCondition;
import ai.traceable.aiapp.protection.config.service.v1.MatchOperator;
import ai.traceable.aiapp.protection.config.service.v1.MatchOperatorCondition;
import ai.traceable.aiapp.protection.config.service.v1.PiiDetectedInPromptRuleData;
import ai.traceable.aiapp.protection.config.service.v1.RuleStatusDetails;
import ai.traceable.aiapp.protection.config.service.v1.SeverityLevel;
import ai.traceable.ratelimiting.config.service.v2.Category;
import ai.traceable.ratelimiting.config.service.v2.CompositeCondition;
import ai.traceable.ratelimiting.config.service.v2.Condition;
import ai.traceable.ratelimiting.config.service.v2.CreateRateLimitingRuleRequest;
import ai.traceable.ratelimiting.config.service.v2.KeyValueCondition;
import ai.traceable.ratelimiting.config.service.v2.LeafCondition;
import ai.traceable.ratelimiting.config.service.v2.RateLimitingRuleData;
import ai.traceable.ratelimiting.config.service.v2.ThresholdActionConfig;
import com.google.protobuf.Value;
import org.junit.jupiter.api.Test;

class AiAppToRateLimitingConverterTest {

  private final AiAppToRateLimitingConverter aiAppToRateLimitingConverter =
      new AiAppToRateLimitingConverter();

  @Test
  void testConvertPiiDetectedInPromptRule() {
    // Create simplified test data for PII detection rule
    AiAppCustomRuleData aiAppRuleData =
        AiAppCustomRuleData.newBuilder()
            .setRuleName("Test PII Rule")
            .setDescription("Test PII detection rule")
            .setEnabled(true)
            .setPiiDetectedInPromptRuleData(
                PiiDetectedInPromptRuleData.newBuilder()
                    .setDatatypeCondition(
                        ai.traceable.aiapp.protection.config.service.v1.DatatypeCondition
                            .newBuilder()
                            .addDatasetIds("dataset-1")
                            .addDatatypeIds("datatype-1")
                            .setRequestBodyCustomLocationCondition(
                                MatchOperatorCondition.newBuilder()
                                    .setOperator(MatchOperator.MATCH_OPERATOR_EQUALS)
                                    .setValue(Value.newBuilder().setStringValue("param"))))
                    .addScopeConditions(
                        ai.traceable.aiapp.protection.config.service.v1.ScopeCondition.newBuilder()
                            .setEntityScope(
                                ai.traceable.aiapp.protection.config.service.v1.ScopeCondition
                                    .EntityScope.newBuilder()
                                    .setEntityType(
                                        ai.traceable.aiapp.protection.config.service.v1
                                            .ScopeCondition.EntityType.ENTITY_TYPE_API)
                                    .addEntityIds("api-1"))))
            .setAction(
                ai.traceable.aiapp.protection.config.service.v1.Action.newBuilder()
                    .setAlert(
                        ai.traceable.aiapp.protection.config.service.v1.Action.Alert.newBuilder()
                            .setSeverityLevel(SeverityLevel.SEVERITY_LEVEL_HIGH)))
            .setRuleStatusDetails(
                RuleStatusDetails.newBuilder()
                    .setRuleCreationSource(RuleStatusDetails.RuleSource.RULE_SOURCE_CUSTOMER)
                    .setInternal(false)
                    .setHidden(false))
            .build();

    // Convert to rate limiting rule
    CreateRateLimitingRuleRequest request =
        aiAppToRateLimitingConverter.convertToCreateRateLimitingRuleRequest(aiAppRuleData);

    // Verify basic conversion
    assertNotNull(request);
    RateLimitingRuleData ruleData = request.getData();

    assertEquals("Test PII Rule", ruleData.getName());
    assertEquals("Test PII detection rule", ruleData.getDescription());
    assertTrue(ruleData.getEnabled());
    assertEquals(Category.CATEGORY_AI_APP_PROTECTION, ruleData.getCategory());
    assertEquals(
        PII_DETECTED_IN_PROMPT_THREAT_TYPE_ID,
        ruleData.getLabelsMap().get(THREAT_TYPE_ID_LABEL_KEY));

    // Verify action conversion
    assertTrue(ruleData.hasTransactionActionConfig());
    assertTrue(ruleData.getTransactionActionConfig().getAction().hasAlert());
    assertEquals(
        ai.traceable.ratelimiting.config.service.v2.Action.EventSeverity.EVENT_SEVERITY_HIGH,
        ruleData.getTransactionActionConfig().getAction().getAlert().getEventSeverity());

    // Verify condition structure for PII detection rule
    assertTrue(ruleData.hasCondition());
    Condition condition = ruleData.getCondition();

    // Should be a composite condition with datatype and scope conditions
    assertTrue(condition.hasCompositeCondition());
    CompositeCondition composite = condition.getCompositeCondition();
    assertEquals(CompositeCondition.LogicalOperator.LOGICAL_OPERATOR_AND, composite.getOperator());
    assertEquals(2, composite.getChildrenCount());

    // Verify datatype condition
    Condition datatypeConditionWrapper = composite.getChildren(0);
    assertTrue(datatypeConditionWrapper.hasLeafCondition());
    LeafCondition datatypeLeaf = datatypeConditionWrapper.getLeafCondition();
    assertTrue(datatypeLeaf.hasDatatypeCondition());

    ai.traceable.ratelimiting.config.service.v2.DatatypeCondition rlDatatypeCondition =
        datatypeLeaf.getDatatypeCondition();
    assertEquals(1, rlDatatypeCondition.getDatasetIdsCount());
    assertEquals("dataset-1", rlDatatypeCondition.getDatasetIds(0));
    assertEquals(1, rlDatatypeCondition.getDatatypeIdsCount());
    assertEquals("datatype-1", rlDatatypeCondition.getDatatypeIds(0));
    assertEquals(
        ai.traceable.ratelimiting.config.service.v2.DataLocation.DATA_LOCATION_REQUEST,
        rlDatatypeCondition.getDataLocation());
    KeyValueCondition.KeyCondition customMatchingCondition =
        rlDatatypeCondition
            .getDatatypeMatching()
            .getRegexBasedMatching()
            .getCustomMatchingLocation()
            .getStaticValueCondition()
            .getKeyCondition();
    assertEquals(
        KeyValueCondition.Type.TYPE_REQUEST_BODY_PARAMETER, customMatchingCondition.getKeyType());
    assertEquals(
        KeyValueCondition.MatchOperator.MATCH_OPERATOR_EQUALS,
        customMatchingCondition.getKeyMatchOperatorCondition().getOperator());
    assertEquals(
        "param",
        customMatchingCondition.getKeyMatchOperatorCondition().getValue().getStringValue());

    // Verify scope condition
    Condition scopeConditionWrapper = composite.getChildren(1);
    assertTrue(scopeConditionWrapper.hasLeafCondition());
    LeafCondition scopeLeaf = scopeConditionWrapper.getLeafCondition();
    assertTrue(scopeLeaf.hasScopeCondition());

    ai.traceable.ratelimiting.config.service.v2.ScopeCondition rlScopeCondition =
        scopeLeaf.getScopeCondition();
    assertTrue(rlScopeCondition.hasEntityScope());
    assertEquals(
        ai.traceable.ratelimiting.config.service.v2.ScopeCondition.EntityType.ENTITY_TYPE_API,
        rlScopeCondition.getEntityScope().getEntityType());
    assertEquals(1, rlScopeCondition.getEntityScope().getEntityIdsCount());
    assertEquals("api-1", rlScopeCondition.getEntityScope().getEntityIds(0));
  }

  @Test
  void testConvertAiRateLimitingRule() {
    // Create simplified test data for AI rate limiting rule
    AiAppCustomRuleData aiAppRuleData =
        AiAppCustomRuleData.newBuilder()
            .setRuleName("Test AI Rate Limiting Rule")
            .setDescription("Test AI rate limiting rule")
            .setEnabled(true)
            .setAiRateLimitingRuleData(
                AiRateLimitingRuleData.newBuilder()
                    .setAiModelTypesCondition(
                        MatchOperatorCondition.newBuilder()
                            .setOperator(MatchOperator.MATCH_OPERATOR_EQUALS)
                            .setValue(Value.newBuilder().setStringValue("gpt-4")))
                    .setAiVendorsCondition(
                        MatchOperatorCondition.newBuilder()
                            .setOperator(MatchOperator.MATCH_OPERATOR_CONTAINS)
                            .setValue(Value.newBuilder().setStringValue("openai")))
                    .addScopeConditions(
                        ai.traceable.aiapp.protection.config.service.v1.ScopeCondition.newBuilder()
                            .setEntityScope(
                                ai.traceable.aiapp.protection.config.service.v1.ScopeCondition
                                    .EntityScope.newBuilder()
                                    .setEntityType(
                                        ai.traceable.aiapp.protection.config.service.v1
                                            .ScopeCondition.EntityType.ENTITY_TYPE_API)
                                    .addEntityIds("api-1")))
                    .setThresholdConfig(
                        ai.traceable.aiapp.protection.config.service.v1
                            .ResourceAccessThresholdConfig.newBuilder()
                            .setUserAggregateType(
                                ai.traceable.aiapp.protection.config.service.v1.UserAggregateType
                                    .USER_AGGREGATE_TYPE_PER_USER)
                            .setApiAggregateType(
                                ai.traceable.aiapp.protection.config.service.v1.ApiAggregateType
                                    .API_AGGREGATE_TYPE_PER_ENDPOINT)
                            .setRollingWindowThresholdConfig(
                                ai.traceable.aiapp.protection.config.service.v1
                                    .ResourceAccessThresholdConfig.RollingWindowThresholdConfig
                                    .newBuilder()
                                    .setCountAllowed(100)
                                    .setDurationIso("PT1H"))))
            .setAction(
                ai.traceable.aiapp.protection.config.service.v1.Action.newBuilder()
                    .setMarkForTesting(
                        ai.traceable.aiapp.protection.config.service.v1.Action.MarkForTesting
                            .newBuilder()))
            .setRuleStatusDetails(
                RuleStatusDetails.newBuilder()
                    .setRuleCreationSource(RuleStatusDetails.RuleSource.RULE_SOURCE_TRACEABLE)
                    .setInternal(true)
                    .setHidden(false))
            .build();

    // Convert to rate limiting rule
    RateLimitingRuleData ruleData =
        aiAppToRateLimitingConverter.convertToRateLimitingRule(aiAppRuleData);

    // Verify basic conversion
    assertNotNull(ruleData);
    assertEquals("Test AI Rate Limiting Rule", ruleData.getName());
    assertEquals("Test AI rate limiting rule", ruleData.getDescription());
    assertTrue(ruleData.getEnabled());
    assertEquals(Category.CATEGORY_AI_APP_PROTECTION, ruleData.getCategory());
    assertEquals(
        AI_RATE_LIMITING_THREAT_TYPE_ID, ruleData.getLabelsMap().get(THREAT_TYPE_ID_LABEL_KEY));

    // Verify threshold action config
    assertEquals(1, ruleData.getThresholdActionConfigsCount());
    ThresholdActionConfig thresholdActionConfig = ruleData.getThresholdActionConfigs(0);
    assertEquals(1, thresholdActionConfig.getActionsCount());
    assertTrue(thresholdActionConfig.getActions(0).hasMarkForTesting());

    // Verify threshold configuration
    assertEquals(1, thresholdActionConfig.getResourceAccessThresholdConfigsCount());
    ai.traceable.ratelimiting.config.service.v2.ResourceAccessThresholdConfig rlThresholdConfig =
        thresholdActionConfig.getResourceAccessThresholdConfigs(0);
    assertEquals(
        ai.traceable.ratelimiting.config.service.v2.UserAggregateType.USER_AGGREGATE_TYPE_PER_USER,
        rlThresholdConfig.getUserAggregateType());
    assertEquals(
        ai.traceable.ratelimiting.config.service.v2.ApiAggregateType
            .API_AGGREGATE_TYPE_PER_ENDPOINT,
        rlThresholdConfig.getApiAggregateType());

    assertTrue(rlThresholdConfig.hasRollingWindowThresholdConfig());
    assertEquals(100, rlThresholdConfig.getRollingWindowThresholdConfig().getCountAllowed());
    assertEquals("PT1H", rlThresholdConfig.getRollingWindowThresholdConfig().getDurationIso());

    // Verify condition structure for AI model types and vendors
    assertTrue(ruleData.hasCondition());
    Condition condition = ruleData.getCondition();

    // Should be a composite condition with model types, vendors, and scope conditions
    assertTrue(condition.hasCompositeCondition());
    CompositeCondition composite = condition.getCompositeCondition();
    assertEquals(CompositeCondition.LogicalOperator.LOGICAL_OPERATOR_AND, composite.getOperator());
    assertEquals(3, composite.getChildrenCount());

    // Verify AI model types condition
    Condition modelTypesConditionWrapper = composite.getChildren(0);
    assertTrue(modelTypesConditionWrapper.hasLeafCondition());
    LeafCondition modelTypesLeaf = modelTypesConditionWrapper.getLeafCondition();
    assertTrue(modelTypesLeaf.hasKeyValueCondition());

    KeyValueCondition modelTypesKeyValue = modelTypesLeaf.getKeyValueCondition();
    assertTrue(modelTypesKeyValue.hasStaticValueCondition());
    KeyValueCondition.StaticValueCondition modelTypesStatic =
        modelTypesKeyValue.getStaticValueCondition();

    // Verify key condition for model types
    assertTrue(modelTypesStatic.hasKeyCondition());
    KeyValueCondition.KeyCondition modelTypesKeyCondition = modelTypesStatic.getKeyCondition();
    assertEquals(KeyValueCondition.Type.TYPE_TAG, modelTypesKeyCondition.getKeyType());
    assertEquals(
        GENAI_MODELS_ATTRIBUTE_KEY,
        modelTypesKeyCondition.getKeyMatchOperatorCondition().getValue().getStringValue());

    // Verify value condition for model types
    assertTrue(modelTypesStatic.hasValueMatchOperatorCondition());
    KeyValueCondition.MatchOperatorCondition modelTypesValueCondition =
        modelTypesStatic.getValueMatchOperatorCondition();
    assertEquals(
        KeyValueCondition.MatchOperator.MATCH_OPERATOR_EQUALS,
        modelTypesValueCondition.getOperator());
    assertEquals("gpt-4", modelTypesValueCondition.getValue().getStringValue());

    // Verify AI vendors condition
    Condition vendorsConditionWrapper = composite.getChildren(1);
    assertTrue(vendorsConditionWrapper.hasLeafCondition());
    LeafCondition vendorsLeaf = vendorsConditionWrapper.getLeafCondition();
    assertTrue(vendorsLeaf.hasKeyValueCondition());

    KeyValueCondition vendorsKeyValue = vendorsLeaf.getKeyValueCondition();
    assertTrue(vendorsKeyValue.hasStaticValueCondition());
    KeyValueCondition.StaticValueCondition vendorsStatic =
        vendorsKeyValue.getStaticValueCondition();

    // Verify key condition for vendors
    assertTrue(vendorsStatic.hasKeyCondition());
    KeyValueCondition.KeyCondition vendorsKeyCondition = vendorsStatic.getKeyCondition();
    assertEquals(KeyValueCondition.Type.TYPE_TAG, vendorsKeyCondition.getKeyType());
    assertEquals(
        GENAI_PROVIDERS_ATTRIBUTE_KEY,
        vendorsKeyCondition.getKeyMatchOperatorCondition().getValue().getStringValue());

    // Verify value condition for vendors
    assertTrue(vendorsStatic.hasValueMatchOperatorCondition());
    KeyValueCondition.MatchOperatorCondition vendorsValueCondition =
        vendorsStatic.getValueMatchOperatorCondition();
    assertEquals(
        KeyValueCondition.MatchOperator.MATCH_OPERATOR_CONTAINS,
        vendorsValueCondition.getOperator());
    assertEquals("openai", vendorsValueCondition.getValue().getStringValue());

    // Verify scope condition
    Condition scopeConditionWrapper = composite.getChildren(2);
    assertTrue(scopeConditionWrapper.hasLeafCondition());
    LeafCondition scopeLeaf = scopeConditionWrapper.getLeafCondition();
    assertTrue(scopeLeaf.hasScopeCondition());

    ai.traceable.ratelimiting.config.service.v2.ScopeCondition rlScopeCondition =
        scopeLeaf.getScopeCondition();
    assertTrue(rlScopeCondition.hasEntityScope());
    assertEquals(
        ai.traceable.ratelimiting.config.service.v2.ScopeCondition.EntityType.ENTITY_TYPE_API,
        rlScopeCondition.getEntityScope().getEntityType());
    assertEquals(1, rlScopeCondition.getEntityScope().getEntityIdsCount());
    assertEquals("api-1", rlScopeCondition.getEntityScope().getEntityIds(0));
  }

  @Test
  void testConvertAiSensitiveDataProtectionRule_singleRequestCondition_buildsCorrectRule() {
    AiAppCustomRuleData aiAppRuleData =
        AiAppCustomRuleData.newBuilder()
            .setRuleName("Test Sensitive Data Rule")
            .setDescription("Test sensitive data protection rule")
            .setEnabled(true)
            .setAiSensitiveDataProtectionRuleData(
                AiSensitiveDataProtectionRuleData.newBuilder()
                    .addDatatypeConditions(
                        DatatypeCondition.newBuilder()
                            .addDatasetIds("ds-1")
                            .addDatatypeIds("dt-1")
                            .setRequestBodyCustomLocationCondition(
                                MatchOperatorCondition.newBuilder()
                                    .setOperator(MatchOperator.MATCH_OPERATOR_EQUALS)
                                    .setValue(Value.newBuilder().setStringValue("body.field")))))
            .setAction(
                ai.traceable.aiapp.protection.config.service.v1.Action.newBuilder()
                    .setAlert(
                        ai.traceable.aiapp.protection.config.service.v1.Action.Alert.newBuilder()
                            .setSeverityLevel(SeverityLevel.SEVERITY_LEVEL_HIGH)))
            .setRuleStatusDetails(
                RuleStatusDetails.newBuilder()
                    .setRuleCreationSource(RuleStatusDetails.RuleSource.RULE_SOURCE_CUSTOMER)
                    .setInternal(false)
                    .setHidden(false))
            .build();

    CreateRateLimitingRuleRequest request =
        aiAppToRateLimitingConverter.convertToCreateRateLimitingRuleRequest(aiAppRuleData);

    assertNotNull(request);
    RateLimitingRuleData ruleData = request.getData();

    assertEquals("Test Sensitive Data Rule", ruleData.getName());
    assertEquals(Category.CATEGORY_AI_APP_PROTECTION, ruleData.getCategory());
    assertEquals(
        AI_SENSITIVE_DATA_PROTECTION_THREAT_TYPE_ID,
        ruleData.getLabelsMap().get(THREAT_TYPE_ID_LABEL_KEY));

    assertTrue(ruleData.hasTransactionActionConfig());
    assertTrue(ruleData.getTransactionActionConfig().getAction().hasAlert());
    assertEquals(
        ai.traceable.ratelimiting.config.service.v2.Action.EventSeverity.EVENT_SEVERITY_HIGH,
        ruleData.getTransactionActionConfig().getAction().getAlert().getEventSeverity());

    assertTrue(ruleData.hasCondition());
    Condition condition = ruleData.getCondition();
    // Single condition is wrapped as a leaf (no composite)
    assertTrue(condition.hasLeafCondition());
    LeafCondition leaf = condition.getLeafCondition();
    assertTrue(leaf.hasDatatypeCondition());

    ai.traceable.ratelimiting.config.service.v2.DatatypeCondition rlDatatypeCondition =
        leaf.getDatatypeCondition();
    assertEquals(1, rlDatatypeCondition.getDatasetIdsCount());
    assertEquals("ds-1", rlDatatypeCondition.getDatasetIds(0));
    assertEquals(1, rlDatatypeCondition.getDatatypeIdsCount());
    assertEquals("dt-1", rlDatatypeCondition.getDatatypeIds(0));
    assertEquals(
        ai.traceable.ratelimiting.config.service.v2.DataLocation.DATA_LOCATION_REQUEST,
        rlDatatypeCondition.getDataLocation());

    KeyValueCondition.KeyCondition customMatchingCondition =
        rlDatatypeCondition
            .getDatatypeMatching()
            .getRegexBasedMatching()
            .getCustomMatchingLocation()
            .getStaticValueCondition()
            .getKeyCondition();
    assertEquals(
        KeyValueCondition.Type.TYPE_REQUEST_BODY_PARAMETER, customMatchingCondition.getKeyType());
    assertEquals(
        KeyValueCondition.MatchOperator.MATCH_OPERATOR_EQUALS,
        customMatchingCondition.getKeyMatchOperatorCondition().getOperator());
    assertEquals(
        "body.field",
        customMatchingCondition.getKeyMatchOperatorCondition().getValue().getStringValue());
  }

  @Test
  void testConvertAiSensitiveDataProtectionRule_responseLocation_setsResponseDataLocation() {
    AiAppCustomRuleData aiAppRuleData =
        AiAppCustomRuleData.newBuilder()
            .setRuleName("Test Sensitive Data Response Rule")
            .setDescription("Response body sensitive data rule")
            .setEnabled(true)
            .setAiSensitiveDataProtectionRuleData(
                AiSensitiveDataProtectionRuleData.newBuilder()
                    .addDatatypeConditions(
                        DatatypeCondition.newBuilder()
                            .addDatasetIds("ds-1")
                            .addDatatypeIds("dt-1")
                            .setResponseBodyCustomLocationCondition(
                                MatchOperatorCondition.newBuilder()
                                    .setOperator(MatchOperator.MATCH_OPERATOR_EQUALS)
                                    .setValue(Value.newBuilder().setStringValue("body.resp")))))
            .setAction(
                ai.traceable.aiapp.protection.config.service.v1.Action.newBuilder()
                    .setAlert(
                        ai.traceable.aiapp.protection.config.service.v1.Action.Alert.newBuilder()
                            .setSeverityLevel(SeverityLevel.SEVERITY_LEVEL_HIGH)))
            .setRuleStatusDetails(
                RuleStatusDetails.newBuilder()
                    .setRuleCreationSource(RuleStatusDetails.RuleSource.RULE_SOURCE_CUSTOMER)
                    .setInternal(false)
                    .setHidden(false))
            .build();

    CreateRateLimitingRuleRequest request =
        aiAppToRateLimitingConverter.convertToCreateRateLimitingRuleRequest(aiAppRuleData);

    assertNotNull(request);
    RateLimitingRuleData ruleData = request.getData();
    assertTrue(ruleData.hasCondition());

    Condition condition = ruleData.getCondition();
    assertTrue(condition.hasLeafCondition());
    LeafCondition leaf = condition.getLeafCondition();
    assertTrue(leaf.hasDatatypeCondition());

    ai.traceable.ratelimiting.config.service.v2.DatatypeCondition rlDatatypeCondition =
        leaf.getDatatypeCondition();
    assertEquals(
        ai.traceable.ratelimiting.config.service.v2.DataLocation.DATA_LOCATION_RESPONSE,
        rlDatatypeCondition.getDataLocation());

    KeyValueCondition.KeyCondition customMatchingCondition =
        rlDatatypeCondition
            .getDatatypeMatching()
            .getRegexBasedMatching()
            .getCustomMatchingLocation()
            .getStaticValueCondition()
            .getKeyCondition();
    assertEquals(
        KeyValueCondition.Type.TYPE_RESPONSE_BODY_PARAMETER, customMatchingCondition.getKeyType());
    assertEquals(
        KeyValueCondition.MatchOperator.MATCH_OPERATOR_EQUALS,
        customMatchingCondition.getKeyMatchOperatorCondition().getOperator());
    assertEquals(
        "body.resp",
        customMatchingCondition.getKeyMatchOperatorCondition().getValue().getStringValue());
  }

  @Test
  void testConvertAiSensitiveDataProtectionRule_multipleConditions_orsConditions() {
    AiAppCustomRuleData aiAppRuleData =
        AiAppCustomRuleData.newBuilder()
            .setRuleName("Test Sensitive Data Multi Rule")
            .setDescription("Multi datatype condition rule")
            .setEnabled(true)
            .setAiSensitiveDataProtectionRuleData(
                AiSensitiveDataProtectionRuleData.newBuilder()
                    .addDatatypeConditions(
                        DatatypeCondition.newBuilder()
                            .addDatasetIds("ds-1")
                            .addDatatypeIds("dt-1")
                            .setRequestBodyCustomLocationCondition(
                                MatchOperatorCondition.newBuilder()
                                    .setOperator(MatchOperator.MATCH_OPERATOR_EQUALS)
                                    .setValue(Value.newBuilder().setStringValue("req.field"))))
                    .addDatatypeConditions(
                        DatatypeCondition.newBuilder()
                            .addDatasetIds("ds-2")
                            .addDatatypeIds("dt-2")
                            .setResponseBodyCustomLocationCondition(
                                MatchOperatorCondition.newBuilder()
                                    .setOperator(MatchOperator.MATCH_OPERATOR_EQUALS)
                                    .setValue(Value.newBuilder().setStringValue("resp.field")))))
            .setAction(
                ai.traceable.aiapp.protection.config.service.v1.Action.newBuilder()
                    .setAlert(
                        ai.traceable.aiapp.protection.config.service.v1.Action.Alert.newBuilder()
                            .setSeverityLevel(SeverityLevel.SEVERITY_LEVEL_HIGH)))
            .setRuleStatusDetails(
                RuleStatusDetails.newBuilder()
                    .setRuleCreationSource(RuleStatusDetails.RuleSource.RULE_SOURCE_CUSTOMER)
                    .setInternal(false)
                    .setHidden(false))
            .build();

    CreateRateLimitingRuleRequest request =
        aiAppToRateLimitingConverter.convertToCreateRateLimitingRuleRequest(aiAppRuleData);

    assertNotNull(request);
    RateLimitingRuleData ruleData = request.getData();
    assertTrue(ruleData.hasCondition());

    Condition condition = ruleData.getCondition();
    // Multiple datatype conditions with no scope conditions => OR composite
    assertTrue(condition.hasCompositeCondition());
    CompositeCondition composite = condition.getCompositeCondition();
    assertEquals(CompositeCondition.LogicalOperator.LOGICAL_OPERATOR_OR, composite.getOperator());
    assertEquals(2, composite.getChildrenCount());

    Condition first = composite.getChildren(0);
    assertTrue(first.hasLeafCondition());
    assertTrue(first.getLeafCondition().hasDatatypeCondition());
    assertEquals(
        ai.traceable.ratelimiting.config.service.v2.DataLocation.DATA_LOCATION_REQUEST,
        first.getLeafCondition().getDatatypeCondition().getDataLocation());

    Condition second = composite.getChildren(1);
    assertTrue(second.hasLeafCondition());
    assertTrue(second.getLeafCondition().hasDatatypeCondition());
    assertEquals(
        ai.traceable.ratelimiting.config.service.v2.DataLocation.DATA_LOCATION_RESPONSE,
        second.getLeafCondition().getDatatypeCondition().getDataLocation());
  }

  @Test
  void testConvertAiSensitiveDataProtectionRule_multipleConditionsWithScope_andsWithScope() {
    AiAppCustomRuleData aiAppRuleData =
        AiAppCustomRuleData.newBuilder()
            .setRuleName("Test Sensitive Data Multi+Scope Rule")
            .setDescription("Multi datatype + scope condition rule")
            .setEnabled(true)
            .setAiSensitiveDataProtectionRuleData(
                AiSensitiveDataProtectionRuleData.newBuilder()
                    .addDatatypeConditions(
                        DatatypeCondition.newBuilder()
                            .addDatasetIds("ds-1")
                            .addDatatypeIds("dt-1")
                            .setRequestBodyCustomLocationCondition(
                                MatchOperatorCondition.newBuilder()
                                    .setOperator(MatchOperator.MATCH_OPERATOR_EQUALS)
                                    .setValue(Value.newBuilder().setStringValue("req.field"))))
                    .addDatatypeConditions(
                        DatatypeCondition.newBuilder()
                            .addDatasetIds("ds-2")
                            .addDatatypeIds("dt-2")
                            .setResponseBodyCustomLocationCondition(
                                MatchOperatorCondition.newBuilder()
                                    .setOperator(MatchOperator.MATCH_OPERATOR_EQUALS)
                                    .setValue(Value.newBuilder().setStringValue("resp.field"))))
                    .addScopeConditions(
                        ai.traceable.aiapp.protection.config.service.v1.ScopeCondition.newBuilder()
                            .setEntityScope(
                                ai.traceable.aiapp.protection.config.service.v1.ScopeCondition
                                    .EntityScope.newBuilder()
                                    .setEntityType(
                                        ai.traceable.aiapp.protection.config.service.v1
                                            .ScopeCondition.EntityType.ENTITY_TYPE_API)
                                    .addEntityIds("api-1"))))
            .setAction(
                ai.traceable.aiapp.protection.config.service.v1.Action.newBuilder()
                    .setAlert(
                        ai.traceable.aiapp.protection.config.service.v1.Action.Alert.newBuilder()
                            .setSeverityLevel(SeverityLevel.SEVERITY_LEVEL_HIGH)))
            .setRuleStatusDetails(
                RuleStatusDetails.newBuilder()
                    .setRuleCreationSource(RuleStatusDetails.RuleSource.RULE_SOURCE_CUSTOMER)
                    .setInternal(false)
                    .setHidden(false))
            .build();

    CreateRateLimitingRuleRequest request =
        aiAppToRateLimitingConverter.convertToCreateRateLimitingRuleRequest(aiAppRuleData);

    assertNotNull(request);
    RateLimitingRuleData ruleData = request.getData();
    assertTrue(ruleData.hasCondition());

    Condition condition = ruleData.getCondition();
    // Top-level: AND composite with [OR-composite of datatype conditions, scope leaf]
    assertTrue(condition.hasCompositeCondition());
    CompositeCondition andComposite = condition.getCompositeCondition();
    assertEquals(
        CompositeCondition.LogicalOperator.LOGICAL_OPERATOR_AND, andComposite.getOperator());
    assertEquals(2, andComposite.getChildrenCount());

    // First child: OR composite with the two datatype conditions
    Condition orChild = andComposite.getChildren(0);
    assertTrue(orChild.hasCompositeCondition());
    CompositeCondition orComposite = orChild.getCompositeCondition();
    assertEquals(CompositeCondition.LogicalOperator.LOGICAL_OPERATOR_OR, orComposite.getOperator());
    assertEquals(2, orComposite.getChildrenCount());
    assertTrue(orComposite.getChildren(0).getLeafCondition().hasDatatypeCondition());
    assertTrue(orComposite.getChildren(1).getLeafCondition().hasDatatypeCondition());

    // Second child: scope leaf
    Condition scopeChild = andComposite.getChildren(1);
    assertTrue(scopeChild.hasLeafCondition());
    LeafCondition scopeLeaf = scopeChild.getLeafCondition();
    assertTrue(scopeLeaf.hasScopeCondition());
    ai.traceable.ratelimiting.config.service.v2.ScopeCondition rlScopeCondition =
        scopeLeaf.getScopeCondition();
    assertTrue(rlScopeCondition.hasEntityScope());
    assertEquals(
        ai.traceable.ratelimiting.config.service.v2.ScopeCondition.EntityType.ENTITY_TYPE_API,
        rlScopeCondition.getEntityScope().getEntityType());
    assertEquals("api-1", rlScopeCondition.getEntityScope().getEntityIds(0));
  }
}
