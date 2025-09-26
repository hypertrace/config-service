package ai.traceable.aiapp.protection.config.service.converter.customsignature;

import static ai.traceable.aiapp.protection.config.service.converter.AiAppConverterConstants.AI_INPUT_EXPLOSION_THREAT_TYPE_ID;
import static ai.traceable.aiapp.protection.config.service.converter.AiAppConverterConstants.GENAI_MODELS_ATTRIBUTE_KEY;
import static ai.traceable.aiapp.protection.config.service.converter.AiAppConverterConstants.GENAI_PROMPT_SIZE_ATTRIBUTE_KEY;
import static ai.traceable.aiapp.protection.config.service.converter.AiAppConverterConstants.GENAI_PROVIDERS_ATTRIBUTE_KEY;
import static ai.traceable.aiapp.protection.config.service.converter.AiAppConverterConstants.MODEL_GOVERNANCE_THREAT_TYPE_ID;
import static ai.traceable.aiapp.protection.config.service.converter.AiAppConverterConstants.THREAT_TYPE_ID_LABEL_KEY;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import ai.traceable.aiapp.protection.config.service.v1.Action;
import ai.traceable.aiapp.protection.config.service.v1.AiAppCustomRule;
import ai.traceable.aiapp.protection.config.service.v1.AiAppCustomRuleData;
import ai.traceable.aiapp.protection.config.service.v1.AiInputExplosionRuleData;
import ai.traceable.aiapp.protection.config.service.v1.EnvironmentScope;
import ai.traceable.aiapp.protection.config.service.v1.MatchOperator;
import ai.traceable.aiapp.protection.config.service.v1.MatchOperatorCondition;
import ai.traceable.aiapp.protection.config.service.v1.ModelGovernanceRuleData;
import ai.traceable.aiapp.protection.config.service.v1.RuleScope;
import ai.traceable.aiapp.protection.config.service.v1.RuleStatusDetails;
import ai.traceable.aiapp.protection.config.service.v1.ScopeCondition;
import ai.traceable.aiapp.protection.config.service.v1.SeverityLevel;
import ai.traceable.customsignature.config.service.v1.AttributeKeyValueExpression;
import ai.traceable.customsignature.config.service.v1.Category;
import ai.traceable.customsignature.config.service.v1.Clause;
import ai.traceable.customsignature.config.service.v1.ClauseGroup;
import ai.traceable.customsignature.config.service.v1.CustomSignatureRule;
import ai.traceable.customsignature.config.service.v1.EventSeverity;
import ai.traceable.customsignature.config.service.v1.EventType;
import ai.traceable.customsignature.config.service.v1.RuleDefinition;
import ai.traceable.customsignature.config.service.v1.RuleEffect;
import ai.traceable.customsignature.config.service.v1.RuleSource;
import ai.traceable.customsignature.config.service.v1.ScopeExpression;
import ai.traceable.customsignature.config.service.v1.StringCondition;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;

class CustomSignatureToAiAppConverterTest {

  private final CustomSignatureToAiAppConverter customSignatureToAiAppConverter =
      new CustomSignatureToAiAppConverter();

  @Test
  void testConvertFromCustomSignatureRule_ModelGovernance() {
    // Arrange
    Map<String, String> labels = new HashMap<>();
    labels.put(THREAT_TYPE_ID_LABEL_KEY, MODEL_GOVERNANCE_THREAT_TYPE_ID);
    labels.put("custom_label", "custom_value");
    labels.put("environment", "production");

    // Create AI model types attribute condition
    StringCondition modelTypesKeyCondition =
        StringCondition.newBuilder()
            .setOperator(
                ai.traceable.customsignature.config.service.v1.MatchOperator.MATCH_OPERATOR_EQUALS)
            .setValue(GENAI_MODELS_ATTRIBUTE_KEY)
            .build();

    StringCondition modelTypesValueCondition =
        StringCondition.newBuilder()
            .setOperator(
                ai.traceable.customsignature.config.service.v1.MatchOperator
                    .MATCH_OPERATOR_CONTAINS)
            .setValue("gpt-4")
            .build();

    AttributeKeyValueExpression modelTypesAttrExpr =
        AttributeKeyValueExpression.newBuilder()
            .setKeyCondition(modelTypesKeyCondition)
            .setValueCondition(modelTypesValueCondition)
            .build();

    Clause modelTypesClause =
        Clause.newBuilder().setAttributeKeyValueExpression(modelTypesAttrExpr).build();

    // Create AI vendors attribute condition
    StringCondition vendorsKeyCondition =
        StringCondition.newBuilder()
            .setOperator(
                ai.traceable.customsignature.config.service.v1.MatchOperator.MATCH_OPERATOR_EQUALS)
            .setValue(GENAI_PROVIDERS_ATTRIBUTE_KEY)
            .build();

    StringCondition vendorsValueCondition =
        StringCondition.newBuilder()
            .setOperator(
                ai.traceable.customsignature.config.service.v1.MatchOperator.MATCH_OPERATOR_EQUALS)
            .setValue("openai")
            .build();

    AttributeKeyValueExpression vendorsAttrExpr =
        AttributeKeyValueExpression.newBuilder()
            .setKeyCondition(vendorsKeyCondition)
            .setValueCondition(vendorsValueCondition)
            .build();

    Clause vendorsClause =
        Clause.newBuilder().setAttributeKeyValueExpression(vendorsAttrExpr).build();

    // Create entity scope condition
    ScopeExpression.EntityScope entityScope =
        ScopeExpression.EntityScope.newBuilder()
            .setEntityType(ScopeExpression.EntityType.ENTITY_TYPE_API)
            .addEntityIds("api-123")
            .addEntityIds("api-456")
            .build();

    ScopeExpression entityScopeExpression =
        ScopeExpression.newBuilder().setEntityScope(entityScope).build();

    Clause entityScopeClause =
        Clause.newBuilder().setScopeExpression(entityScopeExpression).build();

    // Create URL scope condition
    ScopeExpression.UrlScope urlScope =
        ScopeExpression.UrlScope.newBuilder()
            .addUrlRegexes("/api/v1/.*")
            .addUrlRegexes("/api/v2/.*")
            .build();

    ScopeExpression urlScopeExpression = ScopeExpression.newBuilder().setUrlScope(urlScope).build();

    Clause urlScopeClause = Clause.newBuilder().setScopeExpression(urlScopeExpression).build();

    ClauseGroup clauseGroup =
        ClauseGroup.newBuilder()
            .addClauses(modelTypesClause)
            .addClauses(vendorsClause)
            .addClauses(entityScopeClause)
            .addClauses(urlScopeClause)
            .build();

    RuleDefinition ruleDefinition =
        RuleDefinition.newBuilder().setClauseGroup(clauseGroup).putAllLabels(labels).build();

    RuleEffect ruleEffect =
        RuleEffect.newBuilder()
            .setEventType(EventType.EVENT_TYPE_NORMAL_DETECTION)
            .setEventSeverity(EventSeverity.EVENT_SEVERITY_HIGH)
            .build();

    ai.traceable.customsignature.config.service.v1.EnvironmentScope customEnvScope =
        ai.traceable.customsignature.config.service.v1.EnvironmentScope.newBuilder()
            .addEnvironmentIds("env-123")
            .addEnvironmentIds("env-456")
            .build();

    ai.traceable.customsignature.config.service.v1.RuleScope customRuleScope =
        ai.traceable.customsignature.config.service.v1.RuleScope.newBuilder()
            .setEnvironmentScope(customEnvScope)
            .build();

    CustomSignatureRule customSignatureRule =
        CustomSignatureRule.newBuilder()
            .setId("rule-123")
            .setName("Test Model Governance Rule")
            .setDescription("Test description for model governance")
            .setDisabled(false)
            .setHidden(true)
            .setInternal(false)
            .setCategory(Category.CATEGORY_AI_APP_PROTECTION)
            .setRuleSource(RuleSource.RULE_SOURCE_CUSTOMER)
            .setDefinition(ruleDefinition)
            .setEffect(ruleEffect)
            .setRuleScope(customRuleScope)
            .build();

    // Act
    AiAppCustomRule result =
        customSignatureToAiAppConverter.convertFromCustomSignatureRule(customSignatureRule);

    // Assert
    assertNotNull(result);
    assertEquals("rule-123", result.getRuleId());

    AiAppCustomRuleData ruleData = result.getRuleData();
    assertNotNull(ruleData);
    assertEquals("Test Model Governance Rule", ruleData.getRuleName());
    assertEquals("Test description for model governance", ruleData.getDescription());
    assertTrue(ruleData.getEnabled()); // disabled = false -> enabled = true

    // Verify rule status details
    RuleStatusDetails statusDetails = ruleData.getRuleStatusDetails();
    assertNotNull(statusDetails);
    assertTrue(statusDetails.getHidden());
    assertFalse(statusDetails.getInternal());
    assertEquals(
        RuleStatusDetails.RuleSource.RULE_SOURCE_CUSTOMER, statusDetails.getRuleCreationSource());

    // Verify event labels (should exclude threat type ID)
    Map<String, String> eventLabels = ruleData.getEventLabelsMap();
    assertEquals(3, eventLabels.size()); // All labels including threat type
    assertEquals("custom_value", eventLabels.get("custom_label"));
    assertEquals("production", eventLabels.get("environment"));
    assertEquals(MODEL_GOVERNANCE_THREAT_TYPE_ID, eventLabels.get(THREAT_TYPE_ID_LABEL_KEY));

    // Verify model governance rule data
    assertTrue(ruleData.hasModelGovernanceRuleData());
    ModelGovernanceRuleData modelGovernanceData = ruleData.getModelGovernanceRuleData();

    // Verify AI model types condition
    assertTrue(modelGovernanceData.hasAiModelTypesCondition());
    MatchOperatorCondition aiModelTypesCondition = modelGovernanceData.getAiModelTypesCondition();
    assertEquals(MatchOperator.MATCH_OPERATOR_CONTAINS, aiModelTypesCondition.getOperator());
    assertTrue(aiModelTypesCondition.hasValue());
    assertEquals("gpt-4", aiModelTypesCondition.getValue().getStringValue());

    // Verify AI vendors condition
    assertTrue(modelGovernanceData.hasAiVendorsCondition());
    MatchOperatorCondition aiVendorsCondition = modelGovernanceData.getAiVendorsCondition();
    assertEquals(MatchOperator.MATCH_OPERATOR_EQUALS, aiVendorsCondition.getOperator());
    assertTrue(aiVendorsCondition.hasValue());
    assertEquals("openai", aiVendorsCondition.getValue().getStringValue());

    // Verify scope conditions
    assertEquals(2, modelGovernanceData.getScopeConditionsCount());

    boolean foundEntityScope = false;
    boolean foundUrlScope = false;

    for (ScopeCondition scopeCondition : modelGovernanceData.getScopeConditionsList()) {
      if (scopeCondition.hasEntityScope()) {
        foundEntityScope = true;
        ScopeCondition.EntityScope entityScopeCondition = scopeCondition.getEntityScope();
        assertEquals(
            ScopeCondition.EntityType.ENTITY_TYPE_API, entityScopeCondition.getEntityType());
        assertEquals(2, entityScopeCondition.getEntityIdsCount());
        assertTrue(entityScopeCondition.getEntityIdsList().contains("api-123"));
        assertTrue(entityScopeCondition.getEntityIdsList().contains("api-456"));
      } else if (scopeCondition.hasUrlScope()) {
        foundUrlScope = true;
        ScopeCondition.UrlScope urlScopeCondition = scopeCondition.getUrlScope();
        assertEquals(2, urlScopeCondition.getUrlRegexesCount());
        assertTrue(urlScopeCondition.getUrlRegexesList().contains("/api/v1/.*"));
        assertTrue(urlScopeCondition.getUrlRegexesList().contains("/api/v2/.*"));
      }
    }

    assertTrue(foundEntityScope, "Entity scope condition should be present");
    assertTrue(foundUrlScope, "URL scope condition should be present");

    // Verify action
    assertTrue(ruleData.hasAction());
    Action action = ruleData.getAction();
    assertTrue(action.hasAlert());
    assertEquals(SeverityLevel.SEVERITY_LEVEL_HIGH, action.getAlert().getSeverityLevel());

    // Verify rule scope
    assertTrue(ruleData.hasRuleScope());
    RuleScope ruleScope = ruleData.getRuleScope();
    assertTrue(ruleScope.hasEnvironmentScope());
    EnvironmentScope envScope = ruleScope.getEnvironmentScope();
    assertEquals(2, envScope.getEnvironmentIdsCount());
    assertTrue(envScope.getEnvironmentIdsList().contains("env-123"));
    assertTrue(envScope.getEnvironmentIdsList().contains("env-456"));
  }

  @Test
  void testConvertFromCustomSignatureRule_AiInputExplosion() {
    // Arrange
    Map<String, String> labels = new HashMap<>();
    labels.put(THREAT_TYPE_ID_LABEL_KEY, AI_INPUT_EXPLOSION_THREAT_TYPE_ID);
    labels.put("test_label", "test_value");

    // Create prompt size attribute condition
    StringCondition promptSizeKeyCondition =
        StringCondition.newBuilder()
            .setOperator(
                ai.traceable.customsignature.config.service.v1.MatchOperator.MATCH_OPERATOR_EQUALS)
            .setValue(GENAI_PROMPT_SIZE_ATTRIBUTE_KEY)
            .build();

    StringCondition promptSizeValueCondition =
        StringCondition.newBuilder()
            .setOperator(
                ai.traceable.customsignature.config.service.v1.MatchOperator
                    .MATCH_OPERATOR_GREATER_THAN)
            .setValue("5000")
            .build();

    AttributeKeyValueExpression promptSizeAttrExpr =
        AttributeKeyValueExpression.newBuilder()
            .setKeyCondition(promptSizeKeyCondition)
            .setValueCondition(promptSizeValueCondition)
            .build();

    Clause promptSizeClause =
        Clause.newBuilder().setAttributeKeyValueExpression(promptSizeAttrExpr).build();

    // Create URL scope condition
    ScopeExpression.UrlScope urlScope =
        ScopeExpression.UrlScope.newBuilder().addUrlRegexes("/chat/.*").build();

    ScopeExpression urlScopeExpression = ScopeExpression.newBuilder().setUrlScope(urlScope).build();

    Clause urlScopeClause = Clause.newBuilder().setScopeExpression(urlScopeExpression).build();

    ClauseGroup clauseGroup =
        ClauseGroup.newBuilder().addClauses(promptSizeClause).addClauses(urlScopeClause).build();

    RuleDefinition ruleDefinition =
        RuleDefinition.newBuilder().setClauseGroup(clauseGroup).putAllLabels(labels).build();

    RuleEffect ruleEffect =
        RuleEffect.newBuilder()
            .setEventType(EventType.EVENT_TYPE_TESTING_DETECTION)
            .setEventSeverity(EventSeverity.EVENT_SEVERITY_MEDIUM)
            .build();

    CustomSignatureRule customSignatureRule =
        CustomSignatureRule.newBuilder()
            .setId("rule-456")
            .setName("Test Input Explosion Rule")
            .setDescription("Test description for input explosion")
            .setDisabled(true)
            .setHidden(false)
            .setInternal(true)
            .setCategory(Category.CATEGORY_AI_APP_PROTECTION)
            .setRuleSource(RuleSource.RULE_SOURCE_TRACEABLE)
            .setDefinition(ruleDefinition)
            .setEffect(ruleEffect)
            .build();

    // Act
    AiAppCustomRule result =
        customSignatureToAiAppConverter.convertFromCustomSignatureRule(customSignatureRule);

    // Assert
    assertNotNull(result);
    assertEquals("rule-456", result.getRuleId());

    AiAppCustomRuleData ruleData = result.getRuleData();
    assertNotNull(ruleData);
    assertEquals("Test Input Explosion Rule", ruleData.getRuleName());
    assertEquals("Test description for input explosion", ruleData.getDescription());
    assertFalse(ruleData.getEnabled()); // disabled = true -> enabled = false

    // Verify rule status details
    RuleStatusDetails statusDetails = ruleData.getRuleStatusDetails();
    assertNotNull(statusDetails);
    assertFalse(statusDetails.getHidden());
    assertTrue(statusDetails.getInternal());
    assertEquals(
        RuleStatusDetails.RuleSource.RULE_SOURCE_TRACEABLE, statusDetails.getRuleCreationSource());

    // Verify event labels (should include all labels)
    Map<String, String> eventLabels = ruleData.getEventLabelsMap();
    assertEquals(2, eventLabels.size());
    assertEquals("test_value", eventLabels.get("test_label"));
    assertEquals(AI_INPUT_EXPLOSION_THREAT_TYPE_ID, eventLabels.get(THREAT_TYPE_ID_LABEL_KEY));

    // Verify AI input explosion rule data
    assertTrue(ruleData.hasAiInputExplosionRuleData());
    AiInputExplosionRuleData inputExplosionData = ruleData.getAiInputExplosionRuleData();

    // Verify input character limit
    assertEquals(5000, inputExplosionData.getInputCharacterLimit());

    // Verify scope conditions
    assertEquals(1, inputExplosionData.getScopeConditionsCount());
    ScopeCondition scopeCondition = inputExplosionData.getScopeConditions(0);
    assertTrue(scopeCondition.hasUrlScope());
    ScopeCondition.UrlScope urlScopeCondition = scopeCondition.getUrlScope();
    assertEquals(1, urlScopeCondition.getUrlRegexesCount());
    assertEquals("/chat/.*", urlScopeCondition.getUrlRegexes(0));

    // Verify action - should be mark for testing due to EVENT_TYPE_TESTING_DETECTION
    assertTrue(ruleData.hasAction());
    Action action = ruleData.getAction();
    assertTrue(action.hasMarkForTesting());
    assertFalse(action.hasAlert());

    // Verify rule scope - should have default tenant scope since no environment scope provided
    assertTrue(ruleData.hasRuleScope());
    RuleScope ruleScope = ruleData.getRuleScope();
    assertTrue(ruleScope.hasTenantScope());
  }

  @Test
  void testConvertFromCustomSignatureRule_ModelGovernanceWithMarkForTesting() {
    // Arrange
    Map<String, String> labels = new HashMap<>();
    labels.put(THREAT_TYPE_ID_LABEL_KEY, MODEL_GOVERNANCE_THREAT_TYPE_ID);

    // Create AI vendors attribute condition only
    StringCondition vendorsKeyCondition =
        StringCondition.newBuilder()
            .setOperator(
                ai.traceable.customsignature.config.service.v1.MatchOperator.MATCH_OPERATOR_EQUALS)
            .setValue(GENAI_PROVIDERS_ATTRIBUTE_KEY)
            .build();

    StringCondition vendorsValueCondition =
        StringCondition.newBuilder()
            .setOperator(
                ai.traceable.customsignature.config.service.v1.MatchOperator
                    .MATCH_OPERATOR_NOT_EQUAL)
            .setValue("anthropic")
            .build();

    AttributeKeyValueExpression vendorsAttrExpr =
        AttributeKeyValueExpression.newBuilder()
            .setKeyCondition(vendorsKeyCondition)
            .setValueCondition(vendorsValueCondition)
            .build();

    Clause vendorsClause =
        Clause.newBuilder().setAttributeKeyValueExpression(vendorsAttrExpr).build();

    ClauseGroup clauseGroup = ClauseGroup.newBuilder().addClauses(vendorsClause).build();

    RuleDefinition ruleDefinition =
        RuleDefinition.newBuilder().setClauseGroup(clauseGroup).putAllLabels(labels).build();

    RuleEffect ruleEffect =
        RuleEffect.newBuilder()
            .setEventType(EventType.EVENT_TYPE_TESTING_DETECTION)
            .setEventSeverity(EventSeverity.EVENT_SEVERITY_LOW)
            .build();

    CustomSignatureRule customSignatureRule =
        CustomSignatureRule.newBuilder()
            .setId("rule-789")
            .setName("Vendor Governance Test Rule")
            .setDescription("Test rule for vendor governance")
            .setDisabled(false)
            .setHidden(false)
            .setInternal(false)
            .setCategory(Category.CATEGORY_AI_APP_PROTECTION)
            .setRuleSource(RuleSource.RULE_SOURCE_TRACEABLE)
            .setDefinition(ruleDefinition)
            .setEffect(ruleEffect)
            .build();

    // Act
    AiAppCustomRule result =
        customSignatureToAiAppConverter.convertFromCustomSignatureRule(customSignatureRule);

    // Assert
    assertNotNull(result);
    assertEquals("rule-789", result.getRuleId());

    AiAppCustomRuleData ruleData = result.getRuleData();
    assertNotNull(ruleData);
    assertEquals("Vendor Governance Test Rule", ruleData.getRuleName());
    assertEquals("Test rule for vendor governance", ruleData.getDescription());
    assertTrue(ruleData.getEnabled());

    // Verify rule status details - SYSTEM should map to TRACEABLE
    RuleStatusDetails statusDetails = ruleData.getRuleStatusDetails();
    assertNotNull(statusDetails);
    assertFalse(statusDetails.getHidden());
    assertFalse(statusDetails.getInternal());
    assertEquals(
        RuleStatusDetails.RuleSource.RULE_SOURCE_TRACEABLE, statusDetails.getRuleCreationSource());

    // Verify model governance rule data
    assertTrue(ruleData.hasModelGovernanceRuleData());
    ModelGovernanceRuleData modelGovernanceData = ruleData.getModelGovernanceRuleData();

    // Should not have AI model types condition
    assertFalse(modelGovernanceData.hasAiModelTypesCondition());

    // Should have AI vendors condition
    assertTrue(modelGovernanceData.hasAiVendorsCondition());
    MatchOperatorCondition aiVendorsCondition = modelGovernanceData.getAiVendorsCondition();
    assertEquals(MatchOperator.MATCH_OPERATOR_NOT_EQUAL, aiVendorsCondition.getOperator());
    assertTrue(aiVendorsCondition.hasValue());
    assertEquals("anthropic", aiVendorsCondition.getValue().getStringValue());

    // Should have no scope conditions
    assertEquals(0, modelGovernanceData.getScopeConditionsCount());

    // Verify action - should be mark for testing
    assertTrue(ruleData.hasAction());
    Action action = ruleData.getAction();
    assertTrue(action.hasMarkForTesting());
    assertFalse(action.hasAlert());
  }

  @Test
  void testConvertFromCustomSignatureRule_AiInputExplosionWithAlert() {
    // Arrange
    Map<String, String> labels = new HashMap<>();
    labels.put(THREAT_TYPE_ID_LABEL_KEY, AI_INPUT_EXPLOSION_THREAT_TYPE_ID);
    labels.put("priority", "critical");

    // Create prompt size attribute condition
    StringCondition promptSizeKeyCondition =
        StringCondition.newBuilder()
            .setOperator(
                ai.traceable.customsignature.config.service.v1.MatchOperator.MATCH_OPERATOR_EQUALS)
            .setValue(GENAI_PROMPT_SIZE_ATTRIBUTE_KEY)
            .build();

    StringCondition promptSizeValueCondition =
        StringCondition.newBuilder()
            .setOperator(
                ai.traceable.customsignature.config.service.v1.MatchOperator
                    .MATCH_OPERATOR_GREATER_THAN)
            .setValue("50000")
            .build();

    AttributeKeyValueExpression promptSizeAttrExpr =
        AttributeKeyValueExpression.newBuilder()
            .setKeyCondition(promptSizeKeyCondition)
            .setValueCondition(promptSizeValueCondition)
            .build();

    Clause promptSizeClause =
        Clause.newBuilder().setAttributeKeyValueExpression(promptSizeAttrExpr).build();

    ClauseGroup clauseGroup = ClauseGroup.newBuilder().addClauses(promptSizeClause).build();

    RuleDefinition ruleDefinition =
        RuleDefinition.newBuilder().setClauseGroup(clauseGroup).putAllLabels(labels).build();

    RuleEffect ruleEffect =
        RuleEffect.newBuilder()
            .setEventType(EventType.EVENT_TYPE_NORMAL_DETECTION)
            .setEventSeverity(EventSeverity.EVENT_SEVERITY_CRITICAL)
            .build();

    CustomSignatureRule customSignatureRule =
        CustomSignatureRule.newBuilder()
            .setId("rule-critical")
            .setName("Critical Input Size Rule")
            .setDescription("Critical input size detection")
            .setDisabled(false)
            .setHidden(true)
            .setInternal(true)
            .setCategory(Category.CATEGORY_AI_APP_PROTECTION)
            .setRuleSource(RuleSource.RULE_SOURCE_DEFAULT)
            .setDefinition(ruleDefinition)
            .setEffect(ruleEffect)
            .build();

    // Act
    AiAppCustomRule result =
        customSignatureToAiAppConverter.convertFromCustomSignatureRule(customSignatureRule);

    // Assert
    assertNotNull(result);
    assertEquals("rule-critical", result.getRuleId());

    AiAppCustomRuleData ruleData = result.getRuleData();
    assertNotNull(ruleData);
    assertEquals("Critical Input Size Rule", ruleData.getRuleName());
    assertEquals("Critical input size detection", ruleData.getDescription());
    assertTrue(ruleData.getEnabled());

    // Verify rule status details
    RuleStatusDetails statusDetails = ruleData.getRuleStatusDetails();
    assertNotNull(statusDetails);
    assertTrue(statusDetails.getHidden());
    assertTrue(statusDetails.getInternal());
    assertEquals(
        RuleStatusDetails.RuleSource.RULE_SOURCE_DEFAULT, statusDetails.getRuleCreationSource());

    // Verify event labels
    Map<String, String> eventLabels = ruleData.getEventLabelsMap();
    assertEquals(2, eventLabels.size());
    assertEquals("critical", eventLabels.get("priority"));
    assertEquals(AI_INPUT_EXPLOSION_THREAT_TYPE_ID, eventLabels.get(THREAT_TYPE_ID_LABEL_KEY));

    // Verify AI input explosion rule data
    assertTrue(ruleData.hasAiInputExplosionRuleData());
    AiInputExplosionRuleData inputExplosionData = ruleData.getAiInputExplosionRuleData();
    assertEquals(50000, inputExplosionData.getInputCharacterLimit());
    assertEquals(0, inputExplosionData.getScopeConditionsCount());

    // Verify action - should be alert due to EVENT_TYPE_NORMAL_DETECTION
    assertTrue(ruleData.hasAction());
    Action action = ruleData.getAction();
    assertTrue(action.hasAlert());
    assertFalse(action.hasMarkForTesting());
    assertEquals(SeverityLevel.SEVERITY_LEVEL_CRITICAL, action.getAlert().getSeverityLevel());
  }
}
