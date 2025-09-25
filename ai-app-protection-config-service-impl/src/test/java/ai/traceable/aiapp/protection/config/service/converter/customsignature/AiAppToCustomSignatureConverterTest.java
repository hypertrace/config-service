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
import ai.traceable.customsignature.config.service.v1.CreateCustomSignatureRuleRequest;
import ai.traceable.customsignature.config.service.v1.CustomSignatureRule;
import ai.traceable.customsignature.config.service.v1.EventSeverity;
import ai.traceable.customsignature.config.service.v1.RuleSource;
import ai.traceable.customsignature.config.service.v1.ScopeExpression;
import com.google.protobuf.Value;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;

class AiAppToCustomSignatureConverterTest {

  @Test
  void testConvertToCreateCustomSignatureRuleRequest_ModelGovernance() {
    // Arrange
    Map<String, String> eventLabels = new HashMap<>();
    eventLabels.put("custom_label", "custom_value");
    eventLabels.put("environment", "production");

    RuleStatusDetails ruleStatusDetails =
        RuleStatusDetails.newBuilder()
            .setHidden(true)
            .setInternal(false)
            .setRuleCreationSource(RuleStatusDetails.RuleSource.RULE_SOURCE_CUSTOMER)
            .build();

    MatchOperatorCondition aiModelTypesCondition =
        MatchOperatorCondition.newBuilder()
            .setOperator(MatchOperator.MATCH_OPERATOR_CONTAINS)
            .setValue(Value.newBuilder().setStringValue("gpt-4").build())
            .build();

    MatchOperatorCondition aiVendorsCondition =
        MatchOperatorCondition.newBuilder()
            .setOperator(MatchOperator.MATCH_OPERATOR_EQUALS)
            .setValue(Value.newBuilder().setStringValue("openai").build())
            .build();

    ScopeCondition entityScopeCondition =
        ScopeCondition.newBuilder()
            .setEntityScope(
                ScopeCondition.EntityScope.newBuilder()
                    .setEntityType(ScopeCondition.EntityType.ENTITY_TYPE_API)
                    .addEntityIds("api-123")
                    .addEntityIds("api-456")
                    .build())
            .build();

    ScopeCondition urlScopeCondition =
        ScopeCondition.newBuilder()
            .setUrlScope(
                ScopeCondition.UrlScope.newBuilder()
                    .addUrlRegexes("/api/v1/.*")
                    .addUrlRegexes("/api/v2/.*")
                    .build())
            .build();

    ModelGovernanceRuleData modelGovernanceData =
        ModelGovernanceRuleData.newBuilder()
            .setAiModelTypesCondition(aiModelTypesCondition)
            .setAiVendorsCondition(aiVendorsCondition)
            .addScopeConditions(entityScopeCondition)
            .addScopeConditions(urlScopeCondition)
            .build();

    Action action =
        Action.newBuilder()
            .setAlert(
                Action.Alert.newBuilder()
                    .setSeverityLevel(SeverityLevel.SEVERITY_LEVEL_HIGH)
                    .build())
            .build();

    RuleScope ruleScope =
        RuleScope.newBuilder()
            .setEnvironmentScope(
                EnvironmentScope.newBuilder()
                    .addEnvironmentIds("env-123")
                    .addEnvironmentIds("env-456")
                    .build())
            .build();

    AiAppCustomRuleData aiAppRuleData =
        AiAppCustomRuleData.newBuilder()
            .setRuleName("Test Model Governance Rule")
            .setDescription("Test description for model governance")
            .setEnabled(true)
            .setRuleStatusDetails(ruleStatusDetails)
            .putAllEventLabels(eventLabels)
            .setModelGovernanceRuleData(modelGovernanceData)
            .setAction(action)
            .setRuleScope(ruleScope)
            .build();

    // Act
    CreateCustomSignatureRuleRequest result =
        AiAppToCustomSignatureConverter.convertToCreateCustomSignatureRuleRequest(aiAppRuleData);

    // Assert
    assertNotNull(result);
    assertEquals("Test Model Governance Rule", result.getName());
    assertEquals("Test description for model governance", result.getDescription());
    assertTrue(result.getHidden());
    assertFalse(result.getInternal());
    assertEquals(Category.CATEGORY_AI_APP_PROTECTION, result.getCategory());
    assertEquals(RuleSource.RULE_SOURCE_CUSTOMER, result.getRuleSource());

    // Verify rule definition
    assertTrue(result.hasDefinition());
    assertEquals(3, result.getDefinition().getLabelsMap().size()); // 2 event labels + 1 threat type
    assertEquals("custom_value", result.getDefinition().getLabelsMap().get("custom_label"));
    assertEquals("production", result.getDefinition().getLabelsMap().get("environment"));
    assertEquals(
        MODEL_GOVERNANCE_THREAT_TYPE_ID,
        result.getDefinition().getLabelsMap().get(THREAT_TYPE_ID_LABEL_KEY));

    // Verify clause group
    assertTrue(result.getDefinition().hasClauseGroup());
    assertEquals(
        4, result.getDefinition().getClauseGroup().getClausesCount()); // 2 attribute + 2 scope

    // Verify AI model types attribute condition
    boolean foundModelTypesClause = false;
    boolean foundVendorsClause = false;
    int scopeClausesFound = 0;

    for (Clause clause : result.getDefinition().getClauseGroup().getClausesList()) {
      if (clause.hasAttributeKeyValueExpression()) {
        AttributeKeyValueExpression attrExpr = clause.getAttributeKeyValueExpression();
        if (GENAI_MODELS_ATTRIBUTE_KEY.equals(attrExpr.getKeyCondition().getValue())) {
          foundModelTypesClause = true;
          assertEquals(
              ai.traceable.customsignature.config.service.v1.MatchOperator.MATCH_OPERATOR_EQUALS,
              attrExpr.getKeyCondition().getOperator());
          assertEquals(
              ai.traceable.customsignature.config.service.v1.MatchOperator.MATCH_OPERATOR_CONTAINS,
              attrExpr.getValueCondition().getOperator());
          assertEquals("gpt-4", attrExpr.getValueCondition().getValue());
        } else if (GENAI_PROVIDERS_ATTRIBUTE_KEY.equals(attrExpr.getKeyCondition().getValue())) {
          foundVendorsClause = true;
          assertEquals(
              ai.traceable.customsignature.config.service.v1.MatchOperator.MATCH_OPERATOR_EQUALS,
              attrExpr.getKeyCondition().getOperator());
          assertEquals(
              ai.traceable.customsignature.config.service.v1.MatchOperator.MATCH_OPERATOR_EQUALS,
              attrExpr.getValueCondition().getOperator());
          assertEquals("openai", attrExpr.getValueCondition().getValue());
        }
      } else if (clause.hasScopeExpression()) {
        scopeClausesFound++;
        ScopeExpression scopeExpr = clause.getScopeExpression();
        if (scopeExpr.hasEntityScope()) {
          assertEquals(
              ScopeExpression.EntityType.ENTITY_TYPE_API,
              scopeExpr.getEntityScope().getEntityType());
          assertEquals(2, scopeExpr.getEntityScope().getEntityIdsCount());
          assertTrue(scopeExpr.getEntityScope().getEntityIdsList().contains("api-123"));
          assertTrue(scopeExpr.getEntityScope().getEntityIdsList().contains("api-456"));
        } else if (scopeExpr.hasUrlScope()) {
          assertEquals(2, scopeExpr.getUrlScope().getUrlRegexesCount());
          assertTrue(scopeExpr.getUrlScope().getUrlRegexesList().contains("/api/v1/.*"));
          assertTrue(scopeExpr.getUrlScope().getUrlRegexesList().contains("/api/v2/.*"));
        }
      }
    }

    assertTrue(foundModelTypesClause, "AI model types attribute clause should be present");
    assertTrue(foundVendorsClause, "AI vendors attribute clause should be present");
    assertEquals(2, scopeClausesFound, "Should have 2 scope clauses");

    // Verify rule effect
    assertTrue(result.hasEffect());
    assertEquals(EventSeverity.EVENT_SEVERITY_HIGH, result.getEffect().getEventSeverity());

    // Verify rule scope
    assertTrue(result.hasRuleScope());
    assertTrue(result.getRuleScope().hasEnvironmentScope());
    assertEquals(2, result.getRuleScope().getEnvironmentScope().getEnvironmentIdsCount());
    assertTrue(
        result.getRuleScope().getEnvironmentScope().getEnvironmentIdsList().contains("env-123"));
    assertTrue(
        result.getRuleScope().getEnvironmentScope().getEnvironmentIdsList().contains("env-456"));
  }

  @Test
  void testConvertToCreateCustomSignatureRuleRequest_AiInputExplosion() {
    // Arrange
    Map<String, String> eventLabels = new HashMap<>();
    eventLabels.put("test_label", "test_value");

    RuleStatusDetails ruleStatusDetails =
        RuleStatusDetails.newBuilder()
            .setHidden(false)
            .setInternal(true)
            .setRuleCreationSource(RuleStatusDetails.RuleSource.RULE_SOURCE_TRACEABLE)
            .build();

    ScopeCondition urlScopeCondition =
        ScopeCondition.newBuilder()
            .setUrlScope(ScopeCondition.UrlScope.newBuilder().addUrlRegexes("/chat/.*").build())
            .build();

    AiInputExplosionRuleData inputExplosionData =
        AiInputExplosionRuleData.newBuilder()
            .setInputCharacterLimit(5000)
            .addScopeConditions(urlScopeCondition)
            .build();

    Action markForTestingAction =
        Action.newBuilder().setMarkForTesting(Action.MarkForTesting.newBuilder().build()).build();

    AiAppCustomRuleData aiAppRuleData =
        AiAppCustomRuleData.newBuilder()
            .setRuleName("Test Input Explosion Rule")
            .setDescription("Test description for input explosion")
            .setEnabled(false)
            .setRuleStatusDetails(ruleStatusDetails)
            .putAllEventLabels(eventLabels)
            .setAiInputExplosionRuleData(inputExplosionData)
            .setAction(markForTestingAction)
            .build();

    // Act
    CreateCustomSignatureRuleRequest result =
        AiAppToCustomSignatureConverter.convertToCreateCustomSignatureRuleRequest(aiAppRuleData);

    // Assert
    assertNotNull(result);
    assertEquals("Test Input Explosion Rule", result.getName());
    assertEquals("Test description for input explosion", result.getDescription());
    assertFalse(result.getHidden());
    assertTrue(result.getInternal());
    assertEquals(Category.CATEGORY_AI_APP_PROTECTION, result.getCategory());
    assertEquals(RuleSource.RULE_SOURCE_CUSTOMER, result.getRuleSource());

    // Verify rule definition
    assertTrue(result.hasDefinition());
    assertEquals(2, result.getDefinition().getLabelsMap().size()); // 1 event label + 1 threat type
    assertEquals("test_value", result.getDefinition().getLabelsMap().get("test_label"));
    assertEquals(
        AI_INPUT_EXPLOSION_THREAT_TYPE_ID,
        result.getDefinition().getLabelsMap().get(THREAT_TYPE_ID_LABEL_KEY));

    // Verify clause group
    assertTrue(result.getDefinition().hasClauseGroup());
    assertEquals(
        2, result.getDefinition().getClauseGroup().getClausesCount()); // 1 attribute + 1 scope

    // Verify prompt size attribute condition
    boolean foundPromptSizeClause = false;
    boolean foundScopeClause = false;

    for (Clause clause : result.getDefinition().getClauseGroup().getClausesList()) {
      if (clause.hasAttributeKeyValueExpression()) {
        AttributeKeyValueExpression attrExpr = clause.getAttributeKeyValueExpression();
        if (GENAI_PROMPT_SIZE_ATTRIBUTE_KEY.equals(attrExpr.getKeyCondition().getValue())) {
          foundPromptSizeClause = true;
          assertEquals(
              ai.traceable.customsignature.config.service.v1.MatchOperator.MATCH_OPERATOR_EQUALS,
              attrExpr.getKeyCondition().getOperator());
          assertEquals(
              ai.traceable.customsignature.config.service.v1.MatchOperator
                  .MATCH_OPERATOR_GREATER_THAN,
              attrExpr.getValueCondition().getOperator());
          assertEquals("5000", attrExpr.getValueCondition().getValue());
        }
      } else if (clause.hasScopeExpression()) {
        foundScopeClause = true;
        ScopeExpression scopeExpr = clause.getScopeExpression();
        assertTrue(scopeExpr.hasUrlScope());
        assertEquals(1, scopeExpr.getUrlScope().getUrlRegexesCount());
        assertEquals("/chat/.*", scopeExpr.getUrlScope().getUrlRegexes(0));
      }
    }

    assertTrue(foundPromptSizeClause, "Prompt size attribute clause should be present");
    assertTrue(foundScopeClause, "URL scope clause should be present");

    // Verify rule effect - mark for testing should result in testing detection event type
    assertTrue(result.hasEffect());
    // Note: The effect conversion logic should be implemented to handle mark for testing
    // For now, we verify that an effect is present
  }

  @Test
  void testConvertToCustomSignatureRule_ModelGovernance() {
    // Arrange
    Map<String, String> eventLabels = new HashMap<>();
    eventLabels.put("priority", "high");

    RuleStatusDetails ruleStatusDetails =
        RuleStatusDetails.newBuilder()
            .setHidden(true)
            .setInternal(false)
            .setRuleCreationSource(RuleStatusDetails.RuleSource.RULE_SOURCE_CUSTOMER)
            .build();

    MatchOperatorCondition aiModelTypesCondition =
        MatchOperatorCondition.newBuilder()
            .setOperator(MatchOperator.MATCH_OPERATOR_MATCHES_REGEX)
            .setValue(Value.newBuilder().setStringValue("claude-.*").build())
            .build();

    ModelGovernanceRuleData modelGovernanceData =
        ModelGovernanceRuleData.newBuilder()
            .setAiModelTypesCondition(aiModelTypesCondition)
            .build();

    Action action =
        Action.newBuilder()
            .setAlert(
                Action.Alert.newBuilder()
                    .setSeverityLevel(SeverityLevel.SEVERITY_LEVEL_CRITICAL)
                    .build())
            .build();

    AiAppCustomRuleData ruleData =
        AiAppCustomRuleData.newBuilder()
            .setRuleName("Claude Model Governance")
            .setDescription("Governance rule for Claude models")
            .setEnabled(true)
            .setRuleStatusDetails(ruleStatusDetails)
            .putAllEventLabels(eventLabels)
            .setModelGovernanceRuleData(modelGovernanceData)
            .setAction(action)
            .build();

    AiAppCustomRule aiAppCustomRule =
        AiAppCustomRule.newBuilder().setRuleId("rule-123").setRuleData(ruleData).build();

    // Act
    CustomSignatureRule result =
        AiAppToCustomSignatureConverter.convertToCustomSignatureRule(aiAppCustomRule);

    // Assert
    assertNotNull(result);
    assertEquals("rule-123", result.getId());
    assertEquals("Claude Model Governance", result.getName());
    assertEquals("Governance rule for Claude models", result.getDescription());
    assertFalse(result.getDisabled()); // enabled = true -> disabled = false
    assertTrue(result.getHidden());
    assertFalse(result.getInternal());
    assertEquals(Category.CATEGORY_AI_APP_PROTECTION, result.getCategory());
    assertEquals(RuleSource.RULE_SOURCE_CUSTOMER, result.getRuleSource());

    // Verify rule definition
    assertTrue(result.hasDefinition());
    assertEquals(2, result.getDefinition().getLabelsMap().size()); // 1 event label + 1 threat type
    assertEquals("high", result.getDefinition().getLabelsMap().get("priority"));
    assertEquals(
        MODEL_GOVERNANCE_THREAT_TYPE_ID,
        result.getDefinition().getLabelsMap().get(THREAT_TYPE_ID_LABEL_KEY));

    // Verify clause group
    assertTrue(result.getDefinition().hasClauseGroup());
    assertEquals(1, result.getDefinition().getClauseGroup().getClausesCount());

    Clause clause = result.getDefinition().getClauseGroup().getClauses(0);
    assertTrue(clause.hasAttributeKeyValueExpression());
    AttributeKeyValueExpression attrExpr = clause.getAttributeKeyValueExpression();
    assertEquals(GENAI_MODELS_ATTRIBUTE_KEY, attrExpr.getKeyCondition().getValue());
    assertEquals(
        ai.traceable.customsignature.config.service.v1.MatchOperator.MATCH_OPERATOR_EQUALS,
        attrExpr.getKeyCondition().getOperator());
    assertEquals(
        ai.traceable.customsignature.config.service.v1.MatchOperator.MATCH_OPERATOR_MATCHES_REGEX,
        attrExpr.getValueCondition().getOperator());
    assertEquals("claude-.*", attrExpr.getValueCondition().getValue());

    // Verify rule effect
    assertTrue(result.hasEffect());
    assertEquals(EventSeverity.EVENT_SEVERITY_CRITICAL, result.getEffect().getEventSeverity());
  }

  @Test
  void testConvertToCustomSignatureRule_AiInputExplosion() {
    // Arrange
    RuleStatusDetails ruleStatusDetails =
        RuleStatusDetails.newBuilder()
            .setHidden(false)
            .setInternal(true)
            .setRuleCreationSource(RuleStatusDetails.RuleSource.RULE_SOURCE_TRACEABLE)
            .build();

    AiInputExplosionRuleData inputExplosionData =
        AiInputExplosionRuleData.newBuilder().setInputCharacterLimit(10000).build();

    Action markForTestingAction =
        Action.newBuilder().setMarkForTesting(Action.MarkForTesting.newBuilder().build()).build();

    AiAppCustomRuleData ruleData =
        AiAppCustomRuleData.newBuilder()
            .setRuleName("Large Input Detection")
            .setDescription("Detect large AI inputs")
            .setEnabled(false)
            .setRuleStatusDetails(ruleStatusDetails)
            .setAiInputExplosionRuleData(inputExplosionData)
            .setAction(markForTestingAction)
            .build();

    AiAppCustomRule aiAppCustomRule =
        AiAppCustomRule.newBuilder().setRuleId("rule-456").setRuleData(ruleData).build();

    // Act
    CustomSignatureRule result =
        AiAppToCustomSignatureConverter.convertToCustomSignatureRule(aiAppCustomRule);

    // Assert
    assertNotNull(result);
    assertEquals("rule-456", result.getId());
    assertEquals("Large Input Detection", result.getName());
    assertEquals("Detect large AI inputs", result.getDescription());
    assertTrue(result.getDisabled()); // enabled = false -> disabled = true
    assertFalse(result.getHidden());
    assertTrue(result.getInternal());
    assertEquals(Category.CATEGORY_AI_APP_PROTECTION, result.getCategory());
    assertEquals(RuleSource.RULE_SOURCE_CUSTOMER, result.getRuleSource());

    // Verify rule definition
    assertTrue(result.hasDefinition());
    assertEquals(1, result.getDefinition().getLabelsMap().size()); // Only threat type label
    assertEquals(
        AI_INPUT_EXPLOSION_THREAT_TYPE_ID,
        result.getDefinition().getLabelsMap().get(THREAT_TYPE_ID_LABEL_KEY));

    // Verify clause group
    assertTrue(result.getDefinition().hasClauseGroup());
    assertEquals(1, result.getDefinition().getClauseGroup().getClausesCount());

    Clause clause = result.getDefinition().getClauseGroup().getClauses(0);
    assertTrue(clause.hasAttributeKeyValueExpression());
    AttributeKeyValueExpression attrExpr = clause.getAttributeKeyValueExpression();
    assertEquals(GENAI_PROMPT_SIZE_ATTRIBUTE_KEY, attrExpr.getKeyCondition().getValue());
    assertEquals(
        ai.traceable.customsignature.config.service.v1.MatchOperator.MATCH_OPERATOR_EQUALS,
        attrExpr.getKeyCondition().getOperator());
    assertEquals(
        ai.traceable.customsignature.config.service.v1.MatchOperator.MATCH_OPERATOR_GREATER_THAN,
        attrExpr.getValueCondition().getOperator());
    assertEquals("10000", attrExpr.getValueCondition().getValue());

    // Verify rule effect
    assertTrue(result.hasEffect());
    // Note: Mark for testing action should be handled appropriately in the conversion
  }
}
