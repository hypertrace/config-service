package ai.traceable.aiapp.protection.config.service.converter.customsignature;

import static ai.traceable.aiapp.protection.config.service.converter.AiAppConverterConstants.AI_INPUT_EXPLOSION_THREAT_TYPE_ID;
import static ai.traceable.aiapp.protection.config.service.converter.AiAppConverterConstants.GENAI_MODELS_ATTRIBUTE_KEY;
import static ai.traceable.aiapp.protection.config.service.converter.AiAppConverterConstants.GENAI_PROMPT_SIZE_ATTRIBUTE_KEY;
import static ai.traceable.aiapp.protection.config.service.converter.AiAppConverterConstants.GENAI_PROVIDERS_ATTRIBUTE_KEY;
import static ai.traceable.aiapp.protection.config.service.converter.AiAppConverterConstants.MODEL_GOVERNANCE_THREAT_TYPE_ID;
import static ai.traceable.aiapp.protection.config.service.converter.AiAppConverterConstants.THREAT_TYPE_ID_LABEL_KEY;

import ai.traceable.aiapp.protection.config.service.v1.AiAppCustomRule;
import ai.traceable.aiapp.protection.config.service.v1.AiAppCustomRuleData;
import ai.traceable.aiapp.protection.config.service.v1.ScopeCondition;
import ai.traceable.customsignature.config.service.v1.AttributeKeyValueExpression;
import ai.traceable.customsignature.config.service.v1.Category;
import ai.traceable.customsignature.config.service.v1.Clause;
import ai.traceable.customsignature.config.service.v1.ClauseGroup;
import ai.traceable.customsignature.config.service.v1.CreateCustomSignatureRuleRequest;
import ai.traceable.customsignature.config.service.v1.CustomSignatureRule;
import ai.traceable.customsignature.config.service.v1.MatchOperator;
import ai.traceable.customsignature.config.service.v1.RuleDefinition;
import ai.traceable.customsignature.config.service.v1.RuleEffect;
import ai.traceable.customsignature.config.service.v1.RuleScope;
import ai.traceable.customsignature.config.service.v1.RuleSource;
import ai.traceable.customsignature.config.service.v1.ScopeExpression;
import ai.traceable.customsignature.config.service.v1.StringCondition;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class AiAppToCustomSignatureConverter {

  public static CreateCustomSignatureRuleRequest convertToCreateCustomSignatureRuleRequest(
      AiAppCustomRuleData aiAppRuleData) {

    CreateCustomSignatureRuleRequest.Builder requestBuilder =
        CreateCustomSignatureRuleRequest.newBuilder()
            .setName(aiAppRuleData.getRuleName())
            .setDescription(aiAppRuleData.getDescription())
            .setHidden(aiAppRuleData.getRuleStatusDetails().getHidden())
            .setInternal(aiAppRuleData.getRuleStatusDetails().getInternal())
            .setEffect(convertActionToRuleEffect(aiAppRuleData.getAction()))
            .setCategory(Category.CATEGORY_AI_APP_PROTECTION)
            .setRuleSource(RuleSource.RULE_SOURCE_CUSTOMER);

    // Build rule definition
    RuleDefinition.Builder definitionBuilder =
        RuleDefinition.newBuilder().putAllLabels(aiAppRuleData.getEventLabelsMap());

    // Add threat type ID label
    String threatTypeId = getRuleTypeForThreatTypeId(aiAppRuleData);
    definitionBuilder.putLabels(THREAT_TYPE_ID_LABEL_KEY, threatTypeId);

    // Build clause group based on rule type
    ClauseGroup.Builder clauseGroupBuilder = ClauseGroup.newBuilder();

    if (aiAppRuleData.hasModelGovernanceRuleData()) {
      buildModelGovernanceClauseGroup(
          aiAppRuleData.getModelGovernanceRuleData(), clauseGroupBuilder);
    } else if (aiAppRuleData.hasAiInputExplosionRuleData()) {
      buildAiInputExplosionClauseGroup(
          aiAppRuleData.getAiInputExplosionRuleData(), clauseGroupBuilder);
    }

    definitionBuilder.setClauseGroup(clauseGroupBuilder.build());
    requestBuilder.setDefinition(definitionBuilder.build());

    // Set rule scope
    if (aiAppRuleData.hasRuleScope()) {
      requestBuilder.setRuleScope(convertRuleScopeToCustomSignature(aiAppRuleData.getRuleScope()));
    }

    return requestBuilder.build();
  }

  public static CustomSignatureRule convertToCustomSignatureRule(AiAppCustomRule aiAppCustomRule) {

    AiAppCustomRuleData customRuleData = aiAppCustomRule.getRuleData();
    // Set basic rule properties from AiAppCustomRule
    CustomSignatureRule.Builder builder =
        CustomSignatureRule.newBuilder()
            .setId(aiAppCustomRule.getRuleId())
            .setName(customRuleData.getRuleName())
            .setDescription(customRuleData.getDescription())
            .setDisabled(!customRuleData.getEnabled())
            .setHidden(customRuleData.getRuleStatusDetails().getHidden())
            .setInternal(customRuleData.getRuleStatusDetails().getInternal())
            .setCategory(Category.CATEGORY_AI_APP_PROTECTION)
            .setRuleSource(RuleSource.RULE_SOURCE_CUSTOMER);

    // Convert the rule data using existing logic
    AiAppCustomRuleData ruleData = aiAppCustomRule.getRuleData();

    // Build rule definition using existing helper methods
    RuleDefinition.Builder definitionBuilder =
        RuleDefinition.newBuilder().putAllLabels(customRuleData.getEventLabelsMap());

    // Add threat type ID label
    String threatTypeId = getRuleTypeForThreatTypeId(ruleData);
    definitionBuilder.putLabels(THREAT_TYPE_ID_LABEL_KEY, threatTypeId);

    // Build clause group based on rule type using existing logic
    ClauseGroup.Builder clauseGroupBuilder = ClauseGroup.newBuilder();

    if (ruleData.hasModelGovernanceRuleData()) {
      buildModelGovernanceClauseGroup(ruleData.getModelGovernanceRuleData(), clauseGroupBuilder);
    } else if (ruleData.hasAiInputExplosionRuleData()) {
      buildAiInputExplosionClauseGroup(ruleData.getAiInputExplosionRuleData(), clauseGroupBuilder);
    }

    definitionBuilder.setClauseGroup(clauseGroupBuilder.build());
    builder.setDefinition(definitionBuilder.build());

    // Set rule effect (action) using existing conversion logic
    if (ruleData.hasAction()) {
      builder.setEffect(convertActionToRuleEffect(ruleData.getAction()));
    }

    // Set rule scope using existing conversion logic
    if (ruleData.hasRuleScope()) {
      builder.setRuleScope(convertRuleScopeToCustomSignature(ruleData.getRuleScope()));
    }

    return builder.build();
  }

  private static String getRuleTypeForThreatTypeId(AiAppCustomRuleData aiAppRuleData) {
    if (aiAppRuleData.hasModelGovernanceRuleData()) {
      return MODEL_GOVERNANCE_THREAT_TYPE_ID;
    } else if (aiAppRuleData.hasAiInputExplosionRuleData()) {
      return AI_INPUT_EXPLOSION_THREAT_TYPE_ID;
    }
    throw new IllegalArgumentException(
        "Unsupported rule type for custom signature conversion: "
            + aiAppRuleData.getRuleDataCase());
  }

  /** Builds clause group for model governance rule data. */
  private static void buildModelGovernanceClauseGroup(
      ai.traceable.aiapp.protection.config.service.v1.ModelGovernanceRuleData modelGovernanceData,
      ClauseGroup.Builder clauseGroupBuilder) {

    // Add AI model types condition using attribute match
    if (modelGovernanceData.hasAiModelTypesCondition()) {
      Clause modelTypesClause =
          buildAttributeMatchClause(
              GENAI_MODELS_ATTRIBUTE_KEY, modelGovernanceData.getAiModelTypesCondition());
      clauseGroupBuilder.addClauses(modelTypesClause);
    }

    // Add AI vendors condition using attribute match
    if (modelGovernanceData.hasAiVendorsCondition()) {
      Clause vendorsClause =
          buildAttributeMatchClause(
              GENAI_PROVIDERS_ATTRIBUTE_KEY, modelGovernanceData.getAiVendorsCondition());
      clauseGroupBuilder.addClauses(vendorsClause);
    }

    // Add scope conditions
    for (ai.traceable.aiapp.protection.config.service.v1.ScopeCondition scopeCondition :
        modelGovernanceData.getScopeConditionsList()) {
      Clause scopeConditionClause = convertScopeConditionToClause(scopeCondition);
      if (scopeConditionClause != null) {
        clauseGroupBuilder.addClauses(scopeConditionClause);
      }
    }
  }

  /** Builds clause group for AI input explosion rule data. */
  private static void buildAiInputExplosionClauseGroup(
      ai.traceable.aiapp.protection.config.service.v1.AiInputExplosionRuleData aiInputExplosionData,
      ClauseGroup.Builder clauseGroupBuilder) {

    AttributeKeyValueExpression.Builder attrExprBuilder = AttributeKeyValueExpression.newBuilder();

    // Set key condition for genAiPromptSize attribute
    StringCondition keyCondition =
        StringCondition.newBuilder()
            .setValue(GENAI_PROMPT_SIZE_ATTRIBUTE_KEY)
            .setOperator(MatchOperator.MATCH_OPERATOR_EQUALS)
            .build();
    attrExprBuilder.setKeyCondition(keyCondition);

    // Set value condition for the character limit
    StringCondition valueCondition =
        StringCondition.newBuilder()
            .setValue(String.valueOf(aiInputExplosionData.getInputCharacterLimit()))
            .setOperator(MatchOperator.MATCH_OPERATOR_GREATER_THAN)
            .build();
    attrExprBuilder.setValueCondition(valueCondition);

    Clause attributeClause =
        Clause.newBuilder().setAttributeKeyValueExpression(attrExprBuilder.build()).build();
    clauseGroupBuilder.addClauses(attributeClause);

    // Add scope conditions
    for (ai.traceable.aiapp.protection.config.service.v1.ScopeCondition scopeCondition :
        aiInputExplosionData.getScopeConditionsList()) {
      Clause scopeConditionClause = convertScopeConditionToClause(scopeCondition);
      if (scopeConditionClause != null) {
        clauseGroupBuilder.addClauses(convertScopeConditionToClause(scopeCondition));
      }
    }
  }

  /** Builds an attribute match clause for AI model types and vendors. */
  private static Clause buildAttributeMatchClause(
      String attributeKey,
      ai.traceable.aiapp.protection.config.service.v1.MatchOperatorCondition matchCondition) {

    // Build key condition
    StringCondition.Builder keyConditionBuilder =
        StringCondition.newBuilder()
            .setOperator(
                ai.traceable.customsignature.config.service.v1.MatchOperator.MATCH_OPERATOR_EQUALS)
            .setValue(attributeKey);

    // Build value condition with the match operator and value from AI app condition
    StringCondition.Builder valueConditionBuilder =
        StringCondition.newBuilder()
            .setOperator(convertMatchOperator(matchCondition.getOperator()))
            .setValue(matchCondition.getValue().getStringValue());

    // Build AttributeKeyValueExpression
    AttributeKeyValueExpression.Builder attributeExpressionBuilder =
        AttributeKeyValueExpression.newBuilder()
            .setKeyCondition(keyConditionBuilder.build())
            .setValueCondition(valueConditionBuilder.build());

    return Clause.newBuilder()
        .setAttributeKeyValueExpression(attributeExpressionBuilder.build())
        .build();
  }

  private static Clause convertScopeConditionToClause(
      ai.traceable.aiapp.protection.config.service.v1.ScopeCondition scopeCondition) {

    if (scopeCondition
        .getEntityScope()
        .getEntityType()
        .equals(ScopeCondition.EntityType.ENTITY_TYPE_API)) {
      ai.traceable.aiapp.protection.config.service.v1.ScopeCondition.EntityScope entityScope =
          scopeCondition.getEntityScope();

      ai.traceable.customsignature.config.service.v1.ScopeExpression.EntityScope.Builder
          entityScopeBuilder =
              ai.traceable.customsignature.config.service.v1.ScopeExpression.EntityScope
                  .newBuilder()
                  .addAllEntityIds(entityScope.getEntityIdsList())
                  .setEntityType(
                      ai.traceable.customsignature.config.service.v1.ScopeExpression.EntityType
                          .ENTITY_TYPE_API);

      ScopeExpression scopeExpression =
          ScopeExpression.newBuilder().setEntityScope(entityScopeBuilder.build()).build();

      return Clause.newBuilder().setScopeExpression(scopeExpression).build();

    } else if (scopeCondition.hasUrlScope()) {
      ai.traceable.aiapp.protection.config.service.v1.ScopeCondition.UrlScope urlScope =
          scopeCondition.getUrlScope();

      ai.traceable.customsignature.config.service.v1.ScopeExpression.UrlScope customUrlScope =
          ai.traceable.customsignature.config.service.v1.ScopeExpression.UrlScope.newBuilder()
              .addAllUrlRegexes(urlScope.getUrlRegexesList())
              .build();

      ScopeExpression scopeExpression =
          ScopeExpression.newBuilder().setUrlScope(customUrlScope).build();

      return Clause.newBuilder().setScopeExpression(scopeExpression).build();
    }

    return null;
  }

  /** Converts AI app rule scope to custom signature rule scope. */
  private static RuleScope convertRuleScopeToCustomSignature(
      ai.traceable.aiapp.protection.config.service.v1.RuleScope aiAppRuleScope) {
    RuleScope.Builder builder = RuleScope.newBuilder();

    if (aiAppRuleScope.hasEnvironmentScope()) {
      // Convert AI app environment scope to custom signature environment scope
      ai.traceable.customsignature.config.service.v1.EnvironmentScope.Builder envScopeBuilder =
          ai.traceable.customsignature.config.service.v1.EnvironmentScope.newBuilder()
              .addAllEnvironmentIds(aiAppRuleScope.getEnvironmentScope().getEnvironmentIdsList());

      builder.setEnvironmentScope(envScopeBuilder.build());
    }

    return builder.build();
  }

  /** Converts AI app action to custom signature rule effect. */
  private static RuleEffect convertActionToRuleEffect(
      ai.traceable.aiapp.protection.config.service.v1.Action action) {
    RuleEffect.Builder builder = RuleEffect.newBuilder();

    if (action.hasAlert()) {
      builder.setEventSeverity(convertSeverityLevel(action.getAlert().getSeverityLevel()));
    }

    return builder.build();
  }

  /** Converts AI app severity level to custom signature event severity. */
  private static ai.traceable.customsignature.config.service.v1.EventSeverity convertSeverityLevel(
      ai.traceable.aiapp.protection.config.service.v1.SeverityLevel severityLevel) {
    switch (severityLevel) {
      case SEVERITY_LEVEL_LOW:
        return ai.traceable.customsignature.config.service.v1.EventSeverity.EVENT_SEVERITY_LOW;
      case SEVERITY_LEVEL_MEDIUM:
        return ai.traceable.customsignature.config.service.v1.EventSeverity.EVENT_SEVERITY_MEDIUM;
      case SEVERITY_LEVEL_HIGH:
        return ai.traceable.customsignature.config.service.v1.EventSeverity.EVENT_SEVERITY_HIGH;
      case SEVERITY_LEVEL_CRITICAL:
        return ai.traceable.customsignature.config.service.v1.EventSeverity.EVENT_SEVERITY_CRITICAL;
      default:
        return ai.traceable.customsignature.config.service.v1.EventSeverity
            .EVENT_SEVERITY_UNSPECIFIED;
    }
  }

  private static MatchOperator convertMatchOperator(
      ai.traceable.aiapp.protection.config.service.v1.MatchOperator matchOperator) {
    switch (matchOperator) {
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
