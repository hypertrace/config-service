package ai.traceable.aiapp.protection.config.service.converter.customsignature;

import static ai.traceable.aiapp.protection.config.service.converter.AiAppConverterConstants.AI_INPUT_EXPLOSION_TYPE;
import static ai.traceable.aiapp.protection.config.service.converter.AiAppConverterConstants.GENAI_MODELS_ATTRIBUTE_KEY;
import static ai.traceable.aiapp.protection.config.service.converter.AiAppConverterConstants.GENAI_PROMPT_SIZE_ATTRIBUTE_KEY;
import static ai.traceable.aiapp.protection.config.service.converter.AiAppConverterConstants.GENAI_PROVIDERS_ATTRIBUTE_KEY;
import static ai.traceable.aiapp.protection.config.service.converter.AiAppConverterConstants.MODEL_GOVERNANCE_TYPE;
import static ai.traceable.aiapp.protection.config.service.converter.AiAppConverterConstants.THREAT_TYPE_ID_LABEL_KEY;

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
import ai.traceable.aiapp.protection.config.service.v1.TenantScope;
import ai.traceable.customsignature.config.service.v1.Clause;
import ai.traceable.customsignature.config.service.v1.ClauseGroup;
import ai.traceable.customsignature.config.service.v1.ClauseOperator;
import ai.traceable.customsignature.config.service.v1.CustomSignatureRule;
import ai.traceable.customsignature.config.service.v1.ScopeExpression;
import com.google.protobuf.Value;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class CustomSignatureToAiAppConverter {

  public static AiAppCustomRule convertFromCustomSignatureRule(
      CustomSignatureRule customSignatureRule) {
    String threatTypeId =
        customSignatureRule.getDefinition().getLabelsMap().get(THREAT_TYPE_ID_LABEL_KEY);
    if (threatTypeId == null || threatTypeId.isEmpty()) {
      throw new IllegalArgumentException(
          "Missing threatTypeId label in custom signature rule: " + customSignatureRule.getId());
    }

    RuleStatusDetails.Builder ruleStatusDetailsBuilder =
        RuleStatusDetails.newBuilder()
            .setHidden(customSignatureRule.getHidden())
            .setInternal(customSignatureRule.getInternal())
            .setRuleCreationSource(convertRuleSource(customSignatureRule.getRuleSource()));

    AiAppCustomRuleData.Builder aiAppRuleDataBuilder =
        AiAppCustomRuleData.newBuilder()
            .setRuleName(customSignatureRule.getName())
            .setDescription(customSignatureRule.getDescription())
            .setEnabled(!customSignatureRule.getDisabled())
            .setRuleStatusDetails(ruleStatusDetailsBuilder.build())
            .putAllEventLabels(customSignatureRule.getDefinition().getLabelsMap())
            .setRuleScope(convertRuleScope(customSignatureRule.getRuleScope()))
            .setAction(convertAction(customSignatureRule.getEffect()));

    switch (threatTypeId) {
      case MODEL_GOVERNANCE_TYPE:
        aiAppRuleDataBuilder.setModelGovernanceRuleData(
            convertToModelGovernanceRuleData(customSignatureRule));
        break;
      case AI_INPUT_EXPLOSION_TYPE:
        aiAppRuleDataBuilder.setAiInputExplosionRuleData(
            convertToAiInputExplosionRuleData(customSignatureRule));
        break;
      default:
        throw new IllegalArgumentException(
            "Unsupported threatTypeId for custom signature rule conversion: " + threatTypeId);
    }

    return AiAppCustomRule.newBuilder()
        .setRuleId(customSignatureRule.getId())
        .setRuleData(aiAppRuleDataBuilder.build())
        .build();
  }

  /** Converts custom signature rule to model governance rule data. */
  private static ModelGovernanceRuleData convertToModelGovernanceRuleData(
      CustomSignatureRule customSignatureRule) {
    ModelGovernanceRuleData.Builder builder = ModelGovernanceRuleData.newBuilder();

    if (customSignatureRule.hasDefinition()
        && customSignatureRule.getDefinition().hasClauseGroup()) {
      // Extract AI model types and vendors conditions from the clause group
      extractAiConditionsFromClauseGroup(
          customSignatureRule.getDefinition().getClauseGroup(), builder);
    }

    return builder.build();
  }

  /** Converts custom signature rule to AI input explosion rule data. */
  private static AiInputExplosionRuleData convertToAiInputExplosionRuleData(
      CustomSignatureRule customSignatureRule) {
    AiInputExplosionRuleData.Builder builder = AiInputExplosionRuleData.newBuilder();

    if (customSignatureRule.getDefinition().hasClauseGroup()) {
      // Extract AI input explosion conditions from the clause group
      extractAiInputExplosionConditionsFromClauseGroup(
          customSignatureRule.getDefinition().getClauseGroup(), builder);
    }

    return builder.build();
  }

  private static Action convertAction(
      ai.traceable.customsignature.config.service.v1.RuleEffect effect) {
    Action.Builder builder = Action.newBuilder();

    // Check event type to determine if it's mark for testing or alert
    switch (effect.getEventType()) {
      case EVENT_TYPE_TESTING_DETECTION:
        // Mark for testing action
        builder.setMarkForTesting(Action.MarkForTesting.newBuilder().build());
        break;
      case EVENT_TYPE_NORMAL_DETECTION:
        // Alert action with severity level
        Action.Alert.Builder alertBuilder =
            Action.Alert.newBuilder()
                .setSeverityLevel(convertEventSeverityToSeverityLevel(effect.getEventSeverity()));
        builder.setAlert(alertBuilder.build());
        break;
      default:
        break;
    }

    return builder.build();
  }

  /** Converts custom signature event severity to AI app severity level. */
  private static SeverityLevel convertEventSeverityToSeverityLevel(
      ai.traceable.customsignature.config.service.v1.EventSeverity eventSeverity) {
    switch (eventSeverity) {
      case EVENT_SEVERITY_LOW:
        return SeverityLevel.SEVERITY_LEVEL_LOW;
      case EVENT_SEVERITY_MEDIUM:
        return SeverityLevel.SEVERITY_LEVEL_MEDIUM;
      case EVENT_SEVERITY_HIGH:
        return SeverityLevel.SEVERITY_LEVEL_HIGH;
      case EVENT_SEVERITY_CRITICAL:
        return SeverityLevel.SEVERITY_LEVEL_CRITICAL;
      default:
        return SeverityLevel.SEVERITY_LEVEL_UNSPECIFIED;
    }
  }

  /** Converts custom signature rule scope to AI app rule scope. */
  private static RuleScope convertRuleScope(
      ai.traceable.customsignature.config.service.v1.RuleScope customRuleScope) {
    RuleScope.Builder builder = RuleScope.newBuilder();

    if (customRuleScope.hasEnvironmentScope()) {
      // Convert environment scope back to AI app environment scope
      EnvironmentScope.Builder envScopeBuilder =
          EnvironmentScope.newBuilder()
              .addAllEnvironmentIds(customRuleScope.getEnvironmentScope().getEnvironmentIdsList());

      builder.setEnvironmentScope(envScopeBuilder.build());
    } else {
      builder.setTenantScope(TenantScope.getDefaultInstance());
    }

    return builder.build();
  }

  private static void extractAiConditionsFromClauseGroup(
      ClauseGroup clauseGroup, ModelGovernanceRuleData.Builder builder) {

    for (Clause clause : clauseGroup.getClausesList()) {
      if (clause.hasClauseGroup()
          && clause
              .getClauseGroup()
              .getClauseOperator()
              .equals(ClauseOperator.CLAUSE_OPERATOR_AND)) {
        extractAiConditionsFromClauseGroup(clause.getClauseGroup(), builder);
      }
      if (clause.hasAttributeKeyValueExpression()) {
        // Handle attribute match conditions for AI models and vendors
        ai.traceable.customsignature.config.service.v1.AttributeKeyValueExpression attributeExpr =
            clause.getAttributeKeyValueExpression();
        ai.traceable.customsignature.config.service.v1.StringCondition keyCondition =
            attributeExpr.getKeyCondition();
        ai.traceable.customsignature.config.service.v1.StringCondition valueCondition =
            attributeExpr.getValueCondition();

        String attributeKey = keyCondition.getValue();

        if (GENAI_MODELS_ATTRIBUTE_KEY.equals(attributeKey)) {
          MatchOperatorCondition matchCondition =
              reverseStringConditionToMatchOperator(valueCondition);
          builder.setAiModelTypesCondition(matchCondition);
        } else if (GENAI_PROVIDERS_ATTRIBUTE_KEY.equals(attributeKey)) {
          MatchOperatorCondition matchCondition =
              reverseStringConditionToMatchOperator(valueCondition);
          builder.setAiVendorsCondition(matchCondition);
        }
      } else if (clause.hasScopeExpression()) {
        ai.traceable.customsignature.config.service.v1.ScopeExpression scopeExpression =
            clause.getScopeExpression();

        ScopeCondition scopeCondition = convertScopeExpressionToScopeCondition(scopeExpression);
        if (scopeCondition != null) {
          builder.addScopeConditions(scopeCondition);
        }
      }
    }
  }

  private static void extractAiInputExplosionConditionsFromClauseGroup(
      ClauseGroup clauseGroup, AiInputExplosionRuleData.Builder builder) {

    for (Clause clause : clauseGroup.getClausesList()) {
      if (clause.hasClauseGroup()
          && clause
              .getClauseGroup()
              .getClauseOperator()
              .equals(ClauseOperator.CLAUSE_OPERATOR_AND)) {
        extractAiInputExplosionConditionsFromClauseGroup(clause.getClauseGroup(), builder);
      }
      if (clause.hasAttributeKeyValueExpression()) {
        ai.traceable.customsignature.config.service.v1.AttributeKeyValueExpression attributeExpr =
            clause.getAttributeKeyValueExpression();

        if (attributeExpr.hasKeyCondition() && attributeExpr.hasValueCondition()) {
          ai.traceable.customsignature.config.service.v1.StringCondition keyCondition =
              attributeExpr.getKeyCondition();
          ai.traceable.customsignature.config.service.v1.StringCondition valueCondition =
              attributeExpr.getValueCondition();

          if (GENAI_PROMPT_SIZE_ATTRIBUTE_KEY.equals(keyCondition.getValue())) {
            try {
              int inputCharacterLimit = Integer.parseInt(valueCondition.getValue());
              builder.setInputCharacterLimit(inputCharacterLimit);
            } catch (NumberFormatException e) {
              log.warn(
                  "Failed to parse input character limit from value: {}",
                  valueCondition.getValue());
            }
          }
        }
      } else if (clause.hasScopeExpression()) {
        ai.traceable.customsignature.config.service.v1.ScopeExpression scopeExpression =
            clause.getScopeExpression();

        ScopeCondition scopeCondition = convertScopeExpressionToScopeCondition(scopeExpression);
        if (scopeCondition != null) {
          builder.addScopeConditions(scopeCondition);
        }
      }
    }
  }

  /** Converts StringCondition back to MatchOperatorCondition for attribute match conditions. */
  private static MatchOperatorCondition reverseStringConditionToMatchOperator(
      ai.traceable.customsignature.config.service.v1.StringCondition stringCondition) {

    return MatchOperatorCondition.newBuilder()
        .setOperator(reverseCustomSignatureMatchOperatorToAiApp(stringCondition.getOperator()))
        .setValue(Value.newBuilder().setStringValue(stringCondition.getValue()))
        .build();
  }

  private static MatchOperator reverseCustomSignatureMatchOperatorToAiApp(
      ai.traceable.customsignature.config.service.v1.MatchOperator customSignatureOperator) {
    switch (customSignatureOperator) {
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

  /** Converts custom signature rule source to AI app rule creation source. */
  private static RuleStatusDetails.RuleSource convertRuleSource(
      ai.traceable.customsignature.config.service.v1.RuleSource customRuleSource) {
    switch (customRuleSource) {
      case RULE_SOURCE_CUSTOMER:
        return RuleStatusDetails.RuleSource.RULE_SOURCE_CUSTOMER;
      case RULE_SOURCE_TRACEABLE:
        return RuleStatusDetails.RuleSource.RULE_SOURCE_TRACEABLE;
      case RULE_SOURCE_DEFAULT:
        return RuleStatusDetails.RuleSource.RULE_SOURCE_DEFAULT;
      default:
        return RuleStatusDetails.RuleSource.RULE_SOURCE_UNSPECIFIED;
    }
  }

  private static ScopeCondition convertScopeExpressionToScopeCondition(
      ai.traceable.customsignature.config.service.v1.ScopeExpression scopeExpression) {

    if (scopeExpression
        .getEntityScope()
        .getEntityType()
        .equals(ScopeExpression.EntityType.ENTITY_TYPE_API)) {
      ai.traceable.customsignature.config.service.v1.ScopeExpression.EntityScope customEntityScope =
          scopeExpression.getEntityScope();

      ai.traceable.aiapp.protection.config.service.v1.ScopeCondition.EntityScope.Builder
          entityScopeBuilder =
              ai.traceable.aiapp.protection.config.service.v1.ScopeCondition.EntityScope
                  .newBuilder()
                  .addAllEntityIds(customEntityScope.getEntityIdsList())
                  .setEntityType(
                      ai.traceable.aiapp.protection.config.service.v1.ScopeCondition.EntityType
                          .ENTITY_TYPE_API);

      return ScopeCondition.newBuilder().setEntityScope(entityScopeBuilder.build()).build();

    } else if (scopeExpression.hasUrlScope()) {
      ai.traceable.customsignature.config.service.v1.ScopeExpression.UrlScope customUrlScope =
          scopeExpression.getUrlScope();

      ai.traceable.aiapp.protection.config.service.v1.ScopeCondition.UrlScope.Builder
          urlScopeBuilder =
              ai.traceable.aiapp.protection.config.service.v1.ScopeCondition.UrlScope.newBuilder()
                  .addAllUrlRegexes(customUrlScope.getUrlRegexesList());

      return ScopeCondition.newBuilder().setUrlScope(urlScopeBuilder.build()).build();
    }

    return null; // No valid scope condition found
  }
}
