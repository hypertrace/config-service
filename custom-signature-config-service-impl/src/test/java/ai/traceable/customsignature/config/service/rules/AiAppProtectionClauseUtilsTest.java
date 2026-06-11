package ai.traceable.customsignature.config.service.rules;

import static ai.traceable.protection.processing.common.v1.GenAiAttributeType.GEN_AI_ATTRIBUTE_TYPE_MODELS;
import static ai.traceable.protection.processing.common.v1.GenAiAttributeType.GEN_AI_ATTRIBUTE_TYPE_PROVIDERS;
import static ai.traceable.protection.processing.common.v1.utils.ProtoEnumUtils.getStringExtension;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import ai.traceable.customsignature.config.service.v1.AttributeKeyValueExpression;
import ai.traceable.customsignature.config.service.v1.Category;
import ai.traceable.customsignature.config.service.v1.Clause;
import ai.traceable.customsignature.config.service.v1.ClauseGroup;
import ai.traceable.customsignature.config.service.v1.ClauseOperator;
import ai.traceable.customsignature.config.service.v1.CustomSignatureRule;
import ai.traceable.customsignature.config.service.v1.EventType;
import ai.traceable.customsignature.config.service.v1.MatchOperator;
import ai.traceable.customsignature.config.service.v1.RuleDefinition;
import ai.traceable.customsignature.config.service.v1.RuleEffect;
import ai.traceable.customsignature.config.service.v1.RuleEvaluationPoint;
import ai.traceable.customsignature.config.service.v1.StringCondition;
import org.junit.jupiter.api.Test;

class AiAppProtectionClauseUtilsTest {

  private static final String GENAI_MODELS_KEY = getStringExtension(GEN_AI_ATTRIBUTE_TYPE_MODELS);
  private static final String GENAI_PROVIDERS_KEY =
      getStringExtension(GEN_AI_ATTRIBUTE_TYPE_PROVIDERS);

  @Test
  void aiAppProtectionRulesAreExcludedFromEdgeDecisionSupplier() {
    CustomSignatureRule aiAppRule =
        CustomSignatureRule.newBuilder()
            .setId("b7feb949-f061-48f1-9943-8e0edce3860c")
            .setCategory(Category.CATEGORY_AI_APP_PROTECTION)
            .setDefinition(
                RuleDefinition.newBuilder()
                    .setClauseGroup(
                        ClauseGroup.newBuilder()
                            .addClauses(buildGenAiAttributeClause(GENAI_MODELS_KEY))
                            .addClauses(buildGenAiAttributeClause(GENAI_PROVIDERS_KEY))))
            .setEffect(
                RuleEffect.newBuilder()
                    .setEventType(EventType.EVENT_TYPE_DETECTION_AND_BLOCKING)
                    .addRuleEvaluationPoints(RuleEvaluationPoint.RULE_EVALUATION_POINT_EDGE))
            .build();

    assertTrue(
        CustomSignatureRulesEdgeDecisionFilter.getConvertibleAndActiveRules(
                java.util.List.of(aiAppRule))
            .isEmpty());
  }

  @Test
  void genAiAttributeClausesAreNotEdgeDecisionCompatible() {
    ClauseGroup clauseGroup =
        ClauseGroup.newBuilder()
            .setClauseOperator(ClauseOperator.CLAUSE_OPERATOR_AND)
            .addClauses(buildGenAiAttributeClause(GENAI_MODELS_KEY))
            .addClauses(buildGenAiAttributeClause(GENAI_PROVIDERS_KEY))
            .build();

    RuleEffect ruleEffect =
        RuleEffect.newBuilder()
            .setEventType(EventType.EVENT_TYPE_DETECTION_AND_BLOCKING)
            .addRuleEvaluationPoints(RuleEvaluationPoint.RULE_EVALUATION_POINT_EDGE)
            .build();

    assertFalse(CustomSignatureRulesEdgeDecisionFilter.isConvertibleRule(ruleEffect, clauseGroup));
  }

  private static Clause buildGenAiAttributeClause(String attributeKey) {
    return Clause.newBuilder()
        .setAttributeKeyValueExpression(
            AttributeKeyValueExpression.newBuilder()
                .setKeyCondition(
                    StringCondition.newBuilder()
                        .setOperator(MatchOperator.MATCH_OPERATOR_EQUALS)
                        .setValue(attributeKey)))
        .build();
  }
}
