package ai.traceable.aiapp.protection.config.service.firewall.converter;

import static ai.traceable.protection.processing.common.v1.GenAiAttributeType.GEN_AI_ATTRIBUTE_TYPE_PROMPT_SIZE;
import static ai.traceable.protection.processing.common.v1.utils.ProtoEnumUtils.getStringExtension;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import ai.traceable.aiapp.protection.config.service.v1.Action;
import ai.traceable.aiapp.protection.config.service.v1.AiAppCustomRule;
import ai.traceable.aiapp.protection.config.service.v1.AiAppCustomRuleData;
import ai.traceable.aiapp.protection.config.service.v1.AiInputExplosionRuleData;
import ai.traceable.aiapp.protection.config.service.v1.MatchOperator;
import ai.traceable.aiapp.protection.config.service.v1.MatchOperatorCondition;
import ai.traceable.aiapp.protection.config.service.v1.ModelGovernanceRuleData;
import ai.traceable.aiapp.protection.config.service.v1.ScopeCondition;
import ai.traceable.protection.engine.config.customsignature.v1.CustomSignatureConfigContext;
import ai.traceable.protection.engine.config.customsignature.v1.CustomSignatureRuleConfig;
import ai.traceable.protection.engine.config.customsignature.v1.CustomSignatureRuleDefinitionGroup;
import ai.traceable.protection.processing.common.v1.LogicalOperator;
import ai.traceable.protection.processing.common.v1.NumberMatchOperator;
import ai.traceable.protection.processor.condition.expression.v1.KeyValueMatchCondition;
import com.google.protobuf.Value;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class GenAiRuleToCustomSignatureConfigConverterTest {

  private GenAiRuleToCustomSignatureConfigConverter converter;

  @BeforeEach
  void setUp() {
    converter = new GenAiRuleToCustomSignatureConfigConverter();
  }

  @Test
  void convertsInputExplosionBlockingRule() {
    AiAppCustomRule rule =
        AiAppCustomRule.newBuilder()
            .setRuleId("input-explosion-1")
            .setRuleData(
                AiAppCustomRuleData.newBuilder()
                    .setRuleName("Input explosion")
                    .setDescription("Block large prompts")
                    .setEnabled(true)
                    .setAction(Action.newBuilder().setBlock(Action.Block.newBuilder().build()))
                    .setAiInputExplosionRuleData(
                        AiInputExplosionRuleData.newBuilder()
                            .setInputCharacterLimit(500)
                            .addScopeConditions(
                                ScopeCondition.newBuilder()
                                    .setUrlScope(
                                        ScopeCondition.UrlScope.newBuilder()
                                            .addUrlRegexes("/chat/.*")
                                            .build())
                                    .build())
                            .build())
                    .build())
            .build();

    CustomSignatureConfigContext result = converter.convert(List.of(rule));

    assertEquals(1, result.getRuleContextsCount());
    CustomSignatureRuleConfig ruleConfig = result.getRuleContexts(0).getRuleConfigs(0);
    assertEquals("input-explosion-1", ruleConfig.getId());
    assertTrue(ruleConfig.getAction().hasBlockingAction());

    CustomSignatureRuleDefinitionGroup definitionGroup = ruleConfig.getRuleDefinitionGroup();
    assertEquals(LogicalOperator.LOGICAL_OPERATOR_AND, definitionGroup.getOperator());
    assertEquals(2, definitionGroup.getRuleDefinitionsCount());

    KeyValueMatchCondition promptSizeCondition =
        definitionGroup
            .getRuleDefinitions(0)
            .getCustomSignatureConditionExpression()
            .getConditionExpression()
            .getLeafMatchConditionExpression()
            .getKeyValueMatchCondition();
    assertEquals(
        "request.custom",
        promptSizeCondition.getLhsKeyOperand().getKeyMetadata().getFullyQualifiedKeyPrefix());
    assertEquals(
        getStringExtension(GEN_AI_ATTRIBUTE_TYPE_PROMPT_SIZE),
        promptSizeCondition
            .getLhsKeyOperand()
            .getKeyMatchOperation()
            .getStringMatchOperation()
            .getStringValue());
    assertEquals(
        NumberMatchOperator.NUMBER_MATCH_OPERATOR_GT,
        promptSizeCondition
            .getRhsValueMatchOperation()
            .getNumberMatchOperation()
            .getNumberOperator());
    assertEquals(
        500,
        promptSizeCondition.getRhsValueMatchOperation().getNumberMatchOperation().getNumberValue(),
        0.001);
  }

  @Test
  void convertsModelGovernanceBlockingRule() {
    AiAppCustomRule rule =
        AiAppCustomRule.newBuilder()
            .setRuleId("model-governance-1")
            .setRuleData(
                AiAppCustomRuleData.newBuilder()
                    .setRuleName("Model governance")
                    .setEnabled(true)
                    .setAction(Action.newBuilder().setBlock(Action.Block.newBuilder().build()))
                    .setModelGovernanceRuleData(
                        ModelGovernanceRuleData.newBuilder()
                            .setAiVendorsCondition(
                                MatchOperatorCondition.newBuilder()
                                    .setOperator(MatchOperator.MATCH_OPERATOR_NOT_MATCH_REGEX)
                                    .setValue(
                                        Value.newBuilder()
                                            .setStringValue("OpenAI|Anthropic")
                                            .build())
                                    .build())
                            .build())
                    .build())
            .build();

    CustomSignatureConfigContext result = converter.convert(List.of(rule));
    CustomSignatureRuleDefinitionGroup definitionGroup =
        result.getRuleContexts(0).getRuleConfigs(0).getRuleDefinitionGroup();

    assertEquals(1, definitionGroup.getRuleDefinitionsCount());

    KeyValueMatchCondition providersCondition =
        definitionGroup
            .getRuleDefinitions(0)
            .getCustomSignatureConditionExpression()
            .getConditionExpression()
            .getLeafMatchConditionExpression()
            .getKeyValueMatchCondition();
    assertNotNull(providersCondition);
    assertEquals(
        "OpenAI|Anthropic",
        providersCondition.getRhsValueMatchOperation().getStringMatchOperation().getStringValue());
  }

  @Test
  void skipsModelGovernanceRuleWhenNoConditionsAreSet() {
    AiAppCustomRule rule =
        AiAppCustomRule.newBuilder()
            .setRuleId("model-governance-empty")
            .setRuleData(
                AiAppCustomRuleData.newBuilder()
                    .setRuleName("Model governance")
                    .setEnabled(true)
                    .setAction(Action.newBuilder().setBlock(Action.Block.newBuilder().build()))
                    .setModelGovernanceRuleData(ModelGovernanceRuleData.getDefaultInstance())
                    .build())
            .build();

    CustomSignatureConfigContext result = converter.convert(List.of(rule));

    assertEquals(0, result.getRuleContextsCount());
  }

  @Test
  void skipsModelTypesConditionWhenNoMeaningfulValue() {
    AiAppCustomRule rule =
        AiAppCustomRule.newBuilder()
            .setRuleId("model-governance-models-empty")
            .setRuleData(
                AiAppCustomRuleData.newBuilder()
                    .setRuleName("Model governance")
                    .setEnabled(true)
                    .setAction(Action.newBuilder().setBlock(Action.Block.newBuilder().build()))
                    .setModelGovernanceRuleData(
                        ModelGovernanceRuleData.newBuilder()
                            .setAiModelTypesCondition(
                                MatchOperatorCondition.newBuilder()
                                    .setOperator(MatchOperator.MATCH_OPERATOR_EQUALS)
                                    .build())
                            .build())
                    .build())
            .build();

    CustomSignatureConfigContext result = converter.convert(List.of(rule));

    assertEquals(0, result.getRuleContextsCount());
  }

  @Test
  void skipsPoisonedModelTypesConditionWithUnspecifiedOperatorAndEmptyValue() {
    AiAppCustomRule rule =
        AiAppCustomRule.newBuilder()
            .setRuleId("model-governance-poisoned-models")
            .setRuleData(
                AiAppCustomRuleData.newBuilder()
                    .setRuleName("Model governance")
                    .setEnabled(true)
                    .setAction(Action.newBuilder().setBlock(Action.Block.newBuilder().build()))
                    .setModelGovernanceRuleData(
                        ModelGovernanceRuleData.newBuilder()
                            .setAiModelTypesCondition(
                                MatchOperatorCondition.newBuilder()
                                    .setOperator(MatchOperator.MATCH_OPERATOR_UNSPECIFIED)
                                    .setValue(Value.newBuilder().setStringValue("").build())
                                    .build())
                            .setAiVendorsCondition(
                                MatchOperatorCondition.newBuilder()
                                    .setOperator(MatchOperator.MATCH_OPERATOR_NOT_MATCH_REGEX)
                                    .setValue(
                                        Value.newBuilder()
                                            .setStringValue("OpenAI|Anthropic")
                                            .build())
                                    .build())
                            .build())
                    .build())
            .build();

    CustomSignatureConfigContext result = converter.convert(List.of(rule));
    CustomSignatureRuleDefinitionGroup definitionGroup =
        result.getRuleContexts(0).getRuleConfigs(0).getRuleDefinitionGroup();

    assertEquals(1, definitionGroup.getRuleDefinitionsCount());
    KeyValueMatchCondition providersCondition =
        definitionGroup
            .getRuleDefinitions(0)
            .getCustomSignatureConditionExpression()
            .getConditionExpression()
            .getLeafMatchConditionExpression()
            .getKeyValueMatchCondition();
    assertEquals(
        "OpenAI|Anthropic",
        providersCondition.getRhsValueMatchOperation().getStringMatchOperation().getStringValue());
  }

  @Test
  void usesDeterministicScopeEvaluationIdentifier() {
    AiAppCustomRule rule =
        AiAppCustomRule.newBuilder()
            .setRuleId("input-explosion-scope")
            .setRuleData(
                AiAppCustomRuleData.newBuilder()
                    .setRuleName("Input explosion")
                    .setEnabled(true)
                    .setAction(Action.newBuilder().setBlock(Action.Block.newBuilder().build()))
                    .setAiInputExplosionRuleData(
                        AiInputExplosionRuleData.newBuilder()
                            .setInputCharacterLimit(500)
                            .addScopeConditions(
                                ScopeCondition.newBuilder()
                                    .setUrlScope(
                                        ScopeCondition.UrlScope.newBuilder()
                                            .addUrlRegexes("/chat/.*")
                                            .build())
                                    .build())
                            .build())
                    .build())
            .build();

    CustomSignatureConfigContext result = converter.convert(List.of(rule));
    CustomSignatureRuleDefinitionGroup definitionGroup =
        result.getRuleContexts(0).getRuleConfigs(0).getRuleDefinitionGroup();

    assertEquals(
        "scope-url-input-explosion-scope-0",
        definitionGroup
            .getRuleDefinitions(1)
            .getCustomSignatureConditionExpression()
            .getConditionExpressionEvaluationIdentifier());
  }
}
