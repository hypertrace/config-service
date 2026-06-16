package ai.traceable.aiapp.protection.config.service.firewall.converter;

import static ai.traceable.protection.processing.common.v1.GenAiAttributeType.GEN_AI_ATTRIBUTE_TYPE_MODELS;
import static ai.traceable.protection.processing.common.v1.GenAiAttributeType.GEN_AI_ATTRIBUTE_TYPE_PROMPT_SIZE;
import static ai.traceable.protection.processing.common.v1.GenAiAttributeType.GEN_AI_ATTRIBUTE_TYPE_PROVIDERS;
import static ai.traceable.protection.processing.common.v1.utils.ProtoEnumUtils.getStringExtension;

import ai.traceable.aiapp.protection.config.service.v1.AiAppCustomRule;
import ai.traceable.aiapp.protection.config.service.v1.AiAppCustomRuleData;
import ai.traceable.aiapp.protection.config.service.v1.AiInputExplosionRuleData;
import ai.traceable.aiapp.protection.config.service.v1.MatchOperator;
import ai.traceable.aiapp.protection.config.service.v1.MatchOperatorCondition;
import ai.traceable.aiapp.protection.config.service.v1.ModelGovernanceRuleData;
import ai.traceable.aiapp.protection.config.service.v1.ScopeCondition;
import ai.traceable.protection.engine.config.customsignature.v1.BlockingAction;
import ai.traceable.protection.engine.config.customsignature.v1.CustomSignatureConditionExpression;
import ai.traceable.protection.engine.config.customsignature.v1.CustomSignatureConfigContext;
import ai.traceable.protection.engine.config.customsignature.v1.CustomSignatureRuleConfig;
import ai.traceable.protection.engine.config.customsignature.v1.CustomSignatureRuleDefinition;
import ai.traceable.protection.engine.config.customsignature.v1.CustomSignatureRuleDefinitionGroup;
import ai.traceable.protection.engine.config.customsignature.v1.CustomSignatureRulesContext;
import ai.traceable.protection.engine.config.customsignature.v1.RuleAction;
import ai.traceable.protection.processing.common.v1.AttributeType;
import ai.traceable.protection.processing.common.v1.CustomerScope;
import ai.traceable.protection.processing.common.v1.Entity;
import ai.traceable.protection.processing.common.v1.EntityScope;
import ai.traceable.protection.processing.common.v1.EntityType;
import ai.traceable.protection.processing.common.v1.LogicalOperator;
import ai.traceable.protection.processing.common.v1.MessageType;
import ai.traceable.protection.processing.common.v1.MultiMatchOperator;
import ai.traceable.protection.processing.common.v1.NumberMatchOperator;
import ai.traceable.protection.processing.common.v1.Scope;
import ai.traceable.protection.processing.common.v1.ScopeAttributeType;
import ai.traceable.protection.processing.common.v1.ScopeContext;
import ai.traceable.protection.processing.common.v1.ScopeType;
import ai.traceable.protection.processing.common.v1.StringOperator;
import ai.traceable.protection.processing.common.v1.utils.PrefixBuilder;
import ai.traceable.protection.processor.condition.expression.v1.KeyMatchOperand;
import ai.traceable.protection.processor.condition.expression.v1.KeyValueMatchCondition;
import ai.traceable.protection.processor.condition.expression.v1.LeafMatchConditionExpression;
import ai.traceable.protection.processor.condition.expression.v1.MatchConditionExpression;
import ai.traceable.protection.processor.condition.expression.v1.NumberMatchOperation;
import ai.traceable.protection.processor.condition.expression.v1.StringListMatchOperation;
import ai.traceable.protection.processor.condition.expression.v1.StringMatchOperation;
import ai.traceable.protection.processor.condition.expression.v1.ValueMatchOperation;
import com.google.inject.Singleton;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Singleton
public class GenAiRuleToCustomSignatureConfigConverter {

  private static final String GENAI_PROMPT_SIZE_KEY =
      getStringExtension(GEN_AI_ATTRIBUTE_TYPE_PROMPT_SIZE);
  private static final String GENAI_MODELS_KEY = getStringExtension(GEN_AI_ATTRIBUTE_TYPE_MODELS);
  private static final String GENAI_PROVIDERS_KEY =
      getStringExtension(GEN_AI_ATTRIBUTE_TYPE_PROVIDERS);

  private static final String CUSTOM_ATTRIBUTES_PREFIX =
      PrefixBuilder.buildAppendablePrefix(
          MessageType.MESSAGE_TYPE_REQUEST, AttributeType.ATTRIBUTE_TYPE_CUSTOM);

  public CustomSignatureConfigContext convert(List<AiAppCustomRule> rules) {
    Map<ScopeContext, List<CustomSignatureRuleConfig>> ruleConfigsByScope = new HashMap<>();

    for (AiAppCustomRule rule : rules) {
      try {
        CustomSignatureRuleConfig ruleConfig = convertRule(rule);
        if (ruleConfig != null) {
          ScopeContext scopeContext = buildScopeContext(rule);
          ruleConfigsByScope.computeIfAbsent(scopeContext, k -> new ArrayList<>()).add(ruleConfig);
        }
      } catch (Exception e) {
        log.warn("Failed to convert GenAI rule {}: {}", rule.getRuleId(), e.getMessage(), e);
      }
    }

    if (ruleConfigsByScope.isEmpty()) {
      return CustomSignatureConfigContext.getDefaultInstance();
    }

    CustomSignatureConfigContext.Builder builder = CustomSignatureConfigContext.newBuilder();
    for (Map.Entry<ScopeContext, List<CustomSignatureRuleConfig>> entry :
        ruleConfigsByScope.entrySet()) {
      builder.addRuleContexts(
          CustomSignatureRulesContext.newBuilder()
              .setScopeContext(entry.getKey())
              .addAllRuleConfigs(entry.getValue())
              .build());
    }
    return builder.build();
  }

  private ScopeContext buildScopeContext(AiAppCustomRule rule) {
    ScopeContext.Builder builder = ScopeContext.newBuilder();
    ai.traceable.aiapp.protection.config.service.v1.RuleScope ruleScope =
        rule.getRuleData().getRuleScope();

    if (ruleScope.hasEnvironmentScope()) {
      EntityScope.Builder entityScopeBuilder =
          EntityScope.newBuilder().setEntityType(EntityType.ENTITY_TYPE_ENVIRONMENT);
      for (String environmentId : ruleScope.getEnvironmentScope().getEnvironmentIdsList()) {
        entityScopeBuilder.addEntities(
            Entity.newBuilder().setId(environmentId).setName(environmentId).build());
      }
      builder
          .addScopes(Scope.newBuilder().setEntityScope(entityScopeBuilder.build()).build())
          .addScopes(
              Scope.newBuilder().setCustomerScope(CustomerScope.getDefaultInstance()).build());
    } else {
      builder.addScopes(
          Scope.newBuilder().setCustomerScope(CustomerScope.getDefaultInstance()).build());
    }
    return builder.build();
  }

  private CustomSignatureRuleConfig convertRule(AiAppCustomRule rule) {
    if (!rule.hasRuleData()) {
      return null;
    }

    AiAppCustomRuleData ruleData = rule.getRuleData();
    List<CustomSignatureRuleDefinition> ruleDefinitions = new ArrayList<>();
    List<ScopeCondition> scopeConditions;

    if (ruleData.hasAiInputExplosionRuleData()) {
      AiInputExplosionRuleData inputExplosionData = ruleData.getAiInputExplosionRuleData();
      if (inputExplosionData.getInputCharacterLimit() <= 0) {
        log.debug("Rule {} has invalid input character limit, skipping", rule.getRuleId());
        return null;
      }
      ruleDefinitions.add(
          buildPromptSizeDefinition(rule.getRuleId(), inputExplosionData.getInputCharacterLimit()));
      scopeConditions = inputExplosionData.getScopeConditionsList();
    } else if (ruleData.hasModelGovernanceRuleData()) {
      ModelGovernanceRuleData modelGovernanceData = ruleData.getModelGovernanceRuleData();
      if (hasMeaningfulCondition(modelGovernanceData.getAiModelTypesCondition())) {
        ruleDefinitions.add(
            buildGenAiAttributeDefinition(
                rule.getRuleId() + "-models",
                GENAI_MODELS_KEY,
                modelGovernanceData.getAiModelTypesCondition()));
      }
      if (hasMeaningfulCondition(modelGovernanceData.getAiVendorsCondition())) {
        ruleDefinitions.add(
            buildGenAiAttributeDefinition(
                rule.getRuleId() + "-providers",
                GENAI_PROVIDERS_KEY,
                modelGovernanceData.getAiVendorsCondition()));
      }
      scopeConditions = modelGovernanceData.getScopeConditionsList();
    } else {
      return null;
    }

    int scopeIndex = 0;
    for (ScopeCondition scopeCondition : scopeConditions) {
      CustomSignatureRuleDefinition scopeDefinition =
          convertScopeCondition(scopeCondition, rule.getRuleId(), scopeIndex);
      if (scopeDefinition != null) {
        ruleDefinitions.add(scopeDefinition);
      }
      scopeIndex++;
    }

    if (ruleDefinitions.isEmpty()) {
      return null;
    }

    return CustomSignatureRuleConfig.newBuilder()
        .setId(rule.getRuleId())
        .setName(ruleData.getRuleName())
        .setDescription(ruleData.getDescription())
        .setAction(
            RuleAction.newBuilder().setBlockingAction(BlockingAction.getDefaultInstance()).build())
        .setRuleDefinitionGroup(
            CustomSignatureRuleDefinitionGroup.newBuilder()
                .setOperator(LogicalOperator.LOGICAL_OPERATOR_AND)
                .addAllRuleDefinitions(ruleDefinitions)
                .build())
        .build();
  }

  private CustomSignatureRuleDefinition buildPromptSizeDefinition(
      String ruleId, int inputCharacterLimit) {
    MatchConditionExpression matchExpression =
        buildCustomAttributeNumberCondition(
            GENAI_PROMPT_SIZE_KEY,
            NumberMatchOperator.NUMBER_MATCH_OPERATOR_GT,
            inputCharacterLimit);
    return buildRuleDefinition("genai-prompt-size-" + ruleId, matchExpression);
  }

  private CustomSignatureRuleDefinition buildGenAiAttributeDefinition(
      String ruleId, String attributeKey, MatchOperatorCondition matchCondition) {
    String matchValue = matchCondition.getValue().getStringValue();
    MatchConditionExpression matchExpression =
        buildCustomAttributeStringCondition(attributeKey, matchCondition.getOperator(), matchValue);
    return buildRuleDefinition("genai-attribute-" + ruleId, matchExpression);
  }

  private CustomSignatureRuleDefinition convertScopeCondition(
      ScopeCondition scopeCondition, String ruleId, int scopeIndex) {
    switch (scopeCondition.getScopeCase()) {
      case ENTITY_SCOPE:
        return convertEntityScopeCondition(scopeCondition.getEntityScope(), ruleId, scopeIndex);
      case URL_SCOPE:
        return convertUrlScopeCondition(scopeCondition.getUrlScope(), ruleId, scopeIndex);
      default:
        log.debug("Unsupported scope condition type: {}", scopeCondition.getScopeCase());
        return null;
    }
  }

  private CustomSignatureRuleDefinition convertEntityScopeCondition(
      ScopeCondition.EntityScope entityScope, String ruleId, int scopeIndex) {
    if (entityScope.getEntityIdsList().isEmpty()) {
      return null;
    }
    String evaluationIdentifier = "scope-entity-" + ruleId + "-" + scopeIndex;
    if (entityScope.getEntityType() != ScopeCondition.EntityType.ENTITY_TYPE_API) {
      log.debug("Unsupported entity type: {}", entityScope.getEntityType());
      return null;
    }
    String apiKeyPrefix =
        PrefixBuilder.buildAppendablePrefix(
            MessageType.MESSAGE_TYPE_REQUEST,
            ScopeType.SCOPE_TYPE_API,
            ScopeAttributeType.SCOPE_ATTRIBUTE_TYPE_ID);
    return buildRuleDefinition(
        evaluationIdentifier,
        buildStringListAnyEqualsCondition(apiKeyPrefix, entityScope.getEntityIdsList()));
  }

  private CustomSignatureRuleDefinition convertUrlScopeCondition(
      ScopeCondition.UrlScope urlScope, String ruleId, int scopeIndex) {
    if (urlScope.getUrlRegexesList().isEmpty()) {
      return null;
    }
    String evaluationIdentifier = "scope-url-" + ruleId + "-" + scopeIndex;
    String keyPrefix =
        PrefixBuilder.buildAppendablePrefix(
            MessageType.MESSAGE_TYPE_REQUEST, AttributeType.ATTRIBUTE_TYPE_URL);
    String joinedRegex = String.join("|", urlScope.getUrlRegexesList());
    return buildRuleDefinition(
        evaluationIdentifier, buildStringLikeCondition(keyPrefix, joinedRegex));
  }

  private CustomSignatureRuleDefinition buildRuleDefinition(
      String evaluationIdentifier, MatchConditionExpression conditionExpression) {
    return CustomSignatureRuleDefinition.newBuilder()
        .setCustomSignatureConditionExpression(
            CustomSignatureConditionExpression.newBuilder()
                .setConditionExpressionEvaluationIdentifier(evaluationIdentifier)
                .setConditionExpression(conditionExpression)
                .build())
        .build();
  }

  private MatchConditionExpression buildCustomAttributeNumberCondition(
      String attributeKey, NumberMatchOperator numberOperator, int numberValue) {
    return MatchConditionExpression.newBuilder()
        .setLeafMatchConditionExpression(
            LeafMatchConditionExpression.newBuilder()
                .setKeyValueMatchCondition(
                    KeyValueMatchCondition.newBuilder()
                        .setLhsKeyOperand(buildCustomAttributeKeyOperand(attributeKey))
                        .setRhsValueMatchOperation(
                            ValueMatchOperation.newBuilder()
                                .setNumberMatchOperation(
                                    NumberMatchOperation.newBuilder()
                                        .setNumberOperator(numberOperator)
                                        .setNumberValue(numberValue)
                                        .build())
                                .build())
                        .build())
                .build())
        .build();
  }

  private MatchConditionExpression buildCustomAttributeStringCondition(
      String attributeKey, MatchOperator matchOperator, String matchValue) {
    StringOperator valueStringOperator = convertMatchOperator(matchOperator);
    return MatchConditionExpression.newBuilder()
        .setLeafMatchConditionExpression(
            LeafMatchConditionExpression.newBuilder()
                .setKeyValueMatchCondition(
                    KeyValueMatchCondition.newBuilder()
                        .setLhsKeyOperand(buildCustomAttributeKeyOperand(attributeKey))
                        .setRhsValueMatchOperation(
                            ValueMatchOperation.newBuilder()
                                .setStringMatchOperation(
                                    StringMatchOperation.newBuilder()
                                        .setStringOperator(valueStringOperator)
                                        .setStringValue(matchValue)
                                        .build())
                                .build())
                        .build())
                .build())
        .build();
  }

  private KeyMatchOperand buildCustomAttributeKeyOperand(String attributeKey) {
    return KeyMatchOperand.newBuilder()
        .setKeyMetadata(
            KeyMatchOperand.KeyMetadata.newBuilder()
                .setFullyQualifiedKeyPrefix(CUSTOM_ATTRIBUTES_PREFIX)
                .build())
        .setKeyMatchOperation(
            ValueMatchOperation.newBuilder()
                .setStringMatchOperation(
                    StringMatchOperation.newBuilder()
                        .setStringOperator(
                            StringOperator.newBuilder()
                                .setStringOperator(
                                    StringOperator.StringMatchOperator.STRING_MATCH_OPERATOR_EQ)
                                .setIgnoreCase(false)
                                .build())
                        .setStringValue(attributeKey)
                        .build())
                .build())
        .build();
  }

  private MatchConditionExpression buildStringListAnyEqualsCondition(
      String keyPrefix, List<String> stringValues) {
    KeyValueMatchCondition keyValueCondition =
        KeyValueMatchCondition.newBuilder()
            .setLhsKeyOperand(
                KeyMatchOperand.newBuilder()
                    .setKeyMetadata(
                        KeyMatchOperand.KeyMetadata.newBuilder()
                            .setFullyQualifiedKeyPrefix(keyPrefix)
                            .build())
                    .build())
            .setRhsValueMatchOperation(
                ValueMatchOperation.newBuilder()
                    .setStringListMatchOperation(
                        StringListMatchOperation.newBuilder()
                            .setStringOperator(
                                StringOperator.newBuilder()
                                    .setStringOperator(
                                        StringOperator.StringMatchOperator.STRING_MATCH_OPERATOR_EQ)
                                    .setIgnoreCase(false)
                                    .build())
                            .setMultiMatchOperator(MultiMatchOperator.MULTI_MATCH_OPERATOR_ANY)
                            .addAllStringValues(stringValues)
                            .build())
                    .build())
            .build();

    return MatchConditionExpression.newBuilder()
        .setLeafMatchConditionExpression(
            LeafMatchConditionExpression.newBuilder()
                .setKeyValueMatchCondition(keyValueCondition)
                .build())
        .build();
  }

  private MatchConditionExpression buildStringLikeCondition(String keyPrefix, String regex) {
    KeyValueMatchCondition keyValueCondition =
        KeyValueMatchCondition.newBuilder()
            .setLhsKeyOperand(
                KeyMatchOperand.newBuilder()
                    .setKeyMetadata(
                        KeyMatchOperand.KeyMetadata.newBuilder()
                            .setFullyQualifiedKeyPrefix(keyPrefix)
                            .build())
                    .build())
            .setRhsValueMatchOperation(
                ValueMatchOperation.newBuilder()
                    .setStringMatchOperation(
                        StringMatchOperation.newBuilder()
                            .setStringOperator(
                                StringOperator.newBuilder()
                                    .setStringOperator(
                                        StringOperator.StringMatchOperator
                                            .STRING_MATCH_OPERATOR_LIKE)
                                    .setIgnoreCase(false)
                                    .build())
                            .setStringValue(regex)
                            .build())
                    .build())
            .build();

    return MatchConditionExpression.newBuilder()
        .setLeafMatchConditionExpression(
            LeafMatchConditionExpression.newBuilder()
                .setKeyValueMatchCondition(keyValueCondition)
                .build())
        .build();
  }

  private StringOperator convertMatchOperator(MatchOperator matchOperator) {
    StringOperator.StringMatchOperator stringMatchOperator;
    switch (matchOperator) {
      case MATCH_OPERATOR_EQUALS:
        stringMatchOperator = StringOperator.StringMatchOperator.STRING_MATCH_OPERATOR_EQ;
        break;
      case MATCH_OPERATOR_NOT_EQUAL:
        stringMatchOperator = StringOperator.StringMatchOperator.STRING_MATCH_OPERATOR_NOT_EQ;
        break;
      case MATCH_OPERATOR_CONTAINS:
        stringMatchOperator = StringOperator.StringMatchOperator.STRING_MATCH_OPERATOR_CONTAINS;
        break;
      case MATCH_OPERATOR_NOT_CONTAIN:
        stringMatchOperator = StringOperator.StringMatchOperator.STRING_MATCH_OPERATOR_NOT_CONTAINS;
        break;
      case MATCH_OPERATOR_MATCHES_REGEX:
        stringMatchOperator = StringOperator.StringMatchOperator.STRING_MATCH_OPERATOR_LIKE;
        break;
      case MATCH_OPERATOR_NOT_MATCH_REGEX:
        stringMatchOperator = StringOperator.StringMatchOperator.STRING_MATCH_OPERATOR_NOT_LIKE;
        break;
      default:
        stringMatchOperator = StringOperator.StringMatchOperator.STRING_MATCH_OPERATOR_UNSPECIFIED;
        break;
    }
    return StringOperator.newBuilder()
        .setStringOperator(stringMatchOperator)
        .setIgnoreCase(false)
        .build();
  }

  private boolean hasMeaningfulCondition(MatchOperatorCondition condition) {
    if (condition.equals(MatchOperatorCondition.getDefaultInstance())) {
      return false;
    }
    if (!condition.hasValue()) {
      return false;
    }
    if (condition.getValue().getStringValue().isBlank()) {
      return false;
    }
    return condition.getOperator()
            != ai.traceable.aiapp.protection.config.service.v1.MatchOperator
                .MATCH_OPERATOR_UNSPECIFIED
        && condition.getOperator()
            != ai.traceable.aiapp.protection.config.service.v1.MatchOperator.UNRECOGNIZED;
  }
}
