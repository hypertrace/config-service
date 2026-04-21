package ai.traceable.aiapp.protection.config.service.firewall.converter;

import ai.traceable.aiapp.protection.config.service.v1.AiAppCustomRule;
import ai.traceable.aiapp.protection.config.service.v1.DatatypeCondition;
import ai.traceable.aiapp.protection.config.service.v1.MatchOperator;
import ai.traceable.aiapp.protection.config.service.v1.MatchOperatorCondition;
import ai.traceable.aiapp.protection.config.service.v1.PiiDetectedInPromptRuleData;
import ai.traceable.aiapp.protection.config.service.v1.ScopeCondition;
import ai.traceable.data.classification.cache.info.DataClassificationInfo;
import ai.traceable.data.classification.config.service.v1.DataType;
import ai.traceable.protection.engine.config.customsignature.v1.CustomSignatureConditionExpression;
import ai.traceable.protection.engine.config.customsignature.v1.CustomSignatureConfigContext;
import ai.traceable.protection.engine.config.customsignature.v1.CustomSignatureRuleConfig;
import ai.traceable.protection.engine.config.customsignature.v1.CustomSignatureRuleDefinition;
import ai.traceable.protection.engine.config.customsignature.v1.CustomSignatureRuleDefinitionGroup;
import ai.traceable.protection.engine.config.customsignature.v1.CustomSignatureRulesContext;
import ai.traceable.protection.processing.common.v1.AttributeType;
import ai.traceable.protection.processing.common.v1.CustomerScope;
import ai.traceable.protection.processing.common.v1.Entity;
import ai.traceable.protection.processing.common.v1.EntityScope;
import ai.traceable.protection.processing.common.v1.EntityType;
import ai.traceable.protection.processing.common.v1.LogicalOperator;
import ai.traceable.protection.processing.common.v1.MessageType;
import ai.traceable.protection.processing.common.v1.MultiMatchOperator;
import ai.traceable.protection.processing.common.v1.Scope;
import ai.traceable.protection.processing.common.v1.ScopeAttributeType;
import ai.traceable.protection.processing.common.v1.ScopeContext;
import ai.traceable.protection.processing.common.v1.ScopeType;
import ai.traceable.protection.processing.common.v1.StringOperator;
import ai.traceable.protection.processing.common.v1.utils.PrefixBuilder;
import ai.traceable.protection.processor.condition.expression.v1.DataTypeMatchOperation;
import ai.traceable.protection.processor.condition.expression.v1.KeyMatchOperand;
import ai.traceable.protection.processor.condition.expression.v1.KeyValueMatchCondition;
import ai.traceable.protection.processor.condition.expression.v1.LeafMatchConditionExpression;
import ai.traceable.protection.processor.condition.expression.v1.MatchConditionExpression;
import ai.traceable.protection.processor.condition.expression.v1.StringListMatchOperation;
import ai.traceable.protection.processor.condition.expression.v1.StringMatchOperation;
import ai.traceable.protection.processor.condition.expression.v1.UnaryKeyMatchCondition;
import ai.traceable.protection.processor.condition.expression.v1.ValueMatchOperation;
import com.google.inject.Singleton;
import java.util.*;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Singleton
public class PiiRuleToCustomSignatureConfigConverter {

  /**
   * Converts a list of AI App Custom Rules (PII type) to CustomSignatureConfigContext.
   *
   * @param rules the list of PII custom rules
   * @param dataClassificationInfo the data classification information for resolving dataset IDs
   * @return the CustomSignatureConfigContext
   */
  public CustomSignatureConfigContext convert(
      List<AiAppCustomRule> rules, DataClassificationInfo dataClassificationInfo) {

    Map<ScopeContext, List<CustomSignatureRuleConfig>> ruleConfigsByScope = new HashMap<>();

    for (AiAppCustomRule rule : rules) {
      try {
        CustomSignatureRuleConfig ruleConfig = convertRule(rule, dataClassificationInfo);
        if (ruleConfig != null) {
          ScopeContext scopeContext = buildScopeContext(rule);
          ruleConfigsByScope.computeIfAbsent(scopeContext, k -> new ArrayList<>()).add(ruleConfig);
        }
      } catch (Exception e) {
        log.warn("Failed to convert rule {}: {}", rule.getRuleId(), e.getMessage(), e);
      }
    }

    if (ruleConfigsByScope.isEmpty()) {
      return CustomSignatureConfigContext.getDefaultInstance();
    }

    CustomSignatureConfigContext.Builder builder = CustomSignatureConfigContext.newBuilder();
    for (Map.Entry<ScopeContext, List<CustomSignatureRuleConfig>> entry :
        ruleConfigsByScope.entrySet()) {
      CustomSignatureRulesContext rulesContext =
          CustomSignatureRulesContext.newBuilder()
              .setScopeContext(entry.getKey())
              .addAllRuleConfigs(entry.getValue())
              .build();
      builder.addRuleContexts(rulesContext);
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

      builder.addScopes(Scope.newBuilder().setEntityScope(entityScopeBuilder.build()).build());
    } else {
      builder.addScopes(
          Scope.newBuilder().setCustomerScope(CustomerScope.getDefaultInstance()).build());
    }

    return builder.build();
  }

  private CustomSignatureRuleConfig convertRule(
      AiAppCustomRule rule, DataClassificationInfo dataClassificationInfo) {

    if (!rule.hasRuleData()) {
      log.debug("Rule {} has no rule data, skipping", rule.getRuleId());
      return null;
    }

    PiiDetectedInPromptRuleData piiData = rule.getRuleData().getPiiDetectedInPromptRuleData();
    if (!piiData.hasDatatypeCondition()) {
      log.debug("Rule {} has no datatype condition, skipping", rule.getRuleId());
      return null;
    }

    DatatypeCondition datatypeCondition = piiData.getDatatypeCondition();

    // Resolve datatype IDs using datatype→dataset inverse mapping
    Set<String> datatypeIds = resolveDatatypeIds(datatypeCondition, dataClassificationInfo);

    if (datatypeIds.isEmpty()) {
      log.debug("Rule {} has no resolvable datatype IDs, skipping", rule.getRuleId());
      return null;
    }

    // Build the datatype match rule definition
    List<CustomSignatureRuleDefinition> ruleDefinitions = new ArrayList<>();
    ruleDefinitions.add(
        buildDatatypeMatchDefinition(rule.getRuleId(), datatypeCondition, datatypeIds));

    // Convert scope conditions to additional rule definitions
    for (ScopeCondition scopeCondition : piiData.getScopeConditionsList()) {
      CustomSignatureRuleDefinition scopeDef = convertScopeCondition(scopeCondition);
      if (scopeDef != null) {
        ruleDefinitions.add(scopeDef);
      }
    }

    // Build CustomSignatureRuleDefinitionGroup
    CustomSignatureRuleDefinitionGroup ruleDefinitionGroup =
        CustomSignatureRuleDefinitionGroup.newBuilder()
            .setOperator(LogicalOperator.LOGICAL_OPERATOR_AND)
            .addAllRuleDefinitions(ruleDefinitions)
            .build();

    // Build CustomSignatureRuleConfig
    return CustomSignatureRuleConfig.newBuilder()
        .setId(rule.getRuleId())
        .setName(rule.getRuleData().getRuleName())
        .setDescription(rule.getRuleData().getDescription())
        .setRuleDefinitionGroup(ruleDefinitionGroup)
        .build();
  }

  private CustomSignatureRuleDefinition buildDatatypeMatchDefinition(
      String ruleId, DatatypeCondition datatypeCondition, Set<String> datatypeIds) {

    // Build DataTypeMatchOperation - always set keyValueRegexCombineMatch to true
    DataTypeMatchOperation.Builder dataTypeMatchOp =
        DataTypeMatchOperation.newBuilder()
            .addAllDataTypeIds(datatypeIds)
            .setKeyValueRegexCombineMatch(true);

    // Build KeyValueMatchCondition with lhs_key_operand always set to request.body_param prefix
    KeyValueMatchCondition.Builder kvConditionBuilder = KeyValueMatchCondition.newBuilder();

    String bodyParamPrefix =
        PrefixBuilder.buildAppendablePrefix(
            MessageType.MESSAGE_TYPE_REQUEST, AttributeType.ATTRIBUTE_TYPE_BODY_PARAM);

    KeyMatchOperand.Builder keyOperandBuilder =
        KeyMatchOperand.newBuilder()
            .setKeyMetadata(
                KeyMatchOperand.KeyMetadata.newBuilder()
                    .setFullyQualifiedKeyPrefix(bodyParamPrefix)
                    .build());

    // When request_body_custom_location_condition is present, set key_match_operation
    if (datatypeCondition.hasRequestBodyCustomLocationCondition()) {
      MatchOperatorCondition locationCondition =
          datatypeCondition.getRequestBodyCustomLocationCondition();
      if (locationCondition.hasValue()) {
        String keyValue = locationCondition.getValue().getStringValue();
        keyOperandBuilder.setKeyMatchOperation(
            ValueMatchOperation.newBuilder()
                .setStringMatchOperation(
                    StringMatchOperation.newBuilder()
                        .setStringOperator(convertMatchOperator(locationCondition.getOperator()))
                        .setStringValue(keyValue)
                        .build())
                .build());
      }
    }

    kvConditionBuilder.setLhsKeyOperand(keyOperandBuilder.build());

    // Build the match condition chain
    ValueMatchOperation valueMatchOp =
        ValueMatchOperation.newBuilder().setDataTypeMatchOperation(dataTypeMatchOp.build()).build();

    KeyValueMatchCondition kvCondition =
        kvConditionBuilder.setRhsValueMatchOperation(valueMatchOp).build();

    LeafMatchConditionExpression leafExpr =
        LeafMatchConditionExpression.newBuilder().setKeyValueMatchCondition(kvCondition).build();

    MatchConditionExpression matchExpr =
        MatchConditionExpression.newBuilder().setLeafMatchConditionExpression(leafExpr).build();

    String evaluationIdentifier = "pii-rule-" + ruleId;

    CustomSignatureConditionExpression condExpr =
        CustomSignatureConditionExpression.newBuilder()
            .setConditionExpressionEvaluationIdentifier(evaluationIdentifier)
            .setConditionExpression(matchExpr)
            .build();

    return CustomSignatureRuleDefinition.newBuilder()
        .setCustomSignatureConditionExpression(condExpr)
        .build();
  }

  private CustomSignatureRuleDefinition convertScopeCondition(ScopeCondition scopeCondition) {
    switch (scopeCondition.getScopeCase()) {
      case ENTITY_SCOPE:
        return convertEntityScopeCondition(scopeCondition.getEntityScope());
      case URL_SCOPE:
        return convertUrlScopeCondition(scopeCondition.getUrlScope());
      default:
        log.debug("Unsupported scope condition type: {}", scopeCondition.getScopeCase());
        return null;
    }
  }

  private CustomSignatureRuleDefinition convertEntityScopeCondition(
      ScopeCondition.EntityScope entityScope) {
    if (entityScope.getEntityIdsList().isEmpty()) {
      return null;
    }

    String evaluationIdentifier = "scope-entity-" + UUID.randomUUID().toString().substring(0, 8);

    switch (entityScope.getEntityType()) {
      case ENTITY_TYPE_API:
        String apiKeyPrefix =
            PrefixBuilder.buildAppendablePrefix(
                MessageType.MESSAGE_TYPE_REQUEST,
                ScopeType.SCOPE_TYPE_API,
                ScopeAttributeType.SCOPE_ATTRIBUTE_TYPE_ID);
        MatchConditionExpression apiCondition =
            buildStringListAnyEqualsCondition(apiKeyPrefix, entityScope.getEntityIdsList());
        return buildRuleDefinition(evaluationIdentifier, apiCondition);
      default:
        log.debug("Unsupported entity type: {}", entityScope.getEntityType());
        return null;
    }
  }

  private CustomSignatureRuleDefinition convertUrlScopeCondition(ScopeCondition.UrlScope urlScope) {
    if (urlScope.getUrlRegexesList().isEmpty()) {
      return null;
    }

    String evaluationIdentifier = "scope-url-" + UUID.randomUUID().toString().substring(0, 8);
    String keyPrefix =
        PrefixBuilder.buildAppendablePrefix(
            MessageType.MESSAGE_TYPE_REQUEST, AttributeType.ATTRIBUTE_TYPE_URL);
    String joinedRegex = String.join("|", urlScope.getUrlRegexesList());
    MatchConditionExpression urlCondition = buildStringLikeCondition(keyPrefix, joinedRegex);
    return buildRuleDefinition(evaluationIdentifier, urlCondition);
  }

  private CustomSignatureRuleDefinition buildRuleDefinition(
      String evaluationIdentifier, MatchConditionExpression conditionExpression) {
    CustomSignatureConditionExpression condExpr =
        CustomSignatureConditionExpression.newBuilder()
            .setConditionExpressionEvaluationIdentifier(evaluationIdentifier)
            .setConditionExpression(conditionExpression)
            .build();
    return CustomSignatureRuleDefinition.newBuilder()
        .setCustomSignatureConditionExpression(condExpr)
        .build();
  }

  private MatchConditionExpression buildStringListAnyEqualsCondition(
      String keyPrefix, List<String> stringValues) {
    ValueMatchOperation valueOperation =
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
            .build();

    KeyMatchOperand keyOperand =
        KeyMatchOperand.newBuilder()
            .setKeyMetadata(
                KeyMatchOperand.KeyMetadata.newBuilder()
                    .setFullyQualifiedKeyPrefix(keyPrefix)
                    .build())
            .setKeyMatchOperation(valueOperation)
            .build();

    UnaryKeyMatchCondition unaryKeyMatchCondition =
        UnaryKeyMatchCondition.newBuilder().setKeyCondition(keyOperand).build();

    LeafMatchConditionExpression leafExpr =
        LeafMatchConditionExpression.newBuilder()
            .setUnaryKeyMatchCondition(unaryKeyMatchCondition)
            .build();

    return MatchConditionExpression.newBuilder().setLeafMatchConditionExpression(leafExpr).build();
  }

  private MatchConditionExpression buildStringLikeCondition(String keyPrefix, String regex) {
    UnaryKeyMatchCondition unaryKeyCondition =
        UnaryKeyMatchCondition.newBuilder()
            .setKeyCondition(
                KeyMatchOperand.newBuilder()
                    .setKeyMetadata(
                        KeyMatchOperand.KeyMetadata.newBuilder()
                            .setFullyQualifiedKeyPrefix(keyPrefix)
                            .build())
                    .setKeyMatchOperation(
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
                    .build())
            .build();

    LeafMatchConditionExpression leafExpr =
        LeafMatchConditionExpression.newBuilder()
            .setUnaryKeyMatchCondition(unaryKeyCondition)
            .build();

    return MatchConditionExpression.newBuilder().setLeafMatchConditionExpression(leafExpr).build();
  }

  private Set<String> resolveDatatypeIds(
      DatatypeCondition datatypeCondition, DataClassificationInfo dataClassificationInfo) {

    Set<String> datatypeIds = new HashSet<>();

    // Resolve dataset IDs to datatype IDs using datatype→dataset inverse mapping
    List<String> targetDatasetIds = datatypeCondition.getDatasetIdsList();
    if (!targetDatasetIds.isEmpty()) {
      for (Map.Entry<String, DataType> entry :
          dataClassificationInfo.getDataTypeIdToDataTypeMap().entrySet()) {
        DataType dataType = entry.getValue();
        if (dataType.hasRule() && isContainedByTargetDatasets(dataType, targetDatasetIds)) {
          datatypeIds.add(entry.getKey());
        }
      }
    }

    // Add directly specified datatype IDs
    datatypeIds.addAll(datatypeCondition.getDatatypeIdsList());

    return datatypeIds;
  }

  private boolean isContainedByTargetDatasets(DataType dataType, List<String> targetDatasetIds) {
    return dataType.getRule().getDataSetIdList().stream().anyMatch(targetDatasetIds::contains);
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
}
