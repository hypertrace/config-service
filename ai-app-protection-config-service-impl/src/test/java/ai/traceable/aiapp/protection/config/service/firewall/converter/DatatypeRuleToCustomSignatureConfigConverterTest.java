package ai.traceable.aiapp.protection.config.service.firewall.converter;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import ai.traceable.aiapp.protection.config.service.v1.AiAppCustomRule;
import ai.traceable.aiapp.protection.config.service.v1.AiAppCustomRuleData;
import ai.traceable.aiapp.protection.config.service.v1.DatatypeCondition;
import ai.traceable.aiapp.protection.config.service.v1.EnvironmentScope;
import ai.traceable.aiapp.protection.config.service.v1.MatchOperator;
import ai.traceable.aiapp.protection.config.service.v1.MatchOperatorCondition;
import ai.traceable.aiapp.protection.config.service.v1.PiiDetectedInPromptRuleData;
import ai.traceable.aiapp.protection.config.service.v1.RuleScope;
import ai.traceable.aiapp.protection.config.service.v1.ScopeCondition;
import ai.traceable.aiapp.protection.config.service.v1.TenantScope;
import ai.traceable.data.classification.cache.info.DataClassificationInfo;
import ai.traceable.data.classification.config.service.v1.DataSet;
import ai.traceable.data.classification.config.service.v1.DataSetInfo;
import ai.traceable.data.classification.config.service.v1.DataType;
import ai.traceable.data.classification.config.service.v1.DataTypeRule;
import ai.traceable.protection.engine.config.customsignature.v1.CustomSignatureConfigContext;
import ai.traceable.protection.engine.config.customsignature.v1.CustomSignatureRuleConfig;
import ai.traceable.protection.engine.config.customsignature.v1.CustomSignatureRuleDefinition;
import ai.traceable.protection.engine.config.customsignature.v1.CustomSignatureRuleDefinitionGroup;
import ai.traceable.protection.engine.config.customsignature.v1.CustomSignatureRulesContext;
import ai.traceable.protection.processing.common.v1.AttributeType;
import ai.traceable.protection.processing.common.v1.EntityType;
import ai.traceable.protection.processing.common.v1.LogicalOperator;
import ai.traceable.protection.processing.common.v1.MessageType;
import ai.traceable.protection.processing.common.v1.ScopeContext;
import ai.traceable.protection.processing.common.v1.StringOperator;
import ai.traceable.protection.processing.common.v1.utils.PrefixBuilder;
import ai.traceable.protection.processor.condition.expression.v1.KeyValueMatchCondition;
import ai.traceable.protection.processor.condition.expression.v1.MatchConditionExpression;
import com.google.protobuf.Value;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class DatatypeRuleToCustomSignatureConfigConverterTest {

  private DatatypeRuleToCustomSignatureConfigConverter converter;
  private DataClassificationInfo dataClassificationInfo;

  @BeforeEach
  void setUp() {
    converter = new DatatypeRuleToCustomSignatureConfigConverter();

    // Setup test data classification info using datatype→dataset inverse mapping
    DataType dataType1 =
        DataType.newBuilder()
            .setId("datatype-1")
            .setRule(DataTypeRule.newBuilder().addAllDataSetId(List.of("dataset-1")).build())
            .build();
    DataType dataType2 =
        DataType.newBuilder()
            .setId("datatype-2")
            .setRule(DataTypeRule.newBuilder().addAllDataSetId(List.of("dataset-1")).build())
            .build();
    DataType dataType3 =
        DataType.newBuilder()
            .setId("datatype-3")
            .setRule(DataTypeRule.newBuilder().addAllDataSetId(List.of("dataset-2")).build())
            .build();

    DataSet dataSet1 =
        DataSet.newBuilder()
            .setInfo(
                DataSetInfo.newBuilder()
                    .addAllDataTypeIds(List.of("datatype-1", "datatype-2"))
                    .build())
            .build();

    dataClassificationInfo =
        new DataClassificationInfo(
            Map.of(
                "datatype-1", dataType1,
                "datatype-2", dataType2,
                "datatype-3", dataType3),
            Map.of("dataset-1", dataSet1));
  }

  @Test
  void testEmptyRulesList_ReturnsEmptyContext() {
    CustomSignatureConfigContext result = converter.convert(List.of(), dataClassificationInfo);

    assertNotNull(result);
    assertEquals(0, result.getRuleContextsCount());
  }

  @Test
  void testSingleRuleWithDatatypeIds_BuildsCorrectRuleConfig() {
    AiAppCustomRule rule =
        AiAppCustomRule.newBuilder()
            .setRuleId("rule-1")
            .setRuleData(
                AiAppCustomRuleData.newBuilder()
                    .setRuleName("Test PII Rule")
                    .setDescription("Test description")
                    .setPiiDetectedInPromptRuleData(
                        PiiDetectedInPromptRuleData.newBuilder()
                            .setDatatypeCondition(
                                DatatypeCondition.newBuilder()
                                    .addAllDatatypeIds(List.of("datatype-1", "datatype-2"))
                                    .build())
                            .build())
                    .build())
            .build();

    CustomSignatureConfigContext result = converter.convert(List.of(rule), dataClassificationInfo);

    assertNotNull(result);
    assertEquals(1, result.getRuleContextsCount());

    CustomSignatureRulesContext rulesContext = result.getRuleContexts(0);
    assertNotNull(rulesContext.getScopeContext());
    assertEquals(1, rulesContext.getRuleConfigsCount());

    CustomSignatureRuleConfig ruleConfig = rulesContext.getRuleConfigs(0);
    assertEquals("rule-1", ruleConfig.getId());
    assertEquals("Test PII Rule", ruleConfig.getName());
    assertEquals("Test description", ruleConfig.getDescription());

    CustomSignatureRuleDefinitionGroup group = ruleConfig.getRuleDefinitionGroup();
    assertEquals(LogicalOperator.LOGICAL_OPERATOR_AND, group.getOperator());
    assertEquals(1, group.getRuleDefinitionsCount());

    CustomSignatureRuleDefinition definition = group.getRuleDefinitions(0);
    assertTrue(definition.hasCustomSignatureConditionExpression());
    assertTrue(definition.getCustomSignatureConditionExpression().hasConditionExpression());

    MatchConditionExpression matchExpr =
        definition.getCustomSignatureConditionExpression().getConditionExpression();
    assertTrue(matchExpr.hasLeafMatchConditionExpression());
    assertTrue(matchExpr.getLeafMatchConditionExpression().hasKeyValueMatchCondition());

    KeyValueMatchCondition kvCondition =
        matchExpr.getLeafMatchConditionExpression().getKeyValueMatchCondition();
    assertTrue(kvCondition.hasRhsValueMatchOperation());
    assertTrue(kvCondition.getRhsValueMatchOperation().hasDataTypeMatchOperation());

    // Verify lhs_key_operand always has request.body_param prefix
    assertTrue(kvCondition.hasLhsKeyOperand());
    String expectedPrefix =
        PrefixBuilder.buildAppendablePrefix(
            MessageType.MESSAGE_TYPE_REQUEST, AttributeType.ATTRIBUTE_TYPE_BODY_PARAM);
    assertEquals(
        expectedPrefix,
        kvCondition.getLhsKeyOperand().getKeyMetadata().getFullyQualifiedKeyPrefix());

    // Verify keyValueRegexCombineMatch is always true
    assertTrue(
        kvCondition
            .getRhsValueMatchOperation()
            .getDataTypeMatchOperation()
            .getKeyValueRegexCombineMatch());

    List<String> datatypeIds =
        kvCondition.getRhsValueMatchOperation().getDataTypeMatchOperation().getDataTypeIdsList();
    assertEquals(2, datatypeIds.size());
    assertTrue(datatypeIds.contains("datatype-1"));
    assertTrue(datatypeIds.contains("datatype-2"));
  }

  @Test
  void testDatasetIdsResolvedToDatatypeIds() {
    AiAppCustomRule rule =
        AiAppCustomRule.newBuilder()
            .setRuleId("rule-2")
            .setRuleData(
                AiAppCustomRuleData.newBuilder()
                    .setRuleName("Dataset Resolution Rule")
                    .setDescription("Test dataset resolution")
                    .setPiiDetectedInPromptRuleData(
                        PiiDetectedInPromptRuleData.newBuilder()
                            .setDatatypeCondition(
                                DatatypeCondition.newBuilder()
                                    .addAllDatasetIds(List.of("dataset-1"))
                                    .addAllDatatypeIds(List.of("datatype-3"))
                                    .build())
                            .build())
                    .build())
            .build();

    CustomSignatureConfigContext result = converter.convert(List.of(rule), dataClassificationInfo);

    assertNotNull(result);
    assertEquals(1, result.getRuleContextsCount());

    CustomSignatureRulesContext rulesContext = result.getRuleContexts(0);
    CustomSignatureRuleConfig ruleConfig = rulesContext.getRuleConfigs(0);

    List<String> datatypeIds =
        ruleConfig
            .getRuleDefinitionGroup()
            .getRuleDefinitions(0)
            .getCustomSignatureConditionExpression()
            .getConditionExpression()
            .getLeafMatchConditionExpression()
            .getKeyValueMatchCondition()
            .getRhsValueMatchOperation()
            .getDataTypeMatchOperation()
            .getDataTypeIdsList();

    // Should contain datatype-1 and datatype-2 (resolved from dataset-1 via inverse mapping),
    // and datatype-3 directly
    assertEquals(3, datatypeIds.size());
    assertTrue(datatypeIds.contains("datatype-1"));
    assertTrue(datatypeIds.contains("datatype-2"));
    assertTrue(datatypeIds.contains("datatype-3"));
  }

  @Test
  void testDuplicateDatatypeIdsAreDeduplicated() {
    AiAppCustomRule rule =
        AiAppCustomRule.newBuilder()
            .setRuleId("rule-3")
            .setRuleData(
                AiAppCustomRuleData.newBuilder()
                    .setRuleName("Deduplication Rule")
                    .setDescription("Test deduplication")
                    .setPiiDetectedInPromptRuleData(
                        PiiDetectedInPromptRuleData.newBuilder()
                            .setDatatypeCondition(
                                DatatypeCondition.newBuilder()
                                    .addAllDatasetIds(
                                        List.of("dataset-1")) // Resolves to datatype-1, datatype-2
                                    .addAllDatatypeIds(
                                        List.of("datatype-1", "datatype-2")) // Duplicates
                                    .build())
                            .build())
                    .build())
            .build();

    CustomSignatureConfigContext result = converter.convert(List.of(rule), dataClassificationInfo);

    assertNotNull(result);
    assertEquals(1, result.getRuleContextsCount());

    List<String> datatypeIds =
        result
            .getRuleContexts(0)
            .getRuleConfigs(0)
            .getRuleDefinitionGroup()
            .getRuleDefinitions(0)
            .getCustomSignatureConditionExpression()
            .getConditionExpression()
            .getLeafMatchConditionExpression()
            .getKeyValueMatchCondition()
            .getRhsValueMatchOperation()
            .getDataTypeMatchOperation()
            .getDataTypeIdsList();

    // Should only have 2 unique datatype IDs
    assertEquals(2, datatypeIds.size());
    assertTrue(datatypeIds.contains("datatype-1"));
    assertTrue(datatypeIds.contains("datatype-2"));
  }

  @Test
  void testCustomLocationCondition_SetsKeyValueMatchConditionWithKeyOperand() {
    String locationRegex = "user\\.profile\\..*";
    AiAppCustomRule rule =
        AiAppCustomRule.newBuilder()
            .setRuleId("rule-4")
            .setRuleData(
                AiAppCustomRuleData.newBuilder()
                    .setRuleName("Custom Location Rule")
                    .setDescription("Test custom location")
                    .setPiiDetectedInPromptRuleData(
                        PiiDetectedInPromptRuleData.newBuilder()
                            .setDatatypeCondition(
                                DatatypeCondition.newBuilder()
                                    .addAllDatatypeIds(List.of("datatype-1"))
                                    .setRequestBodyCustomLocationCondition(
                                        MatchOperatorCondition.newBuilder()
                                            .setOperator(MatchOperator.MATCH_OPERATOR_MATCHES_REGEX)
                                            .setValue(
                                                Value.newBuilder()
                                                    .setStringValue(locationRegex)
                                                    .build())
                                            .build())
                                    .build())
                            .build())
                    .build())
            .build();

    CustomSignatureConfigContext result = converter.convert(List.of(rule), dataClassificationInfo);

    assertNotNull(result);
    assertEquals(1, result.getRuleContextsCount());

    KeyValueMatchCondition kvCondition =
        result
            .getRuleContexts(0)
            .getRuleConfigs(0)
            .getRuleDefinitionGroup()
            .getRuleDefinitions(0)
            .getCustomSignatureConditionExpression()
            .getConditionExpression()
            .getLeafMatchConditionExpression()
            .getKeyValueMatchCondition();

    // Verify lhs_key_operand has request.body_param prefix
    assertTrue(kvCondition.hasLhsKeyOperand());
    String expectedPrefix =
        PrefixBuilder.buildAppendablePrefix(
            MessageType.MESSAGE_TYPE_REQUEST, AttributeType.ATTRIBUTE_TYPE_BODY_PARAM);
    assertEquals(
        expectedPrefix,
        kvCondition.getLhsKeyOperand().getKeyMetadata().getFullyQualifiedKeyPrefix());

    // Verify key_match_operation uses the location condition's operator and value
    assertTrue(kvCondition.getLhsKeyOperand().hasKeyMatchOperation());
    assertEquals(
        locationRegex,
        kvCondition
            .getLhsKeyOperand()
            .getKeyMatchOperation()
            .getStringMatchOperation()
            .getStringValue());
    assertEquals(
        StringOperator.StringMatchOperator.STRING_MATCH_OPERATOR_LIKE,
        kvCondition
            .getLhsKeyOperand()
            .getKeyMatchOperation()
            .getStringMatchOperation()
            .getStringOperator()
            .getStringOperator());

    // Verify key_value_regex_combine_match is always set on DataTypeMatchOperation
    assertTrue(
        kvCondition
            .getRhsValueMatchOperation()
            .getDataTypeMatchOperation()
            .getKeyValueRegexCombineMatch());
  }

  @Test
  void testRuleWithNoResolvableDatatypeIds_IsSkipped() {
    AiAppCustomRule ruleWithNoDataTypes =
        AiAppCustomRule.newBuilder()
            .setRuleId("rule-skip")
            .setRuleData(
                AiAppCustomRuleData.newBuilder()
                    .setRuleName("Skip Rule")
                    .setDescription("Should be skipped")
                    .setPiiDetectedInPromptRuleData(
                        PiiDetectedInPromptRuleData.newBuilder()
                            .setDatatypeCondition(
                                DatatypeCondition.newBuilder()
                                    .addAllDatasetIds(List.of("non-existent-dataset"))
                                    .build())
                            .build())
                    .build())
            .build();

    AiAppCustomRule validRule =
        AiAppCustomRule.newBuilder()
            .setRuleId("rule-valid")
            .setRuleData(
                AiAppCustomRuleData.newBuilder()
                    .setRuleName("Valid Rule")
                    .setDescription("Should not be skipped")
                    .setPiiDetectedInPromptRuleData(
                        PiiDetectedInPromptRuleData.newBuilder()
                            .setDatatypeCondition(
                                DatatypeCondition.newBuilder()
                                    .addAllDatatypeIds(List.of("datatype-1"))
                                    .build())
                            .build())
                    .build())
            .build();

    CustomSignatureConfigContext result =
        converter.convert(List.of(ruleWithNoDataTypes, validRule), dataClassificationInfo);

    assertNotNull(result);
    assertEquals(1, result.getRuleContextsCount());

    // Only the valid rule should be present
    CustomSignatureRulesContext rulesContext = result.getRuleContexts(0);
    assertEquals(1, rulesContext.getRuleConfigsCount());
    assertEquals("rule-valid", rulesContext.getRuleConfigs(0).getId());
  }

  @Test
  void testAllRulesSkipped_ReturnsEmptyContext() {
    AiAppCustomRule rule1 =
        AiAppCustomRule.newBuilder()
            .setRuleId("rule-skip-1")
            .setRuleData(
                AiAppCustomRuleData.newBuilder()
                    .setRuleName("Skip Rule 1")
                    .setPiiDetectedInPromptRuleData(
                        PiiDetectedInPromptRuleData.newBuilder()
                            .setDatatypeCondition(DatatypeCondition.newBuilder().build())
                            .build())
                    .build())
            .build();

    AiAppCustomRule rule2 =
        AiAppCustomRule.newBuilder()
            .setRuleId("rule-skip-2")
            .setRuleData(
                AiAppCustomRuleData.newBuilder()
                    .setRuleName("Skip Rule 2")
                    .setPiiDetectedInPromptRuleData(
                        PiiDetectedInPromptRuleData.newBuilder()
                            .setDatatypeCondition(
                                DatatypeCondition.newBuilder()
                                    .addAllDatasetIds(List.of("non-existent"))
                                    .build())
                            .build())
                    .build())
            .build();

    CustomSignatureConfigContext result =
        converter.convert(List.of(rule1, rule2), dataClassificationInfo);

    assertNotNull(result);
    assertEquals(0, result.getRuleContextsCount());
  }

  @Test
  void testScopeContextBuiltFromRuleScope() {
    AiAppCustomRule rule =
        AiAppCustomRule.newBuilder()
            .setRuleId("rule-scoped")
            .setRuleData(
                AiAppCustomRuleData.newBuilder()
                    .setRuleName("Scoped Rule")
                    .setDescription("Rule with environment scope")
                    .setRuleScope(
                        RuleScope.newBuilder()
                            .setEnvironmentScope(
                                EnvironmentScope.newBuilder()
                                    .addAllEnvironmentIds(List.of("env-1", "env-2"))
                                    .build())
                            .build())
                    .setPiiDetectedInPromptRuleData(
                        PiiDetectedInPromptRuleData.newBuilder()
                            .setDatatypeCondition(
                                DatatypeCondition.newBuilder()
                                    .addAllDatatypeIds(List.of("datatype-1"))
                                    .build())
                            .build())
                    .build())
            .build();

    CustomSignatureConfigContext result = converter.convert(List.of(rule), dataClassificationInfo);

    assertNotNull(result);
    assertEquals(1, result.getRuleContextsCount());

    ScopeContext scopeContext = result.getRuleContexts(0).getScopeContext();
    assertEquals(1, scopeContext.getScopesCount());
    assertEquals(
        EntityType.ENTITY_TYPE_ENVIRONMENT,
        scopeContext.getScopes(0).getEntityScope().getEntityType());
    assertEquals(2, scopeContext.getScopes(0).getEntityScope().getEntitiesCount());
    assertEquals("env-1", scopeContext.getScopes(0).getEntityScope().getEntities(0).getId());
    assertEquals("env-2", scopeContext.getScopes(0).getEntityScope().getEntities(1).getId());
  }

  @Test
  void testScopeConditions_EntityScope_AddsRuleDefinition() {
    AiAppCustomRule rule =
        AiAppCustomRule.newBuilder()
            .setRuleId("rule-entity-scope")
            .setRuleData(
                AiAppCustomRuleData.newBuilder()
                    .setRuleName("Entity Scoped Rule")
                    .setDescription("Rule with entity scope condition")
                    .setPiiDetectedInPromptRuleData(
                        PiiDetectedInPromptRuleData.newBuilder()
                            .setDatatypeCondition(
                                DatatypeCondition.newBuilder()
                                    .addAllDatatypeIds(List.of("datatype-1"))
                                    .build())
                            .addScopeConditions(
                                ScopeCondition.newBuilder()
                                    .setEntityScope(
                                        ScopeCondition.EntityScope.newBuilder()
                                            .setEntityType(
                                                ScopeCondition.EntityType.ENTITY_TYPE_API)
                                            .addAllEntityIds(List.of("api-1", "api-2"))
                                            .build())
                                    .build())
                            .build())
                    .build())
            .build();

    CustomSignatureConfigContext result = converter.convert(List.of(rule), dataClassificationInfo);

    assertNotNull(result);
    assertEquals(1, result.getRuleContextsCount());

    CustomSignatureRuleDefinitionGroup group =
        result.getRuleContexts(0).getRuleConfigs(0).getRuleDefinitionGroup();

    // Should have 2 definitions: datatype match + entity scope condition
    assertEquals(2, group.getRuleDefinitionsCount());
    assertEquals(LogicalOperator.LOGICAL_OPERATOR_AND, group.getOperator());

    // First definition is the datatype match
    assertTrue(group.getRuleDefinitions(0).hasCustomSignatureConditionExpression());

    // Second definition is the entity scope condition
    CustomSignatureRuleDefinition scopeDef = group.getRuleDefinitions(1);
    assertTrue(scopeDef.hasCustomSignatureConditionExpression());
    assertTrue(
        scopeDef
            .getCustomSignatureConditionExpression()
            .getConditionExpressionEvaluationIdentifier()
            .startsWith("scope-entity-"));
  }

  @Test
  void testScopeConditions_UrlScope_AddsRuleDefinition() {
    AiAppCustomRule rule =
        AiAppCustomRule.newBuilder()
            .setRuleId("rule-url-scope")
            .setRuleData(
                AiAppCustomRuleData.newBuilder()
                    .setRuleName("URL Scoped Rule")
                    .setDescription("Rule with URL scope condition")
                    .setPiiDetectedInPromptRuleData(
                        PiiDetectedInPromptRuleData.newBuilder()
                            .setDatatypeCondition(
                                DatatypeCondition.newBuilder()
                                    .addAllDatatypeIds(List.of("datatype-1"))
                                    .build())
                            .addScopeConditions(
                                ScopeCondition.newBuilder()
                                    .setUrlScope(
                                        ScopeCondition.UrlScope.newBuilder()
                                            .addAllUrlRegexes(List.of("/api/v1/.*", "/api/v2/.*"))
                                            .build())
                                    .build())
                            .build())
                    .build())
            .build();

    CustomSignatureConfigContext result = converter.convert(List.of(rule), dataClassificationInfo);

    assertNotNull(result);
    assertEquals(1, result.getRuleContextsCount());

    CustomSignatureRuleDefinitionGroup group =
        result.getRuleContexts(0).getRuleConfigs(0).getRuleDefinitionGroup();

    // Should have 2 definitions: datatype match + URL scope condition
    assertEquals(2, group.getRuleDefinitionsCount());

    // Second definition is the URL scope condition
    CustomSignatureRuleDefinition scopeDef = group.getRuleDefinitions(1);
    assertTrue(scopeDef.hasCustomSignatureConditionExpression());
    assertTrue(
        scopeDef
            .getCustomSignatureConditionExpression()
            .getConditionExpressionEvaluationIdentifier()
            .startsWith("scope-url-"));
  }

  @Test
  void testKeyValueRegexCombineMatchAlwaysTrue() {
    // Test that keyValueRegexCombineMatch is true even without custom location condition
    AiAppCustomRule rule =
        AiAppCustomRule.newBuilder()
            .setRuleId("rule-no-location")
            .setRuleData(
                AiAppCustomRuleData.newBuilder()
                    .setRuleName("No Location Rule")
                    .setDescription("Rule without custom location")
                    .setPiiDetectedInPromptRuleData(
                        PiiDetectedInPromptRuleData.newBuilder()
                            .setDatatypeCondition(
                                DatatypeCondition.newBuilder()
                                    .addAllDatatypeIds(List.of("datatype-1"))
                                    .build())
                            .build())
                    .build())
            .build();

    CustomSignatureConfigContext result = converter.convert(List.of(rule), dataClassificationInfo);

    KeyValueMatchCondition kvCondition =
        result
            .getRuleContexts(0)
            .getRuleConfigs(0)
            .getRuleDefinitionGroup()
            .getRuleDefinitions(0)
            .getCustomSignatureConditionExpression()
            .getConditionExpression()
            .getLeafMatchConditionExpression()
            .getKeyValueMatchCondition();

    assertTrue(
        kvCondition
            .getRhsValueMatchOperation()
            .getDataTypeMatchOperation()
            .getKeyValueRegexCombineMatch());
  }

  @Test
  void testScopeContextCustomerScopeWhenNoEnvironmentScope() {
    AiAppCustomRule rule =
        AiAppCustomRule.newBuilder()
            .setRuleId("rule-tenant")
            .setRuleData(
                AiAppCustomRuleData.newBuilder()
                    .setRuleName("Tenant Rule")
                    .setDescription("Rule with tenant scope")
                    .setRuleScope(
                        RuleScope.newBuilder()
                            .setTenantScope(TenantScope.getDefaultInstance())
                            .build())
                    .setPiiDetectedInPromptRuleData(
                        PiiDetectedInPromptRuleData.newBuilder()
                            .setDatatypeCondition(
                                DatatypeCondition.newBuilder()
                                    .addAllDatatypeIds(List.of("datatype-1"))
                                    .build())
                            .build())
                    .build())
            .build();

    CustomSignatureConfigContext result = converter.convert(List.of(rule), dataClassificationInfo);

    assertNotNull(result);
    assertEquals(1, result.getRuleContextsCount());

    ScopeContext scopeContext = result.getRuleContexts(0).getScopeContext();
    assertEquals(1, scopeContext.getScopesCount());
    assertTrue(scopeContext.getScopes(0).hasCustomerScope());
  }

  @Test
  void convert_aiSensitiveDataProtection_singleRequestCondition_buildsRuleConfig() {
    AiAppCustomRule rule =
        AiAppCustomRule.newBuilder()
            .setRuleId("rule-ai-sdp-req")
            .setRuleData(
                AiAppCustomRuleData.newBuilder()
                    .setRuleName("AI Sensitive Data Protection Request")
                    .setDescription("Test AI SDP with request body location")
                    .setAiSensitiveDataProtectionRuleData(
                        ai.traceable.aiapp.protection.config.service.v1
                            .AiSensitiveDataProtectionRuleData.newBuilder()
                            .addDatatypeConditions(
                                DatatypeCondition.newBuilder()
                                    .addAllDatatypeIds(List.of("datatype-1"))
                                    .setRequestBodyCustomLocationCondition(
                                        MatchOperatorCondition.newBuilder()
                                            .setOperator(MatchOperator.MATCH_OPERATOR_EQUALS)
                                            .setValue(
                                                Value.newBuilder()
                                                    .setStringValue("$.body.field")
                                                    .build())
                                            .build())
                                    .build())
                            .build())
                    .build())
            .build();

    CustomSignatureConfigContext result = converter.convert(List.of(rule), dataClassificationInfo);

    assertNotNull(result);
    assertEquals(1, result.getRuleContextsCount());

    CustomSignatureRulesContext rulesContext = result.getRuleContexts(0);
    assertEquals(1, rulesContext.getRuleConfigsCount());

    CustomSignatureRuleConfig ruleConfig = rulesContext.getRuleConfigs(0);
    assertEquals("rule-ai-sdp-req", ruleConfig.getId());

    CustomSignatureRuleDefinitionGroup group = ruleConfig.getRuleDefinitionGroup();
    assertEquals(1, group.getRuleDefinitionsCount());

    CustomSignatureRuleDefinition definition = group.getRuleDefinitions(0);
    assertTrue(definition.hasCustomSignatureConditionExpression());

    String evaluationIdentifier =
        definition
            .getCustomSignatureConditionExpression()
            .getConditionExpressionEvaluationIdentifier();
    assertTrue(
        evaluationIdentifier.startsWith("datatype-rule-"),
        "Expected identifier to start with 'datatype-rule-' but got: " + evaluationIdentifier);

    KeyValueMatchCondition kvCondition =
        definition
            .getCustomSignatureConditionExpression()
            .getConditionExpression()
            .getLeafMatchConditionExpression()
            .getKeyValueMatchCondition();

    String expectedRequestPrefix =
        PrefixBuilder.buildAppendablePrefix(
            MessageType.MESSAGE_TYPE_REQUEST, AttributeType.ATTRIBUTE_TYPE_BODY_PARAM);
    assertEquals(
        expectedRequestPrefix,
        kvCondition.getLhsKeyOperand().getKeyMetadata().getFullyQualifiedKeyPrefix());
  }

  @Test
  void convert_aiSensitiveDataProtection_responseLocation_usesResponseBodyPrefix() {
    AiAppCustomRule rule =
        AiAppCustomRule.newBuilder()
            .setRuleId("rule-ai-sdp-resp")
            .setRuleData(
                AiAppCustomRuleData.newBuilder()
                    .setRuleName("AI Sensitive Data Protection Response")
                    .setDescription("Test AI SDP with response body location")
                    .setAiSensitiveDataProtectionRuleData(
                        ai.traceable.aiapp.protection.config.service.v1
                            .AiSensitiveDataProtectionRuleData.newBuilder()
                            .addDatatypeConditions(
                                DatatypeCondition.newBuilder()
                                    .addAllDatatypeIds(List.of("datatype-2"))
                                    .setResponseBodyCustomLocationCondition(
                                        MatchOperatorCondition.newBuilder()
                                            .setOperator(MatchOperator.MATCH_OPERATOR_EQUALS)
                                            .setValue(
                                                Value.newBuilder()
                                                    .setStringValue("$.response.data")
                                                    .build())
                                            .build())
                                    .build())
                            .build())
                    .build())
            .build();

    CustomSignatureConfigContext result = converter.convert(List.of(rule), dataClassificationInfo);

    assertNotNull(result);
    assertEquals(1, result.getRuleContextsCount());

    CustomSignatureRulesContext rulesContext = result.getRuleContexts(0);
    assertEquals(1, rulesContext.getRuleConfigsCount());

    CustomSignatureRuleConfig ruleConfig = rulesContext.getRuleConfigs(0);
    assertEquals(1, ruleConfig.getRuleDefinitionGroup().getRuleDefinitionsCount());

    KeyValueMatchCondition kvCondition =
        ruleConfig
            .getRuleDefinitionGroup()
            .getRuleDefinitions(0)
            .getCustomSignatureConditionExpression()
            .getConditionExpression()
            .getLeafMatchConditionExpression()
            .getKeyValueMatchCondition();

    String expectedResponsePrefix =
        PrefixBuilder.buildAppendablePrefix(
            MessageType.MESSAGE_TYPE_RESPONSE, AttributeType.ATTRIBUTE_TYPE_BODY_PARAM);
    assertEquals(
        expectedResponsePrefix,
        kvCondition.getLhsKeyOperand().getKeyMetadata().getFullyQualifiedKeyPrefix());
  }

  @Test
  void convert_aiSensitiveDataProtection_multipleConditions_emitsMultipleDefinitions() {
    AiAppCustomRule rule =
        AiAppCustomRule.newBuilder()
            .setRuleId("rule-ai-sdp-multi")
            .setRuleData(
                AiAppCustomRuleData.newBuilder()
                    .setRuleName("AI Sensitive Data Protection Multiple")
                    .setDescription("Test AI SDP with multiple conditions")
                    .setAiSensitiveDataProtectionRuleData(
                        ai.traceable.aiapp.protection.config.service.v1
                            .AiSensitiveDataProtectionRuleData.newBuilder()
                            .addDatatypeConditions(
                                DatatypeCondition.newBuilder()
                                    .addAllDatatypeIds(List.of("datatype-1"))
                                    .setRequestBodyCustomLocationCondition(
                                        MatchOperatorCondition.newBuilder()
                                            .setOperator(MatchOperator.MATCH_OPERATOR_EQUALS)
                                            .setValue(
                                                Value.newBuilder()
                                                    .setStringValue("$.request.field")
                                                    .build())
                                            .build())
                                    .build())
                            .addDatatypeConditions(
                                DatatypeCondition.newBuilder()
                                    .addAllDatatypeIds(List.of("datatype-2"))
                                    .setResponseBodyCustomLocationCondition(
                                        MatchOperatorCondition.newBuilder()
                                            .setOperator(MatchOperator.MATCH_OPERATOR_EQUALS)
                                            .setValue(
                                                Value.newBuilder()
                                                    .setStringValue("$.response.field")
                                                    .build())
                                            .build())
                                    .build())
                            .build())
                    .build())
            .build();

    CustomSignatureConfigContext result = converter.convert(List.of(rule), dataClassificationInfo);

    assertNotNull(result);
    assertEquals(1, result.getRuleContextsCount());

    CustomSignatureRulesContext rulesContext = result.getRuleContexts(0);
    assertEquals(1, rulesContext.getRuleConfigsCount());

    CustomSignatureRuleConfig ruleConfig = rulesContext.getRuleConfigs(0);
    CustomSignatureRuleDefinitionGroup group = ruleConfig.getRuleDefinitionGroup();
    assertEquals(2, group.getRuleDefinitionsCount());

    String expectedEvalIdPrefix = "datatype-rule-" + ruleConfig.getId();

    String evalId0 =
        group
            .getRuleDefinitions(0)
            .getCustomSignatureConditionExpression()
            .getConditionExpressionEvaluationIdentifier();
    String evalId1 =
        group
            .getRuleDefinitions(1)
            .getCustomSignatureConditionExpression()
            .getConditionExpressionEvaluationIdentifier();

    assertTrue(
        evalId0.startsWith(expectedEvalIdPrefix),
        "Expected first identifier to start with '"
            + expectedEvalIdPrefix
            + "' but got: "
            + evalId0);
    assertTrue(
        evalId1.startsWith(expectedEvalIdPrefix),
        "Expected second identifier to start with '"
            + expectedEvalIdPrefix
            + "' but got: "
            + evalId1);
    assertTrue(
        evalId0.endsWith("-0"), "Expected first identifier to end with '-0' but got: " + evalId0);
    assertTrue(
        evalId1.endsWith("-1"), "Expected second identifier to end with '-1' but got: " + evalId1);

    KeyValueMatchCondition kvCondition0 =
        group
            .getRuleDefinitions(0)
            .getCustomSignatureConditionExpression()
            .getConditionExpression()
            .getLeafMatchConditionExpression()
            .getKeyValueMatchCondition();

    KeyValueMatchCondition kvCondition1 =
        group
            .getRuleDefinitions(1)
            .getCustomSignatureConditionExpression()
            .getConditionExpression()
            .getLeafMatchConditionExpression()
            .getKeyValueMatchCondition();

    String expectedRequestPrefix =
        PrefixBuilder.buildAppendablePrefix(
            MessageType.MESSAGE_TYPE_REQUEST, AttributeType.ATTRIBUTE_TYPE_BODY_PARAM);
    String expectedResponsePrefix =
        PrefixBuilder.buildAppendablePrefix(
            MessageType.MESSAGE_TYPE_RESPONSE, AttributeType.ATTRIBUTE_TYPE_BODY_PARAM);

    assertEquals(
        expectedRequestPrefix,
        kvCondition0.getLhsKeyOperand().getKeyMetadata().getFullyQualifiedKeyPrefix());
    assertEquals(
        expectedResponsePrefix,
        kvCondition1.getLhsKeyOperand().getKeyMetadata().getFullyQualifiedKeyPrefix());
  }
}
