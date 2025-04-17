package ai.traceable.bot.categorized.policy.service.v1;

import static ai.traceable.datamodel.data.transformation.config.v1.FieldType.FIELD_TYPE_STR;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.when;
import static org.mockito.quality.Strictness.LENIENT;

import ai.traceable.bot.categorized.policy.service.v1.BotScope.BotList;
import ai.traceable.bot.categorized.policy.service.v1.store.CategorizedBotConfigPolicyStore;
import ai.traceable.bot.categorized.policy.service.v1.store.CategorizedBotConfigPolicyStoreManager;
import ai.traceable.bot.categorized.policy.service.v1.store.DefaultPolicyConfig;
import ai.traceable.bot.categorized.policy.service.v1.translator.CategorizedBotConfigPolicyToEdgeDecisionTranslator;
import ai.traceable.bot.categorized.utils.CategorizedBotStringUtil;
import ai.traceable.config.utils.UuidGenerator;
import ai.traceable.datamodel.data.transformation.config.v1.AttributeDerivationMapping;
import ai.traceable.datamodel.data.transformation.config.v1.BinaryOperator;
import ai.traceable.datamodel.data.transformation.config.v1.DataTransformationConfig;
import ai.traceable.datamodel.data.transformation.config.v1.DerivationRule;
import ai.traceable.datamodel.data.transformation.config.v1.FieldType;
import ai.traceable.datamodel.data.transformation.config.v1.GenericMatchCondition;
import ai.traceable.datamodel.data.transformation.config.v1.JexlExpressionConfig;
import ai.traceable.datamodel.data.transformation.config.v1.LogicalMatchCondition;
import ai.traceable.datamodel.data.transformation.config.v1.LogicalMatchOperator;
import ai.traceable.datamodel.data.transformation.config.v1.MatchCondition;
import ai.traceable.datamodel.data.transformation.config.v1.MatchOperator;
import ai.traceable.datamodel.data.transformation.config.v1.StructuredMatchCondition;
import ai.traceable.datamodel.data.transformation.config.v1.VariableDerivationMapping;
import ai.traceable.edge.decision.config.service.v1.EdgeDecision;
import ai.traceable.edge.decision.config.service.v1.EdgeDecisionEngineConfig;
import ai.traceable.edge.decision.config.service.v1.EdgeDecisionRule;
import ai.traceable.edge.decision.config.service.v1.EdgeDecisionRuleCategory;
import ai.traceable.edge.decision.config.service.v1.EdgeDecisionRuleDefinition;
import ai.traceable.edge.decision.config.service.v1.EdgeDecisionRuleScope;
import ai.traceable.edge.decision.config.service.v1.EdgeDecisionRuleScopeCondition;
import ai.traceable.edge.decision.config.service.v1.EdgeDecisionRuleStatus;
import ai.traceable.edge.decision.config.service.v1.EdgeDecisionType;
import ai.traceable.edge.decision.config.service.v1.EdgeInputKind;
import ai.traceable.edge.decision.config.service.v1.PolicyKind;
import ai.traceable.edge.decision.config.service.v1.RuleInfoDecoration;
import ai.traceable.edge.decision.config.service.v1.SignatureRule;
import ai.traceable.entity.fetcher.cache.CachedApiMappingProvider;
import com.google.protobuf.ListValue;
import com.google.protobuf.Value;
import com.google.protobuf.util.Structs;
import com.google.protobuf.util.Values;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import io.grpc.StatusRuntimeException;
import java.util.List;
import org.hypertrace.config.service.change.event.api.ConfigChangeEventGenerator;
import org.hypertrace.config.service.test.MockGenericConfigService;
import org.hypertrace.config.service.v1.ConfigServiceGrpc;
import org.hypertrace.core.grpcutils.client.RequestContextClientCallCredsProviderFactory;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = LENIENT)
class CategorizedBotConfigPolicyServiceTest {

  private CategorizedBotConfigPolicyServiceGrpc.CategorizedBotConfigPolicyServiceBlockingStub stub;
  private MockGenericConfigService mockGenericConfigService;
  @Mock ConfigChangeEventGenerator mockConfigChangeEventGenerator;
  @Mock CachedApiMappingProvider cachedApiMappingProvider;
  @Mock Config mockConfig;

  @BeforeEach
  void beforeEach() {
    final UuidGenerator uuidGenerator = new UuidGenerator();
    mockGenericConfigService =
        new MockGenericConfigService().mockUpsert().mockGet().mockGetAll().mockDelete();
    final ConfigServiceGrpc.ConfigServiceBlockingStub configServiceBlockingStub =
        ConfigServiceGrpc.newBlockingStub(this.mockGenericConfigService.channel());
    when(mockConfig.getConfig("bot.config.service"))
        .thenReturn(
            ConfigFactory.parseString(
                "default.policies = [\n"
                    + "    {\n"
                    + "      \"id\": \"bd35c18f-4287-458c-93dc-0150d9bc7b93\",\n"
                    + "      \"categorizedBotPolicyDetails\": {\n"
                    + "        \"name\": \"Crawlers\",\n"
                    + "        \"description\": \"Default Policy created for Crawlers\",\n"
                    + "        \"botScopes\": [{\n"
                    + "          \"botClassification\": {\n"
                    + "            \"category\": \"Crawlers\"\n"
                    + "          }\n"
                    + "        }],\n"
                    + "        \"categorizedBotPolicyActionConfig\": {\n"
                    + "          \"botAction\": \"CATEGORIZED_BOT_ACTION_MONITOR\"\n"
                    + "        },\n"
                    + "        \"isDefaultPolicy\": true\n"
                    + "      }\n"
                    + "    },\n"
                    + "    {\n"
                    + "      \"id\": \"d1b42153-70d9-4e19-89e5-baad262123b2\",\n"
                    + "      \"categorizedBotPolicyDetails\": {\n"
                    + "        \"name\": \"Monitoring and Development Bots\",\n"
                    + "        \"description\": \"Default Policy created for Monitoring and Development Bots\",\n"
                    + "        \"botScopes\": [{\n"
                    + "          \"botClassification\": {\n"
                    + "            \"category\": \"Monitoring and Development Bots\"\n"
                    + "          }\n"
                    + "        }],\n"
                    + "        \"categorizedBotPolicyActionConfig\": {\n"
                    + "          \"botAction\": \"CATEGORIZED_BOT_ACTION_MONITOR\"\n"
                    + "        },\n"
                    + "        \"isDefaultPolicy\": true\n"
                    + "      }\n"
                    + "    },\n"
                    + "    {\n"
                    + "      \"id\": \"794adce2-32af-42ce-84c6-f78798d27cd9\",\n"
                    + "      \"categorizedBotPolicyDetails\": {\n"
                    + "        \"name\": \"Social Media and Content Bots\",\n"
                    + "        \"description\": \"Default Policy created for Social Media and Content Bots\",\n"
                    + "        \"botScopes\": [{\n"
                    + "          \"botClassification\": {\n"
                    + "            \"category\": \"Social Media and Content Bots\"\n"
                    + "          }\n"
                    + "        }],\n"
                    + "        \"categorizedBotPolicyActionConfig\": {\n"
                    + "          \"botAction\": \"CATEGORIZED_BOT_ACTION_MONITOR\"\n"
                    + "        },\n"
                    + "        \"isDefaultPolicy\": true\n"
                    + "      }\n"
                    + "    },\n"
                    + "    {\n"
                    + "      \"id\": \"a5f2f9b4-4bda-46d7-9a70-319b2371311c\",\n"
                    + "      \"categorizedBotPolicyDetails\": {\n"
                    + "        \"name\": \"Marketing and SEO Bots\",\n"
                    + "        \"description\": \"Default Policy created for Marketing and SEO Bots\",\n"
                    + "        \"botScopes\": [{\n"
                    + "          \"botClassification\": {\n"
                    + "            \"category\": \"Marketing and SEO Bots\"\n"
                    + "          }\n"
                    + "        }],\n"
                    + "        \"categorizedBotPolicyActionConfig\": {\n"
                    + "          \"botAction\": \"CATEGORIZED_BOT_ACTION_MONITOR\"\n"
                    + "        },\n"
                    + "        \"isDefaultPolicy\": true\n"
                    + "      }\n"
                    + "    },\n"
                    + "    {\n"
                    + "      \"id\": \"1fbd03b8-b386-42f4-a681-ba9289f2aece\",\n"
                    + "      \"categorizedBotPolicyDetails\": {\n"
                    + "        \"name\": \"Search and Indexing Bots\",\n"
                    + "        \"description\": \"Default Policy created for Search and Indexing Bots\",\n"
                    + "        \"botScopes\": [{\n"
                    + "          \"botClassification\": {\n"
                    + "            \"category\": \"Search and Indexing Bots\"\n"
                    + "          }\n"
                    + "        }],\n"
                    + "        \"categorizedBotPolicyActionConfig\": {\n"
                    + "          \"botAction\": \"CATEGORIZED_BOT_ACTION_MONITOR\"\n"
                    + "        },\n"
                    + "        \"isDefaultPolicy\": true\n"
                    + "      }\n"
                    + "    },\n"
                    + "    {\n"
                    + "      \"id\": \"03ed4bfd-6314-4792-b525-01dd162c5a51\",\n"
                    + "      \"categorizedBotPolicyDetails\": {\n"
                    + "        \"name\": \"E-commerce and Financial Bots\",\n"
                    + "        \"description\": \"Default Policy created for E-commerce and Financial Bots\",\n"
                    + "        \"botScopes\": [{\n"
                    + "          \"botClassification\": {\n"
                    + "            \"category\": \"E-commerce and Financial Bots\"\n"
                    + "          }\n"
                    + "        }],\n"
                    + "        \"categorizedBotPolicyActionConfig\": {\n"
                    + "          \"botAction\": \"CATEGORIZED_BOT_ACTION_MONITOR\"\n"
                    + "        },\n"
                    + "        \"isDefaultPolicy\": true\n"
                    + "      }\n"
                    + "    }\n"
                    + "  ]"));
    final CategorizedBotConfigPolicyStoreManager categorizedBotConfigPolicyStoreManager =
        new CategorizedBotConfigPolicyStoreManager(
            new CategorizedBotConfigPolicyStore(
                configServiceBlockingStub, mockConfigChangeEventGenerator),
            uuidGenerator,
            new DefaultPolicyConfig(mockConfig));
    final CategorizedBotConfigPolicyToEdgeDecisionTranslator
        categorizedBotConfigPolicyToEdgeDecisionTranslator =
            new CategorizedBotConfigPolicyToEdgeDecisionTranslator(
                categorizedBotConfigPolicyStoreManager, cachedApiMappingProvider);
    mockGenericConfigService
        .addService(
            new CategorizedBotConfigPolicyService(
                categorizedBotConfigPolicyStoreManager,
                categorizedBotConfigPolicyToEdgeDecisionTranslator))
        .start();
    this.stub =
        CategorizedBotConfigPolicyServiceGrpc.newBlockingStub(mockGenericConfigService.channel())
            .withCallCredentials(
                RequestContextClientCallCredsProviderFactory.getClientCallCredsProvider().get());
  }

  @AfterEach
  void afterEach() {
    mockGenericConfigService.shutdown();
  }

  @Test
  void testCrud() {
    final RequestContext requestContext = RequestContext.forTenantId("t1");

    // Test Create
    final CategorizedBotConfigPolicy createdPolicy =
        requestContext
            .call(
                () ->
                    stub.createCategorizedBotConfigPolicy(
                        CreateCategorizedBotConfigPolicyRequest.newBuilder()
                            .setCategorizedBotConfigPolicyDetails(
                                CategorizedBotConfigPolicyDetails.newBuilder()
                                    .setName("Test")
                                    .setDescription("Test")
                                    .setEnabled(false)
                                    .setCategorizedBotPolicyActionConfig(
                                        CategorizedBotPolicyActionConfig.newBuilder()
                                            .setBotAction(
                                                CategorizedBotAction.CATEGORIZED_BOT_ACTION_BLOCK)
                                            .build())
                                    .setCategorizedBotPolicyScope(
                                        CategorizedBotPolicyScope.newBuilder()
                                            .setEnvironmentScope(
                                                EnvironmentScope.newBuilder()
                                                    .addEnvironmentIds("Test")
                                                    .build())
                                            .build())
                                    .addBotScopes(
                                        BotScope.newBuilder()
                                            .setBotList(
                                                BotList.newBuilder().addBotIds("Test").build())
                                            .build())
                                    .build())
                            .build()))
            .getCategorizedBotConfigPolicy();
    assertFalse(createdPolicy.getCategorizedBotPolicyDetails().getEnabled());

    // Test Get policy by id
    final List<CategorizedBotConfigPolicy> expectedPolicies =
        requestContext
            .call(
                () ->
                    stub.getCategorizedBotConfigPolicies(
                        GetCategorizedBotConfigPoliciesRequest.newBuilder()
                            .setCategorizedBotConfigPolicyFilter(
                                CategorizedBotConfigPolicyFilter.newBuilder()
                                    .addCategorizedBotConfigPolicyIds(createdPolicy.getId())
                                    .build())
                            .build()))
            .getCategorizedBotConfigPoliciesList();
    assertEquals(1, expectedPolicies.size());
    assertEquals(createdPolicy, expectedPolicies.get(0));

    // Test Get policy by invalid policy id
    final List<CategorizedBotConfigPolicy> expectedPoliciesInvalidPolicyId =
        requestContext
            .call(
                () ->
                    stub.getCategorizedBotConfigPolicies(
                        GetCategorizedBotConfigPoliciesRequest.newBuilder()
                            .setCategorizedBotConfigPolicyFilter(
                                CategorizedBotConfigPolicyFilter.newBuilder()
                                    .addCategorizedBotConfigPolicyIds("DUMMY")
                                    .build())
                            .build()))
            .getCategorizedBotConfigPoliciesList();
    assertEquals(0, expectedPoliciesInvalidPolicyId.size());

    // Test Get policy by environment
    final List<CategorizedBotConfigPolicy> expectedPoliciesForEnv =
        requestContext
            .call(
                () ->
                    stub.getCategorizedBotConfigPolicies(
                        GetCategorizedBotConfigPoliciesRequest.newBuilder()
                            .setCategorizedBotConfigPolicyFilter(
                                CategorizedBotConfigPolicyFilter.newBuilder()
                                    .addEnvironmentIds(
                                        createdPolicy
                                            .getCategorizedBotPolicyDetails()
                                            .getCategorizedBotPolicyScope()
                                            .getEnvironmentScope()
                                            .getEnvironmentIds(0))
                                    .build())
                            .build()))
            .getCategorizedBotConfigPoliciesList();
    assertEquals(7, expectedPoliciesForEnv.size());
    assertEquals(createdPolicy, expectedPoliciesForEnv.get(0));

    // Test Get policy by invalid environmentIDs
    final List<CategorizedBotConfigPolicy> expectedPoliciesInvalidEnv =
        requestContext
            .call(
                () ->
                    stub.getCategorizedBotConfigPolicies(
                        GetCategorizedBotConfigPoliciesRequest.newBuilder()
                            .setCategorizedBotConfigPolicyFilter(
                                CategorizedBotConfigPolicyFilter.newBuilder()
                                    .addEnvironmentIds("DUMMY")
                                    .build())
                            .build()))
            .getCategorizedBotConfigPoliciesList();
    assertEquals(6, expectedPoliciesInvalidEnv.size());

    // Test Get policy all policies with empty filter
    final List<CategorizedBotConfigPolicy> totalPoliciesEmptyFilter =
        requestContext
            .call(
                () ->
                    stub.getCategorizedBotConfigPolicies(
                        GetCategorizedBotConfigPoliciesRequest.newBuilder()
                            .setCategorizedBotConfigPolicyFilter(
                                CategorizedBotConfigPolicyFilter.newBuilder().build())
                            .build()))
            .getCategorizedBotConfigPoliciesList();
    assertEquals(7, totalPoliciesEmptyFilter.size());

    // Test Get policy all policies with no filter
    final List<CategorizedBotConfigPolicy> totalPoliciesNoFilter =
        requestContext
            .call(
                () ->
                    stub.getCategorizedBotConfigPolicies(
                        GetCategorizedBotConfigPoliciesRequest.newBuilder().build()))
            .getCategorizedBotConfigPoliciesList();
    assertEquals(7, totalPoliciesNoFilter.size());

    // Test update policy
    final CategorizedBotConfigPolicyDetails updatedPolicyDetails =
        createdPolicy.getCategorizedBotPolicyDetails().newBuilderForType().setEnabled(true).build();
    final CategorizedBotConfigPolicy updatedPolicy =
        requestContext
            .call(
                () ->
                    stub.updateCategorizedBotConfigPolicy(
                        UpdateCategorizedBotConfigPolicyRequest.newBuilder()
                            .setCategorizedBotConfigPolicy(
                                CategorizedBotConfigPolicy.newBuilder()
                                    .setCategorizedBotPolicyDetails(updatedPolicyDetails)
                                    .setId(createdPolicy.getId())
                                    .build())
                            .build()))
            .getCategorizedBotConfigPolicy();
    assertEquals(updatedPolicyDetails, updatedPolicy.getCategorizedBotPolicyDetails());
    assertTrue(updatedPolicy.getCategorizedBotPolicyDetails().getEnabled());

    // Test update invalid policy
    assertThrows(
        StatusRuntimeException.class,
        () ->
            requestContext.call(
                () ->
                    stub.updateCategorizedBotConfigPolicy(
                        UpdateCategorizedBotConfigPolicyRequest.newBuilder()
                            .setCategorizedBotConfigPolicy(
                                CategorizedBotConfigPolicy.newBuilder()
                                    .setCategorizedBotPolicyDetails(updatedPolicyDetails)
                                    .setId("dummy")
                                    .build())
                            .build())));

    // Test delete policy
    final CategorizedBotConfigPolicy deletedPolicy =
        requestContext
            .call(
                () ->
                    stub.deleteCategorizedBotConfigPolicy(
                        DeleteCategorizedBotConfigPolicyRequest.newBuilder()
                            .setCategorizedBotConfigPolicyId(updatedPolicy.getId())
                            .build()))
            .getCategorizedBotConfigPolicy();
    assertEquals(updatedPolicy.getId(), deletedPolicy.getId());

    final List<CategorizedBotConfigPolicy> allPolicies =
        requestContext
            .call(
                () ->
                    stub.getCategorizedBotConfigPolicies(
                        GetCategorizedBotConfigPoliciesRequest.newBuilder()
                            .setCategorizedBotConfigPolicyFilter(
                                CategorizedBotConfigPolicyFilter.newBuilder().build())
                            .build()))
            .getCategorizedBotConfigPoliciesList();
    assertEquals(6, allPolicies.size());

    // Test delete policy without providing policy id - noop
    final CategorizedBotConfigPolicy secondCreatedPolicy =
        requestContext
            .call(
                () ->
                    stub.createCategorizedBotConfigPolicy(
                        CreateCategorizedBotConfigPolicyRequest.newBuilder()
                            .setCategorizedBotConfigPolicyDetails(
                                CategorizedBotConfigPolicyDetails.newBuilder()
                                    .setName("Test")
                                    .setDescription("Test")
                                    .setEnabled(true)
                                    .setCategorizedBotPolicyActionConfig(
                                        CategorizedBotPolicyActionConfig.newBuilder()
                                            .setBotAction(
                                                CategorizedBotAction.CATEGORIZED_BOT_ACTION_BLOCK)
                                            .build())
                                    .setCategorizedBotPolicyScope(
                                        CategorizedBotPolicyScope.newBuilder()
                                            .setEnvironmentScope(
                                                EnvironmentScope.newBuilder()
                                                    .addEnvironmentIds("Test")
                                                    .build())
                                            .build())
                                    .addBotScopes(
                                        BotScope.newBuilder()
                                            .setBotList(
                                                BotList.newBuilder().addBotIds("Test").build())
                                            .build())
                                    .build())
                            .build()))
            .getCategorizedBotConfigPolicy();

    final CategorizedBotConfigPolicy deletedSecondPolicy =
        requestContext
            .call(
                () ->
                    stub.deleteCategorizedBotConfigPolicy(
                        DeleteCategorizedBotConfigPolicyRequest.newBuilder().build()))
            .getCategorizedBotConfigPolicy();
    assertEquals(updatedPolicy.getId(), deletedPolicy.getId());

    final List<CategorizedBotConfigPolicy> allCurrentPolicies =
        requestContext
            .call(
                () ->
                    stub.getCategorizedBotConfigPolicies(
                        GetCategorizedBotConfigPoliciesRequest.newBuilder()
                            .setCategorizedBotConfigPolicyFilter(
                                CategorizedBotConfigPolicyFilter.newBuilder().build())
                            .build()))
            .getCategorizedBotConfigPoliciesList();
    assertEquals(7, allCurrentPolicies.size());
  }

  @Test
  void testGetCategorizedBotConfigPolicyEdgeDecisionRules() {
    final RequestContext requestContext = RequestContext.forTenantId("t1");

    final CategorizedBotConfigPolicy createdPolicy =
        requestContext
            .call(
                () ->
                    stub.createCategorizedBotConfigPolicy(
                        CreateCategorizedBotConfigPolicyRequest.newBuilder()
                            .setCategorizedBotConfigPolicyDetails(
                                CategorizedBotConfigPolicyDetails.newBuilder()
                                    .setName("Test")
                                    .setDescription("Test")
                                    .setEnabled(true)
                                    .setCategorizedBotPolicyActionConfig(
                                        CategorizedBotPolicyActionConfig.newBuilder()
                                            .setBotAction(
                                                CategorizedBotAction.CATEGORIZED_BOT_ACTION_BLOCK)
                                            .build())
                                    .setCategorizedBotPolicyScope(
                                        CategorizedBotPolicyScope.newBuilder()
                                            .setEnvironmentScope(
                                                EnvironmentScope.newBuilder()
                                                    .addEnvironmentIds("Test")
                                                    .build())
                                            .build())
                                    .addBotScopes(
                                        BotScope.newBuilder()
                                            .setBotList(
                                                BotList.newBuilder()
                                                    .addBotIds(
                                                        "550e8400-e29b-41d4-a716-446655440000")
                                                    .build())
                                            .build())
                                    .build())
                            .build()))
            .getCategorizedBotConfigPolicy();

    // Test Get policy edge decision rules
    final GetCategorizedBotConfigPolicyEdgeDecisionRulesResponse
        categorizedBotConfigPolicyEdgeDecisionRulesResponse =
            requestContext.call(
                () ->
                    stub.getCategorizedBotConfigPolicyEdgeDecisionRules(
                        GetCategorizedBotConfigPolicyEdgeDecisionRulesRequest.newBuilder()
                            .setCategorizedBotConfigPolicyEdgeDecisionRulesFilter(
                                CategorizedBotConfigPolicyEdgeDecisionRulesFilter.newBuilder()
                                    .addCategorizedBotConfigPolicyIds(createdPolicy.getId())
                                    .build())
                            .build()));

    final VariableDerivationMapping expectedVariableDerivation =
        VariableDerivationMapping.newBuilder()
            .setName("TRACEABLEAI_BOT_bingbot_550e8400_e29b_41d4_a716_446655440000")
            .addRules(
                DerivationRule.newBuilder()
                    .setTransformationConfig(
                        DataTransformationConfig.newBuilder()
                            .setStaticValue(Value.newBuilder().setBoolValue(true))
                            .setOutputType(FieldType.FIELD_TYPE_BOOL))
                    .setMatchCondition(
                        MatchCondition.newBuilder()
                            .setLogicalMatchCondition(
                                LogicalMatchCondition.newBuilder()
                                    .setOperator(LogicalMatchOperator.LOGICAL_MATCH_OPERATOR_AND)
                                    .addConditions(
                                        MatchCondition.newBuilder()
                                            .setLogicalMatchCondition(
                                                LogicalMatchCondition.newBuilder()
                                                    .setOperator(
                                                        LogicalMatchOperator
                                                            .LOGICAL_MATCH_OPERATOR_OR)
                                                    .addConditions(
                                                        MatchCondition.newBuilder()
                                                            .setGenericMatchCondition(
                                                                GenericMatchCondition.newBuilder()
                                                                    .setJexlExpression(
                                                                        JexlExpressionConfig
                                                                            .newBuilder()
                                                                            .setJexlExpression(
                                                                                "ipValidation:isIpAddressInRange('13.66.139.0/24', $s.getIpAddress())")
                                                                            .build()))
                                                            .build())
                                                    .addConditions(
                                                        MatchCondition.newBuilder()
                                                            .setGenericMatchCondition(
                                                                GenericMatchCondition.newBuilder()
                                                                    .setJexlExpression(
                                                                        JexlExpressionConfig
                                                                            .newBuilder()
                                                                            .setJexlExpression(
                                                                                "ipValidation:isIpAddressInRange('40.77.167.0/24', $s.getIpAddress())")
                                                                            .build())
                                                                    .build()))
                                                    .build())
                                            .build())
                                    .addConditions(
                                        MatchCondition.newBuilder()
                                            .setLogicalMatchCondition(
                                                LogicalMatchCondition.newBuilder()
                                                    .setOperator(
                                                        LogicalMatchOperator
                                                            .LOGICAL_MATCH_OPERATOR_OR)
                                                    .addConditions(
                                                        MatchCondition.newBuilder()
                                                            .setStructuredMatchCondition(
                                                                StructuredMatchCondition
                                                                    .newBuilder()
                                                                    .setLhs(
                                                                        AttributeDerivationMapping
                                                                            .newBuilder()
                                                                            .setName("lhs")
                                                                            .setType(FIELD_TYPE_STR)
                                                                            .addRules(
                                                                                DerivationRule
                                                                                    .newBuilder()
                                                                                    .setTransformationConfig(
                                                                                        DataTransformationConfig
                                                                                            .newBuilder()
                                                                                            .setOutputType(
                                                                                                FIELD_TYPE_STR)
                                                                                            .setJexlExpression(
                                                                                                JexlExpressionConfig
                                                                                                    .newBuilder()
                                                                                                    .setJexlExpression(
                                                                                                        "$s.getUserAgent()")))))
                                                                    .setBinaryOperator(
                                                                        BinaryOperator.newBuilder()
                                                                            .setListValue(
                                                                                ListValue
                                                                                    .newBuilder()
                                                                                    .addValues(
                                                                                        Value
                                                                                            .newBuilder()
                                                                                            .setStringValue(
                                                                                                "Mozilla/5.0 (compatible; bingbot/2.0; +http://www.bing.com/bingbot.htm)")
                                                                                            .build())
                                                                                    .build())
                                                                            .setMatchOperator(
                                                                                MatchOperator
                                                                                    .MATCH_OPERATOR_IN))))
                                                    .build())
                                            .build()))
                            .build())
                    .build())
            .build();

    final EdgeDecisionRule expectedRule =
        EdgeDecisionRule.newBuilder()
            .setId(
                CategorizedBotStringUtil.joinStrings(
                    "550e8400-e29b-41d4-a716-446655440000", createdPolicy.getId()))
            .setName(String.format(createdPolicy.getCategorizedBotPolicyDetails().getName()))
            .setRuleScope(
                EdgeDecisionRuleScope.newBuilder()
                    .addScopeConditions(
                        EdgeDecisionRuleScopeCondition.newBuilder()
                            .setEnvironmentScope(
                                ai.traceable.edge.decision.config.service.v1.EnvironmentScope
                                    .newBuilder()
                                    .addEnvironments("Test")
                                    .build())
                            .build())
                    .build())
            .setRuleCategory(
                EdgeDecisionRuleCategory.EDGE_DECISION_RULE_CATEGORY_TRACEABLE_CATEGORIZED_BOTS)
            .setRuleStatus(
                EdgeDecisionRuleStatus.newBuilder().setDisabled(false).setInternal(false).build())
            .setPolicyId(createdPolicy.getId())
            .setPolicyKind(PolicyKind.POLICY_KIND_BOT_MITIGATION)
            .setRuleDefinition(
                EdgeDecisionRuleDefinition.newBuilder()
                    .setEdgeInputKind(EdgeInputKind.EDGE_INPUT_KIND_HTTP_REQUEST)
                    .addRuleVariables(expectedVariableDerivation)
                    .setCustomFields(
                        Values.of(Structs.of("policyId", Values.of(createdPolicy.getId()))))
                    .setSignatureRule(
                        SignatureRule.newBuilder()
                            .setMatchCondition(
                                MatchCondition.newBuilder()
                                    .setGenericMatchCondition(
                                        GenericMatchCondition.newBuilder()
                                            .setJexlExpression(
                                                JexlExpressionConfig.newBuilder()
                                                    .setJexlExpression(
                                                        "TRACEABLEAI_BOT_bingbot_550e8400_e29b_41d4_a716_446655440000 == true")
                                                    .build())
                                            .build())
                                    .build())
                            .build())
                    .build())
            .setRuleDecision(
                EdgeDecision.newBuilder()
                    .setThreatType("Crawlers")
                    .addRuleInfoDecorations(
                        RuleInfoDecoration.newBuilder()
                            .setRuleInfoKey(
                                DataTransformationConfig.newBuilder()
                                    .setStaticValue(
                                        Value.newBuilder().setStringValue("bot_name").build())
                                    .setOutputType(FIELD_TYPE_STR)
                                    .build())
                            .setRuleInfoValue(
                                DataTransformationConfig.newBuilder()
                                    .setStaticValue(
                                        Value.newBuilder().setStringValue("Bingbot").build())
                                    .setOutputType(FIELD_TYPE_STR)
                                    .build())
                            .build())
                    .addRuleInfoDecorations(
                        RuleInfoDecoration.newBuilder()
                            .setRuleInfoKey(
                                DataTransformationConfig.newBuilder()
                                    .setStaticValue(
                                        Value.newBuilder().setStringValue("bot_id").build())
                                    .setOutputType(FIELD_TYPE_STR)
                                    .build())
                            .setRuleInfoValue(
                                DataTransformationConfig.newBuilder()
                                    .setStaticValue(
                                        Value.newBuilder()
                                            .setStringValue("550e8400-e29b-41d4-a716-446655440000")
                                            .build())
                                    .setOutputType(FIELD_TYPE_STR)
                                    .build())
                            .build())
                    .addRuleInfoDecorations(
                        RuleInfoDecoration.newBuilder()
                            .setRuleInfoKey(
                                DataTransformationConfig.newBuilder()
                                    .setStaticValue(
                                        Value.newBuilder().setStringValue("bot_category").build())
                                    .setOutputType(FIELD_TYPE_STR)
                                    .build())
                            .setRuleInfoValue(
                                DataTransformationConfig.newBuilder()
                                    .setStaticValue(
                                        Value.newBuilder().setStringValue("Crawlers").build())
                                    .setOutputType(FIELD_TYPE_STR)
                                    .build())
                            .build())
                    .addRuleInfoDecorations(
                        RuleInfoDecoration.newBuilder()
                            .setRuleInfoKey(
                                DataTransformationConfig.newBuilder()
                                    .setStaticValue(
                                        Value.newBuilder()
                                            .setStringValue("bot_sub_category")
                                            .build())
                                    .setOutputType(FIELD_TYPE_STR)
                                    .build())
                            .setRuleInfoValue(
                                DataTransformationConfig.newBuilder()
                                    .setStaticValue(
                                        Value.newBuilder().setStringValue("Search bots").build())
                                    .setOutputType(FIELD_TYPE_STR)
                                    .build())
                            .build())
                    .addRuleInfoDecorations(
                        RuleInfoDecoration.newBuilder()
                            .setRuleInfoKey(
                                DataTransformationConfig.newBuilder()
                                    .setStaticValue(
                                        Value.newBuilder().setStringValue("bot_policy_id").build())
                                    .setOutputType(FIELD_TYPE_STR)
                                    .build())
                            .setRuleInfoValue(
                                DataTransformationConfig.newBuilder()
                                    .setStaticValue(
                                        Value.newBuilder()
                                            .setStringValue(createdPolicy.getId())
                                            .build())
                                    .setOutputType(FIELD_TYPE_STR)
                                    .build())
                            .build())
                    .setEdgeDecisionType(EdgeDecisionType.EDGE_DECISION_TYPE_BLOCK)
                    .build())
            .build();

    assertEquals(
        expectedRule,
        categorizedBotConfigPolicyEdgeDecisionRulesResponse
            .getEdgeDecisionEngineConfig()
            .getDecisionRulesList()
            .get(0));
  }

  @Test
  void testGetCategorizedBotConfigPolicyEdgeDecisionRulesInvalidBotIds() {
    final RequestContext requestContext = RequestContext.forTenantId("t1");

    final CategorizedBotConfigPolicy createdPolicy =
        requestContext
            .call(
                () ->
                    stub.createCategorizedBotConfigPolicy(
                        CreateCategorizedBotConfigPolicyRequest.newBuilder()
                            .setCategorizedBotConfigPolicyDetails(
                                CategorizedBotConfigPolicyDetails.newBuilder()
                                    .setName("Test")
                                    .setDescription("Test")
                                    .setEnabled(true)
                                    .setCategorizedBotPolicyActionConfig(
                                        CategorizedBotPolicyActionConfig.newBuilder()
                                            .setBotAction(
                                                CategorizedBotAction.CATEGORIZED_BOT_ACTION_BLOCK)
                                            .build())
                                    .setCategorizedBotPolicyScope(
                                        CategorizedBotPolicyScope.newBuilder()
                                            .setEnvironmentScope(
                                                EnvironmentScope.newBuilder()
                                                    .addEnvironmentIds("Test")
                                                    .build())
                                            .build())
                                    .addBotScopes(
                                        BotScope.newBuilder()
                                            .setBotList(
                                                BotList.newBuilder().addBotIds("Test").build())
                                            .build())
                                    .build())
                            .build()))
            .getCategorizedBotConfigPolicy();

    // Test Get policy edge decision rules
    final GetCategorizedBotConfigPolicyEdgeDecisionRulesResponse
        categorizedBotConfigPolicyEdgeDecisionRulesResponse =
            requestContext.call(
                () ->
                    stub.getCategorizedBotConfigPolicyEdgeDecisionRules(
                        GetCategorizedBotConfigPolicyEdgeDecisionRulesRequest.newBuilder()
                            .setCategorizedBotConfigPolicyEdgeDecisionRulesFilter(
                                CategorizedBotConfigPolicyEdgeDecisionRulesFilter.newBuilder()
                                    .addCategorizedBotConfigPolicyIds(createdPolicy.getId())
                                    .build())
                            .build()));

    assertEquals(
        EdgeDecisionEngineConfig.getDefaultInstance(),
        categorizedBotConfigPolicyEdgeDecisionRulesResponse.getEdgeDecisionEngineConfig());
  }
}
