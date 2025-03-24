package ai.traceable.bot.categorized.policy.service.v1;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.quality.Strictness.LENIENT;

import ai.traceable.bot.categorized.policy.service.v1.BotScope.BotList;
import ai.traceable.bot.categorized.policy.service.v1.store.CategorizedBotConfigPolicyStore;
import ai.traceable.bot.categorized.policy.service.v1.store.CategorizedBotConfigPolicyStoreManager;
import ai.traceable.bot.categorized.policy.service.v1.translator.CategorizedBotConfigPolicyToEdgeDecisionTranslator;
import ai.traceable.bot.categorized.utils.CategorizedBotStringUtil;
import ai.traceable.config.utils.UuidGenerator;
import ai.traceable.datamodel.data.transformation.config.v1.DataTransformationConfig;
import ai.traceable.datamodel.data.transformation.config.v1.GenericMatchCondition;
import ai.traceable.datamodel.data.transformation.config.v1.JexlExpressionConfig;
import ai.traceable.datamodel.data.transformation.config.v1.MatchCondition;
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
import com.google.protobuf.Value;
import com.google.protobuf.util.Structs;
import com.google.protobuf.util.Values;
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

  @BeforeEach
  void beforeEach() {
    final UuidGenerator uuidGenerator = new UuidGenerator();
    mockGenericConfigService =
        new MockGenericConfigService().mockUpsert().mockGet().mockGetAll().mockDelete();
    final ConfigServiceGrpc.ConfigServiceBlockingStub configServiceBlockingStub =
        ConfigServiceGrpc.newBlockingStub(this.mockGenericConfigService.channel());
    final CategorizedBotConfigPolicyStoreManager categorizedBotConfigPolicyStoreManager =
        new CategorizedBotConfigPolicyStoreManager(
            new CategorizedBotConfigPolicyStore(
                configServiceBlockingStub, mockConfigChangeEventGenerator),
            uuidGenerator);
    final CategorizedBotConfigPolicyToEdgeDecisionTranslator
        categorizedBotConfigPolicyToEdgeDecisionTranslator =
            new CategorizedBotConfigPolicyToEdgeDecisionTranslator(
                categorizedBotConfigPolicyStoreManager);
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
    assertEquals(1, totalPoliciesEmptyFilter.size());

    // Test Get policy all policies with no filter
    final List<CategorizedBotConfigPolicy> totalPoliciesNoFilter =
        requestContext
            .call(
                () ->
                    stub.getCategorizedBotConfigPolicies(
                        GetCategorizedBotConfigPoliciesRequest.newBuilder().build()))
            .getCategorizedBotConfigPoliciesList();
    assertEquals(1, totalPoliciesNoFilter.size());

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
    assertEquals(0, allPolicies.size());

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
    assertEquals(1, allCurrentPolicies.size());
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
                                                        "TRACEABLEAI_BOT_bing_bot_550e8400-e29b-41d4-a716-446655440000 == true")
                                                    .build())
                                            .build())
                                    .build())
                            .build())
                    .build())
            .setRuleDecision(
                EdgeDecision.newBuilder()
                    .setThreatType("TRACEABLE_CATEGORIZED_BOTS")
                    .addRuleInfoDecorations(
                        RuleInfoDecoration.newBuilder()
                            .setRuleInfoKey(
                                DataTransformationConfig.newBuilder()
                                    .setStaticValue(
                                        Value.newBuilder().setStringValue("bot_name").build())
                                    .build())
                            .setRuleInfoValue(
                                DataTransformationConfig.newBuilder()
                                    .setStaticValue(
                                        Value.newBuilder().setStringValue("Bing bot").build())
                                    .build())
                            .build())
                    .addRuleInfoDecorations(
                        RuleInfoDecoration.newBuilder()
                            .setRuleInfoKey(
                                DataTransformationConfig.newBuilder()
                                    .setStaticValue(
                                        Value.newBuilder().setStringValue("bot_id").build())
                                    .build())
                            .setRuleInfoValue(
                                DataTransformationConfig.newBuilder()
                                    .setStaticValue(
                                        Value.newBuilder()
                                            .setStringValue("550e8400-e29b-41d4-a716-446655440000")
                                            .build())
                                    .build())
                            .build())
                    .addRuleInfoDecorations(
                        RuleInfoDecoration.newBuilder()
                            .setRuleInfoKey(
                                DataTransformationConfig.newBuilder()
                                    .setStaticValue(
                                        Value.newBuilder().setStringValue("bot_category").build())
                                    .build())
                            .setRuleInfoValue(
                                DataTransformationConfig.newBuilder()
                                    .setStaticValue(
                                        Value.newBuilder().setStringValue("Crawlers").build())
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
                                    .build())
                            .setRuleInfoValue(
                                DataTransformationConfig.newBuilder()
                                    .setStaticValue(
                                        Value.newBuilder().setStringValue("Search bots").build())
                                    .build())
                            .build())
                    .addRuleInfoDecorations(
                        RuleInfoDecoration.newBuilder()
                            .setRuleInfoKey(
                                DataTransformationConfig.newBuilder()
                                    .setStaticValue(
                                        Value.newBuilder().setStringValue("bot_policy_id").build())
                                    .build())
                            .setRuleInfoValue(
                                DataTransformationConfig.newBuilder()
                                    .setStaticValue(
                                        Value.newBuilder()
                                            .setStringValue(createdPolicy.getId())
                                            .build())
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
