package ai.traceable.edge.decision.config.service;

import static ai.traceable.datamodel.data.transformation.config.v1.FieldType.FIELD_TYPE_STR;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.openMocks;

import ai.traceable.bot.categorized.config.service.v1.CategorizedBotConfigService;
import ai.traceable.bot.categorized.config.service.v1.CategorizedBotConfigServiceGrpc;
import ai.traceable.bot.categorized.config.service.v1.CategorizedBotConfigServiceGrpc.CategorizedBotConfigServiceBlockingStub;
import ai.traceable.bot.categorized.policy.service.v1.BotScope;
import ai.traceable.bot.categorized.policy.service.v1.BotScope.BotList;
import ai.traceable.bot.categorized.policy.service.v1.CategorizedBotAction;
import ai.traceable.bot.categorized.policy.service.v1.CategorizedBotConfigPolicy;
import ai.traceable.bot.categorized.policy.service.v1.CategorizedBotConfigPolicyDetails;
import ai.traceable.bot.categorized.policy.service.v1.CategorizedBotConfigPolicyService;
import ai.traceable.bot.categorized.policy.service.v1.CategorizedBotConfigPolicyServiceGrpc;
import ai.traceable.bot.categorized.policy.service.v1.CategorizedBotConfigPolicyServiceGrpc.CategorizedBotConfigPolicyServiceBlockingStub;
import ai.traceable.bot.categorized.policy.service.v1.CategorizedBotPolicyActionConfig;
import ai.traceable.bot.categorized.policy.service.v1.CategorizedBotPolicyScope;
import ai.traceable.bot.categorized.policy.service.v1.CreateCategorizedBotConfigPolicyRequest;
import ai.traceable.bot.categorized.policy.service.v1.EnvironmentScope;
import ai.traceable.bot.categorized.policy.service.v1.store.CategorizedBotConfigPolicyStore;
import ai.traceable.bot.categorized.policy.service.v1.store.CategorizedBotConfigPolicyStoreManager;
import ai.traceable.bot.categorized.policy.service.v1.store.DefaultPolicyConfig;
import ai.traceable.bot.categorized.policy.service.v1.translator.CategorizedBotConfigPolicyToEdgeDecisionTranslator;
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
import ai.traceable.edge.decision.config.service.aggregator.attributes.variable.enrich.RuleVariableEnricher;
import ai.traceable.edge.decision.config.service.store.EdgeAttributionRuleStore;
import ai.traceable.edge.decision.config.service.store.EdgeAttributionRuleStoreManager;
import ai.traceable.edge.decision.config.service.store.EdgeCustomResponseStore;
import ai.traceable.edge.decision.config.service.store.EdgeCustomResponseStoreManager;
import ai.traceable.edge.decision.config.service.store.EdgeDecisionConfigStore;
import ai.traceable.edge.decision.config.service.store.EdgeDecisionConfigStoreManager;
import ai.traceable.edge.decision.config.service.store.EdgeDecisionRuleStore;
import ai.traceable.edge.decision.config.service.store.EdgeDecisionRuleStoreManager;
import ai.traceable.edge.decision.config.service.store.EdgeDecisionSpecStore;
import ai.traceable.edge.decision.config.service.store.EdgeDecisionSpecStoreManager;
import ai.traceable.edge.decision.config.service.store.FilterEvaluator;
import ai.traceable.edge.decision.config.service.supplier.EdgeDecisionEngineConfigResolver;
import ai.traceable.edge.decision.config.service.supplier.StoredEdgeDecisionEngineConfigSupplier;
import ai.traceable.edge.decision.config.service.supplier.categorized.bots.CategorizedBotsEdgeDecisionEngineConfigSupplier;
import ai.traceable.edge.decision.config.service.v1.CreateEdgeAttributionRuleRequest;
import ai.traceable.edge.decision.config.service.v1.CreateEdgeAttributionRuleResponse;
import ai.traceable.edge.decision.config.service.v1.CreateEdgeCustomResponseRequest;
import ai.traceable.edge.decision.config.service.v1.CreateEdgeCustomResponseResponse;
import ai.traceable.edge.decision.config.service.v1.CreateEdgeDecisionRuleRequest;
import ai.traceable.edge.decision.config.service.v1.CreateEdgeDecisionRuleResponse;
import ai.traceable.edge.decision.config.service.v1.CreateEdgeDecisionSpecRequest;
import ai.traceable.edge.decision.config.service.v1.CreateEdgeDecisionSpecResponse;
import ai.traceable.edge.decision.config.service.v1.CustomResponse;
import ai.traceable.edge.decision.config.service.v1.DeleteEdgeAttributionRuleRequest;
import ai.traceable.edge.decision.config.service.v1.DeleteEdgeAttributionRuleResponse;
import ai.traceable.edge.decision.config.service.v1.DeleteEdgeCustomResponseRequest;
import ai.traceable.edge.decision.config.service.v1.DeleteEdgeCustomResponseResponse;
import ai.traceable.edge.decision.config.service.v1.DeleteEdgeDecisionRuleRequest;
import ai.traceable.edge.decision.config.service.v1.DeleteEdgeDecisionRuleResponse;
import ai.traceable.edge.decision.config.service.v1.DeleteEdgeDecisionSpecRequest;
import ai.traceable.edge.decision.config.service.v1.DeleteEdgeDecisionSpecResponse;
import ai.traceable.edge.decision.config.service.v1.EdgeAttributionRule;
import ai.traceable.edge.decision.config.service.v1.EdgeAttributionRuleDefinition;
import ai.traceable.edge.decision.config.service.v1.EdgeCustomResponse;
import ai.traceable.edge.decision.config.service.v1.EdgeDecision;
import ai.traceable.edge.decision.config.service.v1.EdgeDecisionConfigServiceGrpc;
import ai.traceable.edge.decision.config.service.v1.EdgeDecisionEngineConfig;
import ai.traceable.edge.decision.config.service.v1.EdgeDecisionRule;
import ai.traceable.edge.decision.config.service.v1.EdgeDecisionRuleCategory;
import ai.traceable.edge.decision.config.service.v1.EdgeDecisionRuleDefinition;
import ai.traceable.edge.decision.config.service.v1.EdgeDecisionRuleScope;
import ai.traceable.edge.decision.config.service.v1.EdgeDecisionRuleScopeCondition;
import ai.traceable.edge.decision.config.service.v1.EdgeDecisionRuleStatus;
import ai.traceable.edge.decision.config.service.v1.EdgeDecisionSpec;
import ai.traceable.edge.decision.config.service.v1.EdgeDecisionSpecDirective;
import ai.traceable.edge.decision.config.service.v1.EdgeDecisionType;
import ai.traceable.edge.decision.config.service.v1.EdgeInputKind;
import ai.traceable.edge.decision.config.service.v1.Filter;
import ai.traceable.edge.decision.config.service.v1.GenericValueFilter;
import ai.traceable.edge.decision.config.service.v1.GetAllEdgeAttributionRulesRequest;
import ai.traceable.edge.decision.config.service.v1.GetAllEdgeAttributionRulesResponse;
import ai.traceable.edge.decision.config.service.v1.GetAllEdgeCustomResponsesRequest;
import ai.traceable.edge.decision.config.service.v1.GetAllEdgeCustomResponsesResponse;
import ai.traceable.edge.decision.config.service.v1.GetAllEdgeDecisionRulesRequest;
import ai.traceable.edge.decision.config.service.v1.GetAllEdgeDecisionRulesResponse;
import ai.traceable.edge.decision.config.service.v1.GetAllEdgeDecisionSpecsRequest;
import ai.traceable.edge.decision.config.service.v1.GetAllEdgeDecisionSpecsResponse;
import ai.traceable.edge.decision.config.service.v1.GetEdgeAttributionRuleRequest;
import ai.traceable.edge.decision.config.service.v1.GetEdgeAttributionRuleResponse;
import ai.traceable.edge.decision.config.service.v1.GetEdgeCustomResponseRequest;
import ai.traceable.edge.decision.config.service.v1.GetEdgeCustomResponseResponse;
import ai.traceable.edge.decision.config.service.v1.GetEdgeDecisionEngineConfigRequest;
import ai.traceable.edge.decision.config.service.v1.GetEdgeDecisionRuleRequest;
import ai.traceable.edge.decision.config.service.v1.GetEdgeDecisionRuleResponse;
import ai.traceable.edge.decision.config.service.v1.GetEdgeDecisionSpecRequest;
import ai.traceable.edge.decision.config.service.v1.GetEdgeDecisionSpecResponse;
import ai.traceable.edge.decision.config.service.v1.GetResolvedEdgeDecisionEngineConfigsRequest;
import ai.traceable.edge.decision.config.service.v1.LogicalFilter;
import ai.traceable.edge.decision.config.service.v1.LogicalOperator;
import ai.traceable.edge.decision.config.service.v1.PolicyKind;
import ai.traceable.edge.decision.config.service.v1.RelationalOperator;
import ai.traceable.edge.decision.config.service.v1.RuleInfoDecoration;
import ai.traceable.edge.decision.config.service.v1.SignatureRule;
import ai.traceable.edge.decision.config.service.v1.UpdateEdgeAttributionRuleRequest;
import ai.traceable.edge.decision.config.service.v1.UpdateEdgeAttributionRuleResponse;
import ai.traceable.edge.decision.config.service.v1.UpdateEdgeCustomResponseRequest;
import ai.traceable.edge.decision.config.service.v1.UpdateEdgeCustomResponseResponse;
import ai.traceable.edge.decision.config.service.v1.UpdateEdgeDecisionRuleRequest;
import ai.traceable.edge.decision.config.service.v1.UpdateEdgeDecisionRuleResponse;
import ai.traceable.edge.decision.config.service.v1.UpdateEdgeDecisionSpecRequest;
import ai.traceable.edge.decision.config.service.v1.UpdateEdgeDecisionSpecResponse;
import ai.traceable.edge.decision.config.service.v1.UpsertEdgeDecisionEngineConfigRequest;
import ai.traceable.edge.decision.config.service.validation.RequestValidator;
import ai.traceable.entity.fetcher.cache.CachedApiMappingProvider;
import com.google.protobuf.Value;
import com.google.protobuf.util.Structs;
import com.google.protobuf.util.Values;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import java.util.List;
import java.util.Set;
import org.hypertrace.config.service.change.event.api.ConfigChangeEventGenerator;
import org.hypertrace.config.service.test.MockGenericConfigService;
import org.hypertrace.config.service.v1.ConfigServiceGrpc;
import org.hypertrace.core.grpcutils.client.RequestContextClientCallCredsProviderFactory;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;

class EdgeDecisionConfigServiceTest {

  private static final String UUID_1 = "t1";

  private EdgeDecisionConfigServiceGrpc.EdgeDecisionConfigServiceBlockingStub stub;
  private CategorizedBotConfigPolicyServiceBlockingStub
      categorizedBotConfigPolicyServiceBlockingStub;
  private MockGenericConfigService mockGenericConfigService;
  @Mock private ConfigChangeEventGenerator eventGenerator;
  @Mock private UuidGenerator uuidGenerator;
  @Mock private RuleVariableEnricher ruleVariableEnricher;
  @Mock ConfigChangeEventGenerator mockConfigChangeEventGenerator;
  @Mock CachedApiMappingProvider cachedApiMappingProvider;
  @Mock private Config mockConfig;
  private CategorizedBotConfigServiceBlockingStub categorizedBotConfigServiceBlockingStub;

  @BeforeEach
  void beforeEach() {
    openMocks(this);
    this.mockGenericConfigService =
        new MockGenericConfigService().mockUpsert().mockGet().mockGetAll().mockDelete();
    ConfigServiceGrpc.ConfigServiceBlockingStub genericStub =
        ConfigServiceGrpc.newBlockingStub(this.mockGenericConfigService.channel());
    EdgeDecisionConfigStoreManager storeManager =
        new EdgeDecisionConfigStoreManager(
            new EdgeDecisionConfigStore(genericStub, eventGenerator));
    EdgeDecisionRuleStoreManager ruleStoreManager =
        new EdgeDecisionRuleStoreManager(
            new EdgeDecisionRuleStore(genericStub, eventGenerator, new FilterEvaluator()));
    EdgeDecisionSpecStoreManager specStoreManager =
        new EdgeDecisionSpecStoreManager(new EdgeDecisionSpecStore(genericStub, eventGenerator));
    EdgeAttributionRuleStoreManager attributionRuleStoreManager =
        new EdgeAttributionRuleStoreManager(
            new EdgeAttributionRuleStore(genericStub, eventGenerator));
    EdgeCustomResponseStoreManager customResponseStoreManager =
        new EdgeCustomResponseStoreManager(
            new EdgeCustomResponseStore(genericStub, eventGenerator));
    StoredEdgeDecisionEngineConfigSupplier storedEdgeDecisionEngineConfigSupplier =
        new StoredEdgeDecisionEngineConfigSupplier(
            storeManager, ruleStoreManager, specStoreManager, attributionRuleStoreManager);
    categorizedBotConfigPolicyServiceBlockingStub =
        CategorizedBotConfigPolicyServiceGrpc.newBlockingStub(mockGenericConfigService.channel())
            .withCallCredentials(
                RequestContextClientCallCredsProviderFactory.getClientCallCredsProvider().get());
    categorizedBotConfigServiceBlockingStub =
        CategorizedBotConfigServiceGrpc.newBlockingStub(mockGenericConfigService.channel())
            .withCallCredentials(
                RequestContextClientCallCredsProviderFactory.getClientCallCredsProvider().get());
    CategorizedBotsEdgeDecisionEngineConfigSupplier
        categorizedBotsEdgeDecisionEngineConfigSupplier =
            new CategorizedBotsEdgeDecisionEngineConfigSupplier(
                categorizedBotConfigPolicyServiceBlockingStub);
    EdgeDecisionEngineConfigResolver resolver =
        new EdgeDecisionEngineConfigResolver(
            Set.of(
                storedEdgeDecisionEngineConfigSupplier,
                categorizedBotsEdgeDecisionEngineConfigSupplier),
            ruleVariableEnricher);
    final ConfigServiceGrpc.ConfigServiceBlockingStub configServiceBlockingStub =
        ConfigServiceGrpc.newBlockingStub(this.mockGenericConfigService.channel());
    when(mockConfig.getConfig("bot.config.service"))
        .thenReturn(ConfigFactory.parseString("default.policies = [" + "]"));
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
    this.mockGenericConfigService
        .addService(
            new EdgeDecisionConfigService(
                ruleStoreManager,
                specStoreManager,
                storeManager,
                attributionRuleStoreManager,
                customResponseStoreManager,
                storedEdgeDecisionEngineConfigSupplier,
                resolver,
                new RequestValidator()))
        .addService(
            new CategorizedBotConfigPolicyService(
                categorizedBotConfigPolicyStoreManager,
                categorizedBotConfigPolicyToEdgeDecisionTranslator))
        .addService(new CategorizedBotConfigService())
        .start();

    this.stub =
        EdgeDecisionConfigServiceGrpc.newBlockingStub(this.mockGenericConfigService.channel())
            .withCallCredentials(
                RequestContextClientCallCredsProviderFactory.getClientCallCredsProvider().get());

    when(uuidGenerator.generateRandomId()).thenReturn(UUID_1);
  }

  @AfterEach
  void afterEach() {
    this.mockGenericConfigService.shutdown();
  }

  @Test
  void testCrudForEdgeDecisionEngineConfigs() {
    RequestContext requestContext = buildRequestContext();
    // Send the original edge decision rule as is
    when(ruleVariableEnricher.enrichRule(any(), any()))
        .thenAnswer(invocation -> invocation.getArgument(1));

    EdgeDecisionEngineConfig created =
        requestContext
            .call(
                () ->
                    stub.upsertEdgeDecisionEngineConfig(
                        upsertEdgeDecisionEngineConfigRequest(UUID_1, "test", 1)))
            .getEdgeDecisionEngineConfig();

    assertNotNull(created.getId());
    assertEquals(UUID_1, created.getId());
    String id = created.getId();

    EdgeDecisionEngineConfig updated =
        requestContext.call(
            () ->
                stub.upsertEdgeDecisionEngineConfig(
                        upsertEdgeDecisionEngineConfigRequest(id, "test", 2))
                    .getEdgeDecisionEngineConfig());

    assertEquals(id, updated.getId());
    assertEquals(2, updated.getVersion());

    var config =
        requestContext.call(
            () ->
                stub.getEdgeDecisionEngineConfig(
                        GetEdgeDecisionEngineConfigRequest.newBuilder().setId(UUID_1).build())
                    .getEdgeDecisionEngineConfig());

    assertEquals(requestContext.getTenantId().get(), config.getId());
  }

  private EdgeDecisionEngineConfig new_config() {
    return EdgeDecisionEngineConfig.newBuilder().setId("t1").setVersion(1).build();
  }

  private UpsertEdgeDecisionEngineConfigRequest upsertEdgeDecisionEngineConfigRequest(
      String id, String name, long version) {
    return UpsertEdgeDecisionEngineConfigRequest.newBuilder()
        .setId(id)
        .setName(name)
        .setVersion(version)
        .build();
  }

  private EdgeDecisionEngineConfig update_config(String id) {
    return EdgeDecisionEngineConfig.newBuilder().setId(id).setVersion(2).build();
  }

  private static RequestContext buildRequestContext() {
    return RequestContext.forTenantId("t1");
  }

  @Test
  void testFilterForEdgeDecisionRules() {
    RequestContext requestContext = buildRequestContext();

    EdgeDecisionRule rule1 = createEdgeDecisionRule(requestContext, "id-1", "policy-1", "api-1");
    EdgeDecisionRule rule2 = createEdgeDecisionRule(requestContext, "id-2", "policy-1", "api-2");
    EdgeDecisionRule rule3 = createEdgeDecisionRule(requestContext, "id-3", "policy-2", "api-1");
    EdgeDecisionRule rule4 = createEdgeDecisionRule(requestContext, "id-4", "policy-1", "api-2");

    Filter filter =
        Filter.newBuilder()
            .setLogicalFilter(
                LogicalFilter.newBuilder()
                    .setOperator(LogicalOperator.LOGICAL_OPERATOR_AND)
                    .addFilter(
                        Filter.newBuilder()
                            .setCustomFieldsFilter(
                                GenericValueFilter.newBuilder()
                                    .setKey("policyId")
                                    .setOperator(RelationalOperator.RELATIONAL_OPERATOR_EQUALS)
                                    .setValue(Values.of("policy-1"))))
                    .addFilter(
                        Filter.newBuilder()
                            .setCustomFieldsFilter(
                                GenericValueFilter.newBuilder()
                                    .setKey("target")
                                    .setOperator(RelationalOperator.RELATIONAL_OPERATOR_EQUALS)
                                    .setValue(Values.of("api-2")))))
            .build();
    Assertions.assertEquals(List.of(rule4, rule2), getEdgeDecisionRules(requestContext, filter));
  }

  @Test
  void testHandleChanges() {
    RequestContext requestContext = buildRequestContext();
    EdgeDecisionEngineConfig modified_config =
        EdgeDecisionEngineConfig.newBuilder()
            .setId("t1")
            .setName("name")
            .addCommonVariables(VariableDerivationMapping.newBuilder().setName("user"))
            .build();

    // Send the original edge decision rule as is
    doReturn(modified_config).when(ruleVariableEnricher).enrichRule(any(), any());

    EdgeDecisionEngineConfig created =
        requestContext
            .call(
                () ->
                    stub.upsertEdgeDecisionEngineConfig(
                        upsertEdgeDecisionEngineConfigRequest(UUID_1, "test", 1)))
            .getEdgeDecisionEngineConfig();

    var config =
        requestContext.call(
            () ->
                stub.getResolvedEdgeDecisionEngineConfigs(
                        GetResolvedEdgeDecisionEngineConfigsRequest.getDefaultInstance())
                    .getEdgeDecisionEngineConfig());

    assertEquals(modified_config, config);
  }

  @Test
  void testCategorizedBotPolicy() {
    RequestContext requestContext = buildRequestContext();
    RuleVariableEnricher variableEnricher = new RuleVariableEnricher(Set.of());

    ArgumentCaptor<RequestContext> requestContextCaptor =
        ArgumentCaptor.forClass(RequestContext.class);
    ArgumentCaptor<EdgeDecisionEngineConfig> configCaptor =
        ArgumentCaptor.forClass(EdgeDecisionEngineConfig.class);
    when(ruleVariableEnricher.enrichRule(requestContextCaptor.capture(), configCaptor.capture()))
        .thenAnswer(
            invocation ->
                variableEnricher.enrichRule(
                    requestContextCaptor.getValue(), configCaptor.getValue()));

    final CategorizedBotConfigPolicy createdPolicy =
        requestContext
            .call(
                () ->
                    categorizedBotConfigPolicyServiceBlockingStub.createCategorizedBotConfigPolicy(
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
                                                    .addEnvironmentIds("test")
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
    assertTrue(createdPolicy.getCategorizedBotPolicyDetails().getEnabled());

    var config =
        requestContext.call(
            () ->
                stub.getResolvedEdgeDecisionEngineConfigs(
                        GetResolvedEdgeDecisionEngineConfigsRequest.getDefaultInstance())
                    .getEdgeDecisionEngineConfig());

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
                                                                                            .setName(
                                                                                                "lhs")
                                                                                            .setType(
                                                                                                FIELD_TYPE_STR)
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
                                                                                                                        "$s.getUserAgent().toLowerCase()")))))
                                                                                    .setBinaryOperator(
                                                                                        BinaryOperator
                                                                                            .newBuilder()
                                                                                            .setStringValue(
                                                                                                "bingbot")
                                                                                            .setMatchOperator(
                                                                                                MatchOperator
                                                                                                    .MATCH_OPERATOR_CONTAINS)))
                                                                            .build())
                                                                    .build())
                                                            .build()))
                                            .build()))
                            .build())
                    .build())
            .build();

    assertEquals(
        EdgeDecisionRule.newBuilder()
            .setId("550e8400-e29b-41d4-a716-446655440000_t1")
            .setName("Test")
            .setRuleCategory(
                EdgeDecisionRuleCategory.EDGE_DECISION_RULE_CATEGORY_TRACEABLE_CATEGORIZED_BOTS)
            .setRuleStatus(EdgeDecisionRuleStatus.getDefaultInstance())
            .setPolicyId("t1")
            .setPolicyKind(PolicyKind.POLICY_KIND_BOT_MITIGATION)
            .setRuleScope(
                EdgeDecisionRuleScope.newBuilder()
                    .addScopeConditions(
                        EdgeDecisionRuleScopeCondition.newBuilder()
                            .setEnvironmentScope(
                                ai.traceable.edge.decision.config.service.v1.EnvironmentScope
                                    .newBuilder()
                                    .addEnvironments("test")
                                    .build())
                            .build())
                    .build())
            .setRuleDefinition(
                EdgeDecisionRuleDefinition.newBuilder()
                    .addRuleVariables(expectedVariableDerivation)
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
                                        Value.newBuilder().setStringValue("BingBot").build())
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
                                    .setStaticValue(Value.newBuilder().setStringValue("t1").build())
                                    .setOutputType(FIELD_TYPE_STR)
                                    .build())
                            .build())
                    .setEdgeDecisionType(EdgeDecisionType.EDGE_DECISION_TYPE_BLOCK)
                    .build())
            .build(),
        config.getDecisionRulesList().stream()
            .filter(
                edgeDecisionRule ->
                    edgeDecisionRule.getId().contains("550e8400-e29b-41d4-a716-446655440000"))
            .findFirst()
            .get());
  }

  @Test
  void testCrudForEdgeDecisionRules() {
    RequestContext requestContext = buildRequestContext();
    EdgeDecisionRule rule1 = createEdgeDecisionRule(requestContext, "id-1", "policy-1", "api-1");
    EdgeDecisionRule rule2 = createEdgeDecisionRule(requestContext, "id-2", "policy-1", "api-2");

    GetAllEdgeDecisionRulesRequest getAllRequest =
        GetAllEdgeDecisionRulesRequest.newBuilder().build();
    GetAllEdgeDecisionRulesResponse getAllResponse = stub.getAllEdgeDecisionRules(getAllRequest);
    Assertions.assertEquals(2, getAllResponse.getEdgeDecisionRulesCount());

    GetEdgeDecisionRuleRequest getRequest =
        GetEdgeDecisionRuleRequest.newBuilder().setId("id-1").build();
    GetEdgeDecisionRuleResponse getResponse = stub.getEdgeDecisionRule(getRequest);
    Assertions.assertEquals(rule1, getResponse.getEdgeDecisionRule());

    DeleteEdgeDecisionRuleRequest deleteRequest =
        DeleteEdgeDecisionRuleRequest.newBuilder().setId("id-1").build();
    DeleteEdgeDecisionRuleResponse deleteResponse = stub.deleteEdgeDecisionRule(deleteRequest);
    Assertions.assertEquals(rule1, deleteResponse.getDeletedEdgeDecisionRule());

    CreateEdgeDecisionRuleRequest createRequest =
        CreateEdgeDecisionRuleRequest.newBuilder().setEdgeDecisionRule(rule1).build();
    CreateEdgeDecisionRuleResponse createResponse = stub.createEdgeDecisionRule(createRequest);
    Assertions.assertEquals(rule1, createResponse.getEdgeDecisionRule());

    UpdateEdgeDecisionRuleRequest updateRequest =
        UpdateEdgeDecisionRuleRequest.newBuilder()
            .setEdgeDecisionRule(rule2)
            .setCurrentVersion(0)
            .build();
    UpdateEdgeDecisionRuleResponse updateResponse = stub.updateEdgeDecisionRule(updateRequest);
    Assertions.assertEquals(rule2, updateResponse.getEdgeDecisionRule());
  }

  @Test
  void testCrudForEdgeDecisionSpecs() {
    RequestContext requestContext = buildRequestContext();
    EdgeDecisionSpec spec1 = createEdgeDecisionSpec(requestContext, "id-1", "policy-1", "api-1");
    EdgeDecisionSpec spec2 = createEdgeDecisionSpec(requestContext, "id-2", "policy-1", "api-2");

    GetAllEdgeDecisionSpecsRequest getAllRequest =
        GetAllEdgeDecisionSpecsRequest.newBuilder().build();
    GetAllEdgeDecisionSpecsResponse getAllResponse = stub.getAllEdgeDecisionSpecs(getAllRequest);
    Assertions.assertEquals(2, getAllResponse.getEdgeDecisionSpecsCount());

    GetEdgeDecisionSpecRequest getRequest =
        GetEdgeDecisionSpecRequest.newBuilder().setId("id-1").build();
    GetEdgeDecisionSpecResponse getResponse = stub.getEdgeDecisionSpec(getRequest);
    Assertions.assertEquals(spec1, getResponse.getEdgeDecisionSpec());

    DeleteEdgeDecisionSpecRequest deleteRequest =
        DeleteEdgeDecisionSpecRequest.newBuilder().setId("id-1").build();
    DeleteEdgeDecisionSpecResponse deleteResponse = stub.deleteEdgeDecisionSpec(deleteRequest);
    Assertions.assertEquals(spec1, deleteResponse.getDeletedEdgeDecisionSpec());

    CreateEdgeDecisionSpecRequest createRequest =
        CreateEdgeDecisionSpecRequest.newBuilder().setEdgeDecisionSpec(spec1).build();
    CreateEdgeDecisionSpecResponse createResponse = stub.createEdgeDecisionSpec(createRequest);
    Assertions.assertEquals(spec1, createResponse.getEdgeDecisionSpec());

    UpdateEdgeDecisionSpecRequest updateRequest =
        UpdateEdgeDecisionSpecRequest.newBuilder()
            .setEdgeDecisionSpec(spec2)
            .setCurrentVersion(0)
            .build();
    UpdateEdgeDecisionSpecResponse updateResponse = stub.updateEdgeDecisionSpec(updateRequest);
    Assertions.assertEquals(spec2, updateResponse.getEdgeDecisionSpec());
  }

  @Test
  void testCrudForEdgeAttributionRules() {
    RequestContext requestContext = buildRequestContext();
    EdgeAttributionRule attributionRule1 =
        createEdgeAttributionRule(requestContext, "id-1", "policy-1", "api-1");
    EdgeAttributionRule attributionRule2 =
        createEdgeAttributionRule(requestContext, "id-2", "policy-1", "api-2");

    GetAllEdgeAttributionRulesRequest getAllRequest =
        GetAllEdgeAttributionRulesRequest.newBuilder().build();
    GetAllEdgeAttributionRulesResponse getAllResponse =
        stub.getAllEdgeAttributionRules(getAllRequest);
    Assertions.assertEquals(2, getAllResponse.getEdgeAttributionRulesCount());

    GetEdgeAttributionRuleRequest getRequest =
        GetEdgeAttributionRuleRequest.newBuilder().setId("id-1").build();
    GetEdgeAttributionRuleResponse getResponse = stub.getEdgeAttributionRule(getRequest);
    Assertions.assertEquals(attributionRule1, getResponse.getEdgeAttributionRule());

    DeleteEdgeAttributionRuleRequest deleteRequest =
        DeleteEdgeAttributionRuleRequest.newBuilder().setId("id-1").build();
    DeleteEdgeAttributionRuleResponse deleteResponse =
        stub.deleteEdgeAttributionRule(deleteRequest);
    Assertions.assertEquals(attributionRule1, deleteResponse.getDeletedEdgeAttributionRule());

    CreateEdgeAttributionRuleRequest createRequest =
        CreateEdgeAttributionRuleRequest.newBuilder()
            .setEdgeAttributionRule(attributionRule1)
            .build();
    CreateEdgeAttributionRuleResponse createResponse =
        stub.createEdgeAttributionRule(createRequest);
    Assertions.assertEquals(attributionRule1, createResponse.getEdgeAttributionRule());

    UpdateEdgeAttributionRuleRequest updateRequest =
        UpdateEdgeAttributionRuleRequest.newBuilder()
            .setEdgeAttributionRule(attributionRule2)
            .setCurrentVersion(0)
            .build();
    UpdateEdgeAttributionRuleResponse updateResponse =
        stub.updateEdgeAttributionRule(updateRequest);
    Assertions.assertEquals(attributionRule2, updateResponse.getEdgeAttributionRule());
  }

  @Test
  void testCrudForEdgeCustomResponses() {
    RequestContext requestContext = buildRequestContext();
    EdgeCustomResponse customResponse1 =
        createEdgeCustomResponse(requestContext, "id-1", "policy-1", "api-1");
    EdgeCustomResponse customResponse2 =
        createEdgeCustomResponse(requestContext, "id-2", "policy-1", "api-2");

    GetAllEdgeCustomResponsesRequest getAllRequest =
        GetAllEdgeCustomResponsesRequest.newBuilder().build();
    GetAllEdgeCustomResponsesResponse getAllResponse =
        stub.getAllEdgeCustomResponses(getAllRequest);
    Assertions.assertEquals(2, getAllResponse.getEdgeCustomResponsesCount());

    GetEdgeCustomResponseRequest getRequest =
        GetEdgeCustomResponseRequest.newBuilder().setId("id-1").build();
    GetEdgeCustomResponseResponse getResponse = stub.getEdgeCustomResponse(getRequest);
    Assertions.assertEquals(customResponse1, getResponse.getEdgeCustomResponse());

    DeleteEdgeCustomResponseRequest deleteRequest =
        DeleteEdgeCustomResponseRequest.newBuilder().setId("id-1").build();
    DeleteEdgeCustomResponseResponse deleteResponse = stub.deleteEdgeCustomResponse(deleteRequest);
    Assertions.assertEquals(customResponse1, deleteResponse.getDeletedEdgeCustomResponse());

    CreateEdgeCustomResponseRequest createRequest =
        CreateEdgeCustomResponseRequest.newBuilder().setEdgeCustomResponse(customResponse1).build();
    CreateEdgeCustomResponseResponse createResponse = stub.createEdgeCustomResponse(createRequest);
    Assertions.assertEquals(customResponse1, createResponse.getEdgeCustomResponse());

    UpdateEdgeCustomResponseRequest updateRequest =
        UpdateEdgeCustomResponseRequest.newBuilder()
            .setEdgeCustomResponse(customResponse2)
            .setCurrentVersion(0)
            .build();
    UpdateEdgeCustomResponseResponse updateResponse = stub.updateEdgeCustomResponse(updateRequest);
    Assertions.assertEquals(customResponse2, updateResponse.getEdgeCustomResponse());
  }

  private EdgeDecisionSpec createEdgeDecisionSpec(
      RequestContext requestContext, String ruleId, String policyId, String apiId) {
    return requestContext
        .call(
            () ->
                stub.createEdgeDecisionSpec(
                    CreateEdgeDecisionSpecRequest.newBuilder()
                        .setEdgeDecisionSpec(buildEdgeDecisionSpec(ruleId, policyId, apiId))
                        .build()))
        .getEdgeDecisionSpec();
  }

  private EdgeDecisionSpec buildEdgeDecisionSpec(String ruleId, String policyId, String apiId) {
    return EdgeDecisionSpec.newBuilder()
        .setId(ruleId)
        .addDecisionSpecDirectives(EdgeDecisionSpecDirective.getDefaultInstance())
        .build();
  }

  private EdgeCustomResponse createEdgeCustomResponse(
      RequestContext requestContext, String ruleId, String policyId, String apiId) {
    return requestContext
        .call(
            () ->
                stub.createEdgeCustomResponse(
                    CreateEdgeCustomResponseRequest.newBuilder()
                        .setEdgeCustomResponse(buildEdgeCustomResponse(ruleId, policyId, apiId))
                        .build()))
        .getEdgeCustomResponse();
  }

  private EdgeCustomResponse buildEdgeCustomResponse(String ruleId, String policyId, String apiId) {
    return EdgeCustomResponse.newBuilder()
        .setId(ruleId)
        .setCustomResponse(CustomResponse.getDefaultInstance())
        .build();
  }

  private EdgeAttributionRule createEdgeAttributionRule(
      RequestContext requestContext, String ruleId, String policyId, String apiId) {
    return requestContext
        .call(
            () ->
                stub.createEdgeAttributionRule(
                    CreateEdgeAttributionRuleRequest.newBuilder()
                        .setEdgeAttributionRule(buildEdgeAttributionRule(ruleId, policyId, apiId))
                        .build()))
        .getEdgeAttributionRule();
  }

  private EdgeAttributionRule buildEdgeAttributionRule(
      String ruleId, String policyId, String apiId) {
    return EdgeAttributionRule.newBuilder()
        .setId(ruleId)
        .setRuleDefinition(
            EdgeAttributionRuleDefinition.newBuilder()
                .setCustomFields(
                    Structs.of("policyId", Values.of(policyId), "target", Values.of(apiId))))
        .build();
  }

  private EdgeDecisionRule createEdgeDecisionRule(
      RequestContext requestContext, String ruleId, String policyId, String apiId) {
    return requestContext
        .call(
            () ->
                stub.createEdgeDecisionRule(
                    CreateEdgeDecisionRuleRequest.newBuilder()
                        .setEdgeDecisionRule(buildEdgeDecisionRule(ruleId, policyId, apiId))
                        .build()))
        .getEdgeDecisionRule();
  }

  private EdgeDecisionRule buildEdgeDecisionRule(String ruleId, String policyId, String apiId) {
    return EdgeDecisionRule.newBuilder()
        .setId(ruleId)
        .setRuleDefinition(
            EdgeDecisionRuleDefinition.newBuilder()
                .setCustomFields(
                    Values.of(
                        Structs.of("policyId", Values.of(policyId), "target", Values.of(apiId)))))
        .build();
  }

  private List<EdgeDecisionRule> getEdgeDecisionRules(
      RequestContext requestContext, Filter filter) {
    return requestContext.call(
        () ->
            stub.getAllEdgeDecisionRules(
                    GetAllEdgeDecisionRulesRequest.newBuilder().setFilter(filter).build())
                .getEdgeDecisionRulesList());
  }
}
