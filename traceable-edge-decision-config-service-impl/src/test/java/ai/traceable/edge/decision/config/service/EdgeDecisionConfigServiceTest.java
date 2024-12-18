package ai.traceable.edge.decision.config.service;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.openMocks;

import ai.traceable.config.utils.UuidGenerator;
import ai.traceable.datamodel.data.transformation.config.v1.VariableDerivationMapping;
import ai.traceable.edge.decision.config.service.aggregator.attributes.RuleVariableEnricher;
import ai.traceable.edge.decision.config.service.store.EdgeDecisionConfigStore;
import ai.traceable.edge.decision.config.service.store.EdgeDecisionConfigStoreManager;
import ai.traceable.edge.decision.config.service.store.EdgeDecisionRuleStore;
import ai.traceable.edge.decision.config.service.store.EdgeDecisionRuleStoreManager;
import ai.traceable.edge.decision.config.service.store.EdgeDecisionSpecStore;
import ai.traceable.edge.decision.config.service.store.EdgeDecisionSpecStoreManager;
import ai.traceable.edge.decision.config.service.store.FilterEvaluator;
import ai.traceable.edge.decision.config.service.supplier.EdgeDecisionEngineConfigResolver;
import ai.traceable.edge.decision.config.service.supplier.EdgeDecisionEngineConfigSupplier;
import ai.traceable.edge.decision.config.service.supplier.StoredEdgeDecisionEngineConfigSupplier;
import ai.traceable.edge.decision.config.service.v1.CreateEdgeDecisionEngineConfigRequest;
import ai.traceable.edge.decision.config.service.v1.CreateEdgeDecisionRuleRequest;
import ai.traceable.edge.decision.config.service.v1.EdgeDecisionConfigServiceGrpc;
import ai.traceable.edge.decision.config.service.v1.EdgeDecisionEngineConfig;
import ai.traceable.edge.decision.config.service.v1.EdgeDecisionRule;
import ai.traceable.edge.decision.config.service.v1.EdgeDecisionRuleDefinition;
import ai.traceable.edge.decision.config.service.v1.Filter;
import ai.traceable.edge.decision.config.service.v1.GenericValueFilter;
import ai.traceable.edge.decision.config.service.v1.GetAllEdgeDecisionRulesRequest;
import ai.traceable.edge.decision.config.service.v1.GetEdgeDecisionEngineConfigRequest;
import ai.traceable.edge.decision.config.service.v1.LogicalFilter;
import ai.traceable.edge.decision.config.service.v1.LogicalOperator;
import ai.traceable.edge.decision.config.service.v1.RelationalOperator;
import ai.traceable.edge.decision.config.service.validation.RequestValidator;
import com.google.protobuf.util.Structs;
import com.google.protobuf.util.Values;
import java.util.Collections;
import java.util.List;
import org.hypertrace.config.service.change.event.api.ConfigChangeEventGenerator;
import org.hypertrace.config.service.test.MockGenericConfigService;
import org.hypertrace.config.service.v1.ConfigServiceGrpc;
import org.hypertrace.core.grpcutils.client.RequestContextClientCallCredsProviderFactory;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;

class EdgeDecisionConfigServiceTest {

  private static final String UUID_1 = "t1";

  private EdgeDecisionConfigServiceGrpc.EdgeDecisionConfigServiceBlockingStub stub;
  private MockGenericConfigService mockGenericConfigService;
  @Mock private ConfigChangeEventGenerator eventGenerator;
  @Mock private UuidGenerator uuidGenerator;
  @Mock private RuleVariableEnricher ruleVariableEnricher;

  @BeforeEach
  void beforeEach() {
    openMocks(this);
    this.mockGenericConfigService =
        new MockGenericConfigService().mockUpsert().mockGet().mockGetAll().mockDelete();
    ConfigServiceGrpc.ConfigServiceBlockingStub genericStub =
        ConfigServiceGrpc.newBlockingStub(this.mockGenericConfigService.channel());
    EdgeDecisionConfigStoreManager storeManager =
        new EdgeDecisionConfigStoreManager(
            new EdgeDecisionConfigStore(genericStub, eventGenerator), ruleVariableEnricher);
    EdgeDecisionRuleStoreManager ruleStoreManager =
        new EdgeDecisionRuleStoreManager(
            new EdgeDecisionRuleStore(genericStub, eventGenerator, new FilterEvaluator()));
    EdgeDecisionSpecStoreManager specStoreManager =
        new EdgeDecisionSpecStoreManager(new EdgeDecisionSpecStore(genericStub, eventGenerator));
    EdgeDecisionEngineConfigSupplier configSupplier =
        new StoredEdgeDecisionEngineConfigSupplier(
            storeManager, ruleStoreManager, specStoreManager);
    StoredEdgeDecisionEngineConfigSupplier storedEdgeDecisionEngineConfigSupplier =
        new StoredEdgeDecisionEngineConfigSupplier(
            storeManager, ruleStoreManager, specStoreManager);
    EdgeDecisionEngineConfigResolver resolver =
        new EdgeDecisionEngineConfigResolver(Collections.singleton(configSupplier));
    this.mockGenericConfigService
        .addService(
            new EdgeDecisionConfigService(
                ruleStoreManager,
                specStoreManager,
                storeManager,
                storedEdgeDecisionEngineConfigSupplier,
                resolver,
                new RequestValidator()))
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
    when(ruleVariableEnricher.enrichRule(anyString(), any()))
        .thenAnswer(invocation -> invocation.getArgument(1));

    EdgeDecisionEngineConfig created =
        requestContext
            .call(
                () ->
                    stub.createEdgeDecisionEngineConfig(
                        CreateEdgeDecisionEngineConfigRequest.newBuilder()
                            .setEdgeDecisionEngineConfig(new_config())
                            .build()))
            .getEdgeDecisionEngineConfig();

    assertNotNull(created.getId());
    assertEquals(UUID_1, created.getId());
    String id = created.getId();

    EdgeDecisionEngineConfig toUpdate = update_config(id);

    EdgeDecisionEngineConfig updated =
        requestContext.call(
            () ->
                stub.createEdgeDecisionEngineConfig(
                        CreateEdgeDecisionEngineConfigRequest.newBuilder()
                            .setEdgeDecisionEngineConfig(toUpdate)
                            .build())
                    .getEdgeDecisionEngineConfig());

    assertEquals(id, updated.getId());
    assertEquals(toUpdate.getVersion(), updated.getVersion());

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
    when(ruleVariableEnricher.enrichRule("t1", new_config())).thenReturn(modified_config);

    EdgeDecisionEngineConfig created =
        requestContext
            .call(
                () ->
                    stub.createEdgeDecisionEngineConfig(
                        CreateEdgeDecisionEngineConfigRequest.newBuilder()
                            .setEdgeDecisionEngineConfig(new_config())
                            .build()))
            .getEdgeDecisionEngineConfig();

    var config =
        requestContext.call(
            () ->
                stub.getEdgeDecisionEngineConfig(
                        GetEdgeDecisionEngineConfigRequest.newBuilder().setId("t1").build())
                    .getEdgeDecisionEngineConfig());

    assertEquals(modified_config, config);
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
