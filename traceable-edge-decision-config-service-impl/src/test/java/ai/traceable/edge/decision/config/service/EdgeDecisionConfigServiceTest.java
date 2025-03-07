package ai.traceable.edge.decision.config.service;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.openMocks;

import ai.traceable.config.utils.UuidGenerator;
import ai.traceable.datamodel.data.transformation.config.v1.VariableDerivationMapping;
import ai.traceable.edge.decision.config.service.aggregator.attributes.RuleVariableEnricher;
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
import ai.traceable.edge.decision.config.service.supplier.EdgeDecisionEngineConfigSupplier;
import ai.traceable.edge.decision.config.service.supplier.StoredEdgeDecisionEngineConfigSupplier;
import ai.traceable.edge.decision.config.service.v1.CreateEdgeAttributionRuleRequest;
import ai.traceable.edge.decision.config.service.v1.CreateEdgeAttributionRuleResponse;
import ai.traceable.edge.decision.config.service.v1.CreateEdgeCustomResponseRequest;
import ai.traceable.edge.decision.config.service.v1.CreateEdgeCustomResponseResponse;
import ai.traceable.edge.decision.config.service.v1.CreateEdgeDecisionEngineConfigRequest;
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
import ai.traceable.edge.decision.config.service.v1.EdgeDecisionConfigServiceGrpc;
import ai.traceable.edge.decision.config.service.v1.EdgeDecisionEngineConfig;
import ai.traceable.edge.decision.config.service.v1.EdgeDecisionRule;
import ai.traceable.edge.decision.config.service.v1.EdgeDecisionRuleDefinition;
import ai.traceable.edge.decision.config.service.v1.EdgeDecisionSpec;
import ai.traceable.edge.decision.config.service.v1.EdgeDecisionSpecDirective;
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
import ai.traceable.edge.decision.config.service.v1.RelationalOperator;
import ai.traceable.edge.decision.config.service.v1.UpdateEdgeAttributionRuleRequest;
import ai.traceable.edge.decision.config.service.v1.UpdateEdgeAttributionRuleResponse;
import ai.traceable.edge.decision.config.service.v1.UpdateEdgeCustomResponseRequest;
import ai.traceable.edge.decision.config.service.v1.UpdateEdgeCustomResponseResponse;
import ai.traceable.edge.decision.config.service.v1.UpdateEdgeDecisionRuleRequest;
import ai.traceable.edge.decision.config.service.v1.UpdateEdgeDecisionRuleResponse;
import ai.traceable.edge.decision.config.service.v1.UpdateEdgeDecisionSpecRequest;
import ai.traceable.edge.decision.config.service.v1.UpdateEdgeDecisionSpecResponse;
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
    EdgeDecisionEngineConfigSupplier configSupplier =
        new StoredEdgeDecisionEngineConfigSupplier(
            storeManager, ruleStoreManager, specStoreManager);
    StoredEdgeDecisionEngineConfigSupplier storedEdgeDecisionEngineConfigSupplier =
        new StoredEdgeDecisionEngineConfigSupplier(
            storeManager, ruleStoreManager, specStoreManager);
    EdgeDecisionEngineConfigResolver resolver =
        new EdgeDecisionEngineConfigResolver(
            Collections.singleton(configSupplier), ruleVariableEnricher);
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
    when(ruleVariableEnricher.enrichRule(requestContext, new_config())).thenReturn(modified_config);

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
                stub.getResolvedEdgeDecisionEngineConfigs(
                        GetResolvedEdgeDecisionEngineConfigsRequest.getDefaultInstance())
                    .getEdgeDecisionEngineConfig());

    assertEquals(modified_config, config);
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
