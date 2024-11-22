package ai.traceable.edge.decision.config.service;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.openMocks;

import ai.traceable.config.utils.UuidGenerator;
import ai.traceable.edge.decision.config.service.store.EdgeDecisionConfigStore;
import ai.traceable.edge.decision.config.service.store.EdgeDecisionConfigStoreManager;
import ai.traceable.edge.decision.config.service.store.EdgeDecisionRuleStore;
import ai.traceable.edge.decision.config.service.store.EdgeDecisionRuleStoreManager;
import ai.traceable.edge.decision.config.service.store.EdgeDecisionSpecStore;
import ai.traceable.edge.decision.config.service.store.EdgeDecisionSpecStoreManager;
import ai.traceable.edge.decision.config.service.v1.CreateEdgeDecisionEngineConfigRequest;
import ai.traceable.edge.decision.config.service.v1.EdgeDecisionConfigServiceGrpc;
import ai.traceable.edge.decision.config.service.v1.EdgeDecisionEngineConfig;
import ai.traceable.edge.decision.config.service.v1.GetEdgeDecisionEngineConfigRequest;
import ai.traceable.edge.decision.config.service.validation.RequestValidator;
import org.hypertrace.config.service.change.event.api.ConfigChangeEventGenerator;
import org.hypertrace.config.service.test.MockGenericConfigService;
import org.hypertrace.config.service.v1.ConfigServiceGrpc;
import org.hypertrace.core.grpcutils.client.RequestContextClientCallCredsProviderFactory;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;

public class EdgeDecisionConfigServiceTest {

  private static final String UUID_1 = "uuid-1";

  private EdgeDecisionConfigServiceGrpc.EdgeDecisionConfigServiceBlockingStub stub;
  private MockGenericConfigService mockGenericConfigService;
  @Mock private ConfigChangeEventGenerator eventGenerator;
  @Mock private UuidGenerator uuidGenerator;

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
        new EdgeDecisionRuleStoreManager(new EdgeDecisionRuleStore(genericStub, eventGenerator));
    EdgeDecisionSpecStoreManager specStoreManager =
        new EdgeDecisionSpecStoreManager(new EdgeDecisionSpecStore(genericStub, eventGenerator));

    this.mockGenericConfigService
        .addService(
            new EdgeDecisionConfigService(
                ruleStoreManager, specStoreManager, storeManager, new RequestValidator()))
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
  public void testCrud() {
    RequestContext requestContext = buildRequestContext();
    var expected = new_config();

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

    assertEquals(UUID_1, config.getId());
  }

  private EdgeDecisionEngineConfig new_config() {
    return EdgeDecisionEngineConfig.newBuilder().setId(UUID_1).setVersion(1).build();
  }

  private EdgeDecisionEngineConfig update_config(String id) {
    return EdgeDecisionEngineConfig.newBuilder().setId(id).setVersion(2).build();
  }

  private static RequestContext buildRequestContext() {
    return RequestContext.forTenantId("t1");
  }
}
