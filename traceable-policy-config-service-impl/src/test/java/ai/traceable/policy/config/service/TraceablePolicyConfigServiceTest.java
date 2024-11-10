package ai.traceable.policy.config.service;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import ai.traceable.config.utils.UuidGenerator;
import ai.traceable.policy.config.service.store.TraceablePolicyConfigStore;
import ai.traceable.policy.config.service.store.TraceablePolicyConfigStoreManager;
import ai.traceable.policy.config.service.v1.DeleteRequest;
import ai.traceable.policy.config.service.v1.GetAllRequest;
import ai.traceable.policy.config.service.v1.GetRequest;
import ai.traceable.policy.config.service.v1.TraceablePolicy;
import ai.traceable.policy.config.service.v1.TraceablePolicyConfigServiceGrpc;
import ai.traceable.policy.config.service.v1.UpsertRequest;
import ai.traceable.policy.config.service.validation.RequestValidator;
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

@ExtendWith(MockitoExtension.class)
class TraceablePolicyConfigServiceTest {

  private static final String UUID_1 = "uuid-1";

  private TraceablePolicyConfigServiceGrpc.TraceablePolicyConfigServiceBlockingStub stub;
  private MockGenericConfigService mockGenericConfigService;
  @Mock private ConfigChangeEventGenerator eventGenerator;
  @Mock private UuidGenerator uuidGenerator;

  @BeforeEach
  void beforeEach() {
    this.mockGenericConfigService =
        new MockGenericConfigService().mockUpsert().mockGet().mockGetAll().mockDelete();
    ConfigServiceGrpc.ConfigServiceBlockingStub genericStub =
        ConfigServiceGrpc.newBlockingStub(this.mockGenericConfigService.channel());
    TraceablePolicyConfigStoreManager storeManager =
        new TraceablePolicyConfigStoreManager(
            uuidGenerator, new TraceablePolicyConfigStore(genericStub, eventGenerator));
    this.mockGenericConfigService
        .addService(new TraceablePolicyConfigService(storeManager, new RequestValidator()))
        .start();

    this.stub =
        TraceablePolicyConfigServiceGrpc.newBlockingStub(this.mockGenericConfigService.channel())
            .withCallCredentials(
                RequestContextClientCallCredsProviderFactory.getClientCallCredsProvider().get());
  }

  @AfterEach
  void afterEach() {
    this.mockGenericConfigService.shutdown();
  }

  @Test
  public void testCrud() {
    RequestContext requestContext = buildRequestContext();
    var expected = new_config();

    TraceablePolicy created =
        requestContext
            .call(() -> stub.upsert(UpsertRequest.newBuilder().setPolicy(new_config()).build()))
            .getPolicy();

    assertNotNull(created.getId());
    assertEquals(UUID_1, created.getId());
    String id = created.getId();

    TraceablePolicy toUpdate = update_config(id);

    TraceablePolicy updated =
        requestContext.call(
            () -> stub.upsert(UpsertRequest.newBuilder().setPolicy(toUpdate).build()).getPolicy());

    assertEquals(id, updated.getId());
    assertEquals(toUpdate.getName(), updated.getName());

    var config =
        requestContext.call(() -> stub.get(GetRequest.newBuilder().setId(id).build()).getPolicy());

    assertEquals(id, config.getId());

    requestContext.call(() -> stub.delete(DeleteRequest.newBuilder().setId(id).build()));

    List<TraceablePolicy> configs =
        requestContext.call(
            () -> stub.getAll(GetAllRequest.newBuilder().build()).getPoliciesList());

    assertEquals(0, configs.size());
  }

  private TraceablePolicy new_config() {
    return TraceablePolicy.newBuilder().setId(UUID_1).setName("key").build();
  }

  private TraceablePolicy update_config(String id) {
    return TraceablePolicy.newBuilder().setId(id).setName("key_updated").build();
  }

  private static RequestContext buildRequestContext() {
    return RequestContext.forTenantId("t1");
  }
}
