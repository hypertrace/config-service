package ai.traceable.edge.bot.config.service;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import ai.traceable.config.utils.UuidGenerator;
import ai.traceable.edge.bot.config.service.store.CaptchaSiteKeyConfigStore;
import ai.traceable.edge.bot.config.service.store.CaptchaSiteKeyConfigStoreManager;
import ai.traceable.edge.bot.config.service.v1.CaptchaSiteKeyConfig;
import ai.traceable.edge.bot.config.service.v1.CaptchaSiteKeyConfigServiceGrpc;
import ai.traceable.edge.bot.config.service.v1.DeleteRequest;
import ai.traceable.edge.bot.config.service.v1.GetAllRequest;
import ai.traceable.edge.bot.config.service.v1.GetRequest;
import ai.traceable.edge.bot.config.service.v1.UpsertRequest;
import ai.traceable.edge.bot.config.service.validation.RequestValidator;
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
class CaptchaSiteKeyConfigServiceTest {

  private static final String UUID_1 = "uuid-1";

  private CaptchaSiteKeyConfigServiceGrpc.CaptchaSiteKeyConfigServiceBlockingStub stub;
  private MockGenericConfigService mockGenericConfigService;
  @Mock private ConfigChangeEventGenerator eventGenerator;
  @Mock private UuidGenerator uuidGenerator;

  @BeforeEach
  void beforeEach() {
    this.mockGenericConfigService =
        new MockGenericConfigService().mockUpsert().mockGet().mockGetAll().mockDelete();
    ConfigServiceGrpc.ConfigServiceBlockingStub genericStub =
        ConfigServiceGrpc.newBlockingStub(this.mockGenericConfigService.channel());
    CaptchaSiteKeyConfigStoreManager storeManager =
        new CaptchaSiteKeyConfigStoreManager(
            uuidGenerator, new CaptchaSiteKeyConfigStore(genericStub, eventGenerator));
    this.mockGenericConfigService
        .addService(new CaptchaSiteKeyConfigService(storeManager, new RequestValidator()))
        .start();

    this.stub =
        CaptchaSiteKeyConfigServiceGrpc.newBlockingStub(this.mockGenericConfigService.channel())
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

    CaptchaSiteKeyConfig created =
        requestContext
            .call(() -> stub.upsert(UpsertRequest.newBuilder().setConfig(new_config()).build()))
            .getConfig();

    assertNotNull(created.getId());
    assertEquals(UUID_1, created.getId());
    String id = created.getId();

    CaptchaSiteKeyConfig toUpdate = update_config(id);

    CaptchaSiteKeyConfig updated =
        requestContext.call(
            () -> stub.upsert(UpsertRequest.newBuilder().setConfig(toUpdate).build()).getConfig());

    assertEquals(id, updated.getId());
    assertEquals(toUpdate.getSiteKey(), updated.getSiteKey());

    var config =
        requestContext.call(() -> stub.get(GetRequest.newBuilder().setId(id).build()).getConfig());

    assertEquals(id, config.getId());

    requestContext.call(() -> stub.delete(DeleteRequest.newBuilder().setId(id).build()));

    List<CaptchaSiteKeyConfig> configs =
        requestContext.call(() -> stub.getAll(GetAllRequest.newBuilder().build()).getConfigsList());

    assertEquals(0, configs.size());
  }

  private CaptchaSiteKeyConfig new_config() {
    return CaptchaSiteKeyConfig.newBuilder().setId(UUID_1).setSiteKey("key").build();
  }

  private CaptchaSiteKeyConfig update_config(String id) {
    return CaptchaSiteKeyConfig.newBuilder().setId(id).setSiteKey("key_updated").build();
  }

  private static RequestContext buildRequestContext() {
    return RequestContext.forTenantId("t1");
  }
}
