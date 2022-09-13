package ai.traceable.blocking.config.service.customsignature;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

import ai.traceable.blocking.config.service.v1.CustomModsecBlockingRules;
import ai.traceable.config.utils.UuidGenerator;
import ai.traceable.customsignature.config.service.v1.CustomSignatureConfigServiceGrpc;
import ai.traceable.customsignature.config.service.v1.CustomSignatureConfigServiceGrpc.CustomSignatureConfigServiceBlockingStub;
import ai.traceable.customsignature.config.service.v1.GetCustomSignatureModsecRulesRequest;
import ai.traceable.customsignature.config.service.v1.GetCustomSignatureModsecRulesResponse;
import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.stub.StreamObserver;
import java.io.IOException;
import java.util.Optional;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class CustomModesecBlockingManagerTest {
  private static final String TENANT_ID = "tenant-id";
  private static final RequestContext REQUEST_CONTEXT = RequestContext.forTenantId(TENANT_ID);

  private final UuidGenerator uuidGenerator = new UuidGenerator();
  private Server mockConfigService;
  private ManagedChannel configServiceChannel;
  private CustomModsecBlockingManager customModsecBlockingManager;

  @BeforeEach
  void setup() throws IOException {
    String serverName = InProcessServerBuilder.generateName();
    mockConfigService =
        InProcessServerBuilder.forName(serverName)
            .directExecutor()
            .addService(new MockCustomSignatureConfigService())
            .build()
            .start();
    configServiceChannel = InProcessChannelBuilder.forName(serverName).directExecutor().build();
    CustomSignatureConfigServiceBlockingStub configServiceStub =
        CustomSignatureConfigServiceGrpc.newBlockingStub(configServiceChannel);
    customModsecBlockingManager =
        new DefaultCustomModsecBlockingManager(configServiceStub, uuidGenerator);
  }

  @AfterEach
  void teardown() {
    this.mockConfigService.shutdown();
    this.configServiceChannel.shutdown();
  }

  @Test
  void testEnabledBlockingRules() {
    CustomModsecBlockingRules blockingRules =
        customModsecBlockingManager.getEnabledBlockingRules(REQUEST_CONTEXT, "", Optional.empty());
    assertEquals("testblob", blockingRules.getCustomModsecRulesBlob());
    assertEquals(uuidGenerator.generateId("testblob"), blockingRules.getHash());

    blockingRules =
        customModsecBlockingManager.getEnabledBlockingRules(
            REQUEST_CONTEXT, blockingRules.getHash(), Optional.empty());
    assertFalse(blockingRules.hasCustomModsecRulesBlob());
    assertEquals(uuidGenerator.generateId("testblob"), blockingRules.getHash());
  }

  private static class MockCustomSignatureConfigService
      extends CustomSignatureConfigServiceGrpc.CustomSignatureConfigServiceImplBase {

    public void getCustomSignatureModsecRules(
        GetCustomSignatureModsecRulesRequest request,
        StreamObserver<GetCustomSignatureModsecRulesResponse> responseObserver) {
      responseObserver.onNext(
          GetCustomSignatureModsecRulesResponse.newBuilder()
              .setModsecRulesBlob("testblob")
              .build());
      responseObserver.onCompleted();
    }
  }
}
