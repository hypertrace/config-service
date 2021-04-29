package ai.traceable.blocking.config.service.customsignature;

import static org.junit.Assert.assertEquals;

import ai.traceable.blocking.config.service.v1.CustomModsecBlockingRules;
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
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class CustomModesecBlockingManagerTest {

  private CustomSignatureConfigServiceBlockingStub configServiceStub;
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
    this.configServiceStub = CustomSignatureConfigServiceGrpc.newBlockingStub(configServiceChannel);
    customModsecBlockingManager = new CustomModsecBlockingManager(configServiceStub);
  }

  @AfterEach
  void teardown() {
    this.mockConfigService.shutdown();
    this.configServiceChannel.shutdown();
  }

  @Test
  void testEnabledBlockingRules() {
    CustomModsecBlockingRules blockingRules = customModsecBlockingManager.getEnabledBlockingRules();
    assertEquals("testblob", blockingRules.getCustomModsecRulesBlob());
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
