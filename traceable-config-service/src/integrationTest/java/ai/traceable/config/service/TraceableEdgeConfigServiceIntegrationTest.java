package ai.traceable.config.service;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import ai.traceable.edge.config.service.v1.AgentCapabilities;
import ai.traceable.edge.config.service.v1.ConfigRequestElement;
import ai.traceable.edge.config.service.v1.GetConfigsRequest;
import ai.traceable.edge.config.service.v1.GetConfigsResponse;
import ai.traceable.edge.config.service.v1.TraceableEdgeConfigServiceGrpc;
import ai.traceable.edge.config.service.v1.TraceableEdgeConfigServiceGrpc.TraceableEdgeConfigServiceBlockingStub;
import org.hypertrace.core.grpcutils.client.RequestContextClientCallCredsProviderFactory;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class TraceableEdgeConfigServiceIntegrationTest
    extends TraceableConfigServiceIntegrationTestBase {

  private static final String TEST_TENANT_ID = "tenant-edge-config-test";
  private static final String ENVIRONMENT_ID = "test-env";
  private static TraceableEdgeConfigServiceBlockingStub stub;

  @BeforeAll
  static void init() {
    stub =
        TraceableEdgeConfigServiceGrpc.newBlockingStub(channelForInternalServices)
            .withCallCredentials(
                RequestContextClientCallCredsProviderFactory.getClientCallCredsProvider().get());
  }

  @Test
  void testGetConfigs_withConfigRequest_returnsResponseWithHash() {
    GetConfigsRequest request =
        GetConfigsRequest.newBuilder()
            .setEnvironment(ENVIRONMENT_ID)
            .addConfigRequests(
                ConfigRequestElement.newBuilder()
                    .setConfigType("EdgeDecisionEngineConfig")
                    .setAgentCapabilities(AgentCapabilities.getDefaultInstance())
                    .build())
            .build();

    GetConfigsResponse response =
        RequestContext.forTenantId(TEST_TENANT_ID).call(() -> stub.getConfigs(request));

    assertNotNull(response);
    assertNotNull(response.getHash());
    assertFalse(response.getHash().isEmpty());
    // Response should have config element for requested type
    assertFalse(response.getConfigResponsesList().isEmpty());
  }
}
