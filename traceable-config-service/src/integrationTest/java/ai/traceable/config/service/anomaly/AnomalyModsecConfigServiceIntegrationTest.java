package ai.traceable.config.service.anomaly;

import ai.traceable.anomaly.config.service.v1.modsec.AnomalyModsecConfigServiceGrpc;
import ai.traceable.config.service.TraceableConfigServiceIntegrationTestBase;
import org.hypertrace.core.grpcutils.client.RequestContextClientCallCredsProviderFactory;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class AnomalyModsecConfigServiceIntegrationTest
    extends TraceableConfigServiceIntegrationTestBase {
  private static AnomalyModsecConfigServiceGrpc.AnomalyModsecConfigServiceBlockingStub
      modsecConfigServiceBlockingStub;

  @BeforeAll
  static void init() {
    modsecConfigServiceBlockingStub =
        AnomalyModsecConfigServiceGrpc.newBlockingStub(managedChannelForInternalServices)
            .withCallCredentials(
                RequestContextClientCallCredsProviderFactory.getClientCallCredsProvider().get());
  }

  @Test
  void getModsecCrsRules() {
    // TODO: Complete Integration Test
  }
}
