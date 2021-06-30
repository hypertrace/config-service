package ai.traceable.localprocessing.config.service.regularmodsec;

import ai.traceable.anomaly.config.service.v1.modsec.AnomalyModsecConfigServiceGrpc;
import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import io.grpc.ManagedChannel;
import org.hypertrace.core.grpcutils.client.RequestContextClientCallCredsProviderFactory;

public class RegularModsecDetectionManagerModule extends AbstractModule {
  @Override
  protected void configure() {
    bind(RegularModsecDetectionManager.class).to(DefaultRegularModsecDetectionManager.class);
  }

  @Provides
  AnomalyModsecConfigServiceGrpc.AnomalyModsecConfigServiceBlockingStub
      providesAnomalyModsecConfigServiceBlockingStub(ManagedChannel channel) {
    return AnomalyModsecConfigServiceGrpc.newBlockingStub(channel)
        .withCallCredentials(
            RequestContextClientCallCredsProviderFactory.getClientCallCredsProvider().get());
  }
}
