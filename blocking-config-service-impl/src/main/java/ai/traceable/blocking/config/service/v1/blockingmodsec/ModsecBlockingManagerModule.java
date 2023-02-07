package ai.traceable.blocking.config.service.v1.blockingmodsec;

import ai.traceable.anomaly.config.service.v1.modsec.AnomalyModsecConfigServiceGrpc;
import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import io.grpc.Channel;
import org.hypertrace.core.grpcutils.client.RequestContextClientCallCredsProviderFactory;

public class ModsecBlockingManagerModule extends AbstractModule {

  @Override
  protected void configure() {
    bind(ModsecBlockingManager.class).to(DefaultModsecBlockingManager.class);
  }

  @Provides
  AnomalyModsecConfigServiceGrpc.AnomalyModsecConfigServiceBlockingStub
      providesAnomalyModsecConfigServiceBlockingStub(Channel channel) {
    return AnomalyModsecConfigServiceGrpc.newBlockingStub(channel)
        .withCallCredentials(
            RequestContextClientCallCredsProviderFactory.getClientCallCredsProvider().get());
  }
}
