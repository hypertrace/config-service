package ai.traceable.blocking.config.service.common.modsec;

import ai.traceable.anomaly.config.service.v1.modsec.AnomalyModsecConfigServiceGrpc;
import ai.traceable.anomaly.config.service.v1.modsec.AnomalyModsecConfigServiceGrpc.AnomalyModsecConfigServiceBlockingStub;
import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import io.grpc.Channel;
import org.hypertrace.core.grpcutils.client.RequestContextClientCallCredsProviderFactory;

public class ModsecCommonModule extends AbstractModule {
  @Provides
  AnomalyModsecConfigServiceBlockingStub providesAnomalyModsecConfigServiceBlockingStub(
      Channel channel) {
    return AnomalyModsecConfigServiceGrpc.newBlockingStub(channel)
        .withCallCredentials(
            RequestContextClientCallCredsProviderFactory.getClientCallCredsProvider().get());
  }
}
