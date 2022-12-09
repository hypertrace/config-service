package ai.traceable.blocking.config.service.blockingpolicy.fetchers;

import ai.traceable.anomaly.config.service.v1.detector.DetectorConfigServiceGrpc.DetectorConfigServiceBlockingStub;
import ai.traceable.anomaly.config.service.v1.global.AnomalyGlobalConfigServiceGrpc.AnomalyGlobalConfigServiceBlockingStub;
import ai.traceable.blocking.config.service.blockingpolicy.fetchers.actor.config.ActorServiceConfig;
import ai.traceable.customsignature.config.service.v1.CustomSignatureConfigServiceGrpc.CustomSignatureConfigServiceBlockingStub;
import ai.traceable.iprange.config.service.v1.IpRangeConfigServiceGrpc.IpRangeConfigServiceBlockingStub;
import ai.traceable.malicioussources.config.service.v1.MaliciousSourcesConfigServiceGrpc.MaliciousSourcesConfigServiceBlockingStub;
import ai.traceable.platform.actor.v1.ActorServiceGrpc;
import ai.traceable.platform.actor.v1.ActorServiceGrpc.ActorServiceBlockingStub;
import ai.traceable.region.config.service.v1.RegionConfigServiceGrpc.RegionConfigServiceBlockingStub;
import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.typesafe.config.Config;
import org.hypertrace.core.grpcutils.client.GrpcChannelRegistry;
import org.hypertrace.core.grpcutils.client.RequestContextClientCallCredsProviderFactory;

public class DataFetcherModule extends AbstractModule {
  private final Config config;

  public DataFetcherModule(Config config) {
    this.config = config;
  }

  @Override
  protected void configure() {
    requireBinding(AnomalyGlobalConfigServiceBlockingStub.class);
    requireBinding(DetectorConfigServiceBlockingStub.class);
    requireBinding(CustomSignatureConfigServiceBlockingStub.class);
    requireBinding(RegionConfigServiceBlockingStub.class);
    requireBinding(IpRangeConfigServiceBlockingStub.class);
    requireBinding(MaliciousSourcesConfigServiceBlockingStub.class);
  }

  @Provides
  ActorServiceConfig providesActorServiceConfig() {
    return new ActorServiceConfig(this.config);
  }

  @Provides
  ActorServiceBlockingStub providesActorServiceBlockingStub(
      ActorServiceConfig actorServiceConfig, GrpcChannelRegistry channelRegistry) {
    return ActorServiceGrpc.newBlockingStub(
            channelRegistry.forPlaintextAddress(
                actorServiceConfig.getHost(), actorServiceConfig.getPort()))
        .withCallCredentials(
            RequestContextClientCallCredsProviderFactory.getClientCallCredsProvider().get());
  }
}
