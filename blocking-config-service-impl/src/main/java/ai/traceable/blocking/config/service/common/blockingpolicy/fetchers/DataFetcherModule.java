package ai.traceable.blocking.config.service.common.blockingpolicy.fetchers;

import ai.traceable.anomaly.config.service.v1.detector.DetectorConfigServiceGrpc;
import ai.traceable.anomaly.config.service.v1.detector.DetectorConfigServiceGrpc.DetectorConfigServiceBlockingStub;
import ai.traceable.anomaly.config.service.v1.global.AnomalyGlobalConfigServiceGrpc;
import ai.traceable.anomaly.config.service.v1.global.AnomalyGlobalConfigServiceGrpc.AnomalyGlobalConfigServiceBlockingStub;
import ai.traceable.blocking.config.service.common.blockingpolicy.fetchers.actor.config.ActorServiceConfig;
import ai.traceable.blocking.config.service.common.blockingpolicy.fetchers.malicioussources.handlers.MaliciousSourcesDataFetcherModule;
import ai.traceable.customsignature.config.service.v1.CustomSignatureConfigServiceGrpc.CustomSignatureConfigServiceBlockingStub;
import ai.traceable.iprange.config.service.v1.IpRangeConfigServiceGrpc.IpRangeConfigServiceBlockingStub;
import ai.traceable.malicioussources.config.service.v1.MaliciousSourcesConfigServiceGrpc.MaliciousSourcesConfigServiceBlockingStub;
import ai.traceable.platform.actor.v1.ActorServiceGrpc;
import ai.traceable.platform.actor.v1.ActorServiceGrpc.ActorServiceBlockingStub;
import ai.traceable.region.config.service.v1.RegionConfigServiceGrpc.RegionConfigServiceBlockingStub;
import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.multibindings.Multibinder;
import com.typesafe.config.Config;
import io.grpc.Channel;
import org.hypertrace.core.grpcutils.client.GrpcChannelRegistry;
import org.hypertrace.core.grpcutils.client.RequestContextClientCallCredsProviderFactory;

public class DataFetcherModule extends AbstractModule {
  private final Config config;
  private final GrpcChannelRegistry grpcChannelRegistry;

  public DataFetcherModule(Config config, GrpcChannelRegistry grpcChannelRegistry) {
    this.config = config;
    this.grpcChannelRegistry = grpcChannelRegistry;
  }

  @Override
  protected void configure() {
    install(new MaliciousSourcesDataFetcherModule());

    Multibinder<DataFetcherBase> managerBaseMultibinder =
        Multibinder.newSetBinder(binder(), DataFetcherBase.class);
    managerBaseMultibinder.addBinding().to(CustomSignatureDataFetcher.class);
    managerBaseMultibinder.addBinding().to(CustomIpBasedDataFetcher.class);
    managerBaseMultibinder.addBinding().to(ActorBasedDataFetcher.class);
    managerBaseMultibinder.addBinding().to(ModsecDataFetcher.class);
    managerBaseMultibinder.addBinding().to(RegionDataFetcher.class);
    managerBaseMultibinder.addBinding().to(MaliciousSourcesDataFetcher.class);

    bind(GrpcChannelRegistry.class).toInstance(grpcChannelRegistry);
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

  @Provides
  AnomalyGlobalConfigServiceBlockingStub providesAnomalyGlobalConfigServiceBlockingStub(
      Channel channel) {
    return AnomalyGlobalConfigServiceGrpc.newBlockingStub(channel)
        .withCallCredentials(
            RequestContextClientCallCredsProviderFactory.getClientCallCredsProvider().get());
  }

  @Provides
  DetectorConfigServiceBlockingStub providesDetectorConfigServiceBlockingStub(Channel channel) {
    return DetectorConfigServiceGrpc.newBlockingStub(channel)
        .withCallCredentials(
            RequestContextClientCallCredsProviderFactory.getClientCallCredsProvider().get());
  }
}
