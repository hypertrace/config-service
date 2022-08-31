package ai.traceable.blocking.config.service;

import ai.traceable.anomaly.config.service.v1.detector.DetectorConfigServiceGrpc;
import ai.traceable.anomaly.config.service.v1.detector.DetectorConfigServiceGrpc.DetectorConfigServiceBlockingStub;
import ai.traceable.anomaly.config.service.v1.global.AnomalyGlobalConfigServiceGrpc;
import ai.traceable.anomaly.config.service.v1.global.AnomalyGlobalConfigServiceGrpc.AnomalyGlobalConfigServiceBlockingStub;
import ai.traceable.blocking.config.service.blockingmodsec.ModsecBlockingManagerModule;
import ai.traceable.blocking.config.service.blockingpolicy.BlockingPolicyConfigurationManagerModule;
import ai.traceable.blocking.config.service.blockingpolicy.fetchers.actor.config.ActorServiceConfig;
import ai.traceable.blocking.config.service.customsignature.CustomModsecBlockingManagerModule;
import ai.traceable.blocking.config.service.entity.EntityQueryServiceConfig;
import ai.traceable.blocking.config.service.regions.RegionBlockingManagerModule;
import ai.traceable.customsignature.config.service.v1.CustomSignatureConfigServiceGrpc;
import ai.traceable.customsignature.config.service.v1.CustomSignatureConfigServiceGrpc.CustomSignatureConfigServiceBlockingStub;
import ai.traceable.iprange.config.service.v1.IpRangeConfigServiceGrpc;
import ai.traceable.iprange.config.service.v1.IpRangeConfigServiceGrpc.IpRangeConfigServiceBlockingStub;
import ai.traceable.platform.actor.v1.ActorServiceGrpc;
import ai.traceable.platform.actor.v1.ActorServiceGrpc.ActorServiceBlockingStub;
import ai.traceable.region.config.service.v1.RegionConfigServiceGrpc;
import ai.traceable.region.config.service.v1.RegionConfigServiceGrpc.RegionConfigServiceBlockingStub;
import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.typesafe.config.Config;
import io.grpc.BindableService;
import io.grpc.Channel;
import java.time.Clock;
import org.hypertrace.core.grpcutils.client.GrpcChannelRegistry;
import org.hypertrace.core.grpcutils.client.RequestContextClientCallCredsProviderFactory;

class BlockingConfigServiceModule extends AbstractModule {
  private final Channel channel;
  private final Config config;

  public BlockingConfigServiceModule(Channel channel, Config config) {
    this.channel = channel;
    this.config = config;
  }

  @Override
  protected void configure() {
    bind(Clock.class).toInstance(Clock.systemUTC());
    bind(BindableService.class).to(BlockingConfigServiceImpl.class);
    bind(Channel.class).toInstance(channel);
    install(new RegionBlockingManagerModule());
    install(new CustomModsecBlockingManagerModule());
    install(new ModsecBlockingManagerModule());
    install(new BlockingPolicyConfigurationManagerModule());
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

  @Provides
  CustomSignatureConfigServiceBlockingStub providesCustomSignatureConfigServiceStub(
      Channel channel) {
    return CustomSignatureConfigServiceGrpc.newBlockingStub(channel)
        .withCallCredentials(
            RequestContextClientCallCredsProviderFactory.getClientCallCredsProvider().get());
  }

  @Provides
  RegionConfigServiceBlockingStub providesRegionConfigServiceBlockingStub(Channel channel) {
    return RegionConfigServiceGrpc.newBlockingStub(channel)
        .withCallCredentials(
            RequestContextClientCallCredsProviderFactory.getClientCallCredsProvider().get());
  }

  @Provides
  IpRangeConfigServiceBlockingStub providesIpRangeConfigServiceBlockingStub(Channel channel) {
    return IpRangeConfigServiceGrpc.newBlockingStub(channel)
        .withCallCredentials(
            RequestContextClientCallCredsProviderFactory.getClientCallCredsProvider().get());
  }

  @Provides
  ActorServiceConfig providesActorServiceConfig() {
    return new ActorServiceConfig(this.config);
  }

  @Provides
  EntityQueryServiceConfig providesEntityQueryServiceConfig() {
    return new EntityQueryServiceConfig(this.config);
  }

  @Provides
  ActorServiceBlockingStub providesActorServiceBlockingStub(ActorServiceConfig actorServiceConfig) {
    GrpcChannelRegistry channelRegistry = new GrpcChannelRegistry();
    return ActorServiceGrpc.newBlockingStub(
            channelRegistry.forPlaintextAddress(
                actorServiceConfig.getHost(), actorServiceConfig.getPort()))
        .withCallCredentials(
            RequestContextClientCallCredsProviderFactory.getClientCallCredsProvider().get());
  }
}
