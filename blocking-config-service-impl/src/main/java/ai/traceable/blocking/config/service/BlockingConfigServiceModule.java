package ai.traceable.blocking.config.service;

import ai.traceable.anomaly.config.service.v1.detector.DetectorConfigServiceGrpc;
import ai.traceable.anomaly.config.service.v1.detector.DetectorConfigServiceGrpc.DetectorConfigServiceBlockingStub;
import ai.traceable.anomaly.config.service.v1.global.AnomalyGlobalConfigServiceGrpc;
import ai.traceable.anomaly.config.service.v1.global.AnomalyGlobalConfigServiceGrpc.AnomalyGlobalConfigServiceBlockingStub;
import ai.traceable.anomaly.config.service.v1.modsec.AnomalyModsecConfigServiceGrpc;
import ai.traceable.anomaly.config.service.v1.modsec.AnomalyModsecConfigServiceGrpc.AnomalyModsecConfigServiceBlockingStub;
import ai.traceable.blocking.config.service.common.blockingpolicy.fetchers.actor.config.ActorServiceConfig;
import ai.traceable.config.utils.refresh.FileRefreshConfig;
import ai.traceable.customsignature.config.service.v1.CustomSignatureConfigServiceGrpc;
import ai.traceable.customsignature.config.service.v1.CustomSignatureConfigServiceGrpc.CustomSignatureConfigServiceBlockingStub;
import ai.traceable.detection.exclusion.config.service.v1.DetectionExclusionConfigServiceGrpc;
import ai.traceable.detection.exclusion.config.service.v1.DetectionExclusionConfigServiceGrpc.DetectionExclusionConfigServiceBlockingStub;
import ai.traceable.iprange.config.service.v1.IpRangeConfigServiceGrpc;
import ai.traceable.iprange.config.service.v1.IpRangeConfigServiceGrpc.IpRangeConfigServiceBlockingStub;
import ai.traceable.ipresolutionstrategy.config.service.v1.IpResolutionStrategyConfigServiceGrpc;
import ai.traceable.ipresolutionstrategy.config.service.v1.IpResolutionStrategyConfigServiceGrpc.IpResolutionStrategyConfigServiceBlockingStub;
import ai.traceable.malicioussources.config.service.v1.MaliciousSourcesConfigServiceGrpc;
import ai.traceable.malicioussources.config.service.v1.MaliciousSourcesConfigServiceGrpc.MaliciousSourcesConfigServiceBlockingStub;
import ai.traceable.platform.actor.v1.ActorServiceGrpc;
import ai.traceable.platform.actor.v1.ActorServiceGrpc.ActorServiceBlockingStub;
import ai.traceable.ratelimiting.config.service.v2.RateLimitingConfigServiceGrpc;
import ai.traceable.ratelimiting.config.service.v2.RateLimitingConfigServiceGrpc.RateLimitingConfigServiceBlockingStub;
import ai.traceable.region.config.service.v1.RegionConfigServiceGrpc;
import ai.traceable.region.config.service.v1.RegionConfigServiceGrpc.RegionConfigServiceBlockingStub;
import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.typesafe.config.Config;
import io.grpc.Channel;
import org.hypertrace.config.objectstore.ClientConfig;
import org.hypertrace.core.grpcutils.client.GrpcChannelRegistry;
import org.hypertrace.core.grpcutils.client.RequestContextClientCallCredsProviderFactory;

public class BlockingConfigServiceModule extends AbstractModule {
  private static final String BLOCKING_CONFIG_SERVICE_CONFIG_NAME = "blocking.config.service";
  private static final String IP_TYPE_BLOCKING_CONFIG = "iptype";

  private final Channel channel;
  private final Config config;
  private final GrpcChannelRegistry grpcChannelRegistry;

  public BlockingConfigServiceModule(
      Channel channel, Config config, GrpcChannelRegistry grpcChannelRegistry) {
    this.channel = channel;
    this.config = config;
    this.grpcChannelRegistry = grpcChannelRegistry;
  }

  @Override
  protected void configure() {
    bind(Channel.class).toInstance(channel);
    bind(Config.class).toInstance(config.getConfig(BLOCKING_CONFIG_SERVICE_CONFIG_NAME));
    bind(GrpcChannelRegistry.class).toInstance(grpcChannelRegistry);
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
  MaliciousSourcesConfigServiceBlockingStub providesMaliciousSourcesConfigServiceBlockingStub(
      Channel channel) {
    return MaliciousSourcesConfigServiceGrpc.newBlockingStub(channel)
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

  @Provides
  AnomalyModsecConfigServiceBlockingStub providesAnomalyModsecConfigServiceBlockingStub(
      Channel channel) {
    return AnomalyModsecConfigServiceGrpc.newBlockingStub(channel)
        .withCallCredentials(
            RequestContextClientCallCredsProviderFactory.getClientCallCredsProvider().get());
  }

  @Provides
  RateLimitingConfigServiceBlockingStub providesRateLimitingConfigServiceBlockingStub(
      Channel channel) {
    return RateLimitingConfigServiceGrpc.newBlockingStub(channel)
        .withCallCredentials(
            RequestContextClientCallCredsProviderFactory.getClientCallCredsProvider().get());
  }

  @Provides
  DetectionExclusionConfigServiceBlockingStub providesDetectionExclusionConfigServiceBlockingStub(
      Channel channel) {
    return DetectionExclusionConfigServiceGrpc.newBlockingStub(channel)
        .withCallCredentials(
            RequestContextClientCallCredsProviderFactory.getClientCallCredsProvider().get());
  }

  @Provides
  IpResolutionStrategyConfigServiceBlockingStub
      providesIpResolutionStrategyConfigServiceBlockingStub(Channel channel) {
    return IpResolutionStrategyConfigServiceGrpc.newBlockingStub(channel)
        .withCallCredentials(
            RequestContextClientCallCredsProviderFactory.getClientCallCredsProvider().get());
  }

  @Provides
  FileRefreshConfig providesFileRefreshConfig() {
    return new FileRefreshConfig(
        config.getConfig(BLOCKING_CONFIG_SERVICE_CONFIG_NAME).getConfig(IP_TYPE_BLOCKING_CONFIG));
  }

  @Provides
  ClientConfig providesClientConfig() {
    return ClientConfig.DEFAULT;
  }
}
