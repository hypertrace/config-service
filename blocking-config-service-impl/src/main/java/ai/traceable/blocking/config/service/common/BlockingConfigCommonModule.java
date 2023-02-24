package ai.traceable.blocking.config.service.common;

import ai.traceable.blocking.config.service.common.blockingpolicy.BlockingPolicyConfigurationCommonModule;
import ai.traceable.blocking.config.service.common.iptype.IpTypeCommonModule;
import ai.traceable.blocking.config.service.common.modsec.ModsecCommonModule;
import ai.traceable.blocking.config.service.common.regions.RegionRulesCommonModule;
import ai.traceable.customsignature.config.service.v1.CustomSignatureConfigServiceGrpc.CustomSignatureConfigServiceBlockingStub;
import ai.traceable.iprange.config.service.v1.IpRangeConfigServiceGrpc;
import ai.traceable.iprange.config.service.v1.IpRangeConfigServiceGrpc.IpRangeConfigServiceBlockingStub;
import ai.traceable.malicioussources.config.service.v1.MaliciousSourcesConfigServiceGrpc;
import ai.traceable.malicioussources.config.service.v1.MaliciousSourcesConfigServiceGrpc.MaliciousSourcesConfigServiceBlockingStub;
import ai.traceable.region.config.service.v1.RegionConfigServiceGrpc;
import ai.traceable.region.config.service.v1.RegionConfigServiceGrpc.RegionConfigServiceBlockingStub;
import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.typesafe.config.Config;
import io.grpc.Channel;
import java.time.Clock;
import org.hypertrace.core.grpcutils.client.GrpcChannelRegistry;
import org.hypertrace.core.grpcutils.client.RequestContextClientCallCredsProviderFactory;

public class BlockingConfigCommonModule extends AbstractModule {
  private final Channel channel;
  private final Config config;
  private final GrpcChannelRegistry grpcChannelRegistry;

  public BlockingConfigCommonModule(
      Channel channel, Config config, GrpcChannelRegistry grpcChannelRegistry) {
    this.channel = channel;
    this.config = config;
    this.grpcChannelRegistry = grpcChannelRegistry;
  }

  @Override
  protected void configure() {
    bind(Clock.class).toInstance(Clock.systemUTC());
    bind(Channel.class).toInstance(channel);
    install(new BlockingPolicyConfigurationCommonModule(config, grpcChannelRegistry));
    install(new IpTypeCommonModule(config));
    install(new RegionRulesCommonModule());
    install(new ModsecCommonModule());
    requireBinding(CustomSignatureConfigServiceBlockingStub.class);
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
}
