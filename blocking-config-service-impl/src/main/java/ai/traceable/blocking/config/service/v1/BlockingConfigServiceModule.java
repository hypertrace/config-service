package ai.traceable.blocking.config.service.v1;

import ai.traceable.blocking.config.service.common.BlockingConfigCommonModule;
import ai.traceable.blocking.config.service.v1.blockingmodsec.ModsecBlockingManagerModule;
import ai.traceable.blocking.config.service.v1.blockingpolicy.BlockingPolicyConfigurationManagerModule;
import ai.traceable.blocking.config.service.v1.customsignature.CustomModsecBlockingManagerModule;
import ai.traceable.blocking.config.service.v1.iptype.IpTypeBlockingManagerModule;
import ai.traceable.blocking.config.service.v1.regions.RegionBlockingManagerModule;
import ai.traceable.customsignature.config.service.v1.CustomSignatureConfigServiceGrpc;
import ai.traceable.customsignature.config.service.v1.CustomSignatureConfigServiceGrpc.CustomSignatureConfigServiceBlockingStub;
import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.typesafe.config.Config;
import io.grpc.BindableService;
import io.grpc.Channel;
import org.hypertrace.core.grpcutils.client.GrpcChannelRegistry;
import org.hypertrace.core.grpcutils.client.RequestContextClientCallCredsProviderFactory;

class BlockingConfigServiceModule extends AbstractModule {
  private static final String BLOCKING_CONFIG_SERVICE_CONFIG_NAME = "blocking.config.service";

  private final Channel channel;
  private final Config config;
  private final GrpcChannelRegistry grpcChannelRegistry;

  public BlockingConfigServiceModule(
      Channel channel, Config config, GrpcChannelRegistry grpcChannelRegistry) {
    this.channel = channel;
    this.config = config.getConfig(BLOCKING_CONFIG_SERVICE_CONFIG_NAME);
    this.grpcChannelRegistry = grpcChannelRegistry;
  }

  @Override
  protected void configure() {
    bind(BindableService.class).to(BlockingConfigServiceImpl.class);
    bind(Channel.class).toInstance(channel);
    install(new RegionBlockingManagerModule());
    install(new CustomModsecBlockingManagerModule());
    install(new ModsecBlockingManagerModule());
    install(new BlockingPolicyConfigurationManagerModule());
    install(new IpTypeBlockingManagerModule());
    install(new BlockingConfigCommonModule(channel, config, grpcChannelRegistry));
  }

  @Provides
  CustomSignatureConfigServiceBlockingStub providesCustomSignatureConfigServiceStub(
      Channel channel) {
    return CustomSignatureConfigServiceGrpc.newBlockingStub(channel)
        .withCallCredentials(
            RequestContextClientCallCredsProviderFactory.getClientCallCredsProvider().get());
  }
}
