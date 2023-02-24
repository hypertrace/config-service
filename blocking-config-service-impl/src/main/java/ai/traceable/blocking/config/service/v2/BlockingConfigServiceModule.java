package ai.traceable.blocking.config.service.v2;

import ai.traceable.blocking.config.service.common.BlockingConfigCommonModule;
import ai.traceable.blocking.config.service.v2.blockingpolicy.BlockingPolicyConfigurationManager;
import ai.traceable.blocking.config.service.v2.blockingpolicy.BlockingPolicyConfigurationManagerModule;
import ai.traceable.blocking.config.service.v2.iptype.IpTypeBlockingManager;
import ai.traceable.blocking.config.service.v2.iptype.IpTypeBlockingManagerModule;
import ai.traceable.blocking.config.service.v2.modsec.ModsecBlockingManager;
import ai.traceable.blocking.config.service.v2.regions.RegionBlockingManager;
import ai.traceable.blocking.config.service.v2.regions.RegionBlockingManagerModule;
import ai.traceable.customsignature.config.service.v1.CustomSignatureConfigServiceGrpc;
import ai.traceable.customsignature.config.service.v1.CustomSignatureConfigServiceGrpc.CustomSignatureConfigServiceBlockingStub;
import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.multibindings.Multibinder;
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
    Multibinder<BlockingConfigManagerBase> managerBaseMultibinder =
        Multibinder.newSetBinder(binder(), BlockingConfigManagerBase.class);
    managerBaseMultibinder.addBinding().to(BlockingPolicyConfigurationManager.class);
    managerBaseMultibinder.addBinding().to(IpTypeBlockingManager.class);
    managerBaseMultibinder.addBinding().to(ModsecBlockingManager.class);
    managerBaseMultibinder.addBinding().to(RegionBlockingManager.class);

    bind(Channel.class).toInstance(channel);
    bind(Config.class).toInstance(config);

    install(new RegionBlockingManagerModule());
    install(new BlockingPolicyConfigurationManagerModule());
    install(new IpTypeBlockingManagerModule());
    install(new BlockingConfigCommonModule(channel, config, grpcChannelRegistry));

    bind(BindableService.class).to(BlockingConfigServiceImpl.class);
  }

  @Provides
  CustomSignatureConfigServiceBlockingStub providesCustomSignatureConfigServiceStub(
      Channel channel) {
    return CustomSignatureConfigServiceGrpc.newBlockingStub(channel)
        .withCallCredentials(
            RequestContextClientCallCredsProviderFactory.getClientCallCredsProvider().get());
  }
}
