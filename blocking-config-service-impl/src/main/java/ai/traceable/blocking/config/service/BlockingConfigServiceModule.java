package ai.traceable.blocking.config.service;

import ai.traceable.blocking.config.service.customsignature.CustomModsecBlockingManagerModule;
import ai.traceable.blocking.config.service.regions.RegionBlockingManagerModule;
import ai.traceable.blocking.config.service.safecrs.SafeCrsBlockingManagerModule;
import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.typesafe.config.Config;
import io.grpc.BindableService;
import io.grpc.ManagedChannel;
import java.time.Clock;

class BlockingConfigServiceModule extends AbstractModule {
  private final ManagedChannel channel;
  private final Config config;

  public BlockingConfigServiceModule(ManagedChannel channel, Config config) {
    this.channel = channel;
    this.config = config.getConfig("blocking.config.service");
  }

  @Override
  protected void configure() {
    bind(Clock.class).toInstance(Clock.systemUTC());
    bind(BindableService.class).to(BlockingConfigServiceImpl.class);
    bind(ManagedChannel.class).toInstance(channel);
    install(new RegionBlockingManagerModule());
    install(new CustomModsecBlockingManagerModule());
    install(new SafeCrsBlockingManagerModule());
  }

  @Provides
  BlockingConfigServiceConfig providesCustomSignatureServiceConfig() {
    return new BlockingConfigServiceConfig(this.config);
  }
}
