package ai.traceable.blocking.config.service;

import ai.traceable.blocking.config.service.customsignature.CustomModsecBlockingManagerModule;
import ai.traceable.blocking.config.service.regions.RegionBlockingManagerModule;
import com.google.inject.AbstractModule;
import io.grpc.BindableService;
import io.grpc.ManagedChannel;
import java.time.Clock;

class BlockingConfigServiceModule extends AbstractModule {
  private final ManagedChannel channel;

  BlockingConfigServiceModule(ManagedChannel channel) {
    this.channel = channel;
  }

  @Override
  protected void configure() {
    bind(Clock.class).toInstance(Clock.systemUTC());
    bind(BindableService.class).to(BlockingConfigServiceImpl.class);
    bind(ManagedChannel.class).toInstance(channel);
    install(new RegionBlockingManagerModule());
    install(new CustomModsecBlockingManagerModule());
  }
}
