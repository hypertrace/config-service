package ai.traceable.blocking.config.service;

import ai.traceable.blocking.config.service.blockingmodsec.ModsecBlockingManagerModule;
import ai.traceable.blocking.config.service.customsignature.CustomModsecBlockingManagerModule;
import ai.traceable.blocking.config.service.regions.RegionBlockingManagerModule;
import com.google.inject.AbstractModule;
import io.grpc.BindableService;
import io.grpc.Channel;
import java.time.Clock;

class BlockingConfigServiceModule extends AbstractModule {
  private final Channel channel;

  public BlockingConfigServiceModule(Channel channel) {
    this.channel = channel;
  }

  @Override
  protected void configure() {
    bind(Clock.class).toInstance(Clock.systemUTC());
    bind(BindableService.class).to(BlockingConfigServiceImpl.class);
    bind(Channel.class).toInstance(channel);
    install(new RegionBlockingManagerModule());
    install(new CustomModsecBlockingManagerModule());
    install(new ModsecBlockingManagerModule());
  }
}
