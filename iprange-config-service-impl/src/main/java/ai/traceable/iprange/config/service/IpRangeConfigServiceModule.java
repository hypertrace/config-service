package ai.traceable.iprange.config.service;

import ai.traceable.iprange.config.service.rules.RulesManagerModule;
import com.google.inject.AbstractModule;
import io.grpc.BindableService;
import io.grpc.ManagedChannel;
import java.time.Clock;

class IpRangeConfigServiceModule extends AbstractModule {
  private final ManagedChannel channel;

  IpRangeConfigServiceModule(ManagedChannel channel) {
    this.channel = channel;
  }

  @Override
  protected void configure() {
    bind(BindableService.class).to(IpRangeConfigServiceImpl.class);
    bind(ManagedChannel.class).toInstance(channel);
    bind(Clock.class).toInstance(Clock.systemUTC());
    install(new RulesManagerModule());
  }
}
