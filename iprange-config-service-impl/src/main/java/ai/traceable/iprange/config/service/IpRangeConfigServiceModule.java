package ai.traceable.iprange.config.service;

import ai.traceable.activity.event.producer.ActivityEventProducer;
import ai.traceable.iprange.config.service.rules.RulesManagerModule;
import com.google.inject.AbstractModule;
import com.typesafe.config.Config;
import io.grpc.BindableService;
import io.grpc.Channel;
import java.time.Clock;

class IpRangeConfigServiceModule extends AbstractModule {
  private final Channel channel;
  private final IpRangeConfigServiceConfig config;
  private final ActivityEventProducer activityEventProducer;

  IpRangeConfigServiceModule(
      Channel channel, Config config, ActivityEventProducer activityEventProducer) {
    this.channel = channel;
    this.config = new IpRangeConfigServiceConfig(config);
    this.activityEventProducer = activityEventProducer;
  }

  @Override
  protected void configure() {
    bind(BindableService.class).to(IpRangeConfigServiceImpl.class);
    bind(Channel.class).toInstance(channel);
    bind(IpRangeConfigServiceConfig.class).toInstance(this.config);
    bind(ActivityEventProducer.class).toInstance(activityEventProducer);
    bind(Clock.class).toInstance(Clock.systemUTC());
    install(new RulesManagerModule());
  }
}
