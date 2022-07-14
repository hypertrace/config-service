package ai.traceable.region.config.service;

import ai.traceable.activity.event.producer.ActivityEventProducer;
import ai.traceable.region.config.service.regions.RegionStoreModule;
import ai.traceable.region.config.service.rules.RulesManagerModule;
import com.google.inject.AbstractModule;
import com.typesafe.config.Config;
import io.grpc.BindableService;
import io.grpc.Channel;

class RegionConfigServiceModule extends AbstractModule {
  private final RegionConfigServiceConfig config;
  private final Channel channel;
  private final ActivityEventProducer activityEventProducer;

  RegionConfigServiceModule(
      Channel channel, Config config, ActivityEventProducer activityEventProducer) {
    this.channel = channel;
    this.config = new RegionConfigServiceConfig(config);
    this.activityEventProducer = activityEventProducer;
  }

  @Override
  protected void configure() {
    bind(BindableService.class).to(RegionConfigServiceImpl.class);
    bind(Channel.class).toInstance(channel);
    bind(ActivityEventProducer.class).toInstance(activityEventProducer);
    bind(RegionConfigServiceConfig.class).toInstance(this.config);
    install(new RegionStoreModule());
    install(new RulesManagerModule());
  }
}
