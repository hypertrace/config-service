package ai.traceable.region.config.service;

import ai.traceable.activity.event.producer.ActivityEventProducer;
import ai.traceable.region.config.service.regions.RegionStoreModule;
import ai.traceable.region.config.service.rules.RulesManagerModule;
import com.google.inject.AbstractModule;
import com.typesafe.config.Config;
import io.grpc.BindableService;
import io.grpc.Channel;
import org.hypertrace.config.service.change.event.api.ConfigChangeEventGenerator;

class RegionConfigServiceModule extends AbstractModule {
  private final RegionConfigServiceConfig config;
  private final Channel channel;
  private final ActivityEventProducer activityEventProducer;
  private final ConfigChangeEventGenerator configChangeEventGenerator;

  RegionConfigServiceModule(
      Channel channel,
      Config config,
      ActivityEventProducer activityEventProducer,
      ConfigChangeEventGenerator configChangeEventGenerator) {
    this.channel = channel;
    this.config = new RegionConfigServiceConfig(config);
    this.activityEventProducer = activityEventProducer;
    this.configChangeEventGenerator = configChangeEventGenerator;
  }

  @Override
  protected void configure() {
    bind(BindableService.class).to(RegionConfigServiceImpl.class);
    bind(Channel.class).toInstance(channel);
    bind(ActivityEventProducer.class).toInstance(activityEventProducer);
    bind(RegionConfigServiceConfig.class).toInstance(this.config);
    bind(ConfigChangeEventGenerator.class).toInstance(configChangeEventGenerator);
    install(new RegionStoreModule());
    install(new RulesManagerModule());
  }
}
