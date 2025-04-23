package ai.traceable.iprange.config.service;

import ai.traceable.activity.event.producer.ActivityEventProducer;
import ai.traceable.iprange.config.service.rules.RulesManagerModule;
import ai.traceable.iprange.config.service.rules.migration.IpRangeRulesMigrationModule;
import com.google.inject.AbstractModule;
import com.typesafe.config.Config;
import io.grpc.BindableService;
import io.grpc.Channel;
import java.time.Clock;
import org.hypertrace.config.service.change.event.api.ConfigChangeEventGenerator;

class IpRangeConfigServiceModule extends AbstractModule {
  private final Channel channel;
  private final IpRangeConfigServiceConfig config;
  private final ActivityEventProducer activityEventProducer;
  private final ConfigChangeEventGenerator configChangeEventGenerator;

  IpRangeConfigServiceModule(
      Channel channel,
      Config config,
      ActivityEventProducer activityEventProducer,
      ConfigChangeEventGenerator configChangeEventGenerator) {
    this.channel = channel;
    this.config = new IpRangeConfigServiceConfig(config);
    this.activityEventProducer = activityEventProducer;
    this.configChangeEventGenerator = configChangeEventGenerator;
  }

  @Override
  protected void configure() {
    bind(BindableService.class).to(IpRangeConfigServiceImpl.class);
    bind(Channel.class).toInstance(channel);
    bind(IpRangeConfigServiceConfig.class).toInstance(this.config);
    bind(ActivityEventProducer.class).toInstance(activityEventProducer);
    bind(ConfigChangeEventGenerator.class).toInstance(configChangeEventGenerator);
    bind(Clock.class).toInstance(Clock.systemUTC());
    install(new RulesManagerModule());
    install(new IpRangeRulesMigrationModule());
  }
}
