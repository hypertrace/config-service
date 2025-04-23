package ai.traceable.malicioussources.config.service;

import ai.traceable.malicioussources.config.service.rules.MaliciousSourcesConfigServiceConfig;
import ai.traceable.malicioussources.config.service.rules.RulesManagerModule;
import ai.traceable.malicioussources.config.service.rules.migration.MaliciousSourcesMigrationModule;
import com.google.inject.AbstractModule;
import com.typesafe.config.Config;
import io.grpc.BindableService;
import io.grpc.Channel;
import java.time.Clock;
import org.hypertrace.config.service.change.event.api.ConfigChangeEventGenerator;

public class MaliciousSourcesConfigServiceModule extends AbstractModule {
  private final Channel channel;
  private final ConfigChangeEventGenerator configChangeEventGenerator;
  private final MaliciousSourcesConfigServiceConfig config;

  MaliciousSourcesConfigServiceModule(
      Channel channel, ConfigChangeEventGenerator configChangeEventGenerator, Config config) {
    this.channel = channel;
    this.configChangeEventGenerator = configChangeEventGenerator;
    this.config = new MaliciousSourcesConfigServiceConfig(config);
  }

  @Override
  protected void configure() {
    bind(BindableService.class).to(MaliciousSourcesConfigServiceImpl.class);
    bind(Channel.class).toInstance(channel);
    bind(MaliciousSourcesConfigServiceConfig.class).toInstance(this.config);
    bind(ConfigChangeEventGenerator.class).toInstance(configChangeEventGenerator);
    bind(Clock.class).toInstance(Clock.systemUTC());
    install(new RulesManagerModule());
    install(new MaliciousSourcesMigrationModule());
  }
}
