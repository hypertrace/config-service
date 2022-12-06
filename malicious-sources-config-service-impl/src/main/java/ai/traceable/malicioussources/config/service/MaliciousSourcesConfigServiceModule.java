package ai.traceable.malicioussources.config.service;

import ai.traceable.malicioussources.config.service.rules.RulesManagerModule;
import com.google.inject.AbstractModule;
import io.grpc.BindableService;
import io.grpc.Channel;
import java.time.Clock;
import org.hypertrace.config.service.change.event.api.ConfigChangeEventGenerator;

public class MaliciousSourcesConfigServiceModule extends AbstractModule {
  private final Channel channel;
  private final ConfigChangeEventGenerator configChangeEventGenerator;

  MaliciousSourcesConfigServiceModule(
      Channel channel, ConfigChangeEventGenerator configChangeEventGenerator) {
    this.channel = channel;
    this.configChangeEventGenerator = configChangeEventGenerator;
  }

  @Override
  protected void configure() {
    bind(BindableService.class).to(MaliciousSourcesConfigServiceImpl.class);
    bind(Channel.class).toInstance(channel);
    bind(ConfigChangeEventGenerator.class).toInstance(configChangeEventGenerator);
    bind(Clock.class).toInstance(Clock.systemUTC());
    install(new RulesManagerModule());
  }
}
