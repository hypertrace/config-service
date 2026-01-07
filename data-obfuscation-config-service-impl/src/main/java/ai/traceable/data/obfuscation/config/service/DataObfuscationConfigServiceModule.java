package ai.traceable.data.obfuscation.config.service;

import ai.traceable.data.obfuscation.config.service.rules.ConfigManagerModule;
import com.google.inject.AbstractModule;
import io.grpc.BindableService;
import io.grpc.Channel;
import java.time.Clock;
import org.hypertrace.config.service.change.event.api.ConfigChangeEventGenerator;

class DataObfuscationConfigServiceModule extends AbstractModule {
  private final Channel channel;
  private final ConfigChangeEventGenerator configChangeEventGenerator;

  DataObfuscationConfigServiceModule(
      Channel channel, ConfigChangeEventGenerator configChangeEventGenerator) {
    this.channel = channel;
    this.configChangeEventGenerator = configChangeEventGenerator;
  }

  @Override
  protected void configure() {
    bind(BindableService.class).to(DataObfuscationConfigServiceImpl.class);
    bind(Channel.class).toInstance(channel);
    bind(ConfigChangeEventGenerator.class).toInstance(configChangeEventGenerator);
    bind(Clock.class).toInstance(Clock.systemUTC());
    install(new ConfigManagerModule());
  }
}
