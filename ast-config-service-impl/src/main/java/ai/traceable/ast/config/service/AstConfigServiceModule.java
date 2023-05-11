package ai.traceable.ast.config.service;

import ai.traceable.ast.config.service.rules.RulesManagerModule;
import com.google.inject.AbstractModule;
import com.typesafe.config.Config;
import io.grpc.BindableService;
import io.grpc.Channel;
import org.hypertrace.config.service.change.event.api.ConfigChangeEventGenerator;

class AstConfigServiceModule extends AbstractModule {
  private final Channel channel;
  private final AstConfigServiceConfig config;
  private final ConfigChangeEventGenerator configChangeEventGenerator;

  AstConfigServiceModule(
      Channel channel, ConfigChangeEventGenerator configChangeEventGenerator, Config config) {
    this.channel = channel;
    this.configChangeEventGenerator = configChangeEventGenerator;
    this.config = new AstConfigServiceConfig(config);
  }

  @Override
  protected void configure() {
    bind(ConfigChangeEventGenerator.class).toInstance(configChangeEventGenerator);
    bind(BindableService.class).to(AstConfigServiceImpl.class);
    bind(Channel.class).toInstance(channel);
    bind(AstConfigServiceConfig.class).toInstance(config);
    install(new RulesManagerModule());
  }
}
