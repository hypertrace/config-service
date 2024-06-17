package ai.traceable.ast.config.service;

import ai.traceable.ast.config.service.configs.AstConfigServiceConfig;
import ai.traceable.ast.config.service.rules.RulesManagerModule;
import ai.traceable.ast.config.service.validation.AstConfigServiceRequestValidator;
import ai.traceable.ast.config.service.validation.AstConfigServiceRequestValidatorImpl;
import com.google.inject.AbstractModule;
import com.typesafe.config.Config;
import io.grpc.BindableService;
import io.grpc.Channel;
import org.hypertrace.config.service.change.event.api.ConfigChangeEventGenerator;

class AstConfigServiceModule extends AbstractModule {
  private final Channel channel;
  private final AstConfigServiceConfig astConfigServiceConfig;
  private final ConfigChangeEventGenerator configChangeEventGenerator;

  AstConfigServiceModule(
      Channel channel, ConfigChangeEventGenerator configChangeEventGenerator, Config config) {
    this.channel = channel;
    this.configChangeEventGenerator = configChangeEventGenerator;
    this.astConfigServiceConfig = new AstConfigServiceConfig(config);
  }

  @Override
  protected void configure() {
    bind(ConfigChangeEventGenerator.class).toInstance(configChangeEventGenerator);
    bind(AstConfigServiceConfig.class).toInstance(astConfigServiceConfig);
    bind(BindableService.class).to(AstConfigServiceImpl.class);
    bind(Channel.class).toInstance(channel);
    bind(AstConfigServiceRequestValidator.class).to(AstConfigServiceRequestValidatorImpl.class);
    install(new RulesManagerModule());
  }
}
