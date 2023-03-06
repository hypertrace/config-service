package ai.traceable.ast.config.service;

import ai.traceable.ast.config.service.rules.RulesManagerModule;
import com.google.inject.AbstractModule;
import com.typesafe.config.Config;
import io.grpc.BindableService;
import io.grpc.Channel;

class AstConfigServiceModule extends AbstractModule {
  private final Channel channel;
  private final AstConfigServiceConfig config;

  AstConfigServiceModule(Channel channel, Config config) {
    this.channel = channel;
    this.config = new AstConfigServiceConfig(config);
  }

  @Override
  protected void configure() {
    bind(BindableService.class).to(AstConfigServiceImpl.class);
    bind(Channel.class).toInstance(channel);
    bind(AstConfigServiceConfig.class).toInstance(config);
    install(new RulesManagerModule());
  }
}
