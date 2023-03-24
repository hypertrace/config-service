package ai.traceable.ast.hooks.config.service;

import com.google.inject.AbstractModule;
import io.grpc.BindableService;
import io.grpc.Channel;

public class AstHooksConfigServiceModule extends AbstractModule {
  private final Channel channel;

  AstHooksConfigServiceModule(Channel channel) {
    this.channel = channel;
  }

  @Override
  protected void configure() {
    bind(BindableService.class).to(AstHooksConfigServiceImpl.class);
    bind(Channel.class).toInstance(channel);
  }
}
