package ai.traceable.ast.hooks.config.service;

import com.google.inject.Guice;
import com.google.inject.Injector;
import io.grpc.BindableService;
import io.grpc.Channel;

public class AstHooksConfigServiceFactory {

  public static BindableService build(Channel channel) {
    Injector injector = Guice.createInjector(new AstHooksConfigServiceModule(channel));
    return injector.getInstance(BindableService.class);
  }
}
