package ai.traceable.ast.config.service;

import com.google.inject.Guice;
import com.google.inject.Injector;
import com.typesafe.config.Config;
import io.grpc.BindableService;
import io.grpc.Channel;

public class AstConfigServiceFactory {
  private AstConfigServiceFactory() {}

  public static BindableService build(Channel channel, Config config) {
    Injector injector = Guice.createInjector(new AstConfigServiceModule(channel, config));
    return injector.getInstance(BindableService.class);
  }
}
