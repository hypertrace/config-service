package ai.traceable.blocking.config.service;

import com.google.inject.Guice;
import com.google.inject.Injector;
import io.grpc.BindableService;
import io.grpc.Channel;

public class BlockingConfigServiceFactory {
  public static BindableService build(Channel channel) {
    Injector injector = Guice.createInjector(new BlockingConfigServiceModule(channel));
    return injector.getInstance(BindableService.class);
  }
}
