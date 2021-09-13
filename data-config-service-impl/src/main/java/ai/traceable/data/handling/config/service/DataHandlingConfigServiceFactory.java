package ai.traceable.data.handling.config.service;

import com.google.inject.Guice;
import com.google.inject.Injector;
import io.grpc.BindableService;
import io.grpc.Channel;

public class DataHandlingConfigServiceFactory {

  private DataHandlingConfigServiceFactory() {}

  public static BindableService build(Channel channel) {
    Injector injector = Guice.createInjector(new DataHandlingConfigServiceModule(channel));
    return injector.getInstance(BindableService.class);
  }
}
