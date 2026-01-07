package ai.traceable.data.obfuscation.config.service;

import com.google.inject.Guice;
import com.google.inject.Injector;
import io.grpc.BindableService;
import io.grpc.Channel;
import org.hypertrace.config.service.change.event.api.ConfigChangeEventGenerator;

public class DataObfuscationConfigServiceFactory {
  private DataObfuscationConfigServiceFactory() {}

  public static BindableService build(
      Channel channel, ConfigChangeEventGenerator configChangeEventGenerator) {
    Injector injector =
        Guice.createInjector(
            new DataObfuscationConfigServiceModule(channel, configChangeEventGenerator));
    return injector.getInstance(BindableService.class);
  }
}
