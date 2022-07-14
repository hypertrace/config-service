package ai.traceable.api.attribute.override.service;

import com.google.inject.Guice;
import com.google.inject.Injector;
import io.grpc.BindableService;
import io.grpc.Channel;
import org.hypertrace.config.service.change.event.api.ConfigChangeEventGenerator;

public class ApiAttributeOverridesServiceFactory {
  public static BindableService build(
      Channel channel, ConfigChangeEventGenerator configChangeEventGenerator) {
    Injector injector =
        Guice.createInjector(
            new ApiAttributeOverrideServiceModule(channel, configChangeEventGenerator));
    return injector.getInstance(BindableService.class);
  }
}
