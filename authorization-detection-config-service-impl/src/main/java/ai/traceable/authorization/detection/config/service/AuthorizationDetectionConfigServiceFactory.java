package ai.traceable.authorization.detection.config.service;

import com.google.inject.Guice;
import com.google.inject.Injector;
import com.typesafe.config.Config;
import io.grpc.BindableService;
import io.grpc.Channel;
import org.hypertrace.config.service.change.event.api.ConfigChangeEventGenerator;

public class AuthorizationDetectionConfigServiceFactory {
  public static BindableService build(
      Channel channel, ConfigChangeEventGenerator configChangeEventGenerator, Config config) {
    Injector injector =
        Guice.createInjector(
            new AuthorizationDetectionConfigServiceModule(
                channel, configChangeEventGenerator, config));
    return injector.getInstance(BindableService.class);
  }
}
