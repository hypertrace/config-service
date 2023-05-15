package ai.traceable.integration.config.service;

import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.TypeLiteral;
import io.grpc.BindableService;
import io.grpc.Channel;
import java.util.Collections;
import java.util.Set;
import org.hypertrace.config.service.change.event.api.ConfigChangeEventGenerator;

public class IntegrationConfigServiceFactory {

  public static Set<BindableService> build(
      Channel channel, ConfigChangeEventGenerator configChangeEventGenerator) {
    Injector injector =
        Guice.createInjector(
            new IntegrationConfigServiceModule(channel, configChangeEventGenerator));
    return Collections.unmodifiableSet(
        injector.getInstance(Key.get(new TypeLiteral<Set<BindableService>>() {})));
  }
}
