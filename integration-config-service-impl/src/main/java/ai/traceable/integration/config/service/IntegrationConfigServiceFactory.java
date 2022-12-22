package ai.traceable.integration.config.service;

import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.TypeLiteral;
import io.grpc.BindableService;
import io.grpc.Channel;
import java.util.Collections;
import java.util.Set;

public class IntegrationConfigServiceFactory {

  public static Set<BindableService> build(Channel channel) {
    Injector injector = Guice.createInjector(new IntegrationConfigServiceModule(channel));
    return Collections.unmodifiableSet(
        injector.getInstance(Key.get(new TypeLiteral<Set<BindableService>>() {})));
  }
}
