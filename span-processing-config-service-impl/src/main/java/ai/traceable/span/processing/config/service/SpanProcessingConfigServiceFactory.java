package ai.traceable.span.processing.config.service;

import com.google.inject.Guice;
import com.google.inject.Injector;
import com.typesafe.config.Config;
import io.grpc.BindableService;
import io.grpc.ManagedChannel;

public class SpanProcessingConfigServiceFactory {
  public static BindableService build(ManagedChannel channel, Config config) {
    Injector injector =
        Guice.createInjector(new SpanProcessingConfigServiceModule(channel, config));
    return injector.getInstance(BindableService.class);
  }
}
