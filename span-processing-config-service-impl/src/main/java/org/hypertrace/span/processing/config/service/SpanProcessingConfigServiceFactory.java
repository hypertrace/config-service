package org.hypertrace.span.processing.config.service;

import com.google.inject.Guice;
import com.google.inject.Injector;
import io.grpc.BindableService;
import io.grpc.ManagedChannel;

public class SpanProcessingConfigServiceFactory {
  public static BindableService build(ManagedChannel channel) {
    Injector injector = Guice.createInjector(new SpanProcessingConfigServiceModule(channel));
    return injector.getInstance(BindableService.class);
  }
}
