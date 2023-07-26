package org.hypertrace.span.processing.config.service;

import com.google.inject.Guice;
import com.google.inject.Injector;
import com.typesafe.config.Config;
import io.grpc.BindableService;
import io.grpc.Channel;
import org.hypertrace.config.service.change.event.api.ConfigChangeEventGenerator;

public class SpanProcessingConfigServiceFactory {
  public static BindableService build(
      Channel channel, Config config, ConfigChangeEventGenerator configChangeEventGenerator) {
    Injector injector =
        Guice.createInjector(
            new SpanProcessingConfigServiceModule(channel, config, configChangeEventGenerator));
    return injector.getInstance(BindableService.class);
  }
}
