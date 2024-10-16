package org.hypertrace.partitioner.config.service;

import com.google.inject.Guice;
import com.google.inject.Injector;
import com.typesafe.config.Config;
import io.grpc.BindableService;
import org.hypertrace.core.serviceframework.spi.PlatformServiceLifecycle;

public class PartitionerConfigServiceFactory {

  @Deprecated
  public static BindableService build(Config config) {
    Injector injector = Guice.createInjector(new PartitionerConfigServiceModule(config));
    return injector.getInstance(BindableService.class);
  }

  public static BindableService build(Config config, PlatformServiceLifecycle lifecycle) {
    Injector injector = Guice.createInjector(new PartitionerConfigServiceModule(config, lifecycle));
    return injector.getInstance(BindableService.class);
  }
}
