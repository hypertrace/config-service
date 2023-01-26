package org.hypertrace.partitioner.config.service;

import com.google.inject.Guice;
import com.google.inject.Injector;
import com.typesafe.config.Config;
import io.grpc.BindableService;

public class PartitionerConfigServiceFactory {
  public static BindableService build(Config config) {
    Injector injector = Guice.createInjector(new PartitionerConfigServiceModule(config));
    return injector.getInstance(BindableService.class);
  }
}
