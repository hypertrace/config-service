package org.hypertrace.partitioner.config.service;

import com.google.inject.Guice;
import com.google.inject.Injector;
import com.typesafe.config.Config;
import io.grpc.BindableService;
import org.hypertrace.core.documentstore.Datastore;

public class PartitionerConfigServiceFactory {

  @Deprecated
  public static BindableService build(Config config) {
    Injector injector = Guice.createInjector(new PartitionerConfigServiceModule(config));
    return injector.getInstance(BindableService.class);
  }

  public static BindableService build(Config config, Datastore datastore) {
    Injector injector = Guice.createInjector(new PartitionerConfigServiceModule(config, datastore));
    return injector.getInstance(BindableService.class);
  }
}
