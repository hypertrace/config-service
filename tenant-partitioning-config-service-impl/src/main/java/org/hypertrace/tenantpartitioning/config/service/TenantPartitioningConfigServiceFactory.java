package org.hypertrace.tenantpartitioning.config.service;

import com.google.inject.Guice;
import com.google.inject.Injector;
import com.typesafe.config.Config;
import io.grpc.BindableService;

public class TenantPartitioningConfigServiceFactory {
  public static BindableService build(Config config) {
    Injector injector = Guice.createInjector(new TenantPartitioningConfigServiceModule(config));
    return injector.getInstance(BindableService.class);
  }
}
