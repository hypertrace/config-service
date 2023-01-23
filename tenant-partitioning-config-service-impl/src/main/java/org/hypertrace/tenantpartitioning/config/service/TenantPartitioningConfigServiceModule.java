package org.hypertrace.tenantpartitioning.config.service;

import com.google.inject.AbstractModule;
import com.typesafe.config.Config;
import io.grpc.BindableService;
import org.hypertrace.tenantpartitioning.config.service.store.TenantPartitionConfigStore;
import org.hypertrace.tenantpartitioning.config.service.store.TenantPartitionConfigStoreProvider;

public class TenantPartitioningConfigServiceModule extends AbstractModule {

  public static final String GENERIC_CONFIG_SERVICE = "generic.config.service";

  private final Config config;

  public TenantPartitioningConfigServiceModule(Config config) {
    this.config = config;
  }

  @Override
  protected void configure() {
    bind(Config.class).toInstance(this.config);
    bind(BindableService.class).to(TenantPartitioningConfigServiceImpl.class);
    bind(TenantPartitionConfigStore.class)
        .toInstance(
            TenantPartitionConfigStoreProvider.getDocumentStore(
                config.getConfig(GENERIC_CONFIG_SERVICE)));
  }
}
