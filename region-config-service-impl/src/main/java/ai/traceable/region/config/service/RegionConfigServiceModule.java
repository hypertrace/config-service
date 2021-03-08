package ai.traceable.region.config.service;

import ai.traceable.region.config.service.regions.RegionStoreModule;
import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.typesafe.config.Config;
import io.grpc.BindableService;

class RegionConfigServiceModule extends AbstractModule {
  private final Config config;

  RegionConfigServiceModule(Config config) {
    this.config = config.getConfig("region.config.service");
  }

  @Override
  protected void configure() {
    bind(BindableService.class).to(RegionConfigServiceImpl.class);
    install(new RegionStoreModule());
  }

  @Provides
  @Singleton
  RegionConfigServiceConfig providesRegionServiceConfig() {
    return new RegionConfigServiceConfig(this.config);
  }
}
