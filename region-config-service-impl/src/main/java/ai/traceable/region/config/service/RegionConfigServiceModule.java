package ai.traceable.region.config.service;

import ai.traceable.region.config.service.regions.RegionStoreModule;
import ai.traceable.region.config.service.rules.RulesManagerModule;
import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.typesafe.config.Config;
import io.grpc.BindableService;
import io.grpc.ManagedChannel;

class RegionConfigServiceModule extends AbstractModule {
  private final Config config;
  private final ManagedChannel channel;

  RegionConfigServiceModule(ManagedChannel channel, Config config) {
    this.channel = channel;
    this.config = config.getConfig("region.config.service");
  }

  @Override
  protected void configure() {
    bind(BindableService.class).to(RegionConfigServiceImpl.class);
    bind(ManagedChannel.class).toInstance(channel);
    install(new RegionStoreModule());
    install(new RulesManagerModule());
  }

  @Provides
  @Singleton
  RegionConfigServiceConfig providesRegionServiceConfig() {
    return new RegionConfigServiceConfig(this.config);
  }
}
