package ai.traceable.region.config.service.regions;

import com.google.inject.AbstractModule;

public class RegionStoreModule extends AbstractModule {

  @Override
  protected void configure() {
    bind(RegionStore.class).to(NeustarRegionStore.class);
  }
}
