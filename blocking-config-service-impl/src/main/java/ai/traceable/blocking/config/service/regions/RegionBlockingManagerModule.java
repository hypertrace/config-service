package ai.traceable.blocking.config.service.regions;

import ai.traceable.region.config.service.v1.RegionConfigServiceGrpc.RegionConfigServiceBlockingStub;
import com.google.inject.AbstractModule;

public class RegionBlockingManagerModule extends AbstractModule {

  @Override
  protected void configure() {
    bind(RegionBlockingManager.class).to(DefaultRegionBlockingManager.class);
    requireBinding(RegionConfigServiceBlockingStub.class);
  }
}
