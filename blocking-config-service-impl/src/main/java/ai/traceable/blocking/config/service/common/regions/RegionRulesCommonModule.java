package ai.traceable.blocking.config.service.common.regions;

import ai.traceable.region.config.service.v1.RegionConfigServiceGrpc.RegionConfigServiceBlockingStub;
import com.google.inject.AbstractModule;

public class RegionRulesCommonModule extends AbstractModule {

  @Override
  protected void configure() {
    requireBinding(RegionConfigServiceBlockingStub.class);
  }
}
