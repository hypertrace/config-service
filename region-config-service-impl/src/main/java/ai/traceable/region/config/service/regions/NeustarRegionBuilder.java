package ai.traceable.region.config.service.regions;

import ai.traceable.config.utils.LatestInstantNamedPathFinder;
import ai.traceable.config.utils.UuidGenerator;
import ai.traceable.region.config.service.RegionConfigServiceConfig;
import com.google.inject.Inject;
import com.google.inject.Singleton;

@Singleton
public class NeustarRegionBuilder extends DefaultRegionBuilder {
  @Inject
  public NeustarRegionBuilder(
      LatestInstantNamedPathFinder latestInstantNamedPathFinder,
      UuidGenerator uuidGenerator,
      RegionConfigServiceConfig config) {
    super(
        latestInstantNamedPathFinder, uuidGenerator, config.getNeustarCountriesDataConfig(), false);
  }
}
