package ai.traceable.region.config.service.regions;

import ai.traceable.region.config.service.RegionConfigServiceConfig;
import com.google.inject.Inject;

public class IpqsRegionStore extends RegionStoreImpl {

  @Inject
  public IpqsRegionStore(
      RegionBuilder regionBuilder,
      RegionConverter regionConverter,
      RegionConfigServiceConfig config) {
    super(regionBuilder, regionConverter, config.getIpqsCountriesDataConfig());
  }
}
