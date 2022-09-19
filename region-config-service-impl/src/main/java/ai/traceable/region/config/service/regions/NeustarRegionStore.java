package ai.traceable.region.config.service.regions;

import ai.traceable.region.config.service.RegionConfigServiceConfig;
import com.google.inject.Inject;

public class NeustarRegionStore extends RegionStoreImpl {

  @Inject
  public NeustarRegionStore(
      RegionBuilder regionBuilder,
      RegionConverter regionConverter,
      RegionConfigServiceConfig config) {
    super(regionBuilder, regionConverter, config.getNeustarCountriesDataPath());
  }
}
