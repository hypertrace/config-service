package ai.traceable.region.config.service.regions;

import com.google.inject.Inject;

public class NeustarRegionStore extends RegionStoreImpl {

  @Inject
  public NeustarRegionStore(
      NeustarRegionBuilder neustarRegionBuilder, RegionConverter regionConverter) {
    super(neustarRegionBuilder, regionConverter);
  }
}
