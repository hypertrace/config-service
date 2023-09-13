package ai.traceable.region.config.service.regions;

import com.google.inject.Inject;

public class IpqsResolvedWithNeustarRegionStore extends RegionStoreImpl {
  @Inject
  public IpqsResolvedWithNeustarRegionStore(
      IpqsResolvedWithNeustarRegionBuilder ipqsResolvedWithNeustarRegionBuilder,
      RegionConverter regionConverter) {
    super(ipqsResolvedWithNeustarRegionBuilder, regionConverter);
  }
}
