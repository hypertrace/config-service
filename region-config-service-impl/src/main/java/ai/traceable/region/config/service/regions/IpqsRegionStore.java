package ai.traceable.region.config.service.regions;

import com.google.inject.Inject;
import com.google.inject.Singleton;

@Singleton
public class IpqsRegionStore extends RegionStoreImpl {

  @Inject
  public IpqsRegionStore(IpqsRegionBuilder ipqsRegionBuilder, RegionConverter regionConverter) {
    super(ipqsRegionBuilder, regionConverter);
  }
}
