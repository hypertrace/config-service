package ai.traceable.region.config.service.regions;

import java.util.Map;
import java.util.function.Supplier;

public interface RegionBuilder {
  Supplier<Map<String, Region>> getLatestDataSupplier();
}
