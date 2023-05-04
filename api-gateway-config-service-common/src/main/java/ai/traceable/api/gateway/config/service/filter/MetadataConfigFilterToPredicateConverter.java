package ai.traceable.api.gateway.config.service.filter;

import ai.traceable.api.gateway.config.service.v1.ConfigMetadata;
import ai.traceable.api.gateway.config.service.v1.MetadataFilter;
import java.util.function.Predicate;

public interface MetadataConfigFilterToPredicateConverter {
  Predicate<ConfigMetadata> CONFIG_METADATA_TAUTOLOGY = route -> true;

  Predicate<ConfigMetadata> convert(final MetadataFilter filter);
}
