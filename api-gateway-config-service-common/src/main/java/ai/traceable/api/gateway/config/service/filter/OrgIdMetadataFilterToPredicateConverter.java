package ai.traceable.api.gateway.config.service.filter;

import ai.traceable.api.gateway.config.service.v1.ConfigMetadata;
import ai.traceable.api.gateway.config.service.v1.MetadataFilter;
import java.util.function.Predicate;

public class OrgIdMetadataFilterToPredicateConverter
    implements MetadataConfigFilterToPredicateConverter {

  @Override
  public Predicate<ConfigMetadata> convert(final MetadataFilter filter) {
    return metadata -> filter.getOrgIds().getOrgIdList().contains(metadata.getOrgId());
  }
}
