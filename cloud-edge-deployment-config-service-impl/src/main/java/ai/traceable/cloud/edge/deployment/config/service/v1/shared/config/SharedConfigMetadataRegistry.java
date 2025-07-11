package ai.traceable.cloud.edge.deployment.config.service.v1.shared.config;

import ai.traceable.cloud.edge.deployment.config.service.v1.ConfigAccessType;
import ai.traceable.cloud.edge.deployment.config.service.v1.SharedConfigMetadata;

public interface SharedConfigMetadataRegistry {
  public SharedConfigMetadata getSharedConfigMetadata(ConfigAccessType accessType);
}
