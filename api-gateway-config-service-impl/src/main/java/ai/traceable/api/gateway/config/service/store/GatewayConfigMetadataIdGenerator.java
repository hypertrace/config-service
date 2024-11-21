package ai.traceable.api.gateway.config.service.store;

import ai.traceable.api.gateway.config.service.v1.ConfigMetadata;
import ai.traceable.api.gateway.config.service.v1.RequestInfo;
import ai.traceable.api.gateway.config.service.v1.SourceInfo;
import ai.traceable.config.utils.UuidGenerator;
import com.google.common.base.Joiner;
import jakarta.inject.Inject;
import lombok.AllArgsConstructor;

@AllArgsConstructor(onConstructor_ = {@Inject})
public class GatewayConfigMetadataIdGenerator {
  private static final Joiner joiner = Joiner.on(":");

  private final UuidGenerator uuidGenerator;

  public String generateId(final ConfigMetadata metadata) {
    final String joined =
        joiner.join(
            metadata.getGatewayType(),
            metadata.getOrgId(),
            (Object[])
                metadata.getSourceInfoList().stream()
                    .map(SourceInfo::getRequestInfo)
                    .map(RequestInfo::getUrl)
                    .toArray(String[]::new));
    return this.uuidGenerator.generateId(joined);
  }
}
