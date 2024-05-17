package ai.traceable.data.protection.config.service;

import ai.traceable.data.protection.config.service.v1.DeleteScopedDataProtectionConfigRequest;
import ai.traceable.data.protection.config.service.v1.GetResolvedScopedDataProtectionConfigRequest;
import ai.traceable.data.protection.config.service.v1.ScopedDataProtectionConfig;
import ai.traceable.data.protection.config.service.v1.UpsertScopedDataProtectionConfigRequest;
import java.util.Optional;
import org.hypertrace.core.grpcutils.context.RequestContext;

public interface ConfigManager {
  Optional<ScopedDataProtectionConfig> getResolvedScopedDataProtectionConfig(
      GetResolvedScopedDataProtectionConfigRequest request, RequestContext requestContext);

  ScopedDataProtectionConfig upsertScopedDataProtectionConfig(
      UpsertScopedDataProtectionConfigRequest request, RequestContext requestContext);

  void deleteScopedDataProtectionConfig(
      DeleteScopedDataProtectionConfigRequest request, RequestContext requestContext);
}
