package ai.traceable.data.protection.config.service;

import ai.traceable.data.protection.config.service.v1.DeleteScopedDataProtectionConfigRequest;
import ai.traceable.data.protection.config.service.v1.GetResolvedScopedDataProtectionConfigRequest;
import ai.traceable.data.protection.config.service.v1.UpsertScopedDataProtectionConfigRequest;
import org.hypertrace.core.grpcutils.context.RequestContext;

public interface ConfigValidator {
  void validateGetRequest(
      RequestContext requestContext, GetResolvedScopedDataProtectionConfigRequest request);

  void validateUpsertRequest(
      RequestContext requestContext, UpsertScopedDataProtectionConfigRequest request);

  void validateDeleteRequest(
      RequestContext requestContext, DeleteScopedDataProtectionConfigRequest request);
}
