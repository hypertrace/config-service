package ai.traceable.ast.config.service.rules;

import ai.traceable.ast.config.service.v1.GetScanPurgeConfigRequest;
import ai.traceable.ast.config.service.v1.UpdateScanPurgeConfigRequest;
import org.hypertrace.core.grpcutils.context.RequestContext;

public interface RulesValidator {
  void validateOrThrow(RequestContext requestContext, UpdateScanPurgeConfigRequest request);

  void validateOrThrow(RequestContext requestContext, GetScanPurgeConfigRequest request);
}
