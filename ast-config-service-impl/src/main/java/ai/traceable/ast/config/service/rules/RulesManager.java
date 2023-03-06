package ai.traceable.ast.config.service.rules;

import ai.traceable.ast.config.service.v1.ScanPurgeConfig;
import ai.traceable.ast.config.service.v1.UpdateScanPurgeConfigRequest;
import java.util.Optional;
import org.hypertrace.core.grpcutils.context.RequestContext;

public interface RulesManager {
  ScanPurgeConfig updateScanPurgeConfig(
      RequestContext requestContext, UpdateScanPurgeConfigRequest request);

  Optional<ScanPurgeConfig> getScanPurgeConfig(RequestContext requestContext);
}
