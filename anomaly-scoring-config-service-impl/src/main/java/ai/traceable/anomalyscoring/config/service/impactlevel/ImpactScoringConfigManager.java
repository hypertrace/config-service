package ai.traceable.anomalyscoring.config.service.impactlevel;

import ai.traceable.anomalyscoring.config.service.v1.ImpactScoringConfig;
import org.hypertrace.core.grpcutils.context.RequestContext;

public interface ImpactScoringConfigManager {
  // get impact scoring config, if present, else return default impact scoring config
  ImpactScoringConfig getImpactScoringConfig(RequestContext requestContext);

  // update if config present, else create
  ImpactScoringConfig upsertImpactScoringConfig(
      RequestContext requestContext, ImpactScoringConfig impactScoringConfig);

  ImpactScoringConfig getDefaultImpactScoringConfig();
}
