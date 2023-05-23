package ai.traceable.anomalyscoring.config.service.confidencelevel;

import ai.traceable.anomalyscoring.config.service.v1.ConfidenceScoringConfig;
import org.hypertrace.core.grpcutils.context.RequestContext;

public interface ConfidenceScoringConfigManager {
  // get Confidence scoring config, if present, else return default confidence score
  ConfidenceScoringConfig getConfidenceScoringConfig(RequestContext requestContext);

  // update if config present, else create
  ConfidenceScoringConfig upsertConfidenceScoringConfig(
      RequestContext requestContext, ConfidenceScoringConfig confidenceScoringConfig);

  ConfidenceScoringConfig getDefaultConfidenceScoringConfig();
}
