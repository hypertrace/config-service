package ai.traceable.threatmanagement.config.service.anomalyscore;

import ai.traceable.threatmanagement.config.service.v1.AnomalyScoreContribution;
import org.hypertrace.core.grpcutils.context.RequestContext;

public interface AnomalyScoreContributionManager {
  // get anomaly score contribution, if present, else return default anomaly score
  // contribution
  AnomalyScoreContribution getAnomalyScoreContribution(RequestContext requestContext);

  // update if config present, else create
  AnomalyScoreContribution upsertAnomalyScoreContribution(
      RequestContext requestContext, AnomalyScoreContribution anomalyScoreContribution);

  AnomalyScoreContribution getDefaultAnomalyScoreContribution();
}
