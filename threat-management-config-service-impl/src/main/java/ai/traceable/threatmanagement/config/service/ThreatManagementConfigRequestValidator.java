package ai.traceable.threatmanagement.config.service;

import ai.traceable.threatmanagement.config.service.v1.GetThreatScoreBoundRequest;
import ai.traceable.threatmanagement.config.service.v1.ThreatScoreBound;
import ai.traceable.threatmanagement.config.service.v1.UpdateThreatScoreBoundRequest;
import org.hypertrace.core.grpcutils.context.RequestContext;

class ThreatManagementConfigRequestValidator {
  public void validateOrThrow(RequestContext requestContext, GetThreatScoreBoundRequest request) {
    this.validateRequestContext(requestContext);
  }

  public void validateOrThrow(
      RequestContext requestContext, UpdateThreatScoreBoundRequest request) {
    this.validateRequestContext(requestContext);
    this.validateThreatScoreBound(request.getThreatScoreBound());
  }

  private void validateThreatScoreBound(ThreatScoreBound threatScoreBound) {
    int mediumScoreUpperBound = threatScoreBound.getMediumScoreUpperBound();
    int highScoreUpperBound = threatScoreBound.getHighScoreUpperBound();
    if (mediumScoreUpperBound < 0 || highScoreUpperBound < 0) {
      throw new IllegalArgumentException("threat score bounds should be non negative");
    }

    if (mediumScoreUpperBound > highScoreUpperBound) {
      throw new IllegalArgumentException(
          "medium score upper bound should be less than high score upper bound");
    }
  }

  private void validateRequestContext(RequestContext requestContext) {
    if (requestContext.getTenantId().isEmpty()) {
      throw new IllegalArgumentException("Missing expected Tenant ID in request");
    }
  }
}
