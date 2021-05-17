package ai.traceable.threatmanagement.config.service;

import ai.traceable.threatmanagement.config.service.v1.GetSecurityEventScoreContributionRequest;
import ai.traceable.threatmanagement.config.service.v1.GetSecurityEventTypeContributionRequest;
import ai.traceable.threatmanagement.config.service.v1.GetThreatScoreBoundRequest;
import ai.traceable.threatmanagement.config.service.v1.SecurityEventScoreContribution;
import ai.traceable.threatmanagement.config.service.v1.SecurityEventTypeContribution;
import ai.traceable.threatmanagement.config.service.v1.SecurityEventTypeContribution.SecurityEventTypeContributionKind;
import ai.traceable.threatmanagement.config.service.v1.ThreatScoreBound;
import ai.traceable.threatmanagement.config.service.v1.UpdateSecurityEventScoreContributionRequest;
import ai.traceable.threatmanagement.config.service.v1.UpdateSecurityEventTypeContributionRequest;
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

  public void validateOrThrow(
      RequestContext requestContext, GetSecurityEventScoreContributionRequest request) {
    this.validateRequestContext(requestContext);
  }

  public void validateOrThrow(
      RequestContext requestContext, UpdateSecurityEventScoreContributionRequest request) {
    this.validateRequestContext(requestContext);
    this.validateSecurityEventScoreContribution(request.getSecurityEventScoreContribution());
  }

  public void validateOrThrow(
      RequestContext requestContext, GetSecurityEventTypeContributionRequest request) {
    this.validateRequestContext(requestContext);
  }

  public void validateOrThrow(
      RequestContext requestContext, UpdateSecurityEventTypeContributionRequest request) {
    this.validateRequestContext(requestContext);
    this.validateSecurityEventTypeContribution(request.getSecurityEventTypeContribution());
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

  private void validateSecurityEventScoreContribution(
      SecurityEventScoreContribution securityEventScoreContribution) {
    int anomalyScore = securityEventScoreContribution.getAnomalyScore();
    int mediumScore = securityEventScoreContribution.getMediumScore();
    int highScore = securityEventScoreContribution.getHighScore();

    if (anomalyScore < 0 || mediumScore < 0 || highScore < 0) {
      throw new IllegalArgumentException(
          "security event score contributions should be non negative");
    }
  }

  private void validateSecurityEventTypeContribution(
      SecurityEventTypeContribution securityEventTypeContribution) {
    SecurityEventTypeContributionKind securityEventTypeContributionKind =
        securityEventTypeContribution.getSecurityEventTypeContributionKind();
    switch (securityEventTypeContributionKind) {
      case UNRECOGNIZED:
      case SECURITY_EVENT_TYPE_CONTRIBUTION_KIND_UNSPECIFIED:
        throw new IllegalArgumentException(
            "security event type contribution kind should be a valid value");
      default:
    }
  }

  private void validateRequestContext(RequestContext requestContext) {
    if (requestContext.getTenantId().isEmpty()) {
      throw new IllegalArgumentException("Missing expected Tenant ID in request");
    }
  }
}
