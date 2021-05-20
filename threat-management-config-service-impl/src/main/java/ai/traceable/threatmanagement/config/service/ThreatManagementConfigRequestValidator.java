package ai.traceable.threatmanagement.config.service;

import ai.traceable.threatmanagement.config.service.v1.GetSecurityEventScoreContributionRequest;
import ai.traceable.threatmanagement.config.service.v1.GetSecurityEventTypeContributionRequest;
import ai.traceable.threatmanagement.config.service.v1.GetThreatAutoBlockingConfigRequest;
import ai.traceable.threatmanagement.config.service.v1.GetThreatScoreBoundRequest;
import ai.traceable.threatmanagement.config.service.v1.SecurityEventScoreContribution;
import ai.traceable.threatmanagement.config.service.v1.SecurityEventTypeContribution;
import ai.traceable.threatmanagement.config.service.v1.SecurityEventTypeContribution.SecurityEventTypeContributionKind;
import ai.traceable.threatmanagement.config.service.v1.ThreatAutoBlockingActionType;
import ai.traceable.threatmanagement.config.service.v1.ThreatScoreBound;
import ai.traceable.threatmanagement.config.service.v1.UpdateSecurityEventScoreContributionRequest;
import ai.traceable.threatmanagement.config.service.v1.UpdateSecurityEventTypeContributionRequest;
import ai.traceable.threatmanagement.config.service.v1.UpdateThreatAutoBlockingConfigRequest;
import ai.traceable.threatmanagement.config.service.v1.UpdateThreatAutoBlockingConfigRequest.ExpirationDetails;
import ai.traceable.threatmanagement.config.service.v1.UpdateThreatScoreBoundRequest;
import java.time.Duration;
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

  public void validateOrThrow(
      RequestContext requestContext, GetThreatAutoBlockingConfigRequest request) {
    this.validateRequestContext(requestContext);
  }

  public void validateOrThrow(
      RequestContext requestContext, UpdateThreatAutoBlockingConfigRequest request) {
    this.validateRequestContext(requestContext);

    ThreatAutoBlockingActionType actionType = request.getActionType();
    this.validateThreatAutoBlockingActionType(actionType);

    switch (actionType) {
      case THREAT_AUTO_BLOCKING_ACTION_TYPE_NO_ACTION:
        if (request.hasExpirationDetails()) {
          throw new IllegalArgumentException(
              "request should not have an expiration for no auto blocking action");
        }
      case THREAT_AUTO_BLOCKING_ACTION_TYPE_BLOCK:
        this.validateThreatAutoBlockingExpirationDetails(request.getExpirationDetails());
      default:
    }
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

  private void validateThreatAutoBlockingActionType(ThreatAutoBlockingActionType actionType) {
    switch (actionType) {
      case UNRECOGNIZED:
      case THREAT_AUTO_BLOCKING_ACTION_TYPE_UNSPECIFIED:
        throw new IllegalArgumentException(
            "threat auto blocking action type should be a valid value");
      default:
    }
  }

  private void validateThreatAutoBlockingExpirationDetails(ExpirationDetails expirationDetails) {
    if (expirationDetails.hasDuration()) {
      this.validateDurationIso8601String(expirationDetails.getDuration());
    }
  }

  private void validateDurationIso8601String(String duration) {
    try {
      Duration.parse(duration);
    } catch (Exception e) {
      throw new IllegalArgumentException(
          String.format("Duration %s should be a valid ISO 8601 format", duration));
    }
  }

  private void validateRequestContext(RequestContext requestContext) {
    if (requestContext.getTenantId().isEmpty()) {
      throw new IllegalArgumentException("Missing expected Tenant ID in request");
    }
  }
}
