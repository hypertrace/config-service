package ai.traceable.threatmanagement.config.service;

import ai.traceable.config.utils.RegexValidator;
import ai.traceable.threatmanagement.config.service.v1.AnomalyScoreContribution;
import ai.traceable.threatmanagement.config.service.v1.ExcludeAutoBlockingConfig;
import ai.traceable.threatmanagement.config.service.v1.GetAnomalyScoreContributionRequest;
import ai.traceable.threatmanagement.config.service.v1.GetSecurityEventScoreContributionRequest;
import ai.traceable.threatmanagement.config.service.v1.GetSecurityEventTypeContributionRequest;
import ai.traceable.threatmanagement.config.service.v1.GetThreatAutoBlockingConfigRequest;
import ai.traceable.threatmanagement.config.service.v1.GetThreatScoreBoundRequest;
import ai.traceable.threatmanagement.config.service.v1.IpReputationThreatScoreConfig;
import ai.traceable.threatmanagement.config.service.v1.SecurityEventScoreContribution;
import ai.traceable.threatmanagement.config.service.v1.SecurityEventTypeContribution;
import ai.traceable.threatmanagement.config.service.v1.SecurityEventTypeContribution.SecurityEventTypeContributionKind;
import ai.traceable.threatmanagement.config.service.v1.StatusCodeThreatScoreConfig;
import ai.traceable.threatmanagement.config.service.v1.ThreatAutoBlockingActionType;
import ai.traceable.threatmanagement.config.service.v1.ThreatScoreBound;
import ai.traceable.threatmanagement.config.service.v1.UpdateAnomalyScoreContributionRequest;
import ai.traceable.threatmanagement.config.service.v1.UpdateIpReputationThreatScoreConfigRequest;
import ai.traceable.threatmanagement.config.service.v1.UpdateSecurityEventScoreContributionRequest;
import ai.traceable.threatmanagement.config.service.v1.UpdateSecurityEventTypeContributionRequest;
import ai.traceable.threatmanagement.config.service.v1.UpdateStatusCodeThreatScoreConfigsRequest;
import ai.traceable.threatmanagement.config.service.v1.UpdateThreatAutoBlockingConfigRequest;
import ai.traceable.threatmanagement.config.service.v1.UpdateThreatAutoBlockingConfigRequest.ExpirationDetails;
import ai.traceable.threatmanagement.config.service.v1.UpdateThreatScoreBoundRequest;
import java.time.Duration;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;
import org.hypertrace.core.grpcutils.context.RequestContext;

class ThreatManagementConfigRequestValidator {

  public void validateOrThrow(
      RequestContext requestContext, UpdateStatusCodeThreatScoreConfigsRequest request) {
    this.validateRequestContext(requestContext);
    this.validateStatusCodeThreatScoreConfig(
        request.getStatusCodeThreatScoreConfigs().getConfigsList());
  }

  public void validateOrThrow(
      RequestContext requestContext, UpdateIpReputationThreatScoreConfigRequest request) {
    this.validateRequestContext(requestContext);
    this.validateIpReputationThreatScoreConfig(request.getIpReputationThreatScoreConfig());
  }

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
      RequestContext requestContext, GetAnomalyScoreContributionRequest request) {
    this.validateRequestContext(requestContext);
  }

  public void validateOrThrow(
      RequestContext requestContext, UpdateAnomalyScoreContributionRequest request) {
    this.validateRequestContext(requestContext);
    this.validateAnomalyScoreContribution(request.getAnomalyScoreContribution());
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

    request.getExcludeConfigsList().forEach(this::validateExcludeAutoBlockingConfig);

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

  public void validateRequestContext(RequestContext requestContext) {
    if (requestContext.getTenantId().isEmpty()) {
      throw new IllegalArgumentException("Missing expected Tenant ID in request");
    }
  }

  private void validateExcludeAutoBlockingConfig(ExcludeAutoBlockingConfig config) {
    config
        .getUserIdRegexesList()
        .forEach(
            userIdRegex -> {
              if (!RegexValidator.validate(userIdRegex).isOk()) {
                throw new IllegalArgumentException(
                    "Invalid user id regex provided: " + userIdRegex);
              }
            });
  }

  private void validateThreatScoreBound(ThreatScoreBound threatScoreBound) {
    int lowScoreUpperBound = threatScoreBound.getLowScoreUpperBound();
    int mediumScoreUpperBound = threatScoreBound.getMediumScoreUpperBound();
    int highScoreUpperBound = threatScoreBound.getHighScoreUpperBound();
    if (lowScoreUpperBound < 0 || mediumScoreUpperBound < 0 || highScoreUpperBound < 0) {
      throw new IllegalArgumentException("threat score bounds should be non negative");
    }

    if (lowScoreUpperBound > mediumScoreUpperBound) {
      throw new IllegalArgumentException(
          "low score upper bound should be less than medium score upper bound");
    }

    if (mediumScoreUpperBound > highScoreUpperBound) {
      throw new IllegalArgumentException(
          "medium score upper bound should be less than high score upper bound");
    }
  }

  private void validateSecurityEventScoreContribution(
      SecurityEventScoreContribution securityEventScoreContribution) {
    int lowScore = securityEventScoreContribution.getLowScore();
    int mediumScore = securityEventScoreContribution.getMediumScore();
    int highScore = securityEventScoreContribution.getHighScore();
    int criticalScore = securityEventScoreContribution.getCriticalScore();

    if (lowScore < 0 || mediumScore < 0 || highScore < 0 || criticalScore < 0) {
      throw new IllegalArgumentException(
          "security event score contributions should be non negative");
    }
  }

  private void validateAnomalyScoreContribution(AnomalyScoreContribution anomalyScoreContribution) {
    int anomalyScore = anomalyScoreContribution.getAnomalyScore();

    if (anomalyScore < 0) {
      throw new IllegalArgumentException("anomaly score contributions should be non negative");
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

  private void validateIpReputationThreatScoreConfig(IpReputationThreatScoreConfig config) {
    if (config.getCriticalIpReputationThreatScoreIncrement() < 0
        || config.getHighIpReputationThreatScoreIncrement() < 0
        || config.getMediumIpReputationThreatScoreIncrement() < 0
        || config.getLowIpReputationThreatScoreIncrement() < 0) {
      throw new IllegalArgumentException(
          "ip reputation score contributions should be non negative");
    }
  }

  private void validateStatusCodeThreatScoreConfig(List<StatusCodeThreatScoreConfig> configs) {
    Set<StatusCodeThreatScoreConfig> statusCodeThreatScoreConfigSet = new HashSet<>();

    configs.forEach(
        config -> {
          if (statusCodeThreatScoreConfigSet.contains(config)) {
            throw new IllegalArgumentException(
                String.format("Duplicate StatusCodeThreatScoreConfig %s found", config));
          }
          statusCodeThreatScoreConfigSet.add(config);
        });

    configs.forEach(
        config -> {
          try {
            Pattern.compile(config.getErrorStatusCodeRegex());
          } catch (Exception e) {
            throw new IllegalArgumentException(
                String.format(
                    "Invalid regex \"%s\" in StatusCodeThreatScoreConfig:%s",
                    config.getErrorStatusCodeRegex(), config));
          }
        });
  }
}
