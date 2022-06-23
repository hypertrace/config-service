package ai.traceable.threatmanagement.config.service;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

import ai.traceable.threatmanagement.config.service.v1.AnomalyScoreContribution;
import ai.traceable.threatmanagement.config.service.v1.IpReputationThreatScoreConfig;
import ai.traceable.threatmanagement.config.service.v1.Protocol;
import ai.traceable.threatmanagement.config.service.v1.SecurityEventScoreContribution;
import ai.traceable.threatmanagement.config.service.v1.SecurityEventTypeContribution;
import ai.traceable.threatmanagement.config.service.v1.SecurityEventTypeContribution.SecurityEventTypeContributionKind;
import ai.traceable.threatmanagement.config.service.v1.SeverityDowngradePolicy;
import ai.traceable.threatmanagement.config.service.v1.StatusCodeThreatScoreConfig;
import ai.traceable.threatmanagement.config.service.v1.StatusCodeThreatScoreConfigs;
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
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class ThreatManagementConfigRequestValidatorTest {
  private static final RequestContext REQUEST_CONTEXT = RequestContext.forTenantId("tenant-1");
  private ThreatManagementConfigRequestValidator requestValidator;

  @BeforeEach
  void setup() {
    this.requestValidator = new ThreatManagementConfigRequestValidator();
  }

  @Test
  void validatePositiveThreatScoreBounds() {
    assertThrows(
        IllegalArgumentException.class,
        () ->
            this.requestValidator.validateOrThrow(
                REQUEST_CONTEXT,
                UpdateThreatScoreBoundRequest.newBuilder()
                    .setThreatScoreBound(ThreatScoreBound.newBuilder().setLowScoreUpperBound(-1))
                    .build()));

    assertThrows(
        IllegalArgumentException.class,
        () ->
            this.requestValidator.validateOrThrow(
                REQUEST_CONTEXT,
                UpdateThreatScoreBoundRequest.newBuilder()
                    .setThreatScoreBound(ThreatScoreBound.newBuilder().setMediumScoreUpperBound(-1))
                    .build()));

    assertThrows(
        IllegalArgumentException.class,
        () ->
            this.requestValidator.validateOrThrow(
                REQUEST_CONTEXT,
                UpdateThreatScoreBoundRequest.newBuilder()
                    .setThreatScoreBound(ThreatScoreBound.newBuilder().setHighScoreUpperBound(-1))
                    .build()));

    assertThrows(
        IllegalArgumentException.class,
        () ->
            this.requestValidator.validateOrThrow(
                REQUEST_CONTEXT,
                UpdateThreatScoreBoundRequest.newBuilder()
                    .setThreatScoreBound(
                        ThreatScoreBound.newBuilder()
                            .setLowScoreUpperBound(-1)
                            .setMediumScoreUpperBound(-1)
                            .setHighScoreUpperBound(-1))
                    .build()));
  }

  @Test
  void validateThreatScoreBounds() {
    assertThrows(
        IllegalArgumentException.class,
        () ->
            this.requestValidator.validateOrThrow(
                REQUEST_CONTEXT,
                UpdateThreatScoreBoundRequest.newBuilder()
                    .setThreatScoreBound(
                        ThreatScoreBound.newBuilder()
                            .setLowScoreUpperBound(200)
                            .setMediumScoreUpperBound(100)
                            .setHighScoreUpperBound(50)
                            .build())
                    .build()));
  }

  @Test
  void validatePositiveSecurityEventScoreContributions() {
    assertThrows(
        IllegalArgumentException.class,
        () ->
            this.requestValidator.validateOrThrow(
                REQUEST_CONTEXT,
                UpdateSecurityEventScoreContributionRequest.newBuilder()
                    .setSecurityEventScoreContribution(
                        SecurityEventScoreContribution.newBuilder()
                            .setLowScore(-1)
                            .setMediumScore(-1)
                            .setHighScore(-1))
                    .build()));
  }

  @Test
  void validatePositiveAnomalyScoreContributions() {
    assertThrows(
        IllegalArgumentException.class,
        () ->
            this.requestValidator.validateOrThrow(
                REQUEST_CONTEXT,
                UpdateAnomalyScoreContributionRequest.newBuilder()
                    .setAnomalyScoreContribution(
                        AnomalyScoreContribution.newBuilder().setAnomalyScore(-1))
                    .build()));
  }

  @Test
  void validateSecurityEventTypeContributionKind() {
    assertThrows(
        IllegalArgumentException.class,
        () ->
            this.requestValidator.validateOrThrow(
                REQUEST_CONTEXT,
                UpdateSecurityEventTypeContributionRequest.newBuilder()
                    .setSecurityEventTypeContribution(
                        SecurityEventTypeContribution.getDefaultInstance())
                    .build()));

    assertThrows(
        IllegalArgumentException.class,
        () ->
            this.requestValidator.validateOrThrow(
                REQUEST_CONTEXT,
                UpdateSecurityEventTypeContributionRequest.newBuilder()
                    .setSecurityEventTypeContribution(
                        SecurityEventTypeContribution.newBuilder()
                            .setSecurityEventTypeContributionKind(
                                SecurityEventTypeContributionKind.UNRECOGNIZED)
                            .build())
                    .build()));
  }

  @Test
  void validateThreatAutoBlockingActionConfigActionType() {
    assertThrows(
        IllegalArgumentException.class,
        () ->
            this.requestValidator.validateOrThrow(
                REQUEST_CONTEXT,
                UpdateThreatAutoBlockingConfigRequest.newBuilder()
                    .setActionType(
                        ThreatAutoBlockingActionType.THREAT_AUTO_BLOCKING_ACTION_TYPE_UNSPECIFIED)
                    .build()));

    assertThrows(
        IllegalArgumentException.class,
        () ->
            this.requestValidator.validateOrThrow(
                REQUEST_CONTEXT, UpdateThreatAutoBlockingConfigRequest.getDefaultInstance()));
  }

  @Test
  void validateThreatAutoBlockingActionConfig_noActionType() {
    assertThrows(
        IllegalArgumentException.class,
        () ->
            this.requestValidator.validateOrThrow(
                REQUEST_CONTEXT,
                UpdateThreatAutoBlockingConfigRequest.newBuilder()
                    .setActionType(
                        ThreatAutoBlockingActionType.THREAT_AUTO_BLOCKING_ACTION_TYPE_NO_ACTION)
                    .setExpirationDetails(ExpirationDetails.newBuilder().setDuration("PT").build())
                    .build()));
  }

  @Test
  void validateThreatAutoBlockingActionConfigExpiration() {
    assertThrows(
        IllegalArgumentException.class,
        () ->
            this.requestValidator.validateOrThrow(
                REQUEST_CONTEXT,
                UpdateThreatAutoBlockingConfigRequest.newBuilder()
                    .setActionType(
                        ThreatAutoBlockingActionType.THREAT_AUTO_BLOCKING_ACTION_TYPE_BLOCK)
                    .setExpirationDetails(ExpirationDetails.newBuilder().setDuration("PT").build())
                    .build()));
  }

  @Test
  void validateUpdateIpReputationThreatScoreConfigRequest() {
    assertThrows(
        IllegalArgumentException.class,
        () ->
            this.requestValidator.validateOrThrow(
                REQUEST_CONTEXT, getUpdateIpReputationThreatScoreConfigRequest(1, 1, 1, -1)));
    assertThrows(
        IllegalArgumentException.class,
        () ->
            this.requestValidator.validateOrThrow(
                REQUEST_CONTEXT, getUpdateIpReputationThreatScoreConfigRequest(1, 1, -1, 1)));
    assertThrows(
        IllegalArgumentException.class,
        () ->
            this.requestValidator.validateOrThrow(
                REQUEST_CONTEXT, getUpdateIpReputationThreatScoreConfigRequest(1, -1, 1, 1)));
    assertThrows(
        IllegalArgumentException.class,
        () ->
            this.requestValidator.validateOrThrow(
                REQUEST_CONTEXT, getUpdateIpReputationThreatScoreConfigRequest(-1, 1, 1, 1)));
    assertDoesNotThrow(
        () ->
            this.requestValidator.validateOrThrow(
                REQUEST_CONTEXT, getUpdateIpReputationThreatScoreConfigRequest(1, 1, 1, 1)));
  }

  @Test
  void validateUpdateStatusCodeThreatScoreConfigsRequest() {

    // duplicate config
    assertThrows(
        IllegalArgumentException.class,
        () ->
            requestValidator.validateOrThrow(
                REQUEST_CONTEXT,
                UpdateStatusCodeThreatScoreConfigsRequest.newBuilder()
                    .setStatusCodeThreatScoreConfigs(
                        StatusCodeThreatScoreConfigs.newBuilder()
                            .addConfigs(
                                getStatusCodeThreatScoreConfig(
                                    Protocol.PROTOCOL_HTTP,
                                    "400",
                                    SeverityDowngradePolicy.SEVERITY_DOWNGRADE_POLICY_ONE_STEP))
                            .addConfigs(
                                getStatusCodeThreatScoreConfig(
                                    Protocol.PROTOCOL_HTTP,
                                    "400",
                                    SeverityDowngradePolicy.SEVERITY_DOWNGRADE_POLICY_ONE_STEP))
                            .build())
                    .build()));

    // invalid regex
    assertThrows(
        IllegalArgumentException.class,
        () ->
            requestValidator.validateOrThrow(
                REQUEST_CONTEXT,
                UpdateStatusCodeThreatScoreConfigsRequest.newBuilder()
                    .setStatusCodeThreatScoreConfigs(
                        StatusCodeThreatScoreConfigs.newBuilder()
                            .addConfigs(
                                getStatusCodeThreatScoreConfig(
                                    Protocol.PROTOCOL_HTTP,
                                    "[",
                                    SeverityDowngradePolicy.SEVERITY_DOWNGRADE_POLICY_ONE_STEP))
                            .build())
                    .build()));
  }

  private UpdateIpReputationThreatScoreConfigRequest getUpdateIpReputationThreatScoreConfigRequest(
      int lowScore, int mediumScore, int highScore, int criticalScore) {
    return UpdateIpReputationThreatScoreConfigRequest.newBuilder()
        .setIpReputationThreatScoreConfig(
            IpReputationThreatScoreConfig.newBuilder()
                .setCriticalIpReputationThreatScoreIncrement(criticalScore)
                .setHighIpReputationThreatScoreIncrement(highScore)
                .setMediumIpReputationThreatScoreIncrement(mediumScore)
                .setLowIpReputationThreatScoreIncrement(lowScore)
                .build())
        .build();
  }

  private StatusCodeThreatScoreConfig getStatusCodeThreatScoreConfig(
      Protocol protocol, String regex, SeverityDowngradePolicy downgradePolicy) {
    return StatusCodeThreatScoreConfig.newBuilder()
        .setProtocol(protocol)
        .setErrorStatusCodeRegex(regex)
        .setSeverityDowngradePolicy(downgradePolicy)
        .build();
  }
}
