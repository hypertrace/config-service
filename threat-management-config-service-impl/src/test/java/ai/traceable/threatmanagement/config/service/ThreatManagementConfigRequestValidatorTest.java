package ai.traceable.threatmanagement.config.service;

import static org.junit.jupiter.api.Assertions.assertThrows;

import ai.traceable.threatmanagement.config.service.v1.SecurityEventScoreContribution;
import ai.traceable.threatmanagement.config.service.v1.ThreatScoreBound;
import ai.traceable.threatmanagement.config.service.v1.UpdateSecurityEventScoreContributionRequest;
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
                            .setMediumScoreUpperBound(100)
                            .setHighScoreUpperBound(50)
                            .build())
                    .build()));
  }

  @Test
  void validatePositiveSecurityEventContributions() {
    assertThrows(
        IllegalArgumentException.class,
        () ->
            this.requestValidator.validateOrThrow(
                REQUEST_CONTEXT,
                UpdateSecurityEventScoreContributionRequest.newBuilder()
                    .setSecurityEventScoreContribution(
                        SecurityEventScoreContribution.newBuilder()
                            .setAnomalyScore(-1)
                            .setMediumScore(-1)
                            .setHighScore(-1))
                    .build()));
  }
}
