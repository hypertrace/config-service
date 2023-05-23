package ai.traceable.anomalyscoring.config.service;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

import ai.traceable.anomalyscoring.config.service.v1.ConfidenceScoreLevelConfig;
import ai.traceable.anomalyscoring.config.service.v1.ConfidenceScoringConfig;
import ai.traceable.anomalyscoring.config.service.v1.ImpactScoreLevelConfig;
import ai.traceable.anomalyscoring.config.service.v1.ImpactScoringConfig;
import ai.traceable.anomalyscoring.config.service.v1.UpdateConfidenceScoringConfigRequest;
import ai.traceable.anomalyscoring.config.service.v1.UpdateImpactScoringConfigRequest;
import io.grpc.StatusRuntimeException;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class AnomalyScoringConfigRequestValidatorTest {
  private static final RequestContext REQUEST_CONTEXT = RequestContext.forTenantId("tenant-1");
  private AnomalyScoringConfigRequestValidator requestValidator;

  @BeforeEach
  void setup() {
    this.requestValidator = new AnomalyScoringConfigRequestValidator();
  }

  @Test
  void validateNegativeImpactScore() {
    checkImpactScoreBounds(-1);
  }

  @Test
  void validateOutOfBoundImpactScore() {
    checkImpactScoreBounds(101);
  }

  @Test
  void validateImpactScore() {
    assertDoesNotThrow(
        () ->
            this.requestValidator.validateOrThrow(
                REQUEST_CONTEXT, buildUpdateImpactScoreLevelConfigRequest(1, 20)));

    assertThrows(
        StatusRuntimeException.class,
        () ->
            this.requestValidator.validateOrThrow(
                REQUEST_CONTEXT, buildUpdateImpactScoreLevelConfigRequest(30, 20)));
  }

  private UpdateImpactScoringConfigRequest buildUpdateImpactScoreLevelConfigRequest(
      int mediumScore, int highScore) {
    return UpdateImpactScoringConfigRequest.newBuilder()
        .setImpactScoringConfig(
            ImpactScoringConfig.newBuilder()
                .setImpactScoreLevelConfig(
                    ImpactScoreLevelConfig.newBuilder()
                        .setMediumLevelMinScore(mediumScore)
                        .setHighLevelMinScore(highScore)
                        .build())
                .build())
        .build();
  }

  private void checkImpactScoreBounds(int score) {
    assertThrows(
        StatusRuntimeException.class,
        () ->
            this.requestValidator.validateOrThrow(
                REQUEST_CONTEXT, buildUpdateImpactScoreLevelConfigRequest(score, 0)));

    assertThrows(
        StatusRuntimeException.class,
        () ->
            this.requestValidator.validateOrThrow(
                REQUEST_CONTEXT, buildUpdateImpactScoreLevelConfigRequest(0, score)));

    assertThrows(
        StatusRuntimeException.class,
        () ->
            this.requestValidator.validateOrThrow(
                REQUEST_CONTEXT, buildUpdateImpactScoreLevelConfigRequest(score, score)));
  }

  @Test
  void validateNegativeConfidenceScore() {
    checkConfidenceScoreBounds(-1);
  }

  @Test
  void validateOutOfBoundConfidenceScore() {
    checkConfidenceScoreBounds(101);
  }

  @Test
  void validateConfidenceScore() {
    assertDoesNotThrow(
        () ->
            this.requestValidator.validateOrThrow(
                REQUEST_CONTEXT, buildUpdateConfidenceScoreLevelConfigRequest(1, 20)));

    assertThrows(
        StatusRuntimeException.class,
        () ->
            this.requestValidator.validateOrThrow(
                REQUEST_CONTEXT, buildUpdateConfidenceScoreLevelConfigRequest(30, 20)));
  }

  private UpdateConfidenceScoringConfigRequest buildUpdateConfidenceScoreLevelConfigRequest(
      int mediumScore, int highScore) {
    return UpdateConfidenceScoringConfigRequest.newBuilder()
        .setConfidenceScoringConfig(
            ConfidenceScoringConfig.newBuilder()
                .setConfidenceScoreLevelConfig(
                    ConfidenceScoreLevelConfig.newBuilder()
                        .setMediumLevelMinScore(mediumScore)
                        .setHighLevelMinScore(highScore)
                        .build())
                .build())
        .build();
  }

  private void checkConfidenceScoreBounds(int score) {
    assertThrows(
        StatusRuntimeException.class,
        () ->
            this.requestValidator.validateOrThrow(
                REQUEST_CONTEXT, buildUpdateConfidenceScoreLevelConfigRequest(score, 0)));

    assertThrows(
        StatusRuntimeException.class,
        () ->
            this.requestValidator.validateOrThrow(
                REQUEST_CONTEXT, buildUpdateConfidenceScoreLevelConfigRequest(0, score)));

    assertThrows(
        StatusRuntimeException.class,
        () ->
            this.requestValidator.validateOrThrow(
                REQUEST_CONTEXT, buildUpdateConfidenceScoreLevelConfigRequest(score, score)));
  }
}
