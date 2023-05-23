package ai.traceable.anomalyscoring.config.service;

import ai.traceable.anomalyscoring.config.service.v1.ConfidenceScoreLevelConfig;
import ai.traceable.anomalyscoring.config.service.v1.GetAnomalyScoringConfigRequest;
import ai.traceable.anomalyscoring.config.service.v1.ImpactScoreLevelConfig;
import ai.traceable.anomalyscoring.config.service.v1.UpdateConfidenceScoringConfigRequest;
import ai.traceable.anomalyscoring.config.service.v1.UpdateImpactScoringConfigRequest;
import io.grpc.Status;
import org.hypertrace.config.validation.GrpcValidatorUtils;
import org.hypertrace.core.grpcutils.context.RequestContext;

class AnomalyScoringConfigRequestValidator {

  private static final String SCORE_TYPE_CONFIDENCE = "Confidence";
  private static final String SCORE_TYPE_IMPACT = "Impact";
  private static final int MAX_SCORE = 100;
  private static final int MIN_SCORE = 0;

  public void validateOrThrow(
      RequestContext requestContext, UpdateImpactScoringConfigRequest request) {
    GrpcValidatorUtils.validateRequestContextOrThrow(requestContext);
    this.validateImpactScoreLevelConfig(
        requestContext, request.getImpactScoringConfig().getImpactScoreLevelConfig());
  }

  public void validateOrThrow(
      RequestContext requestContext, UpdateConfidenceScoringConfigRequest request) {
    GrpcValidatorUtils.validateRequestContextOrThrow(requestContext);
    this.validateConfidenceScoreLevelConfig(
        requestContext, request.getConfidenceScoringConfig().getConfidenceScoreLevelConfig());
  }

  public void validateOrThrow(
      RequestContext requestContext, GetAnomalyScoringConfigRequest request) {
    GrpcValidatorUtils.validateRequestContextOrThrow(requestContext);
  }

  private void validateImpactScoreLevelConfig(
      RequestContext requestContext, ImpactScoreLevelConfig impactScoreLevelConfig) {
    validateCommonScoreLevelConfig(
        requestContext,
        impactScoreLevelConfig.getMediumLevelMinScore(),
        impactScoreLevelConfig.getHighLevelMinScore(),
        SCORE_TYPE_IMPACT);
  }

  private void validateConfidenceScoreLevelConfig(
      RequestContext requestContext, ConfidenceScoreLevelConfig confidenceScoreLevelConfig) {
    validateCommonScoreLevelConfig(
        requestContext,
        confidenceScoreLevelConfig.getMediumLevelMinScore(),
        confidenceScoreLevelConfig.getHighLevelMinScore(),
        SCORE_TYPE_CONFIDENCE);
  }

  private void validateCommonScoreLevelConfig(
      RequestContext requestContext, int mediumScore, int highScore, String scoreType) {

    if (mediumScore < MIN_SCORE || highScore < MIN_SCORE) {
      throwInvalidArgumentException(
          requestContext, String.format("%s level should be non negative", scoreType));
    }

    if (mediumScore > MAX_SCORE || highScore > MAX_SCORE) {
      throwInvalidArgumentException(
          requestContext,
          String.format("%s level min can not be more than %s", scoreType, MAX_SCORE));
    }

    if (mediumScore > highScore) {
      throwInvalidArgumentException(
          requestContext, String.format("%s level medium can not be greater than high", scoreType));
    }
  }

  private void throwInvalidArgumentException(RequestContext requestContext, String description) {
    throw Status.INVALID_ARGUMENT
        .withDescription(description)
        .asRuntimeException(requestContext.buildTrailers());
  }
}
