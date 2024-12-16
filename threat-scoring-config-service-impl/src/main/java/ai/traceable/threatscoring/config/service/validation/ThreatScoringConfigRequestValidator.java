package ai.traceable.threatscoring.config.service.validation;

import static org.hypertrace.config.validation.GrpcValidatorUtils.validateRequestContextOrThrow;

import ai.traceable.threatscoring.config.service.v1.AnomalousEventConfidenceConfig;
import ai.traceable.threatscoring.config.service.v1.OverrideEventConfidenceScoringConfigRequest;
import io.grpc.Status;
import javax.inject.Inject;
import lombok.AllArgsConstructor;
import org.hypertrace.core.grpcutils.context.RequestContext;

@AllArgsConstructor(onConstructor_ = @Inject)
public class ThreatScoringConfigRequestValidator {
  public void validateGetOrDeleteRequest(RequestContext requestContext) {
    validateRequestContextOrThrow(requestContext);
  }

  public void validateOverrideEventConfidenceScoringConfigRequest(
      RequestContext requestContext, OverrideEventConfidenceScoringConfigRequest request) {
    validateRequestContextOrThrow(requestContext);
    validateAnomalousEventConfidenceScoringConfig(
        request.getEventConfidenceScoringConfig().getAnomalousEventConfidenceConfig());
  }

  private void validateAnomalousEventConfidenceScoringConfig(
      AnomalousEventConfidenceConfig anomalousEventConfidenceConfig) {
    if (anomalousEventConfidenceConfig.getMinNumberOfUniqueUnexpectedCharacters() < 0) {
      throw Status.INVALID_ARGUMENT
          .withDescription(
              String.format(
                  "Min number of unique unexpected characters should be > 0 but found %s",
                  anomalousEventConfidenceConfig.getMinNumberOfUniqueUnexpectedCharacters()))
          .asRuntimeException();
    }

    if (anomalousEventConfidenceConfig.getMinNumberOfUnlearntParamsInApi() < 0) {
      throw Status.INVALID_ARGUMENT
          .withDescription(
              String.format(
                  "Min number of unlearnt params in api should be > 0 but found %s",
                  anomalousEventConfidenceConfig.getMinNumberOfUnlearntParamsInApi()))
          .asRuntimeException();
    }
  }
}
