package ai.traceable.risk.config.service.v2;

import static org.hypertrace.config.validation.GrpcValidatorUtils.validateNonDefaultPresenceOrThrow;
import static org.hypertrace.config.validation.GrpcValidatorUtils.validateRequestContextOrThrow;

import io.grpc.Status;
import org.hypertrace.core.grpcutils.context.RequestContext;

public class RiskConfigServiceRequestValidator {

  public void validateContextAndScope(
      RequestContext requestContext, RiskConfigScope riskConfigScope) {
    validateRequestContextOrThrow(requestContext);
    if (riskConfigScope == null) {
      throw Status.INVALID_ARGUMENT
          .withDescription("No scope present in request")
          .asRuntimeException();
    }
    validateEnvironmentScopeOrThrow(riskConfigScope);
  }

  private void validateEnvironmentScopeOrThrow(RiskConfigScope riskConfigScope) {
    if (riskConfigScope.hasEnvironmentScope()) {
      validateNonDefaultPresenceOrThrow(
          riskConfigScope.getEnvironmentScope(), EnvironmentScope.ENVIRONMENT_ID_FIELD_NUMBER);
    }
  }

  public Status validateFactorCategory(RiskFactorCategory scoreCategory) {
    if (scoreCategory.equals(RiskFactorCategory.RISK_FACTOR_CATEGORY_UNSPECIFIED)
        || scoreCategory.equals(RiskFactorCategory.UNRECOGNIZED)) {
      return Status.INVALID_ARGUMENT.withDescription("Invalid Factor Category");
    }
    return Status.OK;
  }

  public Status validateContributorCategory(RiskContributorCategory contributorCategory) {
    if (contributorCategory.equals(RiskContributorCategory.RISK_CONTRIBUTOR_CATEGORY_UNSPECIFIED)
        || contributorCategory.equals(RiskContributorCategory.UNRECOGNIZED)) {
      return Status.INVALID_ARGUMENT.withDescription("Invalid Contributor Category");
    }
    return Status.OK;
  }

  public Status validateElementId(String id) {
    if (id.isBlank()) {
      return Status.INVALID_ARGUMENT.withDescription("Element should have a valid ID");
    }
    return Status.OK;
  }

  public Status validateScoreValue(int score) {
    if (score >= 0 && score <= 10) {
      return Status.OK;
    }
    return Status.OUT_OF_RANGE.withDescription("Score should be between 0 and 10");
  }

  public Status validateScoreCategory(RiskScoreCategory scoreCategory) {
    if (scoreCategory.equals(RiskScoreCategory.RISK_SCORE_CATEGORY_UNSPECIFIED)
        || scoreCategory.equals(RiskScoreCategory.UNRECOGNIZED)) {
      return Status.INVALID_ARGUMENT.withDescription("Invalid Score Category");
    }
    return Status.OK;
  }
}
