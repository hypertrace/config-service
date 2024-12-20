package ai.traceable.risk.config.service.v2.contributors.validator;

import static org.hypertrace.config.validation.GrpcValidatorUtils.validateNonDefaultPresenceOrThrow;

import ai.traceable.risk.config.service.v2.GetRiskContributorConfigsRequest;
import ai.traceable.risk.config.service.v2.ResetRiskContributorConfigsRequest;
import ai.traceable.risk.config.service.v2.RiskConfigServiceRequestValidator;
import ai.traceable.risk.config.service.v2.RiskContributorConfigs;
import ai.traceable.risk.config.service.v2.RiskFactor;
import ai.traceable.risk.config.service.v2.RiskFactorCategory;
import ai.traceable.risk.config.service.v2.UpdateRiskContributorConfigsRequest;
import ai.traceable.risk.config.service.v2.factors.validator.RiskFactorConfigsValidator;
import io.grpc.Status;
import jakarta.inject.Inject;
import lombok.AllArgsConstructor;
import org.hypertrace.core.grpcutils.context.RequestContext;

@AllArgsConstructor(onConstructor_ = {@Inject})
public class RiskContributorConfigsValidatorImpl implements RiskContributorConfigsValidator {

  private final RiskFactorConfigsValidator factorConfigsValidator;
  private final RiskConfigServiceRequestValidator requestValidator;

  @Override
  public void validateGetRiskContributorConfigsRequest(
      RequestContext requestContext, GetRiskContributorConfigsRequest request) {
    requestValidator.validateContextAndScope(requestContext, request.getRiskConfigScope());
  }

  @Override
  public void validateUpdateRiskContributorConfigsRequest(
      RequestContext requestContext, UpdateRiskContributorConfigsRequest request) {
    requestValidator.validateContextAndScope(requestContext, request.getRiskConfigScope());
    validateNonDefaultPresenceOrThrow(
        request,
        UpdateRiskContributorConfigsRequest.RISK_FACTOR_CONFIG_UPDATE_DETAILS_FIELD_NUMBER);
    factorConfigsValidator.validateRiskFactorConfigUpdateDetails(
        request.getRiskFactorConfigUpdateDetailsList());
  }

  @Override
  public void validateResetRiskContributorConfigsRequest(
      RequestContext requestContext, ResetRiskContributorConfigsRequest request) {
    requestValidator.validateContextAndScope(requestContext, request.getRiskConfigScope());
    validateNonDefaultPresenceOrThrow(
        request, ResetRiskContributorConfigsRequest.RISK_FACTOR_CATEGORIES_FIELD_NUMBER);
    for (RiskFactorCategory category : request.getRiskFactorCategoriesList()) {
      Status status = requestValidator.validateFactorCategory(category);
      if (!status.isOk()) {
        throw status.asRuntimeException();
      }
    }
  }

  @Override
  public Status validateRiskContributorConfigs(RiskContributorConfigs configs) {
    if (configs == null || configs.getRiskFactorsList().isEmpty()) {
      return Status.NOT_FOUND.withDescription("No factors found");
    }
    for (RiskFactor factor : configs.getRiskFactorsList()) {
      Status status = factorConfigsValidator.validateRiskFactorConfig(factor.getRiskFactorConfig());
      if (!status.isOk()) {
        return status;
      }
      status = factorConfigsValidator.validateRiskFactorInfo(factor.getRiskFactorInfo());
      if (!status.isOk()) {
        return status;
      }
    }
    return Status.OK;
  }
}
