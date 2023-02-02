package ai.traceable.risk.config.service.v2.factors.validator;

import static org.hypertrace.config.validation.GrpcValidatorUtils.validateNonDefaultPresenceOrThrow;

import ai.traceable.risk.config.service.v2.RiskConfigServiceRequestValidator;
import ai.traceable.risk.config.service.v2.RiskElementConfig;
import ai.traceable.risk.config.service.v2.RiskFactorConfig;
import ai.traceable.risk.config.service.v2.RiskFactorConfigUpdateDetails;
import ai.traceable.risk.config.service.v2.RiskFactorInfo;
import ai.traceable.risk.config.service.v2.elements.validator.RiskElementConfigValidator;
import io.grpc.Status;
import java.util.Collection;
import javax.inject.Inject;
import lombok.AllArgsConstructor;

@AllArgsConstructor(onConstructor_ = {@Inject})
public class RiskFactorConfigsValidatorImpl implements RiskFactorConfigsValidator {

  private final RiskElementConfigValidator elementConfigValidator;
  private final RiskConfigServiceRequestValidator requestValidator;

  @Override
  public void validateRiskFactorConfigUpdateDetails(
      Collection<RiskFactorConfigUpdateDetails> riskFactorConfigUpdateDetailsList) {
    for (RiskFactorConfigUpdateDetails updateDetails : riskFactorConfigUpdateDetailsList) {
      validateNonDefaultPresenceOrThrow(
          updateDetails, RiskFactorConfigUpdateDetails.RISK_FACTOR_CATEGORY_FIELD_NUMBER);

      Status status =
          requestValidator.validateFactorCategory(updateDetails.getRiskFactorCategory());
      if (!status.isOk()) {
        throw status.asRuntimeException();
      }
      if (updateDetails
          .getUpdateCase()
          .equals(RiskFactorConfigUpdateDetails.UpdateCase.UPDATE_NOT_SET)) {
        throw Status.INVALID_ARGUMENT
            .withDescription("No factor config update details provided in request")
            .asRuntimeException();
      }
      if (updateDetails
          .getUpdateCase()
          .equals(RiskFactorConfigUpdateDetails.UpdateCase.RISK_ELEMENT_CONFIG_UPDATES)) {
        elementConfigValidator.validateRiskElementUpdateDetails(
            updateDetails.getRiskElementConfigUpdates());
      }
    }
  }

  @Override
  public Status validateRiskFactorConfig(RiskFactorConfig riskFactorConfig) {
    Status status =
        requestValidator.validateFactorCategory(riskFactorConfig.getRiskFactorCategory());
    if (!status.isOk()) {
      return status;
    }
    if (riskFactorConfig.getRiskElementConfigsList().isEmpty()) {
      return Status.NOT_FOUND.withDescription("No elements found");
    }
    for (RiskElementConfig elementConfig : riskFactorConfig.getRiskElementConfigsList()) {
      status = elementConfigValidator.validateRiskElementConfig(elementConfig);
      if (!status.isOk()) {
        return status;
      }
    }
    return Status.OK;
  }

  @Override
  public Status validateRiskFactorInfo(RiskFactorInfo riskFactorInfo) {
    return requestValidator.validateContributorCategory(
        riskFactorInfo.getRiskContributorCategory());
  }
}
