package ai.traceable.risk.config.service.v2.factors.validator;

import ai.traceable.risk.config.service.v2.RiskFactorConfig;
import ai.traceable.risk.config.service.v2.RiskFactorConfigUpdateDetails;
import ai.traceable.risk.config.service.v2.RiskFactorInfo;
import io.grpc.Status;
import java.util.Collection;

public interface RiskFactorConfigsValidator {

  void validateRiskFactorConfigUpdateDetails(
      Collection<RiskFactorConfigUpdateDetails> riskFactorConfigUpdateDetailsList);

  Status validateRiskFactorConfig(RiskFactorConfig riskFactorConfig);

  Status validateRiskFactorInfo(RiskFactorInfo riskFactorInfo);
}
