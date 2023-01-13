package ai.traceable.risk.config.service.v2.level.validator;

import ai.traceable.risk.config.service.v2.RiskConfigServiceRequestValidator;
import ai.traceable.risk.config.service.v2.RiskLevelConfigValues;
import io.grpc.Status;
import javax.inject.Inject;
import lombok.AllArgsConstructor;

@AllArgsConstructor(onConstructor_ = {@Inject})
public class RiskLevelConfigValidatorImpl implements RiskLevelConfigValidator {

  private final RiskConfigServiceRequestValidator requestValidator;

  @Override
  public Status validateRiskLevelConfigValues(RiskLevelConfigValues values) {
    Status status = requestValidator.validateScoreValue(values.getMediumLevelMinScore());
    if (!status.isOk()) {
      return status;
    }

    status = requestValidator.validateScoreValue(values.getHighLevelMinScore());
    if (!status.isOk()) {
      return status;
    }

    status = requestValidator.validateScoreValue(values.getCriticalLevelMinScore());
    if (!status.isOk()) {
      return status;
    }

    if (!(values.getMediumLevelMinScore() <= values.getHighLevelMinScore()
        && values.getHighLevelMinScore() <= values.getCriticalLevelMinScore())) {
      return Status.INVALID_ARGUMENT.withDescription(
          "Minimum Scores should satisfy the relation Medium <= High <= Critical");
    }
    return Status.OK;
  }
}
