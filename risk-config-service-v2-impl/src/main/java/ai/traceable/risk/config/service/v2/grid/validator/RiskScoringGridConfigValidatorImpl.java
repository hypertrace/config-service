package ai.traceable.risk.config.service.v2.grid.validator;

import static org.hypertrace.config.validation.GrpcValidatorUtils.validateNonDefaultPresenceOrThrow;

import ai.traceable.risk.config.service.v2.GetRiskScoringGridConfigRequest;
import ai.traceable.risk.config.service.v2.ResetRiskScoringGridConfigRequest;
import ai.traceable.risk.config.service.v2.RiskConfigServiceRequestValidator;
import ai.traceable.risk.config.service.v2.RiskScoringGridCell;
import ai.traceable.risk.config.service.v2.RiskScoringGridConfigValues;
import ai.traceable.risk.config.service.v2.UpdateRiskScoringGridConfigRequest;
import io.grpc.Status;
import jakarta.inject.Inject;
import java.util.Collection;
import lombok.AllArgsConstructor;
import org.hypertrace.core.grpcutils.context.RequestContext;

@AllArgsConstructor(onConstructor_ = {@Inject})
public class RiskScoringGridConfigValidatorImpl implements RiskScoringGridConfigValidator {

  private final RiskConfigServiceRequestValidator requestValidator;

  @Override
  public void validateGetRiskScoringGridConfigRequest(
      RequestContext requestContext, GetRiskScoringGridConfigRequest request) {
    requestValidator.validateContextAndScope(requestContext, request.getRiskConfigScope());
  }

  @Override
  public void validateUpdateRiskScoringGridConfigRequest(
      RequestContext requestContext, UpdateRiskScoringGridConfigRequest request) {
    requestValidator.validateContextAndScope(requestContext, request.getRiskConfigScope());
    validateNonDefaultPresenceOrThrow(
        request, UpdateRiskScoringGridConfigRequest.RISK_SCORING_GRID_CELLS_FIELD_NUMBER);
    Status status = validateGridCells(request.getRiskScoringGridCellsList());
    if (!status.isOk()) {
      throw status.asRuntimeException();
    }
  }

  @Override
  public void validateResetRiskScoringGridConfigRequest(
      RequestContext requestContext, ResetRiskScoringGridConfigRequest request) {
    requestValidator.validateContextAndScope(requestContext, request.getRiskConfigScope());
  }

  @Override
  public Status validateRiskScoringGridValues(RiskScoringGridConfigValues values) {
    return validateGridCells(values.getRiskScoringGridCellsList());
  }

  private Status validateGridCells(Collection<RiskScoringGridCell> riskScoringGridCellsList) {
    for (RiskScoringGridCell cell : riskScoringGridCellsList) {
      Status status = requestValidator.validateScoreValue(cell.getScore());
      if (!status.isOk()) {
        return status;
      }

      status = requestValidator.validateScoreCategory(cell.getLikelihoodScoreCategory());
      if (!status.isOk()) {
        return status;
      }

      status = requestValidator.validateScoreCategory(cell.getImpactScoreCategory());
      if (!status.isOk()) {
        return status;
      }
    }
    return Status.OK;
  }
}
