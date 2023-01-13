package ai.traceable.risk.config.service.v2.grid.validator;

import ai.traceable.risk.config.service.v2.GetRiskScoringGridConfigRequest;
import ai.traceable.risk.config.service.v2.ResetRiskScoringGridConfigRequest;
import ai.traceable.risk.config.service.v2.RiskScoringGridConfigValues;
import ai.traceable.risk.config.service.v2.UpdateRiskScoringGridConfigRequest;
import io.grpc.Status;
import org.hypertrace.core.grpcutils.context.RequestContext;

public interface RiskScoringGridConfigValidator {

  void validateGetRiskScoringGridConfigRequest(
      RequestContext requestContext, GetRiskScoringGridConfigRequest request);

  void validateUpdateRiskScoringGridConfigRequest(
      RequestContext requestContext, UpdateRiskScoringGridConfigRequest request);

  void validateResetRiskScoringGridConfigRequest(
      RequestContext requestContext, ResetRiskScoringGridConfigRequest request);

  Status validateRiskScoringGridValues(RiskScoringGridConfigValues values);
}
