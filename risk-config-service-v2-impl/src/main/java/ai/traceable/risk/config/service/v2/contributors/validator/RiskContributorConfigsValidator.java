package ai.traceable.risk.config.service.v2.contributors.validator;

import ai.traceable.risk.config.service.v2.GetRiskContributorConfigsRequest;
import ai.traceable.risk.config.service.v2.ResetRiskContributorConfigsRequest;
import ai.traceable.risk.config.service.v2.RiskContributorConfigs;
import ai.traceable.risk.config.service.v2.UpdateRiskContributorConfigsRequest;
import io.grpc.Status;
import org.hypertrace.core.grpcutils.context.RequestContext;

public interface RiskContributorConfigsValidator {

  void validateGetRiskContributorConfigsRequest(
      RequestContext requestContext, GetRiskContributorConfigsRequest request);

  void validateUpdateRiskContributorConfigsRequest(
      RequestContext requestContext, UpdateRiskContributorConfigsRequest request);

  void validateResetRiskContributorConfigsRequest(
      RequestContext requestContext, ResetRiskContributorConfigsRequest request);

  Status validateRiskContributorConfigs(RiskContributorConfigs configs);
}
