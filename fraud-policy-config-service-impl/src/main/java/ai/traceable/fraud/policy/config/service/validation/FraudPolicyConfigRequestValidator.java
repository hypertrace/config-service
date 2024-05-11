package ai.traceable.fraud.policy.config.service.validation;

import static org.hypertrace.config.validation.GrpcValidatorUtils.validateRequestContextOrThrow;

import ai.traceable.fraud.policy.config.service.v1.CreateFraudPolicyRequest;
import ai.traceable.fraud.policy.config.service.v1.UpdateFraudPolicyRequest;
import io.grpc.Status;
import org.hypertrace.core.grpcutils.context.RequestContext;

public class FraudPolicyConfigRequestValidator {

  public void validateOrThrow(RequestContext requestContext, CreateFraudPolicyRequest request) {
    validateRequestContext(requestContext);
  }

  public void validateOrThrow(RequestContext requestContext, UpdateFraudPolicyRequest request) {
    validateRequestContext(requestContext);
  }

  private static void validateRequestContext(RequestContext requestContext) {
    validateRequestContextOrThrow(requestContext);
    if (requestContext.getTenantId().isEmpty()) {
      throw Status.INVALID_ARGUMENT
          .withDescription("Missing expected Tenant ID")
          .asRuntimeException(requestContext.buildTrailers());
    }
  }
}
