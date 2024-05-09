package ai.traceable.fraud.datamodel.derivation.config.service.validation;

import static org.hypertrace.config.validation.GrpcValidatorUtils.validateRequestContextOrThrow;

import ai.traceable.fraud.datamodel.derivation.config.service.v1.CreateDerivationConfigRequest;
import ai.traceable.fraud.datamodel.derivation.config.service.v1.UpdateDerivationConfigRequest;
import io.grpc.Status;
import org.hypertrace.core.grpcutils.context.RequestContext;

public class FraudDataModelDerivationConfigRequestValidator {

  public void validateOrThrow(
      RequestContext requestContext, CreateDerivationConfigRequest request) {
    validateRequestContext(requestContext);
  }

  public void validateOrThrow(
      RequestContext requestContext, UpdateDerivationConfigRequest request) {
    validateRequestContext(requestContext);
  }

  private static void validateRequestContext(RequestContext requestContext) {
    validateRequestContextOrThrow(requestContext);
    if (requestContext.getTenantId().isEmpty()) {
      throw Status.INVALID_ARGUMENT
          .withDescription("Missing expected Tenant ID")
          .asRuntimeException();
    }
  }
}
