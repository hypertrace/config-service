package ai.traceable.fraud.datamodel.derivation.config.service.validation;

import static org.hypertrace.config.validation.GrpcValidatorUtils.validateRequestContextOrThrow;

import ai.traceable.fraud.datamodel.derivation.config.service.v1.CreateDerivationConfigRequest;
import ai.traceable.fraud.datamodel.derivation.config.service.v1.CreateUserAgentMergeMappingConfigRequest;
import ai.traceable.fraud.datamodel.derivation.config.service.v1.DeleteDerivationConfigsRequest;
import ai.traceable.fraud.datamodel.derivation.config.service.v1.DeleteUserAgentMergeMappingConfigRequest;
import ai.traceable.fraud.datamodel.derivation.config.service.v1.GetUserAgentMergeMappingConfigRequest;
import ai.traceable.fraud.datamodel.derivation.config.service.v1.GetUserAgentMergeMappingConfigsRequest;
import ai.traceable.fraud.datamodel.derivation.config.service.v1.UpdateDerivationConfigRequest;
import ai.traceable.fraud.datamodel.derivation.config.service.v1.UpdateUserAgentMergeMappingConfigRequest;
import ai.traceable.fraud.datamodel.derivation.config.service.v1.UpsertDerivationConfigRequest;
import io.grpc.Status;
import org.hypertrace.core.grpcutils.context.RequestContext;

public class FraudDataModelDerivationConfigRequestValidator {

  public void validateOrThrow(
      RequestContext requestContext, CreateDerivationConfigRequest request) {
    validateRequestContext(requestContext);
  }

  public void validateOrThrow(
      RequestContext requestContext, UpsertDerivationConfigRequest request) {
    validateRequestContext(requestContext);
  }

  public void validateOrThrow(
      RequestContext requestContext, UpdateDerivationConfigRequest request) {
    validateRequestContext(requestContext);
  }

  public void validateOrThrow(
      RequestContext requestContext, DeleteDerivationConfigsRequest request) {
    validateRequestContext(requestContext);
  }

  public void validateOrThrow(
      RequestContext requestContext, DeleteUserAgentMergeMappingConfigRequest request) {
    validateRequestContext(requestContext);
  }

  public void validateOrThrow(
      RequestContext requestContext, GetUserAgentMergeMappingConfigRequest request) {
    validateRequestContext(requestContext);
  }

  public void validateOrThrow(
      RequestContext requestContext, GetUserAgentMergeMappingConfigsRequest request) {
    validateRequestContext(requestContext);
  }

  public void validateOrThrow(
      RequestContext requestContext, CreateUserAgentMergeMappingConfigRequest request) {
    validateRequestContext(requestContext);
  }

  public void validateOrThrow(
      RequestContext requestContext, UpdateUserAgentMergeMappingConfigRequest request) {
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
