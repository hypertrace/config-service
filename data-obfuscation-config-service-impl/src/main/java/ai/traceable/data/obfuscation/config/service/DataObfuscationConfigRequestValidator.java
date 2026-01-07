package ai.traceable.data.obfuscation.config.service;

import static org.hypertrace.config.validation.GrpcValidatorUtils.validateRequestContextOrThrow;

import ai.traceable.data.obfuscation.config.service.v1.CreateDataObfuscationStrategyRequest;
import ai.traceable.data.obfuscation.config.service.v1.ObfuscationStrategyInput;
import ai.traceable.data.obfuscation.config.service.v1.UpdateDataObfuscationStrategyRequest;
import io.grpc.Status;
import org.hypertrace.core.grpcutils.context.RequestContext;

public class DataObfuscationConfigRequestValidator {

  public void validateOrThrow(
      RequestContext requestContext, CreateDataObfuscationStrategyRequest request) {
    validateRequestContextOrThrow(requestContext);
    if (!request.hasObfuscationStrategy()) {
      throw Status.INVALID_ARGUMENT
          .withDescription("Missing obfuscation strategy inside create request")
          .asRuntimeException();
    }
    validateObfuscationStrategy(request.getObfuscationStrategy());
  }

  public void validateOrThrow(RequestContext requestContext) {
    validateRequestContextOrThrow(requestContext);
  }

  public void validateOrThrow(
      RequestContext requestContext, UpdateDataObfuscationStrategyRequest request) {
    validateRequestContextOrThrow(requestContext);
    validateObfuscationStrategy(request.getObfuscationStrategy());
  }

  private void validateObfuscationStrategy(ObfuscationStrategyInput input) {
    if (!input.hasHashStrategy()) {
      throw Status.INVALID_ARGUMENT
          .withDescription("No hash strategy provided in data obfuscation strategy input")
          .asRuntimeException();
    }
  }
}
