package ai.traceable.bot.categorized.config.service.v1.validation;

import static org.hypertrace.config.validation.GrpcValidatorUtils.validateRequestContextOrThrow;

import io.grpc.Status;
import lombok.experimental.UtilityClass;
import org.hypertrace.core.grpcutils.context.RequestContext;

@UtilityClass
public final class CategorizedBotConfigRequestValidator {

  public static void validateRequestContext(final RequestContext requestContext) {
    validateRequestContextOrThrow(requestContext);
    if (requestContext.getTenantId().isEmpty()) {
      throw Status.INVALID_ARGUMENT
          .withDescription("Missing expected Tenant ID")
          .asRuntimeException(requestContext.buildTrailers());
    }
  }
}
