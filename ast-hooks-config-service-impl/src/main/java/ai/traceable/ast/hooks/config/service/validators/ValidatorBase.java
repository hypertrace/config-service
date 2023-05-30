package ai.traceable.ast.hooks.config.service.validators;

import io.grpc.Status;
import org.apache.commons.lang3.StringUtils;
import org.hypertrace.core.grpcutils.context.RequestContext;

public abstract class ValidatorBase {
  protected void validateStringNotBlank(String value, String description) {
    if (StringUtils.isBlank(value)) {
      throw Status.INVALID_ARGUMENT.withDescription(description).asRuntimeException();
    }
  }

  protected RequestContext getRequestContext() {
    return RequestContext.CURRENT.get();
  }
}
