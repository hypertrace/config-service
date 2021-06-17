package ai.traceable.anomaly.config.service.exclusion.validators;

import io.grpc.Status;

public interface RequestValidator<T> {
  Status validate(T request);
}
