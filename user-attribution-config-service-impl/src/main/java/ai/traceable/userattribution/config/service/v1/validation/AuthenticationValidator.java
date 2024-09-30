package ai.traceable.userattribution.config.service.v1.validation;

import ai.traceable.userattribution.config.service.v1.Authentication;
import io.grpc.Status;

public class AuthenticationValidator {
  private static final int MAX_ALLOWED_AUTH_TYPE_LENGTH = 100;

  void validate(final Authentication authentication) {
    if (authentication.getType().isBlank()) {
      throw Status.INVALID_ARGUMENT
          .withDescription("Authentication type cannot be empty")
          .asRuntimeException();
    }

    if (authentication.getType().length() > MAX_ALLOWED_AUTH_TYPE_LENGTH) {
      throw Status.INVALID_ARGUMENT
          .withDescription(
              String.format(
                  "Authentication type cannot exceed %d characters", MAX_ALLOWED_AUTH_TYPE_LENGTH))
          .asRuntimeException();
    }
  }
}
