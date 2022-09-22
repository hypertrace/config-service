package ai.traceable.userattribution.config.service.validation;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

import ai.traceable.userattribution.config.service.v1.Authentication;
import io.grpc.StatusRuntimeException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class AuthenticationValidatorTest {

  private AuthenticationValidator authenticationValidator;

  @BeforeEach
  void setUp() {
    authenticationValidator = new AuthenticationValidator();
  }

  @Test
  void testValidateWithInvalidType_throwsException() {
    assertThrows(
        StatusRuntimeException.class,
        () -> authenticationValidator.validate(Authentication.newBuilder().setType("").build()));
    assertThrows(
        StatusRuntimeException.class,
        () ->
            authenticationValidator.validate(
                Authentication.newBuilder().setType("   \n\t").build()));
  }

  @Test
  void testValidateWithValidType_noException() {
    assertDoesNotThrow(
        () ->
            authenticationValidator.validate(
                Authentication.newBuilder().setType("valid-type").build()));
  }

  @Test
  void testValidateWithLongType_throwsException() {
    assertThrows(
        StatusRuntimeException.class,
        () ->
            authenticationValidator.validate(
                Authentication.newBuilder()
                    .setType(
                        "This is a very long authentication type deliberately created to test "
                            + "whether the validator fails with this value")
                    .build()));
  }
}
