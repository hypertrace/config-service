package ai.traceable.data.obfuscation.config.service;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import ai.traceable.data.obfuscation.config.service.v1.CreateDataObfuscationStrategyRequest;
import ai.traceable.data.obfuscation.config.service.v1.DeleteDataObfuscationStrategyRequest;
import ai.traceable.data.obfuscation.config.service.v1.GetDataObfuscationStrategyRequest;
import ai.traceable.data.obfuscation.config.service.v1.HashStrategy;
import ai.traceable.data.obfuscation.config.service.v1.ObfuscationStrategyInput;
import ai.traceable.data.obfuscation.config.service.v1.ScryptHash;
import ai.traceable.data.obfuscation.config.service.v1.UpdateDataObfuscationStrategyRequest;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class DataObfuscationConfigRequestValidatorTest {

  private static final String TEST_TENANT_ID = "test-tenant-id";
  private static final String TEST_SALT = "test-salt";
  private static final HashStrategy STRATEGY_SCRYPT =
      HashStrategy.newBuilder()
          .setScrypt(
              ScryptHash.newBuilder()
                  .setCost(1)
                  .setBlockSize(2)
                  .setThreads(3)
                  .setKeyLength(5)
                  .build())
          .build();

  private DataObfuscationConfigRequestValidator validator;
  private RequestContext requestContext;

  @BeforeEach
  void setUp() {
    validator = new DataObfuscationConfigRequestValidator();
    requestContext = RequestContext.forTenantId(TEST_TENANT_ID);
  }

  @Test
  void testValidateCreateRequest_validRequest_success() {
    ObfuscationStrategyInput input =
        ObfuscationStrategyInput.newBuilder()
            .setHashStrategy(STRATEGY_SCRYPT)
            .setSalt(TEST_SALT)
            .build();

    CreateDataObfuscationStrategyRequest request =
        CreateDataObfuscationStrategyRequest.newBuilder().setObfuscationStrategy(input).build();

    validator.validateOrThrow(requestContext, request);
  }

  @Test
  void testValidateCreateRequest_missingObfuscationStrategy_throwsInvalidArgument() {
    CreateDataObfuscationStrategyRequest request =
        CreateDataObfuscationStrategyRequest.newBuilder().build();

    StatusRuntimeException exception =
        assertThrows(
            StatusRuntimeException.class, () -> validator.validateOrThrow(requestContext, request));

    assertEquals(Status.INVALID_ARGUMENT.getCode(), exception.getStatus().getCode());
    assertEquals(
        "Missing obfuscation strategy inside create request",
        exception.getStatus().getDescription());
  }

  @Test
  void testValidateCreateRequest_missingHashStrategy_throwsInvalidArgument() {
    ObfuscationStrategyInput input =
        ObfuscationStrategyInput.newBuilder().setSalt(TEST_SALT).build();

    CreateDataObfuscationStrategyRequest request =
        CreateDataObfuscationStrategyRequest.newBuilder().setObfuscationStrategy(input).build();

    StatusRuntimeException exception =
        assertThrows(
            StatusRuntimeException.class, () -> validator.validateOrThrow(requestContext, request));

    assertEquals(Status.INVALID_ARGUMENT.getCode(), exception.getStatus().getCode());
    assertEquals(
        "No hash strategy provided in data obfuscation strategy input",
        exception.getStatus().getDescription());
  }

  @Test
  void testValidateGetRequest_validRequest_success() {
    GetDataObfuscationStrategyRequest request =
        GetDataObfuscationStrategyRequest.newBuilder().build();

    validator.validateOrThrow(requestContext);
  }

  @Test
  void testValidateUpdateRequest_validRequest_success() {
    ObfuscationStrategyInput input =
        ObfuscationStrategyInput.newBuilder()
            .setHashStrategy(STRATEGY_SCRYPT)
            .setSalt(TEST_SALT)
            .build();

    UpdateDataObfuscationStrategyRequest request =
        UpdateDataObfuscationStrategyRequest.newBuilder().setObfuscationStrategy(input).build();

    validator.validateOrThrow(requestContext, request);
  }

  @Test
  void testValidateUpdateRequest_missingHashStrategy_throwsInvalidArgument() {
    ObfuscationStrategyInput input =
        ObfuscationStrategyInput.newBuilder().setSalt(TEST_SALT).build();

    UpdateDataObfuscationStrategyRequest request =
        UpdateDataObfuscationStrategyRequest.newBuilder().setObfuscationStrategy(input).build();

    StatusRuntimeException exception =
        assertThrows(
            StatusRuntimeException.class, () -> validator.validateOrThrow(requestContext, request));

    assertEquals(Status.INVALID_ARGUMENT.getCode(), exception.getStatus().getCode());
    assertEquals(
        "No hash strategy provided in data obfuscation strategy input",
        exception.getStatus().getDescription());
  }

  @Test
  void testValidateDeleteRequest_validRequest_success() {
    DeleteDataObfuscationStrategyRequest request =
        DeleteDataObfuscationStrategyRequest.newBuilder().build();

    validator.validateOrThrow(requestContext);
  }

  @Test
  void testValidateCreateRequest_withEmptySalt_success() {
    ObfuscationStrategyInput input =
        ObfuscationStrategyInput.newBuilder().setHashStrategy(STRATEGY_SCRYPT).setSalt("").build();

    CreateDataObfuscationStrategyRequest request =
        CreateDataObfuscationStrategyRequest.newBuilder().setObfuscationStrategy(input).build();

    validator.validateOrThrow(requestContext, request);
  }

  @Test
  void testValidateUpdateRequest_withEmptySalt_success() {
    ObfuscationStrategyInput input =
        ObfuscationStrategyInput.newBuilder().setHashStrategy(STRATEGY_SCRYPT).setSalt("").build();

    UpdateDataObfuscationStrategyRequest request =
        UpdateDataObfuscationStrategyRequest.newBuilder().setObfuscationStrategy(input).build();

    validator.validateOrThrow(requestContext, request);
  }
}
