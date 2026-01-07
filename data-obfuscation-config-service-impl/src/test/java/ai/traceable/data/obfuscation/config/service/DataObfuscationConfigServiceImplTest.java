package ai.traceable.data.obfuscation.config.service;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import ai.traceable.data.obfuscation.config.service.rules.ConfigManager;
import ai.traceable.data.obfuscation.config.service.v1.CreateDataObfuscationStrategyRequest;
import ai.traceable.data.obfuscation.config.service.v1.CreateDataObfuscationStrategyResponse;
import ai.traceable.data.obfuscation.config.service.v1.DeleteDataObfuscationStrategyRequest;
import ai.traceable.data.obfuscation.config.service.v1.DeleteDataObfuscationStrategyResponse;
import ai.traceable.data.obfuscation.config.service.v1.GetDataObfuscationStrategyRequest;
import ai.traceable.data.obfuscation.config.service.v1.GetDataObfuscationStrategyResponse;
import ai.traceable.data.obfuscation.config.service.v1.HashStrategy;
import ai.traceable.data.obfuscation.config.service.v1.ObfuscationStrategy;
import ai.traceable.data.obfuscation.config.service.v1.ObfuscationStrategyInput;
import ai.traceable.data.obfuscation.config.service.v1.ScryptHash;
import ai.traceable.data.obfuscation.config.service.v1.UpdateDataObfuscationStrategyRequest;
import ai.traceable.data.obfuscation.config.service.v1.UpdateDataObfuscationStrategyResponse;
import io.grpc.Context;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import java.util.List;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class DataObfuscationConfigServiceImplTest {

  private static final String TEST_STRATEGY_ID = "test-strategy-id";
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

  @Mock private DataObfuscationConfigRequestValidator requestValidator;
  @Mock private ConfigManager configManager;
  @Mock private RequestContext requestContext;
  @Mock private StreamObserver<CreateDataObfuscationStrategyResponse> createResponseObserver;
  @Mock private StreamObserver<GetDataObfuscationStrategyResponse> getResponseObserver;
  @Mock private StreamObserver<UpdateDataObfuscationStrategyResponse> updateResponseObserver;
  @Mock private StreamObserver<DeleteDataObfuscationStrategyResponse> deleteResponseObserver;

  @Captor private ArgumentCaptor<CreateDataObfuscationStrategyResponse> createResponseCaptor;
  @Captor private ArgumentCaptor<GetDataObfuscationStrategyResponse> getResponseCaptor;
  @Captor private ArgumentCaptor<UpdateDataObfuscationStrategyResponse> updateResponseCaptor;
  @Captor private ArgumentCaptor<DeleteDataObfuscationStrategyResponse> deleteResponseCaptor;
  @Captor private ArgumentCaptor<Exception> exceptionCaptor;

  private DataObfuscationConfigServiceImpl service;
  private Context prevCtx;

  @BeforeEach
  void setUp() {
    service = new DataObfuscationConfigServiceImpl(requestValidator, configManager);
    requestContext = RequestContext.forTenantId("test-tenant");
    Context testCtx = Context.current().withValue(RequestContext.CURRENT, requestContext);
    prevCtx = testCtx.attach();
  }

  @AfterEach
  void tearDown() {
    Context.current().detach(prevCtx);
  }

  @Test
  void testCreateDataObfuscationStrategy_success() {
    ObfuscationStrategyInput input =
        ObfuscationStrategyInput.newBuilder()
            .setHashStrategy(STRATEGY_SCRYPT)
            .setSalt(TEST_SALT)
            .build();

    CreateDataObfuscationStrategyRequest request =
        CreateDataObfuscationStrategyRequest.newBuilder().setObfuscationStrategy(input).build();

    ObfuscationStrategy strategy =
        ObfuscationStrategy.newBuilder()
            .setId(TEST_STRATEGY_ID)
            .setHashStrategy(STRATEGY_SCRYPT)
            .setSalt(TEST_SALT)
            .build();

    when(configManager.createObfuscationStrategy(requestContext, request)).thenReturn(strategy);

    service.createDataObfuscationStrategy(request, createResponseObserver);

    verify(requestValidator).validateOrThrow(requestContext, request);
    verify(configManager).createObfuscationStrategy(requestContext, request);
    verify(createResponseObserver).onNext(createResponseCaptor.capture());
    verify(createResponseObserver).onCompleted();

    CreateDataObfuscationStrategyResponse response = createResponseCaptor.getValue();
    assertEquals(strategy, response.getObfuscationStrategy());
  }

  @Test
  void testCreateDataObfuscationStrategy_validationFails() {
    CreateDataObfuscationStrategyRequest request =
        CreateDataObfuscationStrategyRequest.newBuilder().build();

    StatusRuntimeException exception =
        Status.INVALID_ARGUMENT.withDescription("Invalid request").asRuntimeException();

    doThrow(exception).when(requestValidator).validateOrThrow(requestContext, request);

    service.createDataObfuscationStrategy(request, createResponseObserver);

    verify(requestValidator).validateOrThrow(requestContext, request);
    verify(createResponseObserver).onError(exceptionCaptor.capture());

    Exception capturedError = exceptionCaptor.getValue();
    assertInstanceOf(StatusRuntimeException.class, capturedError);
    assertEquals(
        Status.INVALID_ARGUMENT.getCode(),
        ((StatusRuntimeException) capturedError).getStatus().getCode());
  }

  @Test
  void testCreateDataObfuscationStrategy_configManagerThrowsException() {
    ObfuscationStrategyInput input =
        ObfuscationStrategyInput.newBuilder()
            .setHashStrategy(STRATEGY_SCRYPT)
            .setSalt(TEST_SALT)
            .build();

    CreateDataObfuscationStrategyRequest request =
        CreateDataObfuscationStrategyRequest.newBuilder().setObfuscationStrategy(input).build();

    StatusRuntimeException exception =
        Status.ALREADY_EXISTS.withDescription("Strategy already exists").asRuntimeException();

    when(configManager.createObfuscationStrategy(requestContext, request)).thenThrow(exception);

    service.createDataObfuscationStrategy(request, createResponseObserver);

    verify(requestValidator).validateOrThrow(requestContext, request);
    verify(configManager).createObfuscationStrategy(requestContext, request);
    verify(createResponseObserver).onError(exceptionCaptor.capture());

    Exception capturedError = exceptionCaptor.getValue();
    assertInstanceOf(StatusRuntimeException.class, capturedError);
    assertEquals(
        Status.ALREADY_EXISTS.getCode(),
        ((StatusRuntimeException) capturedError).getStatus().getCode());
  }

  @Test
  void testGetDataObfuscationStrategy_success() {
    GetDataObfuscationStrategyRequest request =
        GetDataObfuscationStrategyRequest.newBuilder().build();

    ObfuscationStrategy strategy1 =
        ObfuscationStrategy.newBuilder()
            .setId("id1")
            .setHashStrategy(STRATEGY_SCRYPT)
            .setSalt("salt1")
            .build();
    ObfuscationStrategy strategy2 =
        ObfuscationStrategy.newBuilder()
            .setId("id2")
            .setHashStrategy(STRATEGY_SCRYPT)
            .setSalt("salt2")
            .build();

    List<ObfuscationStrategy> strategies = List.of(strategy1, strategy2);

    when(configManager.getObfuscationStrategies(requestContext)).thenReturn(strategies);

    service.getDataObfuscationStrategy(request, getResponseObserver);

    verify(requestValidator).validateOrThrow(requestContext);
    verify(configManager).getObfuscationStrategies(requestContext);
    verify(getResponseObserver).onNext(getResponseCaptor.capture());
    verify(getResponseObserver).onCompleted();

    GetDataObfuscationStrategyResponse response = getResponseCaptor.getValue();
    assertEquals(2, response.getObfuscationStrategiesCount());
    assertEquals(strategies, response.getObfuscationStrategiesList());
  }

  @Test
  void testGetDataObfuscationStrategy_validationFails() {
    GetDataObfuscationStrategyRequest request =
        GetDataObfuscationStrategyRequest.newBuilder().build();

    StatusRuntimeException exception =
        Status.INVALID_ARGUMENT.withDescription("Invalid request").asRuntimeException();

    doThrow(exception).when(requestValidator).validateOrThrow(requestContext);

    service.getDataObfuscationStrategy(request, getResponseObserver);

    verify(requestValidator).validateOrThrow(requestContext);
    verify(getResponseObserver).onError(exceptionCaptor.capture());

    Exception capturedError = exceptionCaptor.getValue();
    assertInstanceOf(StatusRuntimeException.class, capturedError);
    assertEquals(
        Status.INVALID_ARGUMENT.getCode(),
        ((StatusRuntimeException) capturedError).getStatus().getCode());
  }

  @Test
  void testGetDataObfuscationStrategy_configManagerThrowsException() {
    GetDataObfuscationStrategyRequest request =
        GetDataObfuscationStrategyRequest.newBuilder().build();

    RuntimeException exception = new RuntimeException("Database error");

    when(configManager.getObfuscationStrategies(requestContext)).thenThrow(exception);

    service.getDataObfuscationStrategy(request, getResponseObserver);

    verify(requestValidator).validateOrThrow(requestContext);
    verify(configManager).getObfuscationStrategies(requestContext);
    verify(getResponseObserver).onError(exceptionCaptor.capture());

    Exception capturedError = exceptionCaptor.getValue();
    assertInstanceOf(RuntimeException.class, capturedError);
    assertEquals("Database error", capturedError.getMessage());
  }

  @Test
  void testUpdateDataObfuscationStrategy_success() {
    ObfuscationStrategyInput input =
        ObfuscationStrategyInput.newBuilder()
            .setHashStrategy(STRATEGY_SCRYPT)
            .setSalt(TEST_SALT)
            .build();

    UpdateDataObfuscationStrategyRequest request =
        UpdateDataObfuscationStrategyRequest.newBuilder().setObfuscationStrategy(input).build();

    ObfuscationStrategy strategy =
        ObfuscationStrategy.newBuilder()
            .setId(TEST_STRATEGY_ID)
            .setHashStrategy(STRATEGY_SCRYPT)
            .setSalt(TEST_SALT)
            .build();

    when(configManager.updateObfuscationStrategy(requestContext, request)).thenReturn(strategy);

    service.updateDataObfuscationStrategy(request, updateResponseObserver);

    verify(requestValidator).validateOrThrow(requestContext, request);
    verify(configManager).updateObfuscationStrategy(requestContext, request);
    verify(updateResponseObserver).onNext(updateResponseCaptor.capture());
    verify(updateResponseObserver).onCompleted();

    UpdateDataObfuscationStrategyResponse response = updateResponseCaptor.getValue();
    assertEquals(strategy, response.getObfuscationStrategy());
  }

  @Test
  void testUpdateDataObfuscationStrategy_validationFails() {
    UpdateDataObfuscationStrategyRequest request =
        UpdateDataObfuscationStrategyRequest.newBuilder().build();

    StatusRuntimeException exception =
        Status.INVALID_ARGUMENT.withDescription("Invalid request").asRuntimeException();

    doThrow(exception).when(requestValidator).validateOrThrow(requestContext, request);

    service.updateDataObfuscationStrategy(request, updateResponseObserver);

    verify(requestValidator).validateOrThrow(requestContext, request);
    verify(updateResponseObserver).onError(exceptionCaptor.capture());

    Exception capturedError = exceptionCaptor.getValue();
    assertInstanceOf(StatusRuntimeException.class, capturedError);
    assertEquals(
        Status.INVALID_ARGUMENT.getCode(),
        ((StatusRuntimeException) capturedError).getStatus().getCode());
  }

  @Test
  void testUpdateDataObfuscationStrategy_configManagerThrowsException() {
    ObfuscationStrategyInput input =
        ObfuscationStrategyInput.newBuilder()
            .setHashStrategy(STRATEGY_SCRYPT)
            .setSalt(TEST_SALT)
            .build();

    UpdateDataObfuscationStrategyRequest request =
        UpdateDataObfuscationStrategyRequest.newBuilder().setObfuscationStrategy(input).build();

    StatusRuntimeException exception =
        Status.NOT_FOUND.withDescription("Strategy not found").asRuntimeException();

    when(configManager.updateObfuscationStrategy(requestContext, request)).thenThrow(exception);

    service.updateDataObfuscationStrategy(request, updateResponseObserver);

    verify(requestValidator).validateOrThrow(requestContext, request);
    verify(configManager).updateObfuscationStrategy(requestContext, request);
    verify(updateResponseObserver).onError(exceptionCaptor.capture());

    Exception capturedError = exceptionCaptor.getValue();
    assertInstanceOf(StatusRuntimeException.class, capturedError);
    assertEquals(
        Status.NOT_FOUND.getCode(), ((StatusRuntimeException) capturedError).getStatus().getCode());
  }

  @Test
  void testDeleteDataObfuscationStrategy_success() {
    DeleteDataObfuscationStrategyRequest request =
        DeleteDataObfuscationStrategyRequest.newBuilder().build();

    service.deleteDataObfuscationStrategy(request, deleteResponseObserver);

    verify(requestValidator).validateOrThrow(requestContext);
    verify(configManager).deleteObfuscationStrategy(requestContext);
    verify(deleteResponseObserver).onNext(deleteResponseCaptor.capture());
    verify(deleteResponseObserver).onCompleted();

    DeleteDataObfuscationStrategyResponse response = deleteResponseCaptor.getValue();
    assertEquals(DeleteDataObfuscationStrategyResponse.getDefaultInstance(), response);
  }

  @Test
  void testDeleteDataObfuscationStrategy_validationFails() {
    DeleteDataObfuscationStrategyRequest request =
        DeleteDataObfuscationStrategyRequest.newBuilder().build();

    StatusRuntimeException exception =
        Status.INVALID_ARGUMENT.withDescription("Invalid request").asRuntimeException();

    doThrow(exception).when(requestValidator).validateOrThrow(requestContext);

    service.deleteDataObfuscationStrategy(request, deleteResponseObserver);

    verify(requestValidator).validateOrThrow(requestContext);
    verify(deleteResponseObserver).onError(exceptionCaptor.capture());

    Exception capturedError = exceptionCaptor.getValue();
    assertInstanceOf(StatusRuntimeException.class, capturedError);
    assertEquals(
        Status.INVALID_ARGUMENT.getCode(),
        ((StatusRuntimeException) capturedError).getStatus().getCode());
  }

  @Test
  void testDeleteDataObfuscationStrategy_configManagerThrowsException() {
    DeleteDataObfuscationStrategyRequest request =
        DeleteDataObfuscationStrategyRequest.newBuilder().build();

    StatusRuntimeException exception =
        Status.NOT_FOUND.withDescription("Strategy not found").asRuntimeException();

    doThrow(exception).when(configManager).deleteObfuscationStrategy(requestContext);

    service.deleteDataObfuscationStrategy(request, deleteResponseObserver);

    verify(requestValidator).validateOrThrow(requestContext);
    verify(configManager).deleteObfuscationStrategy(requestContext);
    verify(deleteResponseObserver).onError(exceptionCaptor.capture());

    Exception capturedError = exceptionCaptor.getValue();
    assertInstanceOf(StatusRuntimeException.class, capturedError);
    assertEquals(
        Status.NOT_FOUND.getCode(), ((StatusRuntimeException) capturedError).getStatus().getCode());
  }
}
