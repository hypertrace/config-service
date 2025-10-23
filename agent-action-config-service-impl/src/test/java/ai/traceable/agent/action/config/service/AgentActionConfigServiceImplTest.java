package ai.traceable.agent.action.config.service;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import ai.traceable.agent.action.config.service.rules.ConfigManager;
import ai.traceable.agent.action.config.service.v1.AgentAction;
import ai.traceable.agent.action.config.service.v1.CreateAgentActionRequest;
import ai.traceable.agent.action.config.service.v1.CreateAgentActionResponse;
import ai.traceable.agent.action.config.service.v1.DeleteAgentActionRequest;
import ai.traceable.agent.action.config.service.v1.DeleteAgentActionResponse;
import ai.traceable.agent.action.config.service.v1.GetAgentActionsRequest;
import ai.traceable.agent.action.config.service.v1.GetAgentActionsResponse;
import ai.traceable.agent.action.config.service.v1.UpdateAgentActionRequest;
import ai.traceable.agent.action.config.service.v1.UpdateAgentActionResponse;
import com.google.common.collect.ImmutableList;
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
class AgentActionConfigServiceImplTest {

  @Mock private AgentActionRequestValidator requestValidator;
  @Mock private ConfigManager configManager;
  @Mock private StreamObserver<CreateAgentActionResponse> createResponseObserver;
  @Mock private StreamObserver<GetAgentActionsResponse> getResponseObserver;
  @Mock private StreamObserver<UpdateAgentActionResponse> updateResponseObserver;
  @Mock private StreamObserver<DeleteAgentActionResponse> deleteResponseObserver;

  @Captor private ArgumentCaptor<CreateAgentActionResponse> createResponseCaptor;
  @Captor private ArgumentCaptor<GetAgentActionsResponse> getResponseCaptor;
  @Captor private ArgumentCaptor<UpdateAgentActionResponse> updateResponseCaptor;
  @Captor private ArgumentCaptor<DeleteAgentActionResponse> deleteResponseCaptor;
  @Captor private ArgumentCaptor<Throwable> errorCaptor;

  private AgentActionConfigServiceImpl service;
  private RequestContext requestContext;
  private Context prevCtx;

  @BeforeEach
  void setUp() {
    service = new AgentActionConfigServiceImpl(requestValidator, configManager);
    requestContext = RequestContext.forTenantId("test-tenant");
    Context testCtx = Context.current().withValue(RequestContext.CURRENT, requestContext);
    prevCtx = testCtx.attach();
  }

  @AfterEach
  void tearDown() {
    Context.current().detach(prevCtx);
  }

  @Test
  void createAgentAction_success() {
    // Given
    CreateAgentActionRequest request = CreateAgentActionRequest.getDefaultInstance();
    AgentAction expectedAction = AgentAction.getDefaultInstance();

    doNothing().when(requestValidator).validateOrThrow(requestContext, request);
    when(configManager.createAgentAction(requestContext, request)).thenReturn(expectedAction);

    // When
    service.createAgentAction(request, createResponseObserver);

    // Then
    verify(requestValidator).validateOrThrow(requestContext, request);
    verify(configManager).createAgentAction(requestContext, request);
    verify(createResponseObserver).onNext(createResponseCaptor.capture());
    verify(createResponseObserver).onCompleted();

    CreateAgentActionResponse response = createResponseCaptor.getValue();
    assertEquals(expectedAction, response.getAction());
  }

  @Test
  void createAgentAction_validationError() {
    // Given
    CreateAgentActionRequest request = CreateAgentActionRequest.getDefaultInstance();
    StatusRuntimeException expectedError = Status.INVALID_ARGUMENT.asRuntimeException();

    doThrow(expectedError).when(requestValidator).validateOrThrow(requestContext, request);

    // When
    service.createAgentAction(request, createResponseObserver);

    // Then
    verify(createResponseObserver).onError(errorCaptor.capture());
    assertEquals(expectedError, errorCaptor.getValue());
  }

  @Test
  void getAgentActions_success() {
    // Given
    GetAgentActionsRequest request = GetAgentActionsRequest.getDefaultInstance();
    List<AgentAction> expectedActions =
        ImmutableList.of(AgentAction.getDefaultInstance(), AgentAction.getDefaultInstance());

    doNothing().when(requestValidator).validateOrThrow(requestContext, request);
    when(configManager.getAgentActions(eq(requestContext), any())).thenReturn(expectedActions);

    // When
    service.getAgentActions(request, getResponseObserver);

    // Then
    verify(requestValidator).validateOrThrow(requestContext, request);
    verify(configManager).getAgentActions(eq(requestContext), any());
    verify(getResponseObserver).onNext(getResponseCaptor.capture());
    verify(getResponseObserver).onCompleted();

    GetAgentActionsResponse response = getResponseCaptor.getValue();
    assertEquals(expectedActions, response.getActionsList());
  }

  @Test
  void updateAgentAction_success() {
    // Given
    UpdateAgentActionRequest request = UpdateAgentActionRequest.getDefaultInstance();
    AgentAction expectedAction = AgentAction.getDefaultInstance();

    doNothing().when(requestValidator).validateOrThrow(requestContext, request);
    when(configManager.updateAgentAction(requestContext, request)).thenReturn(expectedAction);

    // When
    service.updateAgentAction(request, updateResponseObserver);

    // Then
    verify(requestValidator).validateOrThrow(requestContext, request);
    verify(configManager).updateAgentAction(requestContext, request);
    verify(updateResponseObserver).onNext(updateResponseCaptor.capture());
    verify(updateResponseObserver).onCompleted();

    UpdateAgentActionResponse response = updateResponseCaptor.getValue();
    assertEquals(expectedAction, response.getAction());
  }

  @Test
  void deleteAgentAction_success() {
    // Given
    String actionId = "test-action-id";
    DeleteAgentActionRequest request =
        DeleteAgentActionRequest.newBuilder().setId(actionId).build();

    doNothing().when(requestValidator).validateOrThrow(requestContext, request);
    doNothing().when(configManager).deleteAgentAction(requestContext, actionId);

    // When
    service.deleteAgentAction(request, deleteResponseObserver);

    // Then
    verify(requestValidator).validateOrThrow(requestContext, request);
    verify(configManager).deleteAgentAction(requestContext, actionId);
    verify(deleteResponseObserver).onNext(deleteResponseCaptor.capture());
    verify(deleteResponseObserver).onCompleted();

    DeleteAgentActionResponse response = deleteResponseCaptor.getValue();
    assertNotNull(response);
  }

  @Test
  void deleteAgentAction_error() {
    // Given
    String actionId = "test-action-id";
    DeleteAgentActionRequest request =
        DeleteAgentActionRequest.newBuilder().setId(actionId).build();
    StatusRuntimeException expectedError = Status.NOT_FOUND.asRuntimeException();

    doNothing().when(requestValidator).validateOrThrow(requestContext, request);
    doThrow(expectedError).when(configManager).deleteAgentAction(requestContext, actionId);

    // When
    service.deleteAgentAction(request, deleteResponseObserver);

    // Then
    verify(deleteResponseObserver).onError(errorCaptor.capture());
    assertEquals(expectedError, errorCaptor.getValue());
  }

  @Test
  void errorHandling() {
    // Test that any exception is properly caught and passed to the response observer
    GetAgentActionsRequest request = GetAgentActionsRequest.getDefaultInstance();
    RuntimeException expectedError = new RuntimeException("Test exception");

    doThrow(expectedError).when(requestValidator).validateOrThrow(requestContext, request);

    service.getAgentActions(request, getResponseObserver);

    verify(getResponseObserver).onError(errorCaptor.capture());
    assertEquals(expectedError, errorCaptor.getValue());
  }
}
