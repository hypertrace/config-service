package ai.traceable.external.agent.action.config.service;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import ai.traceable.agent.action.config.service.v1.AgentAction;
import ai.traceable.agent.action.config.service.v1.AgentActionConfigServiceGrpc;
import ai.traceable.agent.action.config.service.v1.AgentActionDetails;
import ai.traceable.agent.action.config.service.v1.ConfigMutationAction;
import ai.traceable.config.utils.UuidGenerator;
import ai.traceable.external.agent.action.config.service.v1.AgentScope;
import ai.traceable.external.agent.action.config.service.v1.GetAgentActionsRequest;
import ai.traceable.external.agent.action.config.service.v1.GetAgentActionsResponse;
import ai.traceable.external.agent.action.config.service.v1.ScopedActionRequest;
import com.google.protobuf.Timestamp;
import io.grpc.Context;
import io.grpc.stub.StreamObserver;
import java.time.Instant;
import java.util.stream.Stream;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class ExternalAgentActionConfigServiceImplTest {
  static class ActionDetails {
    ai.traceable.agent.action.config.service.v1.AgentActionDetails input;
    ai.traceable.external.agent.action.config.service.v1.AgentActionDetails expected;

    ActionDetails(
        ai.traceable.agent.action.config.service.v1.AgentActionDetails input,
        ai.traceable.external.agent.action.config.service.v1.AgentActionDetails expected) {
      this.input = input;
      this.expected = expected;
    }
  }

  static Stream<ActionDetails> provideAgentActions() {
    return Stream.of(
        new ActionDetails(
            ai.traceable.agent.action.config.service.v1.AgentActionDetails.newBuilder()
                .setRestartAgentAction(
                    ai.traceable.agent.action.config.service.v1.RestartAgentAction.newBuilder())
                .build(),
            ai.traceable.external.agent.action.config.service.v1.AgentActionDetails.newBuilder()
                .setRestartAgentAction(
                    ai.traceable.external.agent.action.config.service.v1.RestartAgentAction
                        .newBuilder())
                .build()),
        new ActionDetails(
            ai.traceable.agent.action.config.service.v1.AgentActionDetails.newBuilder()
                .setFetchDebugInformationAction(
                    ai.traceable.agent.action.config.service.v1.FetchDebugInformationAction
                        .newBuilder())
                .build(),
            ai.traceable.external.agent.action.config.service.v1.AgentActionDetails.newBuilder()
                .setFetchDebugInformationAction(
                    ai.traceable.external.agent.action.config.service.v1.FetchDebugInformationAction
                        .newBuilder())
                .build()),
        new ActionDetails(
            ai.traceable.agent.action.config.service.v1.AgentActionDetails.newBuilder()
                .setUpdateConfigAction(
                    ai.traceable.agent.action.config.service.v1.ConfigMutationAction.newBuilder()
                        .putEnvironmentVariables("TA_ENVIRONMENT", "production"))
                .build(),
            ai.traceable.external.agent.action.config.service.v1.AgentActionDetails.newBuilder()
                .setUpdateConfigAction(
                    ai.traceable.external.agent.action.config.service.v1.ConfigMutationAction
                        .newBuilder()
                        .putEnvironmentVariables("TA_ENVIRONMENT", "production"))
                .build()));
  }

  @ParameterizedTest
  @MethodSource("provideAgentActions")
  void convertActionDetails(ActionDetails tc) {
    assertEquals(tc.expected, ExternalAgentActionConfigServiceImpl.convertActionDetails(tc.input));
  }

  @Test
  void convertAction() {
    Instant time = Instant.now().plusSeconds(100);
    assertEquals(
        ai.traceable.external.agent.action.config.service.v1.AgentAction.newBuilder()
            .setHash("test-hash")
            .setExpirationEpochMillis(time.toEpochMilli())
            .addActionDetails(
                ai.traceable.external.agent.action.config.service.v1.AgentActionDetails.newBuilder()
                    .setUpdateConfigAction(
                        ai.traceable.external.agent.action.config.service.v1.ConfigMutationAction
                            .newBuilder()
                            .putEnvironmentVariables("TA_ENVIRONMENT", "production")))
            .build(),
        ExternalAgentActionConfigServiceImpl.convertAction(
            AgentAction.newBuilder()
                .setId("test-hash")
                .setExpirationTimestamp(
                    Timestamp.newBuilder()
                        .setSeconds(time.getEpochSecond())
                        .setNanos(time.getNano())
                        .build())
                .addActionDetails(
                    AgentActionDetails.newBuilder()
                        .setUpdateConfigAction(
                            ConfigMutationAction.newBuilder()
                                .putEnvironmentVariables("TA_ENVIRONMENT", "production")))
                .build()));
  }

  @Mock private ExternalAgentActionRequestValidator requestValidator;

  @Mock
  private AgentActionConfigServiceGrpc.AgentActionConfigServiceBlockingStub
      agentActionConfigServiceStub;

  @Mock private UuidGenerator uuidGenerator;

  private ExternalAgentActionConfigServiceImpl service;

  private StreamObserver<GetAgentActionsResponse> responseObserver;
  private RequestContext requestContext;

  private Context prevCtx;

  @BeforeEach
  void setUp() {
    responseObserver = mock(StreamObserver.class);
    requestContext = RequestContext.forTenantId("test-tenant");
    Context testCtx = Context.current().withValue(RequestContext.CURRENT, requestContext);
    prevCtx = testCtx.attach();
    service =
        new ExternalAgentActionConfigServiceImpl(
            agentActionConfigServiceStub, requestValidator, uuidGenerator);
  }

  @AfterEach
  void tearDown() {
    Context.current().detach(prevCtx);
  }

  @Test
  void getAgentActionsSkipRequestsWithoutScope() {
    // Given
    GetAgentActionsRequest request =
        GetAgentActionsRequest.newBuilder()
            .addAgentActionRequests(ScopedActionRequest.newBuilder())
            .build();

    when(uuidGenerator.generateId(anyList())).thenReturn("complete-hash");
    service.getAgentActions(request, responseObserver);

    // Then
    verify(requestValidator).validateOrThrow(requestContext, request);
    verify(responseObserver)
        .onNext(GetAgentActionsResponse.newBuilder().setHash("complete-hash").build());
    verify(responseObserver).onCompleted();
    verifyNoInteractions(agentActionConfigServiceStub);
  }

  @Test
  void getAgentActions_shouldProcessScopedRequest() {
    // Given
    AgentScope scope =
        AgentScope.newBuilder()
            .setModuleName("test-module")
            .setModuleVersion("1.0.0")
            .setServiceInstanceId("instance-1")
            .setDeploymentName("deploy-1")
            .build();

    GetAgentActionsRequest request =
        GetAgentActionsRequest.newBuilder()
            .addAgentActionRequests(
                ScopedActionRequest.newBuilder()
                    .setScope(scope)
                    .setPreviousHash("old-hash")
                    .build())
            .build();

    AgentAction internalAction = AgentAction.newBuilder().setId("action-1").build();

    when(agentActionConfigServiceStub.getAgentActions(any()))
        .thenReturn(
            ai.traceable.agent.action.config.service.v1.GetAgentActionsResponse.newBuilder()
                .addActions(internalAction)
                .build());

    when(uuidGenerator.generateId(anyList())).thenReturn("new-hash").thenReturn("global-hash");

    // When
    service.getAgentActions(request, responseObserver);

    // Then
    ArgumentCaptor<GetAgentActionsResponse> responseCaptor =
        ArgumentCaptor.forClass(GetAgentActionsResponse.class);
    verify(responseObserver).onNext(responseCaptor.capture());
    verify(responseObserver).onCompleted();

    GetAgentActionsResponse response = responseCaptor.getValue();
    assertEquals(1, response.getAgentActionsCount());
    assertEquals(scope, response.getAgentActions(0).getScope());
    assertEquals("global-hash", response.getHash());
    assertEquals(1, response.getAgentActions(0).getActionsCount());
    assertEquals(
        ai.traceable.external.agent.action.config.service.v1.AgentAction.newBuilder()
            .setHash("action-1")
            .build(),
        response.getAgentActions(0).getActions(0));
  }

  @Test
  void getAgentActions_shouldNotReturnActionsWhenGlobalHashMatches() {
    // Given
    AgentScope scope =
        AgentScope.newBuilder()
            .setModuleName("test-module")
            .setModuleVersion("1.0.0")
            .setServiceInstanceId("test-id")
            .setDeploymentName("test-deployment")
            .build();

    GetAgentActionsRequest request =
        GetAgentActionsRequest.newBuilder()
            .addAgentActionRequests(
                ScopedActionRequest.newBuilder().setScope(scope).setPreviousHash("same-hash"))
            .setPreviousHash("same-global-hash")
            .build();

    when(uuidGenerator.generateId(anyList()))
        .thenReturn("same-hash")
        .thenReturn("same-global-hash");
    AgentAction internalAction = AgentAction.newBuilder().setId("action-1").build();

    when(agentActionConfigServiceStub.getAgentActions(any()))
        .thenReturn(
            ai.traceable.agent.action.config.service.v1.GetAgentActionsResponse.newBuilder()
                .addActions(internalAction)
                .build());

    // When
    service.getAgentActions(request, responseObserver);

    // Then
    ArgumentCaptor<GetAgentActionsResponse> responseCaptor =
        ArgumentCaptor.forClass(GetAgentActionsResponse.class);
    verify(responseObserver).onNext(responseCaptor.capture());

    GetAgentActionsResponse response = responseCaptor.getValue();
    assertTrue(response.getAgentActionsList().isEmpty());
    assertEquals("same-global-hash", response.getHash());
  }

  @Test
  void getAgentActions_shouldNotReturnActionsWhenScopedHashMatches() {
    // Given
    AgentScope scope =
        AgentScope.newBuilder()
            .setModuleName("test-module")
            .setModuleVersion("1.0.0")
            .setServiceInstanceId("test-id")
            .setDeploymentName("test-deployment")
            .build();

    GetAgentActionsRequest request =
        GetAgentActionsRequest.newBuilder()
            .addAgentActionRequests(
                ScopedActionRequest.newBuilder().setScope(scope).setPreviousHash("same-hash"))
            .setPreviousHash("global-hash")
            .build();

    when(uuidGenerator.generateId(anyList())).thenReturn("same-hash").thenReturn("new-global-hash");
    AgentAction internalAction = AgentAction.newBuilder().setId("action-1").build();

    when(agentActionConfigServiceStub.getAgentActions(any()))
        .thenReturn(
            ai.traceable.agent.action.config.service.v1.GetAgentActionsResponse.newBuilder()
                .addActions(internalAction)
                .build());

    // When
    service.getAgentActions(request, responseObserver);

    // Then
    ArgumentCaptor<GetAgentActionsResponse> responseCaptor =
        ArgumentCaptor.forClass(GetAgentActionsResponse.class);
    verify(responseObserver).onNext(responseCaptor.capture());

    GetAgentActionsResponse response = responseCaptor.getValue();
    assertEquals(1, response.getAgentActionsCount());
    assertEquals(
        ai.traceable.external.agent.action.config.service.v1.ScopedActionResponse.newBuilder()
            .setScope(scope)
            .setHash("same-hash")
            .build(),
        response.getAgentActions(0));
    assertEquals("new-global-hash", response.getHash());
  }

  @Test
  void getAgentActions_shouldNotReturnActionsExpiredActions() {
    // Given
    AgentScope scope =
        AgentScope.newBuilder()
            .setModuleName("test-module")
            .setModuleVersion("1.0.0")
            .setServiceInstanceId("test-id")
            .setDeploymentName("test-deployment")
            .build();

    GetAgentActionsRequest request =
        GetAgentActionsRequest.newBuilder()
            .addAgentActionRequests(
                ScopedActionRequest.newBuilder().setScope(scope).setPreviousHash("same-hash"))
            .setPreviousHash("global-hash")
            .build();

    when(uuidGenerator.generateId(anyList())).thenReturn("new-hash").thenReturn("new-global-hash");
    AgentAction internalAction = AgentAction.newBuilder().setId("action-1").build();
    AgentAction expiredAction =
        AgentAction.newBuilder()
            .setId("action-2")
            .setExpirationTimestamp(Timestamp.newBuilder())
            .build();

    when(agentActionConfigServiceStub.getAgentActions(any()))
        .thenReturn(
            ai.traceable.agent.action.config.service.v1.GetAgentActionsResponse.newBuilder()
                .addActions(internalAction)
                .addActions(expiredAction)
                .build());

    // When
    service.getAgentActions(request, responseObserver);

    // Then
    ArgumentCaptor<GetAgentActionsResponse> responseCaptor =
        ArgumentCaptor.forClass(GetAgentActionsResponse.class);
    verify(responseObserver).onNext(responseCaptor.capture());

    GetAgentActionsResponse response = responseCaptor.getValue();
    assertEquals(1, response.getAgentActionsCount());
    assertEquals(
        ai.traceable.external.agent.action.config.service.v1.ScopedActionResponse.newBuilder()
            .setScope(scope)
            .addActions(ExternalAgentActionConfigServiceImpl.convertAction(internalAction))
            .setHash("new-hash")
            .build(),
        response.getAgentActions(0));
    assertEquals("new-global-hash", response.getHash());
  }

  @Test
  void getAgentActions_shouldHandleMultipleScopedRequests() {
    // Given
    AgentScope scope1 = AgentScope.newBuilder().setModuleName("module-1").build();
    AgentScope scope2 = AgentScope.newBuilder().setModuleName("module-2").build();

    GetAgentActionsRequest request =
        GetAgentActionsRequest.newBuilder()
            .addAgentActionRequests(
                ScopedActionRequest.newBuilder().setScope(scope1).setPreviousHash("hash1").build())
            .addAgentActionRequests(
                ScopedActionRequest.newBuilder().setScope(scope2).setPreviousHash("hash2").build())
            .build();

    AgentAction action1 = AgentAction.newBuilder().setId("action-1").build();
    AgentAction action2 = AgentAction.newBuilder().setId("action-2").build();
    when(agentActionConfigServiceStub.getAgentActions(any()))
        .thenReturn(
            ai.traceable.agent.action.config.service.v1.GetAgentActionsResponse.newBuilder()
                .addActions(action1)
                .build())
        .thenReturn(
            ai.traceable.agent.action.config.service.v1.GetAgentActionsResponse.newBuilder()
                .addActions(action2)
                .build());

    when(uuidGenerator.generateId(anyList()))
        .thenReturn("new-hash-1")
        .thenReturn("new-hash-2")
        .thenReturn("final-hash");

    // When
    service.getAgentActions(request, responseObserver);

    // Then
    verify(agentActionConfigServiceStub, times(2)).getAgentActions(any());
    ArgumentCaptor<GetAgentActionsResponse> responseCaptor =
        ArgumentCaptor.forClass(GetAgentActionsResponse.class);
    verify(responseObserver).onNext(responseCaptor.capture());

    GetAgentActionsResponse response = responseCaptor.getValue();
    assertEquals(2, response.getAgentActionsCount());
    assertEquals(
        ai.traceable.external.agent.action.config.service.v1.ScopedActionResponse.newBuilder()
            .setScope(scope1)
            .addActions(ExternalAgentActionConfigServiceImpl.convertAction(action1))
            .setHash("new-hash-1")
            .build(),
        response.getAgentActions(0));
    assertEquals(
        ai.traceable.external.agent.action.config.service.v1.ScopedActionResponse.newBuilder()
            .setScope(scope2)
            .addActions(ExternalAgentActionConfigServiceImpl.convertAction(action2))
            .setHash("new-hash-2")
            .build(),
        response.getAgentActions(1));
    assertEquals("final-hash", response.getHash());
  }

  @Test
  void getAgentActions_shouldHandleServiceError() {
    // Given
    GetAgentActionsRequest request = GetAgentActionsRequest.getDefaultInstance();
    RuntimeException expectedError = new RuntimeException("Service error");

    doThrow(expectedError).when(requestValidator).validateOrThrow(any(), any());

    // When
    service.getAgentActions(request, responseObserver);

    // Then
    verify(responseObserver).onError(same(expectedError));
    verify(responseObserver, never()).onNext(any());
    verify(responseObserver, never()).onCompleted();
  }
}
