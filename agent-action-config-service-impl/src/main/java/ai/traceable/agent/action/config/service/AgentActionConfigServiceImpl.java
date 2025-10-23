package ai.traceable.agent.action.config.service;

import ai.traceable.agent.action.config.service.rules.ConfigManager;
import ai.traceable.agent.action.config.service.v1.AgentActionConfigServiceGrpc.AgentActionConfigServiceImplBase;
import ai.traceable.agent.action.config.service.v1.CreateAgentActionRequest;
import ai.traceable.agent.action.config.service.v1.CreateAgentActionResponse;
import ai.traceable.agent.action.config.service.v1.DeleteAgentActionRequest;
import ai.traceable.agent.action.config.service.v1.DeleteAgentActionResponse;
import ai.traceable.agent.action.config.service.v1.GetAgentActionsRequest;
import ai.traceable.agent.action.config.service.v1.GetAgentActionsResponse;
import ai.traceable.agent.action.config.service.v1.UpdateAgentActionRequest;
import ai.traceable.agent.action.config.service.v1.UpdateAgentActionResponse;
import com.google.inject.Inject;
import io.grpc.stub.StreamObserver;
import lombok.extern.slf4j.Slf4j;
import org.hypertrace.core.grpcutils.context.RequestContext;

@Slf4j
class AgentActionConfigServiceImpl extends AgentActionConfigServiceImplBase {

  private final AgentActionRequestValidator requestValidator;
  private final ConfigManager configManager;

  @Inject
  AgentActionConfigServiceImpl(
      AgentActionRequestValidator requestValidator, ConfigManager configManager) {
    this.requestValidator = requestValidator;
    this.configManager = configManager;
  }

  @Override
  public void createAgentAction(
      CreateAgentActionRequest request,
      StreamObserver<CreateAgentActionResponse> responseObserver) {
    try {
      RequestContext requestContext = RequestContext.CURRENT.get();
      requestValidator.validateOrThrow(requestContext, request);
      CreateAgentActionResponse response =
          CreateAgentActionResponse.newBuilder()
              .setAction(configManager.createAgentAction(requestContext, request))
              .build();

      responseObserver.onNext(response);
      responseObserver.onCompleted();
    } catch (Exception e) {
      log.error("Failed to create agent action", e);
      responseObserver.onError(e);
    }
  }

  @Override
  public void getAgentActions(
      GetAgentActionsRequest request, StreamObserver<GetAgentActionsResponse> responseObserver) {
    try {
      RequestContext requestContext = RequestContext.CURRENT.get();
      requestValidator.validateOrThrow(requestContext, request);

      GetAgentActionsResponse response =
          GetAgentActionsResponse.newBuilder()
              .addAllActions(configManager.getAgentActions(requestContext, request.getFilter()))
              .build();

      responseObserver.onNext(response);
      responseObserver.onCompleted();
    } catch (Exception e) {
      log.error("Failed to get agent actions", e);
      responseObserver.onError(e);
    }
  }

  @Override
  public void updateAgentAction(
      UpdateAgentActionRequest request,
      StreamObserver<UpdateAgentActionResponse> responseObserver) {
    try {
      RequestContext requestContext = RequestContext.CURRENT.get();
      requestValidator.validateOrThrow(requestContext, request);

      UpdateAgentActionResponse response =
          UpdateAgentActionResponse.newBuilder()
              .setAction(configManager.updateAgentAction(requestContext, request))
              .build();

      responseObserver.onNext(response);
      responseObserver.onCompleted();
    } catch (Exception e) {
      log.error("Failed to update agent action", e);
      responseObserver.onError(e);
    }
  }

  @Override
  public void deleteAgentAction(
      DeleteAgentActionRequest request,
      StreamObserver<DeleteAgentActionResponse> responseObserver) {
    try {
      RequestContext requestContext = RequestContext.CURRENT.get();
      requestValidator.validateOrThrow(requestContext, request);

      configManager.deleteAgentAction(requestContext, request.getId());
      DeleteAgentActionResponse response = DeleteAgentActionResponse.newBuilder().build();

      responseObserver.onNext(response);
      responseObserver.onCompleted();
    } catch (Exception e) {
      log.error("Failed to delete agent action", e);
      responseObserver.onError(e);
    }
  }
}
