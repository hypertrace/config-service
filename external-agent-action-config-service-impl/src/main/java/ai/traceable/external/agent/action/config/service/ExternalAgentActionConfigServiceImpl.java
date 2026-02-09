package ai.traceable.external.agent.action.config.service;

import ai.traceable.agent.action.config.service.v1.AgentAction;
import ai.traceable.agent.action.config.service.v1.AgentActionConfigServiceGrpc.AgentActionConfigServiceBlockingStub;
import ai.traceable.agent.action.config.service.v1.AgentActionDetails;
import ai.traceable.agent.action.config.service.v1.CompositeFilter;
import ai.traceable.agent.action.config.service.v1.Filter;
import ai.traceable.agent.action.config.service.v1.GetAgentActionsFilter;
import ai.traceable.agent.action.config.service.v1.LeafFilter;
import ai.traceable.config.utils.UuidGenerator;
import ai.traceable.external.agent.action.config.service.v1.AgentActionServiceGrpc.AgentActionServiceImplBase;
import ai.traceable.external.agent.action.config.service.v1.AgentScope;
import ai.traceable.external.agent.action.config.service.v1.ConfigMutationAction;
import ai.traceable.external.agent.action.config.service.v1.FetchDebugInformationAction;
import ai.traceable.external.agent.action.config.service.v1.GetAgentActionsRequest;
import ai.traceable.external.agent.action.config.service.v1.GetAgentActionsResponse;
import ai.traceable.external.agent.action.config.service.v1.RestartAgentAction;
import ai.traceable.external.agent.action.config.service.v1.ScopedActionRequest;
import ai.traceable.external.agent.action.config.service.v1.ScopedActionResponse;
import com.google.inject.Inject;
import com.google.protobuf.Timestamp;
import io.grpc.stub.StreamObserver;
import java.time.Instant;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.hypertrace.core.grpcutils.context.RequestContext;

@Slf4j
public class ExternalAgentActionConfigServiceImpl extends AgentActionServiceImplBase {
  private static final Comparator<Timestamp> PROTO_TS_COMPARATOR =
      Comparator.comparingLong(Timestamp::getSeconds).thenComparingInt(Timestamp::getNanos);

  @Inject
  public ExternalAgentActionConfigServiceImpl(
      AgentActionConfigServiceBlockingStub agentActionConfigServiceStub,
      ExternalAgentActionRequestValidator requestValidator,
      UuidGenerator uuidGenerator) {
    this.agentActionConfigServiceStub = agentActionConfigServiceStub;
    this.requestValidator = requestValidator;
    this.uuidGenerator = uuidGenerator;
  }

  private final AgentActionConfigServiceBlockingStub agentActionConfigServiceStub;
  private final ExternalAgentActionRequestValidator requestValidator;
  private final UuidGenerator uuidGenerator;

  @Override
  public void getAgentActions(
      GetAgentActionsRequest request, StreamObserver<GetAgentActionsResponse> responseObserver) {
    try {
      RequestContext requestContext = RequestContext.CURRENT.get();
      requestValidator.validateOrThrow(requestContext, request);

      GetAgentActionsResponse.Builder responseBuilder = GetAgentActionsResponse.newBuilder();
      for (ScopedActionRequest scopedReq : request.getAgentActionRequestsList()) {
        // skip incoming requests without a scope
        if (scopedReq == null || !scopedReq.hasScope()) {
          continue;
        }

        AgentScope scope = scopedReq.getScope();
        ScopedActionResponse.Builder scopedResBuilder =
            ScopedActionResponse.newBuilder().setScope(scope);

        ai.traceable.agent.action.config.service.v1.GetAgentActionsRequest internalReq =
            ai.traceable.agent.action.config.service.v1.GetAgentActionsRequest.newBuilder()
                .setFilter(
                    GetAgentActionsFilter.newBuilder()
                        .setFilter(
                            CompositeFilter.newBuilder()
                                .setOperator(CompositeFilter.LogicalOperator.LOGICAL_OPERATOR_AND)
                                .addFilters(
                                    Filter.newBuilder()
                                        .setLeaf(
                                            LeafFilter.newBuilder()
                                                .setModuleName(scope.getModuleName())
                                                .setModuleVersion(scope.getModuleVersion())
                                                .setServiceInstanceId(scope.getServiceInstanceId())
                                                .setDeploymentName(scope.getDeploymentName())))))
                .build();

        ai.traceable.agent.action.config.service.v1.GetAgentActionsResponse internalResponse =
            agentActionConfigServiceStub.getAgentActions(internalReq);

        // filter out expired actions
        List<AgentAction> actionsList =
            internalResponse.getActionsList().stream()
                .filter(
                    action ->
                        !action.hasExpirationTimestamp()
                            || Instant.now()
                                .isBefore(
                                    Instant.ofEpochSecond(
                                        action.getExpirationTimestamp().getSeconds(),
                                        action.getExpirationTimestamp().getNanos())))
                .sorted(
                    Comparator.comparing(
                        a -> a.hasMetadata() ? a.getMetadata().getLastUpdatedTimestamp() : null,
                        Comparator.nullsLast(PROTO_TS_COMPARATOR)))
                .collect(Collectors.toUnmodifiableList());

        String hash =
            uuidGenerator.generateId(
                actionsList.stream()
                    .map(AgentAction::getId)
                    .collect(Collectors.toUnmodifiableList()));

        scopedResBuilder.setHash(hash);

        // only populate the actions if the hash has changed
        if (!scopedReq.getPreviousHash().equals(hash)) {
          for (AgentAction action : actionsList) {
            scopedResBuilder.addActions(convertAction(action));
          }
        }
        responseBuilder.addAgentActions(scopedResBuilder);
      }

      String hash =
          uuidGenerator.generateId(
              responseBuilder.getAgentActionsList().stream()
                  .map(ScopedActionResponse::getHash)
                  .collect(Collectors.toUnmodifiableList()));
      responseBuilder.setHash(hash);
      // if no scoped actions has hash changed, then no need to send even the empty objects
      if (hash.equals(request.getPreviousHash())) {
        responseBuilder.clearAgentActions();
      }

      responseObserver.onNext(responseBuilder.build());
      responseObserver.onCompleted();
    } catch (Exception e) {
      log.error("Failed to retrieve agent actions", e);
      responseObserver.onError(e);
    }
  }

  static ai.traceable.external.agent.action.config.service.v1.AgentAction convertAction(
      AgentAction action) {

    ai.traceable.external.agent.action.config.service.v1.AgentAction.Builder builder =
        ai.traceable.external.agent.action.config.service.v1.AgentAction.newBuilder()
            .setHash(action.getId());
    if (action.hasExpirationTimestamp()) {
      Instant expirationTime =
          Instant.ofEpochSecond(
              action.getExpirationTimestamp().getSeconds(),
              action.getExpirationTimestamp().getNanos());
      builder.setExpirationEpochMillis(expirationTime.toEpochMilli());
    }
    for (AgentActionDetails details : action.getActionDetailsList()) {
      builder.addActionDetails(convertActionDetails(details));
    }
    return builder.build();
  }

  static ai.traceable.external.agent.action.config.service.v1.AgentActionDetails
      convertActionDetails(AgentActionDetails details) {
    ai.traceable.external.agent.action.config.service.v1.AgentActionDetails.Builder builder =
        ai.traceable.external.agent.action.config.service.v1.AgentActionDetails.newBuilder();
    switch (details.getDetailsCase()) {
      case RESTART_AGENT_ACTION:
        builder.setRestartAgentAction(RestartAgentAction.newBuilder());
        break;
      case FETCH_DEBUG_INFORMATION_ACTION:
        builder.setFetchDebugInformationAction(FetchDebugInformationAction.newBuilder());
        break;
      case UPDATE_CONFIG_ACTION:
        builder.setUpdateConfigAction(
            ConfigMutationAction.newBuilder()
                .putAllEnvironmentVariables(
                    details.getUpdateConfigAction().getEnvironmentVariablesMap()));
        break;
      default:
        log.error("Unknown action details case: {}", details.getDetailsCase());
    }
    return builder.build();
  }
}
