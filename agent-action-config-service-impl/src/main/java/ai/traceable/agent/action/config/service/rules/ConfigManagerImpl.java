package ai.traceable.agent.action.config.service.rules;

import ai.traceable.agent.action.config.service.v1.AgentAction;
import ai.traceable.agent.action.config.service.v1.AgentActionInput;
import ai.traceable.agent.action.config.service.v1.AgentActionMetadata;
import ai.traceable.agent.action.config.service.v1.CreateAgentActionRequest;
import ai.traceable.agent.action.config.service.v1.GetAgentActionsFilter;
import ai.traceable.agent.action.config.service.v1.UpdateAgentActionRequest;
import ai.traceable.config.utils.UuidGenerator;
import com.google.inject.Inject;
import com.google.protobuf.util.Timestamps;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import java.util.List;
import java.util.Optional;
import org.hypertrace.core.grpcutils.context.RequestContext;

public class ConfigManagerImpl implements ConfigManager {

  private final AgentActionStore agentActionStore;
  private final UuidGenerator uuidGenerator;

  @Inject
  public ConfigManagerImpl(AgentActionStore agentActionStore, UuidGenerator uuidGenerator) {
    this.agentActionStore = agentActionStore;
    this.uuidGenerator = uuidGenerator;
  }

  @Override
  public List<AgentAction> getAgentActions(
      RequestContext requestContext, GetAgentActionsFilter filter) {
    if (filter.equals(GetAgentActionsFilter.getDefaultInstance())) {
      return agentActionStore.getAllConfigData(requestContext);
    }
    return agentActionStore.getAllConfigData(requestContext, filter);
  }

  @Override
  public AgentAction createAgentAction(
      RequestContext requestContext, CreateAgentActionRequest request) {
    String actionId = uuidGenerator.generateRandomId();
    AgentActionInput input = request.getAction();

    AgentAction.Builder action =
        AgentAction.newBuilder()
            .setId(actionId)
            .setScope(input.getScope())
            .addAllActionDetails(input.getDetailsList());

    if (input.hasExpirationTimestamp()) {
      action.setExpirationTimestamp(input.getExpirationTimestamp());
    }

    action.setMetadata(createAgentActionMetadata());
    return agentActionStore.upsertObject(requestContext, action.build()).getData();
  }

  @Override
  public AgentAction updateAgentAction(
      RequestContext requestContext, UpdateAgentActionRequest request) {
    if (!doesAgentActionExist(requestContext, request.getId())) {
      throw Status.NOT_FOUND
          .withDescription("Agent action with provided Id not found")
          .asRuntimeException(requestContext.buildTrailers());
    }

    AgentActionInput input = request.getAction();
    AgentAction.Builder action =
        AgentAction.newBuilder()
            .setId(request.getId())
            .setScope(input.getScope())
            .addAllActionDetails(input.getDetailsList());

    if (input.hasExpirationTimestamp()) {
      action.setExpirationTimestamp(input.getExpirationTimestamp());
    }

    action.setMetadata(createAgentActionMetadata());
    return agentActionStore.upsertObject(requestContext, action.build()).getData();
  }

  @Override
  public void deleteAgentAction(RequestContext requestContext, String id)
      throws StatusRuntimeException {
    if (agentActionStore.deleteObject(requestContext, id).isEmpty()) {
      throw Status.NOT_FOUND.asRuntimeException(requestContext.buildTrailers());
    }
  }

  private boolean doesAgentActionExist(RequestContext requestContext, String ruleId) {
    Optional<AgentAction> optionalRule = agentActionStore.getData(requestContext, ruleId);
    return optionalRule.isPresent();
  }

  private AgentActionMetadata.Builder createAgentActionMetadata() {
    return AgentActionMetadata.newBuilder().setLastUpdatedTimestamp(Timestamps.now());
  }
}
