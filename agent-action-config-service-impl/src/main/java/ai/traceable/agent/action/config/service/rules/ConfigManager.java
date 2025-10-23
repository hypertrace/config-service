package ai.traceable.agent.action.config.service.rules;

import ai.traceable.agent.action.config.service.v1.AgentAction;
import ai.traceable.agent.action.config.service.v1.CreateAgentActionRequest;
import ai.traceable.agent.action.config.service.v1.GetAgentActionsFilter;
import ai.traceable.agent.action.config.service.v1.UpdateAgentActionRequest;
import io.grpc.StatusRuntimeException;
import java.util.List;
import org.hypertrace.core.grpcutils.context.RequestContext;

public interface ConfigManager {

  List<AgentAction> getAgentActions(RequestContext requestContext, GetAgentActionsFilter filter);

  AgentAction createAgentAction(RequestContext requestContext, CreateAgentActionRequest request);

  AgentAction updateAgentAction(RequestContext requestContext, UpdateAgentActionRequest request);

  void deleteAgentAction(RequestContext requestContext, String id) throws StatusRuntimeException;
}
