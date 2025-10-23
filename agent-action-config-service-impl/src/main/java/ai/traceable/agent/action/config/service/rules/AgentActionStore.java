package ai.traceable.agent.action.config.service.rules;

import ai.traceable.agent.action.config.service.v1.AgentAction;
import ai.traceable.agent.action.config.service.v1.AgentScope;
import ai.traceable.agent.action.config.service.v1.CompositeFilter;
import ai.traceable.agent.action.config.service.v1.Filter;
import ai.traceable.agent.action.config.service.v1.GetAgentActionsFilter;
import ai.traceable.agent.action.config.service.v1.LeafFilter;
import com.google.inject.Inject;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Value;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;
import org.hypertrace.config.objectstore.IdentifiedObjectStoreWithFilter;
import org.hypertrace.config.proto.converter.ConfigProtoConverter;
import org.hypertrace.config.service.change.event.api.ConfigChangeEventGenerator;
import org.hypertrace.config.service.v1.ConfigServiceGrpc.ConfigServiceBlockingStub;

@Slf4j
public class AgentActionStore
    extends IdentifiedObjectStoreWithFilter<AgentAction, GetAgentActionsFilter> {

  public static final String AGENT_ACTION_CONFIG_NAMESPACE = "agentAction";
  public static final String AGENT_ACTION_CONFIG_RESOURCE_NAME = "agentActionConfig";

  @Inject
  public AgentActionStore(
      ConfigServiceBlockingStub configServiceBlockingStub,
      ConfigChangeEventGenerator configChangeEventGenerator) {
    super(
        configServiceBlockingStub,
        AGENT_ACTION_CONFIG_NAMESPACE,
        AGENT_ACTION_CONFIG_RESOURCE_NAME,
        configChangeEventGenerator);
  }

  @Override
  protected Optional<AgentAction> filterConfigData(
      AgentAction action, GetAgentActionsFilter filter) {
    // if an action has no scope, then it matches for every request
    // if a filter is empty, then it matches for every request
    if (!action.hasScope() || !filter.hasFilter()) {
      return Optional.of(action);
    }

    return Optional.of(action)
        .filter(
            agentAction ->
                isCompositeFilterMatchingScope(filter.getFilter(), agentAction.getScope()));
  }

  @Override
  protected Optional<AgentAction> buildDataFromValue(Value value) {
    try {
      AgentAction.Builder builder = AgentAction.newBuilder();
      ConfigProtoConverter.mergeFromValue(value, builder);
      return Optional.of(builder.build());
    } catch (InvalidProtocolBufferException e) {
      log.error("Error deserializing value into Agent Action", e);
    }

    return Optional.empty();
  }

  @Override
  protected Value buildValueFromData(AgentAction data) {
    try {
      return ConfigProtoConverter.convertToValue(data);
    } catch (InvalidProtocolBufferException e) {
      log.error("Error serializing Agent Action into Value", e);
    }

    return Value.newBuilder().getDefaultInstanceForType();
  }

  @Override
  protected String getContextFromData(AgentAction action) {
    return action.getId();
  }

  private boolean isCompositeFilterMatchingScope(CompositeFilter filter, AgentScope scope) {
    return filter.getOperator() == CompositeFilter.LogicalOperator.LOGICAL_OPERATOR_OR
        ? filter.getFiltersList().stream()
            .anyMatch(filterObj -> isFilterMatchingScope(filterObj, scope))
        : filter.getFiltersList().stream()
            .allMatch(filterObj -> isFilterMatchingScope(filterObj, scope));
  }

  private boolean isFilterMatchingScope(Filter filter, AgentScope scope) {
    // invalid filter
    if (!filter.hasLeaf() && !filter.hasChildren()) {
      return false;
    }

    if (filter.hasChildren()) {
      return isCompositeFilterMatchingScope(filter.getChildren(), scope);
    }

    // for a leaf filter, match true if scope has the corresponding field empty
    // or the scope has an exact match for this field.
    LeafFilter leaf = filter.getLeaf();
    if (scope.hasDeploymentName() && !scope.getDeploymentName().equals(leaf.getDeploymentName())) {
      return false;
    }
    if (scope.hasModuleName() && !scope.getModuleName().equals(leaf.getModuleName())) {
      return false;
    }
    if (scope.hasModuleVersion() && !scope.getModuleVersion().equals(leaf.getModuleVersion())) {
      return false;
    }
    if (scope.hasServiceInstanceId()
        && !scope.getServiceInstanceId().equals(leaf.getServiceInstanceId())) {
      return false;
    }

    return true;
  }
}
