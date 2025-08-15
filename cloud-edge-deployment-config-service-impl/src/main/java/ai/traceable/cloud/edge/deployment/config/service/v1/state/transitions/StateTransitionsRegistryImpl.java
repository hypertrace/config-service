package ai.traceable.cloud.edge.deployment.config.service.v1.state.transitions;

import ai.traceable.cloud.edge.deployment.config.service.v1.Action;
import ai.traceable.cloud.edge.deployment.config.service.v1.ConfigAccessType;
import ai.traceable.cloud.edge.deployment.config.service.v1.DeploymentStatus;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import lombok.Value;

public class StateTransitionsRegistryImpl implements StateTransitionsRegistry {
  private static final Set<String> UNMAPPED_DEPLOYMENT_STATUSES =
      Set.of("DEPLOYMENT_STATUS_INIT", "DEPLOYMENT_STATUS_DELETED");

  private final Map<DeploymentStatusTuple, Map<Action, List<DeploymentStatus>>>
      deploymentStatusTuplesMap;

  public StateTransitionsRegistryImpl() {
    deploymentStatusTuplesMap = new HashMap<>();
    StateTransitionsConfig stateTransitionsConfig =
        StateTransitionsParser.getStateTransitionsConfig();
    stateTransitionsConfig
        .getStates()
        .forEach(
            state ->
                state
                    .getTransitions()
                    .forEach(
                        transition ->
                            transition.forEach(
                                (action, value) -> {
                                  List<DeploymentStatus> nextStates =
                                      convertStates(value.getNextStates());
                                  convertAccessTypes(value.getAccessTypes())
                                      .forEach(
                                          accessType -> {
                                            DeploymentStatusTuple deploymentStatusTuple =
                                                new DeploymentStatusTuple(
                                                    convertState(state.getCurrentState()),
                                                    accessType);
                                            Map<Action, List<DeploymentStatus>> actionListMap =
                                                deploymentStatusTuplesMap.computeIfAbsent(
                                                    deploymentStatusTuple, k -> new HashMap<>());
                                            actionListMap.put(Action.valueOf(action), nextStates);
                                          });
                                })));
  }

  private List<ConfigAccessType> convertAccessTypes(List<String> accessTypes) {
    return accessTypes.stream()
        .map(ConfigAccessType::valueOf)
        .collect(Collectors.toUnmodifiableList());
  }

  private List<DeploymentStatus> convertStates(List<String> nextStates) {
    return nextStates.stream()
        // ignore states that are not defined as deployment status
        .filter(nextState -> !UNMAPPED_DEPLOYMENT_STATUSES.contains(nextState))
        .map(DeploymentStatus::valueOf)
        .collect(Collectors.toUnmodifiableList());
  }

  private DeploymentStatus convertState(String state) {
    // handle states that are not defined as deployment status
    return UNMAPPED_DEPLOYMENT_STATUSES.contains(state) ? null : DeploymentStatus.valueOf(state);
  }

  @Override
  public boolean isActionAllowed(
      DeploymentStatus currentState, ConfigAccessType accessType, Action action) {
    return Optional.ofNullable(
            deploymentStatusTuplesMap.get(new DeploymentStatusTuple(currentState, accessType)))
        .map(value -> value.containsKey(action))
        .orElse(false);
  }

  @Override
  public List<DeploymentStatus> getNextStates(
      DeploymentStatus currentState, ConfigAccessType accessType, Action action) {
    return Optional.ofNullable(
            deploymentStatusTuplesMap.get(new DeploymentStatusTuple(currentState, accessType)))
        .map(value -> value.getOrDefault(action, List.of()))
        .orElse(List.of());
  }

  @Override
  public Map<Action, List<DeploymentStatus>> getActionsMap(
      DeploymentStatus currentState, ConfigAccessType accessType) {
    return Optional.ofNullable(
            deploymentStatusTuplesMap.get(new DeploymentStatusTuple(currentState, accessType)))
        .orElse(Collections.emptyMap());
  }

  @Value
  static class DeploymentStatusTuple {
    DeploymentStatus deploymentStatus;
    ConfigAccessType configAccessType;
  }
}
