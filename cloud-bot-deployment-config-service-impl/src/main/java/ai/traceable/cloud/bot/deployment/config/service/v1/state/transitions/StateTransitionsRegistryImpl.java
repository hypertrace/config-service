package ai.traceable.cloud.bot.deployment.config.service.v1.state.transitions;

import ai.traceable.cloud.bot.deployment.config.service.v1.DeploymentStatus;
import java.nio.file.AccessDeniedException;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

public class StateTransitionsRegistryImpl implements StateTransitionsRegistry {
  private final Map<DeploymentStatus, EnumMap<Action, DeploymentStatus>> stateTransitionsMap;

  public StateTransitionsRegistryImpl() {
    stateTransitionsMap = new HashMap<>();
    StateTransitionsConfig config = StateTransitionsParser.getStateTransitionsConfig();

    for (StateTransitionsState state : config.getStates()) {
      DeploymentStatus currentState = DeploymentStatus.valueOf(state.getCurrentState());

      for (Map<String, StateTransition> transition : state.getTransitions()) {
        for (Map.Entry<String, StateTransition> entry : transition.entrySet()) {
          String actionStr = entry.getKey();
          StateTransition value = entry.getValue();

          DeploymentStatus nextState = DeploymentStatus.valueOf(value.getNextState());
          Action action = Action.valueOf(actionStr);

          // Ignore accessTypes as we're removing that concept
          stateTransitionsMap
              .computeIfAbsent(currentState, k -> new EnumMap<>(Action.class))
              .put(action, nextState);
        }
      }
    }
  }

  @Override
  public boolean isActionAllowed(DeploymentStatus currentState, Action action) {
    return Optional.ofNullable(stateTransitionsMap.get(currentState))
        .map(actionMap -> actionMap.containsKey(action))
        .orElse(false);
  }

  @Override
  public DeploymentStatus getNextState(DeploymentStatus currentState, Action action) {
    return Optional.ofNullable(stateTransitionsMap.get(currentState))
        .map(actionMap -> actionMap.get(action))
        .orElse(DeploymentStatus.DEPLOYMENT_STATUS_UNSPECIFIED);
  }

  @Override
  public DeploymentStatus checkAndGetNextState(DeploymentStatus currentState, Action action)
      throws AccessDeniedException {
    if (!isActionAllowed(currentState, action)) {
      throw new AccessDeniedException(
          String.format("Action %s is not allowed for deployment status %s", action, currentState));
    }
    return getNextState(currentState, action);
  }
}
