package ai.traceable.cloud.edge.deployment.config.service.v1.state.transitions;

import ai.traceable.cloud.edge.deployment.config.service.v1.Action;
import ai.traceable.cloud.edge.deployment.config.service.v1.ConfigAccessType;
import ai.traceable.cloud.edge.deployment.config.service.v1.DeploymentStatus;
import java.util.List;
import java.util.Map;

public interface StateTransitionsRegistry {
  boolean isActionAllowed(
      DeploymentStatus currentState, ConfigAccessType accessType, Action action);

  List<DeploymentStatus> getNextStates(
      DeploymentStatus currentState, ConfigAccessType accessType, Action action);

  Map<Action, List<DeploymentStatus>> getActionsMap(
      DeploymentStatus currentState, ConfigAccessType accessType);

  boolean isActionAllowed(DeploymentStatus currentState, Action action);

  List<DeploymentStatus> getNextStates(DeploymentStatus currentState, Action action);
}
