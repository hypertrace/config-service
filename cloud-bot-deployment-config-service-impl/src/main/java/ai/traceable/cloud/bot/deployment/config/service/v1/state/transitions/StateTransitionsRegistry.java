package ai.traceable.cloud.bot.deployment.config.service.v1.state.transitions;

import ai.traceable.cloud.bot.deployment.config.service.v1.DeploymentStatus;
import java.nio.file.AccessDeniedException;

public interface StateTransitionsRegistry {
  boolean isActionAllowed(DeploymentStatus currentState, Action action);

  DeploymentStatus getNextState(DeploymentStatus currentState, Action action);

  DeploymentStatus checkAndGetNextState(DeploymentStatus currentState, Action action)
      throws AccessDeniedException;
}
