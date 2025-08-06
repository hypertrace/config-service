package ai.traceable.cloud.edge.deployment.config.service.v1.state.transitions;

import static org.junit.jupiter.api.Assertions.*;

import ai.traceable.cloud.edge.deployment.config.service.v1.Action;
import ai.traceable.cloud.edge.deployment.config.service.v1.ConfigAccessType;
import ai.traceable.cloud.edge.deployment.config.service.v1.DeploymentStatus;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class StateTransitionsRegistryImplTest {

  @Test
  void testIsActionAllowed() {
    StateTransitionsRegistryImpl registry = new StateTransitionsRegistryImpl();
    // From YAML: START, ACTION_EDIT, CONFIG_ACCESS_TYPE_GLOBAL is allowed
    assertTrue(
        registry.isActionAllowed(
            null, ConfigAccessType.CONFIG_ACCESS_TYPE_GLOBAL, Action.ACTION_CREATE));
    // From YAML: DEPLOYMENT_STATUS_REQUESTED, ACTION_EDIT, CONFIG_ACCESS_TYPE_DEVOPS is allowed
    assertTrue(
        registry.isActionAllowed(
            DeploymentStatus.DEPLOYMENT_STATUS_REQUESTED,
            ConfigAccessType.CONFIG_ACCESS_TYPE_DEVOPS,
            Action.ACTION_EDIT));
    // Not allowed: DEPLOYMENT_STATUS_REQUESTED, ACTION_DELETE, CONFIG_ACCESS_TYPE_DEVOPS
    assertFalse(
        registry.isActionAllowed(
            DeploymentStatus.DEPLOYMENT_STATUS_REQUESTED,
            ConfigAccessType.CONFIG_ACCESS_TYPE_DEVOPS,
            Action.ACTION_DELETE));
  }

  @Test
  void testGetActionsMap() {
    StateTransitionsRegistryImpl registry = new StateTransitionsRegistryImpl();
    Map<Action, List<DeploymentStatus>> actionsMap =
        registry.getActionsMap(
            DeploymentStatus.DEPLOYMENT_STATUS_REQUESTED,
            ConfigAccessType.CONFIG_ACCESS_TYPE_DEVOPS);
    assertTrue(actionsMap.containsKey(Action.ACTION_EDIT));
    assertTrue(actionsMap.containsKey(Action.ACTION_HOLD));
    assertTrue(actionsMap.containsKey(Action.ACTION_UPDATE_STATUS));
    List<DeploymentStatus> nextStates = actionsMap.get(Action.ACTION_EDIT);
    assertNotNull(nextStates);
    assertTrue(nextStates.contains(DeploymentStatus.DEPLOYMENT_STATUS_REQUESTED));
  }
}
