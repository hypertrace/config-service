package ai.traceable.cloud.bot.deployment.config.service.v1.state.transitions;

import static org.junit.jupiter.api.Assertions.*;

import ai.traceable.cloud.bot.deployment.config.service.v1.DeploymentStatus;
import java.nio.file.AccessDeniedException;
import org.junit.jupiter.api.Test;

class StateTransitionsRegistryImplTest {

  @Test
  void testCheckAndGetNextStateWithHardDelete() {
    StateTransitionsRegistryImpl registry = new StateTransitionsRegistryImpl();
    assertDoesNotThrow(
        () ->
            registry.checkAndGetNextState(
                DeploymentStatus.DEPLOYMENT_STATUS_REQUESTED, Action.ACTION_DELETE));
  }

  @Test
  void testCheckAndGetNextState() throws AccessDeniedException {
    StateTransitionsRegistryImpl registry = new StateTransitionsRegistryImpl();

    // Test special case: DEPLOYMENT_STATUS_UNSPECIFIED with ACTION_CREATE
    assertEquals(
        DeploymentStatus.DEPLOYMENT_STATUS_REQUESTED,
        registry.checkAndGetNextState(
            DeploymentStatus.DEPLOYMENT_STATUS_UNSPECIFIED, Action.ACTION_CREATE));

    // Test normal case: DEPLOYMENT_STATUS_REQUESTED with ACTION_EDIT_BREAKING
    assertEquals(
        DeploymentStatus.DEPLOYMENT_STATUS_REQUESTED,
        registry.checkAndGetNextState(
            DeploymentStatus.DEPLOYMENT_STATUS_REQUESTED, Action.ACTION_EDIT_BREAKING));

    // Test hard-delete special case: DEPLOYMENT_STATUS_REQUESTED with ACTION_DELETE
    assertEquals(
        DeploymentStatus.DEPLOYMENT_STATUS_DEPLOYMENT_DELETED,
        registry.checkAndGetNextState(
            DeploymentStatus.DEPLOYMENT_STATUS_REQUESTED, Action.ACTION_DELETE));
  }

  @Test
  void testCheckAndGetNextState_AccessDenied() {
    StateTransitionsRegistryImpl registry = new StateTransitionsRegistryImpl();
    // Test normal case: DEPLOYMENT_STATUS_REQUESTED with ACTION_BLOCK (not allowed for customer)
    AccessDeniedException exception =
        assertThrows(
            AccessDeniedException.class,
            () ->
                registry.checkAndGetNextState(
                    DeploymentStatus.DEPLOYMENT_STATUS_REQUESTED, Action.ACTION_BLOCK));
    assertTrue(exception.getMessage().contains("Action ACTION_BLOCK is not allowed"));

    //  BREAKING EDIT when IN_PROGRESS state (not allowed)
    exception =
        assertThrows(
            AccessDeniedException.class,
            () ->
                registry.checkAndGetNextState(
                    DeploymentStatus.DEPLOYMENT_STATUS_CHANGE_IN_PROGRESS,
                    Action.ACTION_EDIT_BREAKING));
    assertTrue(exception.getMessage().contains("Action ACTION_EDIT_BREAKING is not allowed"));
  }
}
