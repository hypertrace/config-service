package ai.traceable.anomaly.config.service.trainer.trainingaction;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import ai.traceable.anomaly.config.service.common.AnomalyConfigValidator;
import ai.traceable.anomaly.config.service.v1.AnomalyConfigScope;
import ai.traceable.anomaly.config.service.v1.AnomalyServiceScope;
import ai.traceable.anomaly.config.service.v1.trainer.ForceLearnAction;
import ai.traceable.anomaly.config.service.v1.trainer.GetAllTrainingActionsRequest;
import ai.traceable.anomaly.config.service.v1.trainer.GetTrainingActionRequest;
import ai.traceable.anomaly.config.service.v1.trainer.PauseEntityLearnAction;
import ai.traceable.anomaly.config.service.v1.trainer.ThresholdFamily;
import ai.traceable.anomaly.config.service.v1.trainer.TrainingAction;
import ai.traceable.anomaly.config.service.v1.trainer.UpsertTrainingActionRequest;
import ai.traceable.anomaly.config.service.v1.trainer.UserRoleAction;
import ai.traceable.anomaly.config.service.v1.trainer.UserScopeAction;
import io.grpc.Status;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class TrainingActionValidatorTest {
  private AnomalyConfigValidator configValidator;
  private TrainingActionValidator actionValidator;

  @BeforeEach
  void setUp() {
    configValidator = spy(new AnomalyConfigValidator());
    this.actionValidator = spy(new TrainingActionValidatorImpl(configValidator));
  }

  @Test
  public void testGetAllTrainingActionsRequest() {
    // GetAllTrainingActionsRequest doesn't expect any input
    Status status = actionValidator.validate(GetAllTrainingActionsRequest.newBuilder().build());
    assertEquals(Status.OK.getCode(), status.getCode());
  }

  @Test
  public void testUpsertTrainingActionRequest() {
    // test empty anomaly config scope
    Status status = actionValidator.validate(UpsertTrainingActionRequest.newBuilder().build());
    assertEquals(Status.INVALID_ARGUMENT.getCode(), status.getCode());
    assertTrue(status.getDescription().contains("should have a valid config scope"));
    verify(configValidator, times(0)).validate(any(AnomalyConfigScope.class));

    // test service scope without serviceId
    status =
        actionValidator.validate(
            UpsertTrainingActionRequest.newBuilder()
                .setConfigScope(
                    AnomalyConfigScope.newBuilder()
                        .setServiceScope(AnomalyServiceScope.newBuilder().build())
                        .build())
                .build());
    assertTrue(status.getDescription().contains("SERVICE Scope should have valid Service ID"));

    // test missing training action
    status =
        actionValidator.validate(
            UpsertTrainingActionRequest.newBuilder()
                .setConfigScope(
                    AnomalyConfigScope.newBuilder()
                        .setServiceScope(AnomalyServiceScope.newBuilder().setId("service1").build())
                        .build())
                .build());
    assertEquals(Status.INVALID_ARGUMENT.getCode(), status.getCode());
    assertTrue(status.getDescription().contains("should have a valid training action"));

    // test empty training action
    status =
        actionValidator.validate(
            UpsertTrainingActionRequest.newBuilder()
                .setConfigScope(
                    AnomalyConfigScope.newBuilder()
                        .setServiceScope(AnomalyServiceScope.newBuilder().setId("service1").build())
                        .build())
                .setTrainingAction(TrainingAction.newBuilder().build())
                .build());
    assertEquals(Status.INVALID_ARGUMENT.getCode(), status.getCode());
    assertTrue(status.getDescription().contains("TrainingAction should have a valid action"));

    // test force_learn training action without threshold family
    status =
        actionValidator.validate(
            UpsertTrainingActionRequest.newBuilder()
                .setConfigScope(
                    AnomalyConfigScope.newBuilder()
                        .setServiceScope(AnomalyServiceScope.newBuilder().setId("service1").build())
                        .build())
                .setTrainingAction(
                    TrainingAction.newBuilder()
                        .setForceLearnAction(ForceLearnAction.newBuilder().build())
                        .build())
                .build());
    assertEquals(Status.INVALID_ARGUMENT.getCode(), status.getCode());
    assertTrue(
        status
            .getDescription()
            .contains("Force learn action should have a valid threshold family"));

    // test force_learn training action with threshold family
    status =
        actionValidator.validate(
            UpsertTrainingActionRequest.newBuilder()
                .setConfigScope(
                    AnomalyConfigScope.newBuilder()
                        .setServiceScope(AnomalyServiceScope.newBuilder().setId("service1").build())
                        .build())
                .setTrainingAction(
                    TrainingAction.newBuilder()
                        .setForceLearnAction(
                            ForceLearnAction.newBuilder()
                                .setThresholdFamily(ThresholdFamily.THRESHOLD_FAMILY_LIMITED_IP)
                                .build())
                        .build())
                .build());
    assertEquals(Status.OK.getCode(), status.getCode());

    // test user_role_action with pause_entity_learn_action
    status =
        actionValidator.validate(
            UpsertTrainingActionRequest.newBuilder()
                .setConfigScope(
                    AnomalyConfigScope.newBuilder()
                        .setServiceScope(AnomalyServiceScope.newBuilder().setId("service1").build())
                        .build())
                .setTrainingAction(
                    TrainingAction.newBuilder()
                        .setUserRoleAction(
                            UserRoleAction.newBuilder()
                                .setPauseEntityLearnAction(
                                    PauseEntityLearnAction.newBuilder()
                                        .setDisabledAll(true)
                                        .build())
                                .build())
                        .build())
                .build());
    assertEquals(Status.OK.getCode(), status.getCode());

    // test user_scope_action with pause_entity_learn_action
    status =
        actionValidator.validate(
            UpsertTrainingActionRequest.newBuilder()
                .setConfigScope(
                    AnomalyConfigScope.newBuilder()
                        .setServiceScope(AnomalyServiceScope.newBuilder().setId("service1").build())
                        .build())
                .setTrainingAction(
                    TrainingAction.newBuilder()
                        .setUserScopeAction(
                            UserScopeAction.newBuilder()
                                .setPauseEntityLearnAction(
                                    PauseEntityLearnAction.newBuilder()
                                        .setDisabledAll(true)
                                        .build())
                                .build())
                        .build())
                .build());
    assertEquals(Status.OK.getCode(), status.getCode());

    // test user_role_action with invalid action
    status =
        actionValidator.validate(
            UpsertTrainingActionRequest.newBuilder()
                .setConfigScope(
                    AnomalyConfigScope.newBuilder()
                        .setServiceScope(AnomalyServiceScope.newBuilder().setId("service1").build())
                        .build())
                .setTrainingAction(
                    TrainingAction.newBuilder()
                        .setUserRoleAction(UserRoleAction.newBuilder().build())
                        .build())
                .build());
    assertEquals(Status.INVALID_ARGUMENT.getCode(), status.getCode());
    assertTrue(status.getDescription().contains("TrainingAction should have a valid action"));

    // test user_scope_action with invalid action
    status =
        actionValidator.validate(
            UpsertTrainingActionRequest.newBuilder()
                .setConfigScope(
                    AnomalyConfigScope.newBuilder()
                        .setServiceScope(AnomalyServiceScope.newBuilder().setId("service1").build())
                        .build())
                .setTrainingAction(
                    TrainingAction.newBuilder()
                        .setUserScopeAction(UserScopeAction.newBuilder().build())
                        .build())
                .build());
    assertEquals(Status.INVALID_ARGUMENT.getCode(), status.getCode());
    assertTrue(status.getDescription().contains("TrainingAction should have a valid action"));

    // test pause_entity_learn_action with invalid action
    status =
        actionValidator.validate(
            UpsertTrainingActionRequest.newBuilder()
                .setConfigScope(
                    AnomalyConfigScope.newBuilder()
                        .setServiceScope(AnomalyServiceScope.newBuilder().setId("service1").build())
                        .build())
                .setTrainingAction(
                    TrainingAction.newBuilder()
                        .setUserScopeAction(
                            UserScopeAction.newBuilder()
                                .setPauseEntityLearnAction(
                                    PauseEntityLearnAction.newBuilder().build())
                                .build())
                        .build())
                .build());
    assertEquals(Status.INVALID_ARGUMENT.getCode(), status.getCode());
  }

  @Test
  void testGetTrainingActionRequest() {
    // test empty anomaly config scope
    Status status = actionValidator.validate(GetTrainingActionRequest.newBuilder().build());
    assertEquals(Status.INVALID_ARGUMENT.getCode(), status.getCode());
    assertTrue(status.getDescription().contains("should have a valid config scope"));
    verify(configValidator, times(0)).validate(any(AnomalyConfigScope.class));

    // test service scope without serviceId
    status =
        actionValidator.validate(
            GetTrainingActionRequest.newBuilder()
                .setConfigScope(
                    AnomalyConfigScope.newBuilder()
                        .setServiceScope(AnomalyServiceScope.newBuilder().build())
                        .build())
                .build());
    assertTrue(status.getDescription().contains("SERVICE Scope should have valid Service ID"));

    // test valid config scope
    status =
        actionValidator.validate(
            GetTrainingActionRequest.newBuilder()
                .setConfigScope(
                    AnomalyConfigScope.newBuilder()
                        .setServiceScope(AnomalyServiceScope.newBuilder().setId("service1").build())
                        .build())
                .build());
    assertEquals(Status.OK.getCode(), status.getCode());
  }
}
