package ai.traceable.anomaly.config.service.trainer.trainingaction;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import ai.traceable.anomaly.config.service.v1.AnomalyApiScope;
import ai.traceable.anomaly.config.service.v1.AnomalyConfigScope;
import ai.traceable.anomaly.config.service.v1.AnomalyCustomerScope;
import ai.traceable.anomaly.config.service.v1.AnomalyServiceScope;
import ai.traceable.anomaly.config.service.v1.trainer.ForceLearnAction;
import ai.traceable.anomaly.config.service.v1.trainer.PauseAction;
import ai.traceable.anomaly.config.service.v1.trainer.ResumeAction;
import ai.traceable.anomaly.config.service.v1.trainer.ScopedTrainingActionConfig;
import ai.traceable.anomaly.config.service.v1.trainer.ThresholdFamily;
import ai.traceable.anomaly.config.service.v1.trainer.TrainingAction;
import ai.traceable.anomaly.config.service.v1.trainer.TrainingAction.ActionCase;
import ai.traceable.anomaly.config.service.v1.trainer.TrainingActionConfig;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Value;
import java.time.Clock;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;

public class TrainingActionConverterTest {
  private final Clock clock = Clock.systemUTC();
  private final TrainingActionConverter converter = new TrainingActionConverter(clock);
  private final AnomalyServiceScope serviceScope =
      AnomalyServiceScope.newBuilder().setId("service").build();
  private final AnomalyApiScope apiScope =
      AnomalyApiScope.newBuilder().setId("api").setServiceScope(serviceScope).build();
  private final AnomalyConfigScope customerConfigScope =
      AnomalyConfigScope.newBuilder()
          .setCustomerScope(AnomalyCustomerScope.getDefaultInstance())
          .build();
  private final AnomalyConfigScope serviceConfigScope =
      AnomalyConfigScope.newBuilder().setServiceScope(serviceScope).build();
  private final AnomalyConfigScope apiConfigScope =
      AnomalyConfigScope.newBuilder().setApiScope(apiScope).build();

  @Test
  public void testConvert() throws InvalidProtocolBufferException {
    long time1 = clock.millis();
    ScopedTrainingActionConfig scopedActionConfig =
        ScopedTrainingActionConfig.newBuilder()
            .addTrainingActionConfig(
                TrainingActionConfig.newBuilder()
                    .setTimestamp(time1)
                    .setTrainingAction(
                        TrainingAction.newBuilder()
                            .setPauseAction(PauseAction.newBuilder().build())
                            .build())
                    .build())
            .build();

    Value configValue = converter.convert(scopedActionConfig);
    assertEquals(scopedActionConfig, converter.convert(configValue));
  }

  @Test
  public void testTimeBasedMergeScopedActionConfigs() {
    {
      // case when preferred ScopedTrainingActionConfig is empty
      ScopedTrainingActionConfig fallbackScopedActionConfig =
          ScopedTrainingActionConfig.newBuilder()
              .setConfigScope(apiConfigScope)
              .addTrainingActionConfig(
                  TrainingActionConfig.newBuilder()
                      .setTimestamp(clock.millis())
                      .setTrainingAction(
                          TrainingAction.newBuilder()
                              .setPauseAction(PauseAction.newBuilder().build())
                              .build())
                      .build())
              .build();
      ScopedTrainingActionConfig mergedScopedActionConfig =
          converter.timeBasedMerge(
              ScopedTrainingActionConfig.getDefaultInstance(), fallbackScopedActionConfig);
      assertEquals(fallbackScopedActionConfig, mergedScopedActionConfig);
      assertEquals(apiConfigScope, mergedScopedActionConfig.getConfigScope());
    }

    {
      // case when fallback ScopedTrainingActionConfig is empty
      ScopedTrainingActionConfig preferredScopedActionConfig =
          ScopedTrainingActionConfig.newBuilder()
              .setConfigScope(serviceConfigScope)
              .addTrainingActionConfig(
                  TrainingActionConfig.newBuilder()
                      .setTimestamp(clock.millis())
                      .setTrainingAction(
                          TrainingAction.newBuilder()
                              .setPauseAction(PauseAction.newBuilder().build())
                              .build())
                      .build())
              .build();
      ScopedTrainingActionConfig mergedScopedActionConfig =
          converter.timeBasedMerge(
              preferredScopedActionConfig, ScopedTrainingActionConfig.getDefaultInstance());
      assertEquals(preferredScopedActionConfig, mergedScopedActionConfig);
      assertEquals(serviceConfigScope, mergedScopedActionConfig.getConfigScope());
    }

    {
      // case when different action types are present in preferred and fallback
      // ScopedTrainingActionConfig
      long time1 = clock.millis();
      PauseAction pauseAction = PauseAction.newBuilder().build();
      ScopedTrainingActionConfig fallbackScopedActionConfig =
          ScopedTrainingActionConfig.newBuilder()
              .setConfigScope(serviceConfigScope)
              .addTrainingActionConfig(
                  TrainingActionConfig.newBuilder()
                      .setTimestamp(time1)
                      .setTrainingAction(
                          TrainingAction.newBuilder().setPauseAction(pauseAction).build())
                      .build())
              .build();
      long time2 = clock.millis();
      ResumeAction resumeAction = ResumeAction.newBuilder().build();
      ScopedTrainingActionConfig preferredScopedActionConfig =
          ScopedTrainingActionConfig.newBuilder()
              .setConfigScope(apiConfigScope)
              .addTrainingActionConfig(
                  TrainingActionConfig.newBuilder()
                      .setTimestamp(time2)
                      .setTrainingAction(
                          TrainingAction.newBuilder().setResumeAction(resumeAction).build())
                      .build())
              .build();

      ScopedTrainingActionConfig mergedScopedActionConfig =
          converter.timeBasedMerge(preferredScopedActionConfig, fallbackScopedActionConfig);
      assertEquals(apiConfigScope, mergedScopedActionConfig.getConfigScope());
      Map<ActionCase, TrainingActionConfig> actionConfigMap =
          getActionConfigMap(mergedScopedActionConfig);
      assertEquals(2, actionConfigMap.size());
      TrainingActionConfig trainingActionConfig = actionConfigMap.get(ActionCase.PAUSE_ACTION);
      assertEquals(time1, trainingActionConfig.getTimestamp());
      assertEquals(pauseAction, trainingActionConfig.getTrainingAction().getPauseAction());
      trainingActionConfig = actionConfigMap.get(ActionCase.RESUME_ACTION);
      assertEquals(time2, trainingActionConfig.getTimestamp());
      assertEquals(resumeAction, trainingActionConfig.getTrainingAction().getResumeAction());
    }

    {
      // case when same action type is present in preferred and fallback ScopedTrainingActionConfig
      long time1 = clock.millis();
      ForceLearnAction forceLearnAction1 =
          ForceLearnAction.newBuilder()
              .setThresholdFamily(ThresholdFamily.THRESHOLD_FAMILY_LIMITED_IP)
              .build();
      ScopedTrainingActionConfig fallbackScopedActionConfig =
          ScopedTrainingActionConfig.newBuilder()
              .setConfigScope(serviceConfigScope)
              .addTrainingActionConfig(
                  TrainingActionConfig.newBuilder()
                      .setTimestamp(time1)
                      .setTrainingAction(
                          TrainingAction.newBuilder()
                              .setForceLearnAction(forceLearnAction1)
                              .build())
                      .build())
              .build();
      long time2 = clock.millis();
      ForceLearnAction forceLearnAction2 =
          ForceLearnAction.newBuilder()
              .setThresholdFamily(ThresholdFamily.THRESHOLD_FAMILY_DIVERSE_IP_DIVERSE_USER)
              .build();
      ScopedTrainingActionConfig preferredScopedActionConfig =
          ScopedTrainingActionConfig.newBuilder()
              .setConfigScope(apiConfigScope)
              .addTrainingActionConfig(
                  TrainingActionConfig.newBuilder()
                      .setTimestamp(time2)
                      .setTrainingAction(
                          TrainingAction.newBuilder()
                              .setForceLearnAction(forceLearnAction2)
                              .build())
                      .build())
              .build();

      ScopedTrainingActionConfig mergedScopedActionConfig =
          converter.timeBasedMerge(preferredScopedActionConfig, fallbackScopedActionConfig);
      assertEquals(apiConfigScope, mergedScopedActionConfig.getConfigScope());
      Map<ActionCase, TrainingActionConfig> actionConfigMap =
          getActionConfigMap(mergedScopedActionConfig);
      assertEquals(1, actionConfigMap.size());
      TrainingActionConfig trainingActionConfig =
          actionConfigMap.get(ActionCase.FORCE_LEARN_ACTION);
      assertEquals(time2, trainingActionConfig.getTimestamp());
      assertEquals(
          forceLearnAction2, trainingActionConfig.getTrainingAction().getForceLearnAction());
    }
  }

  @Test
  public void testMergeActionWithScopedActionConfig() {
    {
      // case when there is no existing ScopedTrainingActionConfig
      // input action is force_learn
      ForceLearnAction forceLearnAction =
          ForceLearnAction.newBuilder()
              .setThresholdFamily(ThresholdFamily.THRESHOLD_FAMILY_LIMITED_IP)
              .build();
      TrainingAction inputTrainingAction =
          TrainingAction.newBuilder().setForceLearnAction(forceLearnAction).build();
      long mergeRequestTime = clock.millis();
      ScopedTrainingActionConfig mergedScopedActionConfig =
          converter.merge(
              customerConfigScope,
              inputTrainingAction,
              ScopedTrainingActionConfig.getDefaultInstance());
      assertEquals(customerConfigScope, mergedScopedActionConfig.getConfigScope());
      Map<ActionCase, TrainingActionConfig> actionConfigMap =
          getActionConfigMap(mergedScopedActionConfig);
      assertEquals(1, actionConfigMap.size());
      TrainingActionConfig actionConfig = actionConfigMap.get(ActionCase.FORCE_LEARN_ACTION);
      assertEquals(forceLearnAction, actionConfig.getTrainingAction().getForceLearnAction());
      assertTrue(actionConfig.getTimestamp() >= mergeRequestTime);
    }

    {
      // case when input action is not present in existing ScopedTrainingActionConfig
      // existing action is pause
      TrainingActionConfig pauseActionConfig =
          TrainingActionConfig.newBuilder()
              .setTimestamp(clock.millis())
              .setTrainingAction(
                  TrainingAction.newBuilder()
                      .setPauseAction(PauseAction.newBuilder().build())
                      .build())
              .build();
      ScopedTrainingActionConfig existingScopedActionConfig =
          ScopedTrainingActionConfig.newBuilder()
              .setConfigScope(customerConfigScope)
              .addTrainingActionConfig(pauseActionConfig)
              .build();
      // input action is force_learn
      ForceLearnAction forceLearnAction =
          ForceLearnAction.newBuilder()
              .setThresholdFamily(ThresholdFamily.THRESHOLD_FAMILY_LIMITED_IP)
              .build();
      TrainingAction inputTrainingAction =
          TrainingAction.newBuilder().setForceLearnAction(forceLearnAction).build();
      long mergeRequestTime = clock.millis();
      ScopedTrainingActionConfig mergedScopedActionConfig =
          converter.merge(customerConfigScope, inputTrainingAction, existingScopedActionConfig);
      assertEquals(customerConfigScope, mergedScopedActionConfig.getConfigScope());
      Map<ActionCase, TrainingActionConfig> actionConfigMap =
          getActionConfigMap(mergedScopedActionConfig);
      assertEquals(2, actionConfigMap.size());
      assertEquals(pauseActionConfig, actionConfigMap.get(ActionCase.PAUSE_ACTION));
      TrainingActionConfig forceLearnActionConfig =
          actionConfigMap.get(ActionCase.FORCE_LEARN_ACTION);
      assertEquals(
          forceLearnAction, forceLearnActionConfig.getTrainingAction().getForceLearnAction());
      assertTrue(forceLearnActionConfig.getTimestamp() >= mergeRequestTime);
    }

    {
      // case when input action is present in existing ScopedTrainingActionConfig
      // existing action is force_learn with THRESHOLD_FAMILY_LIMITED_IP
      TrainingActionConfig existingForceLearnActionConfig =
          TrainingActionConfig.newBuilder()
              .setTimestamp(clock.millis())
              .setTrainingAction(
                  TrainingAction.newBuilder()
                      .setForceLearnAction(
                          ForceLearnAction.newBuilder()
                              .setThresholdFamily(ThresholdFamily.THRESHOLD_FAMILY_LIMITED_IP)
                              .build())
                      .build())
              .build();
      ScopedTrainingActionConfig existingScopedActionConfig =
          ScopedTrainingActionConfig.newBuilder()
              .setConfigScope(customerConfigScope)
              .addTrainingActionConfig(existingForceLearnActionConfig)
              .build();
      // input action is force_learn with THRESHOLD_FAMILY_DIVERSE_IP_DIVERSE_USER
      ForceLearnAction inputForceLearnAction =
          ForceLearnAction.newBuilder()
              .setThresholdFamily(ThresholdFamily.THRESHOLD_FAMILY_DIVERSE_IP_DIVERSE_USER)
              .build();
      TrainingAction inputTrainingAction =
          TrainingAction.newBuilder().setForceLearnAction(inputForceLearnAction).build();
      long mergeRequestTime = clock.millis();
      ScopedTrainingActionConfig mergedScopedActionConfig =
          converter.merge(customerConfigScope, inputTrainingAction, existingScopedActionConfig);
      assertEquals(customerConfigScope, mergedScopedActionConfig.getConfigScope());
      Map<ActionCase, TrainingActionConfig> actionConfigMap =
          getActionConfigMap(mergedScopedActionConfig);
      assertEquals(1, actionConfigMap.size());
      TrainingActionConfig forceLearnActionConfig =
          actionConfigMap.get(ActionCase.FORCE_LEARN_ACTION);
      assertEquals(
          inputForceLearnAction, forceLearnActionConfig.getTrainingAction().getForceLearnAction());
      assertTrue(forceLearnActionConfig.getTimestamp() >= mergeRequestTime);
    }
  }

  private Map<ActionCase, TrainingActionConfig> getActionConfigMap(
      ScopedTrainingActionConfig scopedActionConfig) {
    return scopedActionConfig.getTrainingActionConfigList().stream()
        .collect(
            Collectors.toMap(
                trainingActionConfig -> trainingActionConfig.getTrainingAction().getActionCase(),
                Function.identity()));
  }
}
