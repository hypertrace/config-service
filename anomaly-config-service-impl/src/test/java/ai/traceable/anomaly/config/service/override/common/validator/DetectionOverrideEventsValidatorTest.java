package ai.traceable.anomaly.config.service.override.common.validator;

import static org.junit.jupiter.api.Assertions.assertEquals;

import ai.traceable.anomaly.config.service.v1.AnomalyConfidenceLevel;
import ai.traceable.anomaly.config.service.v1.AnomalyEventFamily;
import ai.traceable.anomaly.config.service.v1.override.DetectionOverrideAnomalousAttributesConfig;
import ai.traceable.anomaly.config.service.v1.override.DetectionOverrideAnomalousBehaviour;
import ai.traceable.anomaly.config.service.v1.override.DetectionOverrideConfidence;
import ai.traceable.anomaly.config.service.v1.override.DetectionOverrideEventConfig;
import ai.traceable.anomaly.config.service.v1.override.DetectionOverrideTargetEventsConfig;
import ai.traceable.anomaly.config.service.v1.override.DetectionOverrideThreatType;
import io.grpc.Status;
import java.util.List;
import org.junit.jupiter.api.Test;

public class DetectionOverrideEventsValidatorTest {

  private final DetectionOverrideEventsValidator eventsValidator =
      new DetectionOverrideEventsValidator();

  @Test
  public void testValidateTargetEventsConfig() {
    DetectionOverrideTargetEventsConfig config =
        DetectionOverrideTargetEventsConfig.getDefaultInstance();
    Status status = eventsValidator.validateTargetEventsConfig(config);
    assertEquals(Status.INVALID_ARGUMENT.getCode(), status.getCode());
    assertEquals(
        "Target events config should have a non empty events config.", status.getDescription());

    config =
        DetectionOverrideTargetEventsConfig.newBuilder()
            .addEventConfigs(
                DetectionOverrideEventConfig.newBuilder()
                    .setConfidence(
                        DetectionOverrideConfidence.newBuilder()
                            .setConfidenceLevel(
                                AnomalyConfidenceLevel.ANOMALY_CONFIDENCE_LEVEL_LOW)))
            .setAnomalousAttributes(DetectionOverrideAnomalousAttributesConfig.getDefaultInstance())
            .build();
    status = eventsValidator.validateTargetEventsConfig(config);
    assertEquals(Status.INVALID_ARGUMENT.getCode(), status.getCode());
    assertEquals(
        "Attribute names should have at least one name or one regex", status.getDescription());

    config =
        DetectionOverrideTargetEventsConfig.newBuilder()
            .addEventConfigs(
                DetectionOverrideEventConfig.newBuilder()
                    .setConfidence(
                        DetectionOverrideConfidence.newBuilder()
                            .setConfidenceLevel(
                                AnomalyConfidenceLevel.ANOMALY_CONFIDENCE_LEVEL_LOW)))
            .setAnomalousAttributes(
                DetectionOverrideAnomalousAttributesConfig.newBuilder()
                    .addAttributeNames("test-name"))
            .build();
    assertEquals(Status.OK, eventsValidator.validateTargetEventsConfig(config));
  }

  @Test
  public void testValidateEventsConfig() {
    // invalid events config case
    DetectionOverrideEventConfig eventConfig = DetectionOverrideEventConfig.newBuilder().build();
    DetectionOverrideTargetEventsConfig config =
        DetectionOverrideTargetEventsConfig.newBuilder()
            .addAllEventConfigs(List.of(eventConfig))
            .build();
    Status status = eventsValidator.validateTargetEventsConfig(config);
    assertEquals(Status.INVALID_ARGUMENT.getCode(), status.getCode());
    assertEquals(
        "Invalid case of events config " + eventConfig.getConfigCase(), status.getDescription());

    // config case == confidence

    eventConfig =
        DetectionOverrideEventConfig.newBuilder()
            .setConfidence(DetectionOverrideConfidence.getDefaultInstance())
            .build();
    config =
        DetectionOverrideTargetEventsConfig.newBuilder()
            .addAllEventConfigs(List.of(eventConfig))
            .build();
    status = eventsValidator.validateTargetEventsConfig(config);
    assertEquals(Status.INVALID_ARGUMENT.getCode(), status.getCode());
    assertEquals("Event config should have a valid confidence value", status.getDescription());

    eventConfig =
        DetectionOverrideEventConfig.newBuilder()
            .setConfidence(
                DetectionOverrideConfidence.newBuilder()
                    .setConfidenceLevel(AnomalyConfidenceLevel.ANOMALY_CONFIDENCE_LEVEL_LOW))
            .build();
    config =
        DetectionOverrideTargetEventsConfig.newBuilder()
            .addAllEventConfigs(List.of(eventConfig))
            .build();
    assertEquals(Status.OK, eventsValidator.validateTargetEventsConfig(config));

    // config case == threat type

    eventConfig =
        DetectionOverrideEventConfig.newBuilder()
            .setThreatType(DetectionOverrideThreatType.getDefaultInstance())
            .build();
    config =
        DetectionOverrideTargetEventsConfig.newBuilder()
            .addAllEventConfigs(List.of(eventConfig))
            .build();
    status = eventsValidator.validateTargetEventsConfig(config);
    assertEquals(Status.INVALID_ARGUMENT.getCode(), status.getCode());
    assertEquals("Threat type shouldn't have an empty threat type", status.getDescription());

    eventConfig =
        DetectionOverrideEventConfig.newBuilder()
            .setThreatType(
                DetectionOverrideThreatType.newBuilder()
                    .setThreatType("test")
                    .setEventFamily(AnomalyEventFamily.ANOMALY_EVENT_FAMILY_RATE_LIMIT))
            .build();
    config =
        DetectionOverrideTargetEventsConfig.newBuilder()
            .addAllEventConfigs(List.of(eventConfig))
            .build();
    assertEquals(Status.OK, eventsValidator.validateTargetEventsConfig(config));

    // config case == anomalous behaviour

    eventConfig =
        DetectionOverrideEventConfig.newBuilder()
            .setAnomalousBehaviour(DetectionOverrideAnomalousBehaviour.getDefaultInstance())
            .build();
    config =
        DetectionOverrideTargetEventsConfig.newBuilder()
            .addAllEventConfigs(List.of(eventConfig))
            .build();
    status = eventsValidator.validateTargetEventsConfig(config);
    assertEquals(Status.INVALID_ARGUMENT.getCode(), status.getCode());
    assertEquals("Anomalous behaviour shouldn't have an empty id", status.getDescription());

    eventConfig =
        DetectionOverrideEventConfig.newBuilder()
            .setAnomalousBehaviour(
                DetectionOverrideAnomalousBehaviour.newBuilder().setBehaviourId("test-id"))
            .build();
    config =
        DetectionOverrideTargetEventsConfig.newBuilder()
            .addAllEventConfigs(List.of(eventConfig))
            .build();
    status = eventsValidator.validateTargetEventsConfig(config);
    assertEquals(Status.INVALID_ARGUMENT.getCode(), status.getCode());
    assertEquals("Anomalous behaviour should have a threat type", status.getDescription());

    eventConfig =
        DetectionOverrideEventConfig.newBuilder()
            .setAnomalousBehaviour(
                DetectionOverrideAnomalousBehaviour.newBuilder()
                    .setBehaviourId("test-id")
                    .setThreatType(
                        DetectionOverrideThreatType.newBuilder()
                            .setThreatType("test")
                            .setEventFamily(AnomalyEventFamily.ANOMALY_EVENT_FAMILY_RATE_LIMIT)))
            .build();
    config =
        DetectionOverrideTargetEventsConfig.newBuilder()
            .addAllEventConfigs(List.of(eventConfig))
            .build();
    assertEquals(Status.OK, eventsValidator.validateTargetEventsConfig(config));
  }

  @Test
  public void testValidateMultipleEventsConfig() {
    DetectionOverrideEventConfig eventConfig1 =
        DetectionOverrideEventConfig.newBuilder()
            .setConfidence(
                DetectionOverrideConfidence.newBuilder()
                    .setConfidenceLevel(AnomalyConfidenceLevel.ANOMALY_CONFIDENCE_LEVEL_LOW))
            .build();
    DetectionOverrideEventConfig eventConfig2 =
        DetectionOverrideEventConfig.newBuilder()
            .setAnomalousBehaviour(
                DetectionOverrideAnomalousBehaviour.newBuilder().setBehaviourId("test-id"))
            .build();

    Status status =
        eventsValidator.validateTargetEventsConfig(
            DetectionOverrideTargetEventsConfig.newBuilder()
                .addAllEventConfigs(List.of(eventConfig1, eventConfig2))
                .build());
    assertEquals(Status.INVALID_ARGUMENT.getCode(), status.getCode());
    assertEquals("Anomalous behaviour should have a threat type", status.getDescription());

    eventConfig1 =
        DetectionOverrideEventConfig.newBuilder()
            .setAnomalousBehaviour(
                DetectionOverrideAnomalousBehaviour.newBuilder()
                    .setBehaviourId("test-id")
                    .setThreatType(
                        DetectionOverrideThreatType.newBuilder()
                            .setThreatType("test")
                            .setEventFamily(AnomalyEventFamily.ANOMALY_EVENT_FAMILY_RATE_LIMIT)))
            .build();
    eventConfig2 =
        DetectionOverrideEventConfig.newBuilder()
            .setThreatType(DetectionOverrideThreatType.getDefaultInstance())
            .build();

    status =
        eventsValidator.validateTargetEventsConfig(
            DetectionOverrideTargetEventsConfig.newBuilder()
                .addAllEventConfigs(List.of(eventConfig1, eventConfig2))
                .build());
    assertEquals(Status.INVALID_ARGUMENT.getCode(), status.getCode());
    assertEquals("Threat type shouldn't have an empty threat type", status.getDescription());

    eventConfig1 =
        DetectionOverrideEventConfig.newBuilder()
            .setThreatType(
                DetectionOverrideThreatType.newBuilder()
                    .setThreatType("test")
                    .setEventFamily(AnomalyEventFamily.ANOMALY_EVENT_FAMILY_RATE_LIMIT))
            .build();
    eventConfig2 =
        DetectionOverrideEventConfig.newBuilder()
            .setConfidence(
                DetectionOverrideConfidence.newBuilder()
                    .setConfidenceLevel(AnomalyConfidenceLevel.ANOMALY_CONFIDENCE_LEVEL_LOW))
            .build();

    status =
        eventsValidator.validateTargetEventsConfig(
            DetectionOverrideTargetEventsConfig.newBuilder()
                .addAllEventConfigs(List.of(eventConfig1, eventConfig2))
                .build());
    assertEquals(Status.OK, status);
  }
}
