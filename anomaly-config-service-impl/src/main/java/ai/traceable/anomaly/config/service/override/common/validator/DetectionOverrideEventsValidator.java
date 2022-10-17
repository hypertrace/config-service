package ai.traceable.anomaly.config.service.override.common.validator;

import ai.traceable.anomaly.config.service.v1.AnomalyConfidenceLevel;
import ai.traceable.anomaly.config.service.v1.AnomalyEventFamily;
import ai.traceable.anomaly.config.service.v1.override.DetectionOverrideAnomalousAttributesConfig;
import ai.traceable.anomaly.config.service.v1.override.DetectionOverrideAnomalousBehaviour;
import ai.traceable.anomaly.config.service.v1.override.DetectionOverrideEventConfig;
import ai.traceable.anomaly.config.service.v1.override.DetectionOverrideTargetEventsConfig;
import ai.traceable.anomaly.config.service.v1.override.DetectionOverrideThreatType;
import ai.traceable.config.utils.RegexValidator;
import io.grpc.Status;
import java.util.List;
import java.util.function.Predicate;
import lombok.NonNull;

public class DetectionOverrideEventsValidator {

  public Status validateTargetEventsConfig(@NonNull DetectionOverrideTargetEventsConfig config) {
    if (config.getEventConfigsList().isEmpty()) {
      return Status.INVALID_ARGUMENT.withDescription(
          "Target events config should have a non empty events config.");
    }

    Status status;
    if ((status = validateEventsConfig(config.getEventConfigsList())) != Status.OK) {
      return status;
    }
    if (config.hasAnomalousAttributes()) {
      return validateAnomalousAttributesConfig(config.getAnomalousAttributes());
    }
    return Status.OK;
  }

  private Status validateAnomalousAttributesConfig(
      @NonNull DetectionOverrideAnomalousAttributesConfig config) {
    if (config.getAttributeNamesList().isEmpty() && config.getAttributeRegexesList().isEmpty()) {
      return Status.INVALID_ARGUMENT.withDescription(
          "Attribute names should have at least one name or one regex");
    }
    return config.getAttributeRegexesList().stream()
        .map(RegexValidator::validate)
        .filter(Predicate.not(Status::isOk))
        .findFirst()
        .orElse(Status.OK);
  }

  private Status validateEventsConfig(List<DetectionOverrideEventConfig> eventConfigList) {
    for (DetectionOverrideEventConfig config : eventConfigList) {
      switch (config.getConfigCase()) {
        case CONFIDENCE:
          if (config
              .getConfidence()
              .getConfidenceLevel()
              .equals(AnomalyConfidenceLevel.ANOMALY_CONFIDENCE_LEVEL_UNSPECIFIED)) {
            return Status.INVALID_ARGUMENT.withDescription(
                "Event config should have a valid confidence value");
          }
          break;
        case THREAT_TYPE:
          Status status = validateThreatType(config.getThreatType());
          if (status != Status.OK) {
            return status;
          }
          break;
        case ANOMALOUS_BEHAVIOUR:
          status = validateAnomalousBehaviour(config.getAnomalousBehaviour());
          if (status != Status.OK) {
            return status;
          }
          break;
        default:
          return Status.INVALID_ARGUMENT.withDescription(
              String.format("Invalid case of events config %s", config.getConfigCase()));
      }
    }
    return Status.OK;
  }

  private Status validateAnomalousBehaviour(
      @NonNull DetectionOverrideAnomalousBehaviour anomalousBehaviour) {
    if (anomalousBehaviour.getBehaviourId().isEmpty()) {
      return Status.INVALID_ARGUMENT.withDescription(
          "Anomalous behaviour shouldn't have an empty id");
    }
    if (!anomalousBehaviour.hasThreatType()) {
      return Status.INVALID_ARGUMENT.withDescription(
          "Anomalous behaviour should have a threat type");
    }
    return validateThreatType(anomalousBehaviour.getThreatType());
  }

  private Status validateThreatType(@NonNull DetectionOverrideThreatType threatType) {
    if (threatType.getThreatType().isEmpty()) {
      return Status.INVALID_ARGUMENT.withDescription(
          "Threat type shouldn't have an empty threat type");
    }

    if (threatType.getEventFamily().equals(AnomalyEventFamily.ANOMALY_EVENT_FAMILY_UNSPECIFIED)) {
      return Status.INVALID_ARGUMENT.withDescription(
          "Event family should have a valid anomaly event family");
    }

    return Status.OK;
  }
}
