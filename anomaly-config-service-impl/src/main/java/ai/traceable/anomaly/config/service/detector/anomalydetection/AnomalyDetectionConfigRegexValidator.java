package ai.traceable.anomaly.config.service.detector.anomalydetection;

import ai.traceable.anomaly.config.service.v1.detector.ApiDefinitionMetadataAnomalyDetectionConfig;
import ai.traceable.anomaly.config.service.v1.detector.ApiStateBasedAnomalyDetectionConfig;
import ai.traceable.anomaly.config.service.v1.detector.MultiValuedStringParamRule;
import ai.traceable.anomaly.config.service.v1.detector.SessionDefinitionMetadataAnomalyDetectionConfig;
import ai.traceable.config.utils.RegexValidator;
import io.grpc.Status;
import java.util.function.Predicate;

class AnomalyDetectionConfigRegexValidator {

  public Status validateSessionDefinitionMetadataAnomalyDetectionConfigRegex(
      SessionDefinitionMetadataAnomalyDetectionConfig detectionConfig) {
    Status status = Status.OK;
    switch (detectionConfig.getConfigCase()) {
      case OBJECT_BOLA:
        status =
            detectionConfig.getObjectBola().getMultiValuedStringParamRules().getRulesList().stream()
                .map(rule -> RegexValidator.validateRegex(rule.getKeyRegex()))
                .filter(Predicate.not(Status::isOk))
                .findFirst()
                .orElse(Status.OK);

        if (!status.isOk()) {
          return status;
        }
        status =
            detectionConfig.getObjectBola().getMultiValuedStringParamRules().getRulesList().stream()
                .filter(MultiValuedStringParamRule::hasValueRegex)
                .map(
                    multiValuedStringParamRule ->
                        RegexValidator.validateRegex(multiValuedStringParamRule.getValueRegex()))
                .filter(Predicate.not(Status::isOk))
                .findFirst()
                .orElse(Status.OK);
        break;
      default:
    }
    return status;
  }

  public Status validateApiDefinitionMetadataAnomalyDetectionConfigRegex(
      ApiDefinitionMetadataAnomalyDetectionConfig detectionConfig) {
    Status status = Status.OK;
    switch (detectionConfig.getConfigCase()) {
      case MISSING_PARAM:
        status =
            detectionConfig.getMissingParam().getSevereRegexStrings().getValuesList().stream()
                .map(RegexValidator::validateRegex)
                .filter(Predicate.not(Status::isOk))
                .findFirst()
                .orElse(Status.OK);

        if (!status.isOk()) {
          return status;
        }
        status =
            detectionConfig.getMissingParam().getAuthRegexStrings().getValuesList().stream()
                .map(RegexValidator::validateRegex)
                .filter(Predicate.not(Status::isOk))
                .findFirst()
                .orElse(Status.OK);
        break;

      case UNKNOWN_PARAM:
        status =
            detectionConfig.getUnknownParam().getSevereRegexStrings().getValuesList().stream()
                .map(RegexValidator::validateRegex)
                .filter(Predicate.not(Status::isOk))
                .findFirst()
                .orElse(Status.OK);
        break;
      default:
    }
    return status;
  }

  public Status validateApiStateBasedAnomalyDetectionConfigRegex(
      ApiStateBasedAnomalyDetectionConfig detectionConfig) {
    Status status = Status.OK;
    switch (detectionConfig.getConfigCase()) {
      case UNDER_DISCOVERY_API:
        status =
            detectionConfig
                .getUnderDiscoveryApi()
                .getRejectUrlRegexStrings()
                .getValuesList()
                .stream()
                .map(RegexValidator::validateRegex)
                .filter(Predicate.not(Status::isOk))
                .findFirst()
                .orElse(Status.OK);
        break;

      case UNDER_THRESHOLD_LEARNING_API:
        status =
            detectionConfig
                .getUnderThresholdLearningApi()
                .getRejectUrlRegexStrings()
                .getValuesList()
                .stream()
                .map(RegexValidator::validateRegex)
                .filter(Predicate.not(Status::isOk))
                .findFirst()
                .orElse(Status.OK);
        break;
      default:
    }
    return status;
  }
}
