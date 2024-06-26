package ai.traceable.anomaly.config.service.detector.anomalydetection;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import ai.traceable.anomaly.config.service.v1.StringList;
import ai.traceable.anomaly.config.service.v1.detector.ApiDefinitionMetadataAnomalyDetectionConfig;
import ai.traceable.anomaly.config.service.v1.detector.ApiStateBasedAnomalyDetectionConfig;
import ai.traceable.anomaly.config.service.v1.detector.MissingParamAnomalyConfig;
import ai.traceable.anomaly.config.service.v1.detector.MultiValuedStringParamRule;
import ai.traceable.anomaly.config.service.v1.detector.MultiValuedStringParamRulesList;
import ai.traceable.anomaly.config.service.v1.detector.ObjectBolaAnomalyConfig;
import ai.traceable.anomaly.config.service.v1.detector.SessionDefinitionMetadataAnomalyDetectionConfig;
import ai.traceable.anomaly.config.service.v1.detector.UnderDiscoveryApiAnomalyConfig;
import ai.traceable.anomaly.config.service.v1.detector.UnderThresholdLearningApiAnomalyConfig;
import ai.traceable.anomaly.config.service.v1.detector.UnknownParamAnomalyConfig;
import io.grpc.Status;
import java.util.List;
import org.junit.jupiter.api.Test;

public class AnomalyDetectionConfigRegexValidatorTest {

  private final AnomalyDetectionConfigRegexValidator anomalyDetectionConfigRegexValidator =
      new AnomalyDetectionConfigRegexValidator();

  @Test
  void testValidateSessionDefinitionMetadataAnomalyDetectionConfigRegex() {
    SessionDefinitionMetadataAnomalyDetectionConfig detectionConfig =
        SessionDefinitionMetadataAnomalyDetectionConfig.newBuilder()
            .setObjectBola(
                ObjectBolaAnomalyConfig.newBuilder()
                    .setMultiValuedStringParamRules(
                        MultiValuedStringParamRulesList.newBuilder()
                            .addAllRules(
                                List.of(
                                    MultiValuedStringParamRule.newBuilder()
                                        .setKeyRegex("[")
                                        .setValueDelimiter("/")
                                        .setValueRegex("]")
                                        .build()))
                            .build())
                    .build())
            .build();

    Status status =
        anomalyDetectionConfigRegexValidator
            .validateSessionDefinitionMetadataAnomalyDetectionConfigRegex(detectionConfig);
    assertEquals(Status.INVALID_ARGUMENT.getCode(), status.getCode());
    assertTrue(status.getDescription().contains("Invalid Regex pattern: ["));

    detectionConfig =
        SessionDefinitionMetadataAnomalyDetectionConfig.newBuilder()
            .setObjectBola(
                ObjectBolaAnomalyConfig.newBuilder()
                    .setMultiValuedStringParamRules(
                        MultiValuedStringParamRulesList.newBuilder()
                            .addAllRules(
                                List.of(
                                    MultiValuedStringParamRule.newBuilder()
                                        .setKeyRegex(
                                            "\"/^[(]{0,1}[0-9]{3}[)]{0,1}[-\\\\s\\\\.]{0,1}[0-9]{3}[-\\\\s\\\\.]{0,1}[0-9]{4}$/\\n\"")
                                        .setValueDelimiter("/")
                                        .setValueRegex("]")
                                        .build()))
                            .build())
                    .build())
            .build();

    status =
        anomalyDetectionConfigRegexValidator
            .validateSessionDefinitionMetadataAnomalyDetectionConfigRegex(detectionConfig);
    assertEquals(Status.OK.getCode(), status.getCode());
  }

  @Test
  void testValidateApiDefinitionMetadataAnomalyDetectionConfigRegex() {
    ApiDefinitionMetadataAnomalyDetectionConfig detectionConfig =
        ApiDefinitionMetadataAnomalyDetectionConfig.newBuilder()
            .setMissingParam(
                MissingParamAnomalyConfig.newBuilder()
                    .setSevereRegexStrings(
                        StringList.newBuilder().addAllValues(List.of("[")).build())
                    .setAuthRegexStrings(StringList.newBuilder().addAllValues(List.of("[")).build())
                    .build())
            .build();
    Status status =
        anomalyDetectionConfigRegexValidator
            .validateApiDefinitionMetadataAnomalyDetectionConfigRegex(detectionConfig);

    assertEquals(Status.INVALID_ARGUMENT.getCode(), status.getCode());
    assertTrue(status.getDescription().contains("Invalid Regex pattern: ["));
    detectionConfig =
        ApiDefinitionMetadataAnomalyDetectionConfig.newBuilder()
            .setMissingParam(
                MissingParamAnomalyConfig.newBuilder()
                    .setSevereRegexStrings(
                        StringList.newBuilder()
                            .addAllValues(
                                List.of(
                                    "\"/^[(]{0,1}[0-9]{3}[)]{0,1}[-\\\\s\\\\.]{0,1}[0-9]{3}[-\\\\s\\\\.]{0,1}[0-9]{4}$/\\n\""))
                            .build())
                    .setAuthRegexStrings(
                        StringList.newBuilder()
                            .addAllValues(
                                List.of(
                                    "\"/^[(]{0,1}[0-9]{3}[)]{0,1}[-\\\\s\\\\.]{0,1}[0-9]{3}[-\\\\s\\\\.]{0,1}[0-9]{4}$/\\n\""))
                            .build())
                    .build())
            .build();
    status =
        anomalyDetectionConfigRegexValidator
            .validateApiDefinitionMetadataAnomalyDetectionConfigRegex(detectionConfig);

    assertEquals(Status.OK.getCode(), status.getCode());

    detectionConfig =
        ApiDefinitionMetadataAnomalyDetectionConfig.newBuilder()
            .setUnknownParam(
                UnknownParamAnomalyConfig.newBuilder()
                    .setSevereRegexStrings(
                        StringList.newBuilder()
                            .addAllValues(
                                List.of(
                                    "/^[(]{0,1}[0-9]{3}[)]{0,1}[-\\s\\.]{0,1}[0-9]{3}[-\\s\\.]{0,1}[0-9]{4}$/"))
                            .build())
                    .build())
            .build();
    status =
        anomalyDetectionConfigRegexValidator
            .validateApiDefinitionMetadataAnomalyDetectionConfigRegex(detectionConfig);

    assertEquals(Status.OK.getCode(), status.getCode());
    detectionConfig =
        ApiDefinitionMetadataAnomalyDetectionConfig.newBuilder()
            .setUnknownParam(
                UnknownParamAnomalyConfig.newBuilder()
                    .setSevereRegexStrings(
                        StringList.newBuilder().addAllValues(List.of("[")).build())
                    .build())
            .build();

    status =
        anomalyDetectionConfigRegexValidator
            .validateApiDefinitionMetadataAnomalyDetectionConfigRegex(detectionConfig);
    assertEquals(Status.INVALID_ARGUMENT.getCode(), status.getCode());
    assertTrue(status.getDescription().contains("Invalid Regex pattern: ["));
  }

  @Test
  void testValidateRegexWithNonWideApiStateBasedAnomalyDetectionConfigRegexWithWide() {
    ApiStateBasedAnomalyDetectionConfig detectionConfig =
        ApiStateBasedAnomalyDetectionConfig.newBuilder()
            .setUnderDiscoveryApi(
                UnderDiscoveryApiAnomalyConfig.newBuilder()
                    .setRejectUrlRegexStrings(
                        StringList.newBuilder()
                            .addAllValues(
                                List.of(
                                    "/^[(]{0,1}[0-9]{3}[)]{0,1}[-\\s\\.]{0,1}[0-9]{3}[-\\s\\.]{0,1}[0-9]{4}$/\n"))
                            .build())
                    .build())
            .build();
    Status status =
        anomalyDetectionConfigRegexValidator.validateApiStateBasedAnomalyDetectionConfigRegex(
            detectionConfig);
    assertEquals(Status.OK.getCode(), status.getCode());

    detectionConfig =
        ApiStateBasedAnomalyDetectionConfig.newBuilder()
            .setUnderDiscoveryApi(
                UnderDiscoveryApiAnomalyConfig.newBuilder()
                    .setRejectUrlRegexStrings(
                        StringList.newBuilder().addAllValues(List.of("[")).build())
                    .build())
            .build();
    status =
        anomalyDetectionConfigRegexValidator.validateApiStateBasedAnomalyDetectionConfigRegex(
            detectionConfig);
    assertEquals(Status.INVALID_ARGUMENT.getCode(), status.getCode());
    assertTrue(status.getDescription().contains("Invalid Regex pattern: ["));

    detectionConfig =
        ApiStateBasedAnomalyDetectionConfig.newBuilder()
            .setUnderThresholdLearningApi(
                UnderThresholdLearningApiAnomalyConfig.newBuilder()
                    .setRejectUrlRegexStrings(
                        StringList.newBuilder().addAllValues(List.of("[")).build())
                    .build())
            .build();

    status =
        anomalyDetectionConfigRegexValidator.validateApiStateBasedAnomalyDetectionConfigRegex(
            detectionConfig);
    assertEquals(Status.INVALID_ARGUMENT.getCode(), status.getCode());
    assertTrue(status.getDescription().contains("Invalid Regex pattern: ["));

    detectionConfig =
        ApiStateBasedAnomalyDetectionConfig.newBuilder()
            .setUnderThresholdLearningApi(
                UnderThresholdLearningApiAnomalyConfig.newBuilder()
                    .setRejectUrlRegexStrings(
                        StringList.newBuilder()
                            .addAllValues(
                                List.of(
                                    "/^[(]{0,1}[0-9]{3}[)]{0,1}[-\\s\\.]{0,1}[0-9]{3}[-\\s\\.]{0,1}[0-9]{4}$/\n"))
                            .build())
                    .build())
            .build();
    status =
        anomalyDetectionConfigRegexValidator.validateApiStateBasedAnomalyDetectionConfigRegex(
            detectionConfig);
    assertEquals(Status.OK.getCode(), status.getCode());
  }
}
