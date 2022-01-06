package ai.traceable.anomaly.config.service.detector.anomalydetection;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import ai.traceable.anomaly.config.service.common.AnomalyConfigValidator;
import ai.traceable.anomaly.config.service.v1.AnomalyConfigScope;
import ai.traceable.anomaly.config.service.v1.AnomalyCustomerScope;
import ai.traceable.anomaly.config.service.v1.detector.AnomalyDetectionConfig;
import ai.traceable.anomaly.config.service.v1.detector.AnomalySubRuleConfig;
import ai.traceable.anomaly.config.service.v1.detector.GetScopedAnomalyDetectionConfigRequest;
import ai.traceable.anomaly.config.service.v1.detector.ModsecurityAnomalyDetectionConfig;
import ai.traceable.anomaly.config.service.v1.detector.ScopedAnomalyDetectionConfig;
import ai.traceable.anomaly.config.service.v1.detector.UpdateScopedAnomalyDetectionConfigRequest;
import io.grpc.Status;
import java.util.List;
import org.junit.jupiter.api.Test;

public class AnomalyDetectionConfigValidatorTest {

  private final AnomalyConfigValidator anomalyConfigValidator = new AnomalyConfigValidator();
  private final AnomalyDetectionConfigValidator validator =
      new AnomalyDetectionConfigValidator(anomalyConfigValidator);
  private final AnomalyConfigScope configScope =
      AnomalyConfigScope.newBuilder()
          .setCustomerScope(AnomalyCustomerScope.getDefaultInstance())
          .build();

  @Test
  void testGetRequest() {
    Status status = validator.validate(GetScopedAnomalyDetectionConfigRequest.getDefaultInstance());
    assertEquals(Status.INVALID_ARGUMENT.getCode(), status.getCode());
    assertTrue(status.getDescription().contains("valid config scope"));

    status =
        validator.validate(
            GetScopedAnomalyDetectionConfigRequest.newBuilder()
                .setConfigScope(configScope)
                .build());
    assertEquals(Status.OK.getCode(), status.getCode());
  }

  @Test
  void testModsecUpdateValidation() {
    UpdateScopedAnomalyDetectionConfigRequest updateRequest;
    AnomalyDetectionConfig anomalyDetectionConfig1, anomalyDetectionConfig2;
    AnomalySubRuleConfig subRuleConfig1, subRuleConfig2;

    Status status =
        validator.validate(UpdateScopedAnomalyDetectionConfigRequest.getDefaultInstance());
    assertEquals(Status.INVALID_ARGUMENT.getCode(), status.getCode());
    assertTrue(status.getDescription().contains("valid config scope"));

    anomalyDetectionConfig1 =
        AnomalyDetectionConfig.newBuilder()
            .setModsecurityAnomalyDetectionConfig(
                ModsecurityAnomalyDetectionConfig.newBuilder().setAnomalyRuleId("rule1").build())
            .build();
    anomalyDetectionConfig2 =
        AnomalyDetectionConfig.newBuilder()
            .setModsecurityAnomalyDetectionConfig(
                ModsecurityAnomalyDetectionConfig.newBuilder().setAnomalyRuleId("rule1").build())
            .build();
    updateRequest =
        UpdateScopedAnomalyDetectionConfigRequest.newBuilder()
            .setScopedAnomalyDetectionConfig(
                ScopedAnomalyDetectionConfig.newBuilder()
                    .setConfigScope(configScope)
                    .addAllAnomalyDetectionConfigs(
                        List.of(anomalyDetectionConfig1, anomalyDetectionConfig2))
                    .build())
            .build();

    status = validator.validate(updateRequest);

    assertEquals(Status.INVALID_ARGUMENT.getCode(), status.getCode());
    assertTrue(
        status.getDescription().contains("should have only one modsec config with ruleId: rule1"));

    subRuleConfig1 = AnomalySubRuleConfig.newBuilder().setSubRuleId("subRule1").build();
    subRuleConfig2 = AnomalySubRuleConfig.newBuilder().setSubRuleId("subRule1").build();

    anomalyDetectionConfig1 =
        AnomalyDetectionConfig.newBuilder()
            .setModsecurityAnomalyDetectionConfig(
                ModsecurityAnomalyDetectionConfig.newBuilder()
                    .setAnomalyRuleId("rule1")
                    .addAllSubRuleConfigs(List.of(subRuleConfig1, subRuleConfig2))
                    .build())
            .build();
    updateRequest =
        UpdateScopedAnomalyDetectionConfigRequest.newBuilder()
            .setScopedAnomalyDetectionConfig(
                ScopedAnomalyDetectionConfig.newBuilder()
                    .setConfigScope(configScope)
                    .addAnomalyDetectionConfigs(anomalyDetectionConfig1)
                    .build())
            .build();

    status = validator.validate(updateRequest);

    assertEquals(Status.INVALID_ARGUMENT.getCode(), status.getCode());
    assertTrue(
        status
            .getDescription()
            .contains(
                "should have only one subRule config with subRuleId: subRule1 in modsec config with ruleId: rule1"));

    anomalyDetectionConfig1 =
        AnomalyDetectionConfig.newBuilder()
            .setModsecurityAnomalyDetectionConfig(
                ModsecurityAnomalyDetectionConfig.newBuilder()
                    .setAnomalyRuleId("rule1")
                    .addSubRuleConfigs(subRuleConfig1)
                    .build())
            .build();
    anomalyDetectionConfig2 =
        AnomalyDetectionConfig.newBuilder()
            .setModsecurityAnomalyDetectionConfig(
                ModsecurityAnomalyDetectionConfig.newBuilder()
                    .setAnomalyRuleId("rule2")
                    .addSubRuleConfigs(subRuleConfig1)
                    .build())
            .build();
    updateRequest =
        UpdateScopedAnomalyDetectionConfigRequest.newBuilder()
            .setScopedAnomalyDetectionConfig(
                ScopedAnomalyDetectionConfig.newBuilder()
                    .setConfigScope(configScope)
                    .addAllAnomalyDetectionConfigs(
                        List.of(anomalyDetectionConfig1, anomalyDetectionConfig2))
                    .build())
            .build();

    status = validator.validate(updateRequest);

    assertEquals(Status.OK.getCode(), status.getCode());
  }
}
