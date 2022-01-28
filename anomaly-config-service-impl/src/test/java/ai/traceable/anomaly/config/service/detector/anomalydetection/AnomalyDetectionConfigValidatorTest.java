package ai.traceable.anomaly.config.service.detector.anomalydetection;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import ai.traceable.anomaly.config.service.common.AnomalyConfigValidator;
import ai.traceable.anomaly.config.service.registry.apidef.ApiDefinitionRegistryImpl;
import ai.traceable.anomaly.config.service.registry.common.ConfigConverter;
import ai.traceable.anomaly.config.service.v1.AnomalyConfigScope;
import ai.traceable.anomaly.config.service.v1.AnomalyCustomerScope;
import ai.traceable.anomaly.config.service.v1.detector.AnomalyDetectionConfig;
import ai.traceable.anomaly.config.service.v1.detector.AnomalySubRuleConfig;
import ai.traceable.anomaly.config.service.v1.detector.ApiDefinitionMetadataAnomalyDetectionConfig;
import ai.traceable.anomaly.config.service.v1.detector.ApiStateBasedAnomalyDetectionConfig;
import ai.traceable.anomaly.config.service.v1.detector.BlockingMetadataAnomalyDetectionConfig;
import ai.traceable.anomaly.config.service.v1.detector.ContentSizeAnomalyConfig;
import ai.traceable.anomaly.config.service.v1.detector.CustomIpAnomalyConfig;
import ai.traceable.anomaly.config.service.v1.detector.CustomRegionAnomalyConfig;
import ai.traceable.anomaly.config.service.v1.detector.GetScopedAnomalyDetectionConfigRequest;
import ai.traceable.anomaly.config.service.v1.detector.IntegerAnomalyConfig;
import ai.traceable.anomaly.config.service.v1.detector.LearntApiAnomalyConfig;
import ai.traceable.anomaly.config.service.v1.detector.ModsecurityAllDetectionConfig;
import ai.traceable.anomaly.config.service.v1.detector.ModsecurityAnomalyDetectionConfig;
import ai.traceable.anomaly.config.service.v1.detector.ModsecurityAnomalyRuleConfig;
import ai.traceable.anomaly.config.service.v1.detector.ScopedAnomalyDetectionConfig;
import ai.traceable.anomaly.config.service.v1.detector.UnderDiscoveryApiAnomalyConfig;
import ai.traceable.anomaly.config.service.v1.detector.UpdateScopedAnomalyDetectionConfigRequest;
import io.grpc.Status;
import java.util.List;
import org.junit.jupiter.api.Test;

public class AnomalyDetectionConfigValidatorTest {

  private final AnomalyConfigValidator anomalyConfigValidator = new AnomalyConfigValidator();
  private final AnomalyDetectionConfigValidator validator =
      new AnomalyDetectionConfigValidator(
          anomalyConfigValidator, new ApiDefinitionRegistryImpl(new ConfigConverter()));
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
                ModsecurityAnomalyDetectionConfig.newBuilder()
                    .setModsecAnomalyRule(
                        ModsecurityAnomalyRuleConfig.newBuilder()
                            .setAnomalyRuleId("rule1")
                            .build()))
            .build();
    anomalyDetectionConfig2 =
        AnomalyDetectionConfig.newBuilder()
            .setModsecurityAnomalyDetectionConfig(
                ModsecurityAnomalyDetectionConfig.newBuilder()
                    .setModsecAnomalyRule(
                        ModsecurityAnomalyRuleConfig.newBuilder()
                            .setAnomalyRuleId("rule1")
                            .build()))
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
                    .setModsecAnomalyRule(
                        ModsecurityAnomalyRuleConfig.newBuilder()
                            .setAnomalyRuleId("rule1")
                            .addAllSubRuleConfigs(List.of(subRuleConfig1, subRuleConfig2))
                            .build()))
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

    AnomalyDetectionConfig modsecAllDetectionConfig =
        AnomalyDetectionConfig.newBuilder()
            .setModsecurityAnomalyDetectionConfig(
                ModsecurityAnomalyDetectionConfig.newBuilder()
                    .setModsecAllDetection(ModsecurityAllDetectionConfig.getDefaultInstance()))
            .build();

    updateRequest =
        UpdateScopedAnomalyDetectionConfigRequest.newBuilder()
            .setScopedAnomalyDetectionConfig(
                ScopedAnomalyDetectionConfig.newBuilder()
                    .setConfigScope(configScope)
                    .addAllAnomalyDetectionConfigs(
                        List.of(modsecAllDetectionConfig, modsecAllDetectionConfig))
                    .build())
            .build();

    status = validator.validate(updateRequest);

    assertEquals(Status.INVALID_ARGUMENT.getCode(), status.getCode());
    assertTrue(
        status
            .getDescription()
            .contains(
                "UpdateScopedAnomalyDetectionConfigRequest should have only one modsecAllDetectionConfig"));

    anomalyDetectionConfig1 =
        AnomalyDetectionConfig.newBuilder()
            .setModsecurityAnomalyDetectionConfig(
                ModsecurityAnomalyDetectionConfig.newBuilder()
                    .setModsecAnomalyRule(
                        ModsecurityAnomalyRuleConfig.newBuilder()
                            .setAnomalyRuleId("rule1")
                            .addSubRuleConfigs(subRuleConfig1)
                            .build()))
            .build();
    anomalyDetectionConfig2 =
        AnomalyDetectionConfig.newBuilder()
            .setModsecurityAnomalyDetectionConfig(
                ModsecurityAnomalyDetectionConfig.newBuilder()
                    .setModsecAnomalyRule(
                        ModsecurityAnomalyRuleConfig.newBuilder()
                            .setAnomalyRuleId("rule2")
                            .addSubRuleConfigs(subRuleConfig1)
                            .build()))
            .build();
    updateRequest =
        UpdateScopedAnomalyDetectionConfigRequest.newBuilder()
            .setScopedAnomalyDetectionConfig(
                ScopedAnomalyDetectionConfig.newBuilder()
                    .setConfigScope(configScope)
                    .addAllAnomalyDetectionConfigs(
                        List.of(
                            anomalyDetectionConfig1,
                            anomalyDetectionConfig2,
                            modsecAllDetectionConfig))
                    .build())
            .build();

    status = validator.validate(updateRequest);

    assertEquals(Status.OK.getCode(), status.getCode());
  }

  @Test
  void testStateBasedUpdateValidation() {
    UpdateScopedAnomalyDetectionConfigRequest request;

    AnomalyDetectionConfig detectionConfig1 =
        AnomalyDetectionConfig.newBuilder()
            .setApiStateBasedAnomalyDetectionConfig(
                ApiStateBasedAnomalyDetectionConfig.newBuilder()
                    .setLearntApi(LearntApiAnomalyConfig.newBuilder().build())
                    .build())
            .build();

    AnomalyDetectionConfig detectionConfig2 =
        AnomalyDetectionConfig.newBuilder()
            .setApiStateBasedAnomalyDetectionConfig(
                ApiStateBasedAnomalyDetectionConfig.newBuilder()
                    .setLearntApi(LearntApiAnomalyConfig.newBuilder().build())
                    .build())
            .build();

    request =
        UpdateScopedAnomalyDetectionConfigRequest.newBuilder()
            .setScopedAnomalyDetectionConfig(
                ScopedAnomalyDetectionConfig.newBuilder()
                    .setConfigScope(configScope)
                    .addAllAnomalyDetectionConfigs(List.of(detectionConfig1, detectionConfig2)))
            .build();

    Status status = validator.validate(request);
    assertEquals(Status.INVALID_ARGUMENT.getCode(), status.getCode());
    assertTrue(status.getDescription().contains("Duplicate key LEARNT_API"));

    detectionConfig2 =
        AnomalyDetectionConfig.newBuilder()
            .setApiStateBasedAnomalyDetectionConfig(
                ApiStateBasedAnomalyDetectionConfig.newBuilder()
                    .setUnderDiscoveryApi(UnderDiscoveryApiAnomalyConfig.getDefaultInstance())
                    .build())
            .build();

    request =
        UpdateScopedAnomalyDetectionConfigRequest.newBuilder()
            .setScopedAnomalyDetectionConfig(
                ScopedAnomalyDetectionConfig.newBuilder()
                    .setConfigScope(configScope)
                    .addAllAnomalyDetectionConfigs(List.of(detectionConfig1, detectionConfig2)))
            .build();

    status = validator.validate(request);

    assertEquals(Status.OK.getCode(), status.getCode());
  }

  @Test
  void testApiDefinitionUpdateValidation() {
    UpdateScopedAnomalyDetectionConfigRequest request;

    AnomalyDetectionConfig detectionConfig1 =
        AnomalyDetectionConfig.newBuilder()
            .setApiDefinitionMetadataAnomalyDetectionConfig(
                ApiDefinitionMetadataAnomalyDetectionConfig.newBuilder()
                    .setAnomalyRuleId("rule1")
                    .setInteger(IntegerAnomalyConfig.getDefaultInstance()))
            .build();

    request =
        UpdateScopedAnomalyDetectionConfigRequest.newBuilder()
            .setScopedAnomalyDetectionConfig(
                ScopedAnomalyDetectionConfig.newBuilder()
                    .setConfigScope(configScope)
                    .addAllAnomalyDetectionConfigs(List.of(detectionConfig1)))
            .build();

    Status status = validator.validate(request);

    assertEquals(Status.INVALID_ARGUMENT.getCode(), status.getCode());
    assertTrue(
        status.getDescription().contains("Invalid api definition detection config ruleId: rule1"));

    detectionConfig1 =
        AnomalyDetectionConfig.newBuilder()
            .setApiDefinitionMetadataAnomalyDetectionConfig(
                ApiDefinitionMetadataAnomalyDetectionConfig.newBuilder()
                    .setAnomalyRuleId("contentSize")
                    .setInteger(IntegerAnomalyConfig.getDefaultInstance()))
            .build();
    request =
        UpdateScopedAnomalyDetectionConfigRequest.newBuilder()
            .setScopedAnomalyDetectionConfig(
                ScopedAnomalyDetectionConfig.newBuilder()
                    .setConfigScope(configScope)
                    .addAllAnomalyDetectionConfigs(List.of(detectionConfig1)))
            .build();

    status = validator.validate(request);
    assertEquals(Status.INVALID_ARGUMENT.getCode(), status.getCode());
    assertTrue(
        status
            .getDescription()
            .contains("Invalid api definition detection config type for ruleId: contentSize"));

    detectionConfig1 =
        AnomalyDetectionConfig.newBuilder()
            .setApiDefinitionMetadataAnomalyDetectionConfig(
                ApiDefinitionMetadataAnomalyDetectionConfig.newBuilder()
                    .setAnomalyRuleId("integer")
                    .setInteger(IntegerAnomalyConfig.getDefaultInstance()))
            .build();

    request =
        UpdateScopedAnomalyDetectionConfigRequest.newBuilder()
            .setScopedAnomalyDetectionConfig(
                ScopedAnomalyDetectionConfig.newBuilder()
                    .setConfigScope(configScope)
                    .addAllAnomalyDetectionConfigs(List.of(detectionConfig1, detectionConfig1)))
            .build();

    status = validator.validate(request);

    assertEquals(Status.INVALID_ARGUMENT.getCode(), status.getCode());
    assertTrue(
        status
            .getDescription()
            .contains("should have only one api definition detection config for ruleId: integer"));

    detectionConfig1 =
        AnomalyDetectionConfig.newBuilder()
            .setApiDefinitionMetadataAnomalyDetectionConfig(
                ApiDefinitionMetadataAnomalyDetectionConfig.newBuilder()
                    .setInteger(IntegerAnomalyConfig.getDefaultInstance()))
            .build();

    request =
        UpdateScopedAnomalyDetectionConfigRequest.newBuilder()
            .setScopedAnomalyDetectionConfig(
                ScopedAnomalyDetectionConfig.newBuilder()
                    .setConfigScope(configScope)
                    .addAllAnomalyDetectionConfigs(List.of(detectionConfig1, detectionConfig1)))
            .build();

    status = validator.validate(request);

    assertEquals(Status.INVALID_ARGUMENT.getCode(), status.getCode());
    assertTrue(
        status
            .getDescription()
            .contains(
                "should have only one api definition detection config for configCase: INTEGER"));

    AnomalyDetectionConfig detectionConfig2 =
        AnomalyDetectionConfig.newBuilder()
            .setApiDefinitionMetadataAnomalyDetectionConfig(
                ApiDefinitionMetadataAnomalyDetectionConfig.newBuilder()
                    .setAnomalyRuleId("contentSize")
                    .setContentSize(ContentSizeAnomalyConfig.getDefaultInstance()))
            .build();

    request =
        UpdateScopedAnomalyDetectionConfigRequest.newBuilder()
            .setScopedAnomalyDetectionConfig(
                ScopedAnomalyDetectionConfig.newBuilder()
                    .setConfigScope(configScope)
                    .addAllAnomalyDetectionConfigs(List.of(detectionConfig1, detectionConfig2)))
            .build();

    status = validator.validate(request);

    assertEquals(Status.OK.getCode(), status.getCode());

    detectionConfig1 =
        AnomalyDetectionConfig.newBuilder()
            .setApiDefinitionMetadataAnomalyDetectionConfig(
                ApiDefinitionMetadataAnomalyDetectionConfig.getDefaultInstance())
            .build();

    request =
        UpdateScopedAnomalyDetectionConfigRequest.newBuilder()
            .setScopedAnomalyDetectionConfig(
                ScopedAnomalyDetectionConfig.newBuilder()
                    .setConfigScope(configScope)
                    .addAllAnomalyDetectionConfigs(List.of(detectionConfig1)))
            .build();
    status = validator.validate(request);

    assertEquals(Status.INVALID_ARGUMENT.getCode(), status.getCode());
    assertTrue(status.getDescription().contains("Invalid api definition detection config"));
  }

  @Test
  void testBlockingUpdateValidation() {
    UpdateScopedAnomalyDetectionConfigRequest request;

    AnomalyDetectionConfig detectionConfig1 =
        AnomalyDetectionConfig.newBuilder()
            .setBlockingMetadataAnomalyDetectionConfig(
                BlockingMetadataAnomalyDetectionConfig.newBuilder()
                    .setCustomIp(CustomIpAnomalyConfig.getDefaultInstance())
                    .build())
            .build();

    request =
        UpdateScopedAnomalyDetectionConfigRequest.newBuilder()
            .setScopedAnomalyDetectionConfig(
                ScopedAnomalyDetectionConfig.newBuilder()
                    .setConfigScope(configScope)
                    .addAllAnomalyDetectionConfigs(List.of(detectionConfig1, detectionConfig1)))
            .build();

    Status status = validator.validate(request);
    assertEquals(Status.INVALID_ARGUMENT.getCode(), status.getCode());
    assertTrue(status.getDescription().contains("Duplicate key CUSTOM_IP"));

    AnomalyDetectionConfig detectionConfig2 =
        AnomalyDetectionConfig.newBuilder()
            .setBlockingMetadataAnomalyDetectionConfig(
                BlockingMetadataAnomalyDetectionConfig.newBuilder()
                    .setCustomRegion(CustomRegionAnomalyConfig.getDefaultInstance())
                    .build())
            .build();

    request =
        UpdateScopedAnomalyDetectionConfigRequest.newBuilder()
            .setScopedAnomalyDetectionConfig(
                ScopedAnomalyDetectionConfig.newBuilder()
                    .setConfigScope(configScope)
                    .addAllAnomalyDetectionConfigs(List.of(detectionConfig1, detectionConfig2)))
            .build();

    status = validator.validate(request);

    assertEquals(Status.OK.getCode(), status.getCode());
  }
}
