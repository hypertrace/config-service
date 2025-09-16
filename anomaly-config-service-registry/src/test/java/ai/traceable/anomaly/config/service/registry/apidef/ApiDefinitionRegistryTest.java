package ai.traceable.anomaly.config.service.registry.apidef;

import static ai.traceable.anomaly.config.service.v1.AnomalySubRuleType.ANOMALY_SUB_RULE_TYPE_REGULAR;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import ai.traceable.anomaly.config.service.registry.common.ConfigConverter;
import ai.traceable.anomaly.config.service.v1.AnomalyEventDetails;
import ai.traceable.anomaly.config.service.v1.AnomalySeverityLevel;
import ai.traceable.anomaly.config.service.v1.detector.ApiDefinitionMetadataAnomalyDetectionConfig;
import ai.traceable.anomaly.config.service.v1.detector.BflaAnomalyConfig;
import ai.traceable.anomaly.config.service.v1.detector.ContentExplosionAnomalyConfig;
import ai.traceable.anomaly.config.service.v1.detector.ContentSizeAnomalyConfig;
import ai.traceable.anomaly.config.service.v1.detector.ContentTypeAnomalyConfig;
import ai.traceable.anomaly.config.service.v1.detector.DeviceAnomalyConfig;
import ai.traceable.anomaly.config.service.v1.detector.EnumerationsAnomalyConfig;
import ai.traceable.anomaly.config.service.v1.detector.GqlaAnomalyConfig;
import ai.traceable.anomaly.config.service.v1.detector.HttpStatusAnomalyConfig;
import ai.traceable.anomaly.config.service.v1.detector.IntegerAnomalyConfig;
import ai.traceable.anomaly.config.service.v1.detector.JwtAnomalyConfig;
import ai.traceable.anomaly.config.service.v1.detector.MissingParamAnomalyConfig;
import ai.traceable.anomaly.config.service.v1.detector.SpecialCharacterAnomalyConfig;
import ai.traceable.anomaly.config.service.v1.detector.SsrfAnomalyConfig;
import ai.traceable.anomaly.config.service.v1.detector.TypeAnomalyConfig;
import ai.traceable.anomaly.config.service.v1.detector.UnknownParamAnomalyConfig;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;

public class ApiDefinitionRegistryTest {

  private final ApiDefinitionRegistry apiDefinitionRegistry =
      new ApiDefinitionRegistryImpl(new ConfigConverter());

  @Test
  void testRuleIds() {
    Set<String> ruleIdsFromApiDefRuleInfos =
        apiDefinitionRegistry.getApiDefRuleInfos().values().stream()
            .map(
                anomalyRuleInfo -> {
                  if (anomalyRuleInfo.getRuleId().equals("bfla")) {
                    verifyEventDetails(anomalyRuleInfo.getEventDetails());
                  }
                  String jwtRuleId = "jwt";
                  String gqlaRuleId = "gqla";
                  if (anomalyRuleInfo.getRuleId().equals(jwtRuleId)) {
                    assertEquals(8, anomalyRuleInfo.getSubRuleInfosCount());

                    anomalyRuleInfo
                        .getSubRuleInfosList()
                        .forEach(
                            subRuleInfo -> {
                              assertEquals(1, subRuleInfo.getSubRuleTypesCount());
                              assertEquals(
                                  ANOMALY_SUB_RULE_TYPE_REGULAR, subRuleInfo.getSubRuleTypes(0));
                              assertFalse(subRuleInfo.getEventLabelsMap().isEmpty());
                              assertNotEquals(
                                  AnomalySeverityLevel.ANOMALY_SEVERITY_LEVEL_UNSPECIFIED,
                                  subRuleInfo.getSeverityLevel());
                              assertTrue(subRuleInfo.getRuleId().startsWith(jwtRuleId));
                              assertTrue(subRuleInfo.getEventLabelsCount() > 0);
                              if (subRuleInfo.getRuleId().equals("jwt_exp")) {
                                verifyEventDetails(subRuleInfo.getEventDetails());
                              }
                            });
                  } else if (anomalyRuleInfo.getRuleId().equals(gqlaRuleId)) {
                    assertEquals(8, anomalyRuleInfo.getSubRuleInfosCount());

                    anomalyRuleInfo
                        .getSubRuleInfosList()
                        .forEach(
                            subRuleInfo -> {
                              assertFalse(subRuleInfo.getEventLabelsMap().isEmpty());
                              assertNotEquals(
                                  AnomalySeverityLevel.ANOMALY_SEVERITY_LEVEL_UNSPECIFIED,
                                  subRuleInfo.getSeverityLevel());
                              assertTrue(subRuleInfo.getRuleId().startsWith(gqlaRuleId));
                              assertTrue(subRuleInfo.getEventLabelsCount() > 0);
                              if (subRuleInfo.getRuleId().equals("gqla_fs")) {
                                verifyEventDetails(subRuleInfo.getEventDetails());
                              }
                            });
                  } else {
                    assertEquals(0, anomalyRuleInfo.getSubRuleInfosCount());
                    assertTrue(anomalyRuleInfo.getEventLabelsCount() > 0);
                  }
                  return anomalyRuleInfo.getRuleId();
                })
            .collect(Collectors.toSet());

    Set<String> ruleIdsFromApiDefDetectionConfigs =
        apiDefinitionRegistry.getApiDefRuleIdToDetectionConfigMap().values().stream()
            .map(ApiDefinitionMetadataAnomalyDetectionConfig::getAnomalyRuleId)
            .collect(Collectors.toSet());

    assertEquals(ruleIdsFromApiDefRuleInfos, ruleIdsFromApiDefDetectionConfigs);
  }

  private void verifyEventDetails(AnomalyEventDetails eventDetails) {
    assertFalse(eventDetails.getDescription().isBlank());
    assertFalse(eventDetails.getMitigation().isBlank());
    assertFalse(eventDetails.getImpact().isBlank());
    assertFalse(eventDetails.getReferences().isBlank());
  }

  @Test
  void testRuleIdToConfigMap() {
    Map<String, ApiDefinitionMetadataAnomalyDetectionConfig> ruleIdToConfigMap =
        apiDefinitionRegistry.getApiDefRuleIdToDetectionConfigMap();

    Map<String, ApiDefinitionMetadataAnomalyDetectionConfig> expectedMap = new HashMap<>();

    expectedMap.put(
        "ssrf",
        ApiDefinitionMetadataAnomalyDetectionConfig.newBuilder()
            .setAnomalyRuleId("ssrf")
            .setSsrf(SsrfAnomalyConfig.getDefaultInstance())
            .build());
    expectedMap.put(
        "unknownParam",
        ApiDefinitionMetadataAnomalyDetectionConfig.newBuilder()
            .setAnomalyRuleId("unknownParam")
            .setUnknownParam(UnknownParamAnomalyConfig.getDefaultInstance())
            .build());
    expectedMap.put(
        "httpStatus",
        ApiDefinitionMetadataAnomalyDetectionConfig.newBuilder()
            .setAnomalyRuleId("httpStatus")
            .setHttpStatus(HttpStatusAnomalyConfig.getDefaultInstance())
            .build());
    expectedMap.put(
        "contentSize",
        ApiDefinitionMetadataAnomalyDetectionConfig.newBuilder()
            .setAnomalyRuleId("contentSize")
            .setContentSize(ContentSizeAnomalyConfig.getDefaultInstance())
            .build());
    expectedMap.put(
        "missingParam",
        ApiDefinitionMetadataAnomalyDetectionConfig.newBuilder()
            .setAnomalyRuleId("missingParam")
            .setMissingParam(MissingParamAnomalyConfig.getDefaultInstance())
            .build());
    expectedMap.put(
        "integer",
        ApiDefinitionMetadataAnomalyDetectionConfig.newBuilder()
            .setAnomalyRuleId("integer")
            .setInteger(IntegerAnomalyConfig.getDefaultInstance())
            .build());
    expectedMap.put(
        "type",
        ApiDefinitionMetadataAnomalyDetectionConfig.newBuilder()
            .setAnomalyRuleId("type")
            .setType(TypeAnomalyConfig.getDefaultInstance())
            .build());
    expectedMap.put(
        "device",
        ApiDefinitionMetadataAnomalyDetectionConfig.newBuilder()
            .setAnomalyRuleId("device")
            .setDevice(DeviceAnomalyConfig.getDefaultInstance())
            .build());
    expectedMap.put(
        "contentType",
        ApiDefinitionMetadataAnomalyDetectionConfig.newBuilder()
            .setAnomalyRuleId("contentType")
            .setContentType(ContentTypeAnomalyConfig.getDefaultInstance())
            .build());
    expectedMap.put(
        "enum",
        ApiDefinitionMetadataAnomalyDetectionConfig.newBuilder()
            .setAnomalyRuleId("enum")
            .setEnum(EnumerationsAnomalyConfig.getDefaultInstance())
            .build());
    expectedMap.put(
        "bfla",
        ApiDefinitionMetadataAnomalyDetectionConfig.newBuilder()
            .setAnomalyRuleId("bfla")
            .setBfla(BflaAnomalyConfig.getDefaultInstance())
            .build());
    expectedMap.put(
        "jwt",
        ApiDefinitionMetadataAnomalyDetectionConfig.newBuilder()
            .setAnomalyRuleId("jwt")
            .setJwt(JwtAnomalyConfig.getDefaultInstance())
            .build());
    expectedMap.put(
        "gqla",
        ApiDefinitionMetadataAnomalyDetectionConfig.newBuilder()
            .setAnomalyRuleId("gqla")
            .setGqla(GqlaAnomalyConfig.getDefaultInstance())
            .build());
    expectedMap.put(
        "specialCharacter",
        ApiDefinitionMetadataAnomalyDetectionConfig.newBuilder()
            .setAnomalyRuleId("specialCharacter")
            .setSpecialCharacter(SpecialCharacterAnomalyConfig.getDefaultInstance())
            .build());
    expectedMap.put(
        "contentExplosion",
        ApiDefinitionMetadataAnomalyDetectionConfig.newBuilder()
            .setAnomalyRuleId("contentExplosion")
            .setContentExplosion(ContentExplosionAnomalyConfig.getDefaultInstance())
            .build());

    assertEquals(expectedMap, ruleIdToConfigMap);
  }
}
