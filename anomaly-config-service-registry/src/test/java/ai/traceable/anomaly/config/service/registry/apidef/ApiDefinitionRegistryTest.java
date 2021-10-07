package ai.traceable.anomaly.config.service.registry.apidef;

import static org.junit.jupiter.api.Assertions.assertEquals;

import ai.traceable.anomaly.config.service.registry.common.ConfigConverter;
import ai.traceable.anomaly.config.service.v1.AnomalyRuleInfo;
import ai.traceable.anomaly.config.service.v1.apidef.ApiDefinitionTrainerConfig;
import java.util.Map;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;

public class ApiDefinitionRegistryTest {

  @Test
  public void testRules() {
    ApiDefinitionRegistryImpl apiDefRulesRegistry =
        new ApiDefinitionRegistryImpl(new ConfigConverter());

    Map<String, AnomalyRuleInfo> anomalyRuleInfos = apiDefRulesRegistry.getApiDefRuleInfos();
    assertEquals(10, anomalyRuleInfos.size());
    assertEquals(
        "contentSize :: Content Size Anomaly\n"
            + "contentType :: Content Type Anomaly\n"
            + "device :: Unexpected User Agent\n"
            + "enum :: Invalid Enumerations\n"
            + "httpStatus :: Unexpected HTTP Response Code\n"
            + "integer :: Value Out of Range\n"
            + "ssrf :: Server-Side Request Forgery (SSRF)\n"
            + "type :: Type Anomaly\n"
            + "unknownParam :: Unrecognized Field\n"
            + "xxe :: XML External Entity Injection",
        anomalyRuleInfos.values().stream()
            .map(
                anomalyRuleInfo ->
                    anomalyRuleInfo.getRuleId() + " :: " + anomalyRuleInfo.getRuleName())
            .sorted()
            .collect(Collectors.joining("\n")));
  }

  @Test
  void testTrainerConfig() {
    ApiDefinitionRegistry apiDefinitionRegistry =
        new ApiDefinitionRegistryImpl(new ConfigConverter());
    ApiDefinitionTrainerConfig apiDefinitionTrainerConfig =
        apiDefinitionRegistry.getApiDefinitionTrainerConfig();
    assertEquals(28, apiDefinitionTrainerConfig.getApiDefinitionApplierConfigsCount());
    assertEquals(
        "API_ACCESSORS\n"
            + "API_PARAM_CONTAINS_URL\n"
            + "CONTENT_SIZE\n"
            + "CONTENT_TYPE\n"
            + "CONTENT_TYPE_OPTIONS\n"
            + "COUNT\n"
            + "DEVICE\n"
            + "DIGIT_LENGTH\n"
            + "ENUM\n"
            + "HSTS_SECURITY_HEADER\n"
            + "HTTP_STATUS\n"
            + "IS_EXTERNAL_API\n"
            + "JAVA_SERIALIZED_OBJECT\n"
            + "LACK_OF_ENCRYPTION\n"
            + "PARAM_ACCESSORS\n"
            + "PARAM_TYPE_ACCESSORS\n"
            + "PERSISTENT_COOKIE_CONTAINS_SENSITIVE_DATA\n"
            + "PII\n"
            + "PII_SENSITIVE_DATA\n"
            + "QUERY_PARAM_CONTAINS_SENSITIVE_DATA\n"
            + "SERVICE_USES_BASIC_AUTH\n"
            + "SPECIAL_CHARS\n"
            + "SSRF_DOMAIN\n"
            + "SSRF_PROTOCOL\n"
            + "THRESHOLDS_FAMILY\n"
            + "TYPE\n"
            + "USER_AGENT\n"
            + "USER_ROLE",
        apiDefinitionTrainerConfig.getApiDefinitionApplierConfigsList().stream()
            .map(
                apiDefinitionApplierConfig ->
                    apiDefinitionApplierConfig.getApplierConfigCase().toString())
            .sorted()
            .collect(Collectors.joining("\n")));
  }
}
