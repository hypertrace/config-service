package ai.traceable.anomaly.config.service.apidef.trainer;

import static org.junit.jupiter.api.Assertions.assertEquals;

import ai.traceable.anomaly.config.service.registry.apidef.ApiDefinitionRegistry;
import ai.traceable.anomaly.config.service.registry.apidef.ApiDefinitionRegistryImpl;
import ai.traceable.anomaly.config.service.registry.common.ConfigConverter;
import ai.traceable.anomaly.config.service.v1.apidef.*;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Value;
import java.util.List;
import org.junit.jupiter.api.Test;

class ApiDefinitionTrainerConfigConverterTest {
  private final ApiDefinitionTrainerConfigConverter configConverter =
      new ApiDefinitionTrainerConfigConverter();

  @Test
  void testConvert() throws InvalidProtocolBufferException {
    ApiDefinitionTrainerConfig defaultConfig = getDefaultConfig();

    ApiDefinitionTrainerConfig config;
    ApiDefinitionTrainerConfig resultConfig;
    ApiDefinitionApplierConfig applierConfig;
    Value value;

    config =
        ApiDefinitionTrainerConfig.newBuilder()
            .addApiDefinitionApplierConfigs(
                ApiDefinitionApplierConfig.newBuilder()
                    .setLackOfEncryption(
                        LackOfEncryptionVulnerabilityApplierConfig.newBuilder()
                            .setMinNumberOfCalls(500)
                            .build())
                    .build())
            .build();
    value = configConverter.convert(config);
    assertEquals(config, configConverter.convert(value));

    resultConfig = configConverter.convert(value, defaultConfig, false);
    applierConfig =
        getApplierConfig(
            resultConfig, ApiDefinitionApplierConfig.ApplierConfigCase.LACK_OF_ENCRYPTION);
    assertEquals(500, applierConfig.getLackOfEncryption().getMinNumberOfCalls());
    // Asserting default value exists
    assertEquals(90, applierConfig.getLackOfEncryption().getMinPercentOfHttpsCalls());

    ApiDefinitionApplierConfig applierConfig1 =
        ApiDefinitionApplierConfig.newBuilder()
            .setContentSize(
                ContentSizeMetadataApplierConfig.newBuilder().setRequestRangeSize(200).build())
            .build();

    ApiDefinitionApplierConfig applierConfig2 =
        ApiDefinitionApplierConfig.newBuilder()
            .setQueryParamContainsSensitiveData(
                QueryParamContainsSensitiveDataVulnerabilityApplierConfig.newBuilder()
                    .setMinPercentOfPiiType(20)
                    .build())
            .build();

    value = configConverter.convert(defaultConfig);
    resultConfig =
        configConverter.convert(
            value,
            ApiDefinitionTrainerConfig.newBuilder()
                .addAllApiDefinitionApplierConfigs(List.of(applierConfig1, applierConfig2))
                .build(),
            true);

    applierConfig =
        getApplierConfig(resultConfig, ApiDefinitionApplierConfig.ApplierConfigCase.CONTENT_SIZE);
    assertEquals(200, applierConfig.getContentSize().getRequestRangeSize());
    // Asserting default value exists
    assertEquals(1000, applierConfig.getContentSize().getResponseRangeSize());

    applierConfig =
        getApplierConfig(
            resultConfig,
            ApiDefinitionApplierConfig.ApplierConfigCase.QUERY_PARAM_CONTAINS_SENSITIVE_DATA);
    assertEquals(20, applierConfig.getQueryParamContainsSensitiveData().getMinPercentOfPiiType());
    // Asserting default value exists
    assertEquals(
        2, applierConfig.getQueryParamContainsSensitiveData().getMinTotalPiiTypeOccurrences());
  }

  private ApiDefinitionTrainerConfig getDefaultConfig() {
    ConfigConverter configConverter = new ConfigConverter();
    ApiDefinitionRegistry apiDefinitionRegistry = new ApiDefinitionRegistryImpl(configConverter);
    return apiDefinitionRegistry.getApiDefinitionTrainerConfig();
  }

  private ApiDefinitionApplierConfig getApplierConfig(
      ApiDefinitionTrainerConfig config,
      ApiDefinitionApplierConfig.ApplierConfigCase applierConfigCase) {
    for (ApiDefinitionApplierConfig applierConfig : config.getApiDefinitionApplierConfigsList()) {
      if (applierConfig.getApplierConfigCase() == applierConfigCase) {
        return applierConfig;
      }
    }
    return null;
  }
}
