package ai.traceable.anomaly.config.service.apidef.trainer;

import ai.traceable.anomaly.config.service.v1.apidef.ApiDefinitionApplierConfig;
import ai.traceable.anomaly.config.service.v1.apidef.ApiDefinitionTrainerConfig;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Value;
import java.util.EnumMap;
import org.hypertrace.config.proto.converter.ConfigProtoConverter;

public class ApiDefinitionTrainerConfigConverter {
  public ApiDefinitionTrainerConfig convert(Value config) throws InvalidProtocolBufferException {
    ApiDefinitionTrainerConfig.Builder builder = ApiDefinitionTrainerConfig.newBuilder();
    if (config != null && config.getKindCase() != Value.KindCase.NULL_VALUE) {
      ConfigProtoConverter.mergeFromValue(config, builder);
    }
    return builder.build();
  }

  /**
   * @param value
   * @param apiDefinitionTrainerConfig
   * @param isPassedConfigPreferred set to true when applier configuration from {@param
   *     apiDefinitionTrainerConfig} is preferred and false otherwise
   * @return ApiDefinitionTrainerConfig
   * @throws InvalidProtocolBufferException
   */
  public ApiDefinitionTrainerConfig convert(
      Value value,
      ApiDefinitionTrainerConfig apiDefinitionTrainerConfig,
      boolean isPassedConfigPreferred)
      throws InvalidProtocolBufferException {
    ApiDefinitionTrainerConfig.Builder builder = ApiDefinitionTrainerConfig.newBuilder();
    if (value != null && value.getKindCase() != Value.KindCase.NULL_VALUE) {
      ConfigProtoConverter.mergeFromValue(value, builder);
    }

    return mergeConfig(apiDefinitionTrainerConfig, builder.build(), isPassedConfigPreferred);
  }

  public Value convert(ApiDefinitionTrainerConfig config) throws InvalidProtocolBufferException {
    return ConfigProtoConverter.convertToValue(config);
  }

  private ApiDefinitionTrainerConfig mergeConfig(
      ApiDefinitionTrainerConfig passedTrainerConfig,
      ApiDefinitionTrainerConfig defaultTrainerConfig,
      boolean isPassedConfigPreferred) {
    EnumMap<ApiDefinitionApplierConfig.ApplierConfigCase, ApiDefinitionApplierConfig>
        applierConfigMap = new EnumMap<>(ApiDefinitionApplierConfig.ApplierConfigCase.class);

    passedTrainerConfig
        .getApiDefinitionApplierConfigsList()
        .forEach(
            apiDefinitionApplierConfig ->
                applierConfigMap.put(
                    apiDefinitionApplierConfig.getApplierConfigCase(), apiDefinitionApplierConfig));

    defaultTrainerConfig
        .getApiDefinitionApplierConfigsList()
        .forEach(
            apiDefinitionApplierConfig -> {
              ApiDefinitionApplierConfig.ApplierConfigCase applierConfigCase =
                  apiDefinitionApplierConfig.getApplierConfigCase();
              if (applierConfigMap.containsKey(applierConfigCase)) {
                if (isPassedConfigPreferred) {
                  applierConfigMap.put(
                      applierConfigCase,
                      apiDefinitionApplierConfig.toBuilder()
                          .mergeFrom(applierConfigMap.get(applierConfigCase))
                          .build());
                } else {
                  applierConfigMap.put(
                      applierConfigCase,
                      applierConfigMap.get(applierConfigCase).toBuilder()
                          .mergeFrom(apiDefinitionApplierConfig)
                          .build());
                }
              } else {
                applierConfigMap.put(applierConfigCase, apiDefinitionApplierConfig);
              }
            });

    return ApiDefinitionTrainerConfig.newBuilder()
        .addAllApiDefinitionApplierConfigs(applierConfigMap.values())
        .build();
  }
}
