package ai.traceable.external.data.classification.config.service;

import ai.traceable.external.data.classification.config.service.v1.DataParsingRule;
import ai.traceable.external.data.classification.config.service.v1.DataType;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigObject;
import java.util.List;
import java.util.stream.Collectors;
import lombok.Getter;
import lombok.SneakyThrows;
import org.hypertrace.config.proto.converter.ConfigProtoConverter;

class ExternalDataClassificationConfig {
  private static final String EXTERNAL_DATA_CLASSIFICATION_CONFIG_SERVICE =
      "external.data.classification.config.service";
  private static final String DATA_PARSING_RULES =
      EXTERNAL_DATA_CLASSIFICATION_CONFIG_SERVICE + ".data.parsing.rules";
  private static final String DEFAULT_EXTERNAL_DATA_TYPES =
      EXTERNAL_DATA_CLASSIFICATION_CONFIG_SERVICE + ".default.external.data.types";

  @Getter List<DataParsingRule> defaultDataParsingRules;
  @Getter List<DataType> defaultExternalDataTypes;

  ExternalDataClassificationConfig(Config config) {
    this.defaultDataParsingRules =
        config.getObjectList(DATA_PARSING_RULES).stream()
            .map(this::buildDataParsingRuleFromConfig)
            .collect(Collectors.toUnmodifiableList());
    this.defaultExternalDataTypes =
        config.getObjectList(DEFAULT_EXTERNAL_DATA_TYPES).stream()
            .map(this::buildDataTypeFromConfig)
            .collect(Collectors.toUnmodifiableList());
  }

  @SneakyThrows
  private DataParsingRule buildDataParsingRuleFromConfig(ConfigObject configObject) {
    DataParsingRule.Builder builder = DataParsingRule.newBuilder();
    ConfigProtoConverter.mergeFromJsonString(configObject.render(), builder);
    return builder.build();
  }

  @SneakyThrows
  private DataType buildDataTypeFromConfig(ConfigObject configObject) {
    DataType.Builder builder = DataType.newBuilder();
    ConfigProtoConverter.mergeFromJsonString(configObject.render(), builder);
    return builder.build();
  }
}
