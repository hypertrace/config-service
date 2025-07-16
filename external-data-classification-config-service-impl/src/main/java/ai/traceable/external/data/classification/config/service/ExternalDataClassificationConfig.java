package ai.traceable.external.data.classification.config.service;

import ai.traceable.external.data.classification.config.service.v1.DataParsingRule;
import ai.traceable.external.data.classification.config.service.v1.DataType;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigObject;
import java.time.Duration;
import java.util.Collections;
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
  private static final String SSE_DATA_PARSING_RULES =
      EXTERNAL_DATA_CLASSIFICATION_CONFIG_SERVICE + "sse.data.parsing.rules";
  private static final String DEFAULT_EXTERNAL_DATA_TYPES =
      EXTERNAL_DATA_CLASSIFICATION_CONFIG_SERVICE + ".default.external.data.types";
  private static final String CACHE_THREAD_POOL_SIZE =
      EXTERNAL_DATA_CLASSIFICATION_CONFIG_SERVICE + ".cache.thread.pool.size";
  private static final String CACHE_REFRESH =
      EXTERNAL_DATA_CLASSIFICATION_CONFIG_SERVICE + ".cache.refresh";
  private static final String CACHE_MAX_SIZE =
      EXTERNAL_DATA_CLASSIFICATION_CONFIG_SERVICE + ".cache.maxSize";

  @Getter List<DataParsingRule> defaultDataParsingRules;
  @Getter List<DataParsingRule> defaultSseDataParsingRules;
  @Getter List<DataType> defaultExternalDataTypes;
  @Getter int cacheThreadPoolSize;
  @Getter Duration cacheRefreshDuration;
  @Getter int cacheMaxSize;

  ExternalDataClassificationConfig(Config config) {
    this.defaultDataParsingRules =
        config.hasPath(DATA_PARSING_RULES)
            ? config.getObjectList(DATA_PARSING_RULES).stream()
                .map(this::buildDataParsingRuleFromConfig)
                .collect(Collectors.toUnmodifiableList())
            : Collections.emptyList();
    this.defaultSseDataParsingRules =
        config.hasPath(SSE_DATA_PARSING_RULES)
            ? config.getObjectList(SSE_DATA_PARSING_RULES).stream()
                .map(this::buildDataParsingRuleFromConfig)
                .collect(Collectors.toUnmodifiableList())
            : Collections.emptyList();
    this.defaultExternalDataTypes =
        config.hasPath(DEFAULT_EXTERNAL_DATA_TYPES)
            ? config.getObjectList(DEFAULT_EXTERNAL_DATA_TYPES).stream()
                .map(this::buildDataTypeFromConfig)
                .collect(Collectors.toUnmodifiableList())
            : Collections.emptyList();
    this.cacheThreadPoolSize = config.getInt(CACHE_THREAD_POOL_SIZE);
    this.cacheRefreshDuration = config.getDuration(CACHE_REFRESH);
    this.cacheMaxSize = config.getInt(CACHE_MAX_SIZE);
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

  Duration getCacheExpirationDuration() {
    // By default, allow one refresh duration grace before evicting.
    return this.getCacheRefreshDuration().multipliedBy(2);
  }
}
