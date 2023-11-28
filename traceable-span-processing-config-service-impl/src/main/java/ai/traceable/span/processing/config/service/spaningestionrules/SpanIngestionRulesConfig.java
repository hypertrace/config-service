package ai.traceable.span.processing.config.service.spaningestionrules;

import ai.traceable.span.processing.config.service.v1.KeyValueRetentionRule;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigObject;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import lombok.SneakyThrows;
import org.hypertrace.config.proto.converter.ConfigProtoConverter;

class SpanIngestionRulesConfig {

  private static final String SPAN_PROCESSING_CONFIG_SERVICE = "span.processing.config.service";
  private static final String DEFAULT_REQUEST_HEADER_RETENTION_RULES =
      SPAN_PROCESSING_CONFIG_SERVICE + ".default.request.header.retention.rules";
  private static final String DEFAULT_RESPONSE_HEADER_RETENTION_RULES =
      SPAN_PROCESSING_CONFIG_SERVICE + ".default.response.header.retention.rules";
  private static final String DEFAULT_ATTRIBUTE_RETENTION_RULES =
      SPAN_PROCESSING_CONFIG_SERVICE + ".default.attribute.retention.rules";

  private final List<KeyValueRetentionRule> defaultRequestHeaderRetentionRules;
  private final List<KeyValueRetentionRule> defaultResponseHeaderRetentionRules;
  private final List<KeyValueRetentionRule> defaultAttributeRetentionRules;

  SpanIngestionRulesConfig(Config config) {
    if (config.hasPath(DEFAULT_REQUEST_HEADER_RETENTION_RULES)) {
      this.defaultRequestHeaderRetentionRules =
          config.getObjectList(DEFAULT_REQUEST_HEADER_RETENTION_RULES).stream()
              .map(this::buildRetentionRuleFromConfig)
              .collect(Collectors.toUnmodifiableList());
    } else {
      this.defaultRequestHeaderRetentionRules = Collections.emptyList();
    }

    if (config.hasPath(DEFAULT_RESPONSE_HEADER_RETENTION_RULES)) {
      this.defaultResponseHeaderRetentionRules =
          config.getObjectList(DEFAULT_RESPONSE_HEADER_RETENTION_RULES).stream()
              .map(this::buildRetentionRuleFromConfig)
              .collect(Collectors.toUnmodifiableList());
    } else {
      this.defaultResponseHeaderRetentionRules = Collections.emptyList();
    }

    if (config.hasPath(DEFAULT_ATTRIBUTE_RETENTION_RULES)) {
      this.defaultAttributeRetentionRules =
          config.getObjectList(DEFAULT_ATTRIBUTE_RETENTION_RULES).stream()
              .map(this::buildRetentionRuleFromConfig)
              .collect(Collectors.toUnmodifiableList());
    } else {
      this.defaultAttributeRetentionRules = Collections.emptyList();
    }
  }

  List<KeyValueRetentionRule> getDefaultRequestHeaderRetentionRules() {
    return defaultRequestHeaderRetentionRules;
  }

  List<KeyValueRetentionRule> getDefaultResponseHeaderRetentionRules() {
    return defaultResponseHeaderRetentionRules;
  }

  List<KeyValueRetentionRule> getDefaultAttributeRetentionRules() {
    return defaultAttributeRetentionRules;
  }

  @SneakyThrows
  private KeyValueRetentionRule buildRetentionRuleFromConfig(ConfigObject object) {
    KeyValueRetentionRule.Builder builder = KeyValueRetentionRule.newBuilder();
    ConfigProtoConverter.mergeFromJsonString(object.render(), builder);
    return builder.build();
  }
}
