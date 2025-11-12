package ai.traceable.attribute.resolution.config.service.v1.config;

import ai.traceable.attribute.resolution.config.service.v1.AttributeResolutionConfig;
import ai.traceable.attribute.resolution.config.service.v1.validation.AttributeResolutionConfigValidator;
import com.google.inject.Inject;
import com.google.protobuf.Message;
import com.google.protobuf.util.JsonFormat;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigRenderOptions;
import io.grpc.Status;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.stream.Collectors;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class AttributeResolutionDefaultConfig {

  private static final String DEFAULT_ATTRIBUTE_RESOLUTION_CONFIG_FILE_PATH =
      "default-attribute-resolution-configs.conf";
  private static final String ATTRIBUTE_RESOLUTION_CONFIGS_PATH = "attributeResolutionConfigs";
  private static final JsonFormat.Parser JSON_PARSER = JsonFormat.parser().ignoringUnknownFields();
  private static final ConfigRenderOptions CONFIG_RENDER_CONCISE = ConfigRenderOptions.concise();

  private final AttributeResolutionConfigValidator configValidator;
  @Getter private final Map<String, AttributeResolutionConfig> defaultAttributeResolutionConfigMap;

  @Inject
  public AttributeResolutionDefaultConfig(AttributeResolutionConfigValidator configValidator) {
    this.configValidator = configValidator;
    defaultAttributeResolutionConfigMap = buildDefaultAttributeResolutionConfigMap();
  }

  private Map<String, AttributeResolutionConfig> buildDefaultAttributeResolutionConfigMap() {
    try {
      Map<String, AttributeResolutionConfig> map =
          ConfigFactory.parseResources(DEFAULT_ATTRIBUTE_RESOLUTION_CONFIG_FILE_PATH)
              .getConfigList(ATTRIBUTE_RESOLUTION_CONFIGS_PATH)
              .stream()
              .map(this::convertToConfig)
              .collect(
                  Collectors.toMap(
                      AttributeResolutionConfig::getId,
                      entry -> entry,
                      (existing, replacement) -> existing,
                      LinkedHashMap::new));
      log.info("Loaded {} default attribute resolution configs", map.size());
      return Collections.unmodifiableMap(map);
    } catch (Exception e) {
      log.warn("Failed to load default attribute resolution configs: {}", e.getMessage());
      return Collections.emptyMap();
    }
  }

  private AttributeResolutionConfig convertToConfig(Config config) {
    AttributeResolutionConfig.Builder builder = AttributeResolutionConfig.newBuilder();
    mergeFromConfig(config, builder);
    AttributeResolutionConfig attributeResolutionConfig = builder.build();
    try {
      configValidator.validateAttributeResolutionConfigData(attributeResolutionConfig.getData());
      return attributeResolutionConfig;
    } catch (Exception e) {
      throw Status.INVALID_ARGUMENT
          .withDescription(
              String.format(
                  "Invalid attribute resolution config data : %s", attributeResolutionConfig))
          .asRuntimeException();
    }
  }

  private void mergeFromConfig(Config config, Message.Builder builder) {
    try {
      JSON_PARSER.merge(config.root().render(CONFIG_RENDER_CONCISE), builder);
    } catch (Exception e) {
      throw Status.INVALID_ARGUMENT
          .withDescription(String.format("Failed to parse configuration for config: %s", config))
          .asRuntimeException();
    }
  }
}
