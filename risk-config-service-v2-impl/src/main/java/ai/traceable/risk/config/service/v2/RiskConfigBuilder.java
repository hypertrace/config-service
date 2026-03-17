package ai.traceable.risk.config.service.v2;

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import com.google.protobuf.util.JsonFormat;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigRenderOptions;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public abstract class RiskConfigBuilder<M extends Message> {

  private static final JsonFormat.Parser JSON_PARSER = JsonFormat.parser().ignoringUnknownFields();
  private static final ConfigRenderOptions CONFIG_RENDER_CONCISE = ConfigRenderOptions.concise();

  public M mergeConfigs(Config config, String filePath) {
    return mergeConfigs(buildFromConfig(config), buildFromConfigFile(filePath));
  }

  public abstract M.Builder getNewBuilder();

  public abstract M mergeConfigs(M highPriorityConfig, M lowPriorityConfig);

  @SuppressWarnings("unchecked")
  private M buildFromConfig(Config config) {
    M.Builder builder = getNewBuilder();
    try {
      JSON_PARSER.merge(config.root().render(CONFIG_RENDER_CONCISE), builder);
    } catch (InvalidProtocolBufferException e) {
      log.error("Build config failed for the provided config: {}", config);
    }
    return (M) builder.build();
  }

  @SuppressWarnings("unchecked")
  public M buildFromConfigFile(String filePath) {
    M.Builder builder = getNewBuilder();
    try {
      JSON_PARSER.merge(loadConfigFile(filePath).root().render(CONFIG_RENDER_CONCISE), builder);
    } catch (InvalidProtocolBufferException e) {
      log.error("Build config failed for the provided config file path: {}", filePath);
    }
    return (M) builder.build();
  }

  private Config loadConfigFile(String filePath) {
    try {
      return ConfigFactory.parseResources(filePath);
    } catch (Exception e) {
      throw new RuntimeException(String.format("Unable to read config file: %s", filePath), e);
    }
  }
}
