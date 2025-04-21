package ai.traceable.genai.config.service.v1.genai.config;

import ai.traceable.genai.config.service.v1.GenAiConfig;
import ai.traceable.genai.config.service.v1.GenAiConfigServiceConfig;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigRenderOptions;
import jakarta.inject.Inject;
import jakarta.inject.Provider;
import jakarta.inject.Singleton;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Singleton
@AllArgsConstructor(onConstructor_ = {@Inject})
public class DefaultGenAiConfigProvider implements Provider<GenAiConfig> {

  private static final JsonFormat.Parser JSON_PARSER = JsonFormat.parser().ignoringUnknownFields();
  private static final ConfigRenderOptions CONFIG_RENDER_CONCISE = ConfigRenderOptions.concise();

  private final GenAiConfigServiceConfig configServiceConfig;

  @Override
  public GenAiConfig get() {
    Config config = configServiceConfig.getGenAiConfig();
    GenAiConfig.Builder builder = GenAiConfig.newBuilder();
    try {
      JSON_PARSER.merge(config.root().render(CONFIG_RENDER_CONCISE), builder);
    } catch (InvalidProtocolBufferException e) {
      log.error("Build config failed for the provided config: {}", config, e);
    }
    return builder.build();
  }
}
