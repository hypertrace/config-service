package ai.traceable.genai.config.service.v1;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import lombok.Getter;

@Getter
public class GenAiConfigServiceConfig {

  private static final String GEN_AI_CONFIG_PATH = "genAiConfig";

  private final Config genAiConfig;

  public GenAiConfigServiceConfig(Config config) {
    this.genAiConfig = readGenAiConfig(config);
  }

  private Config readGenAiConfig(Config config) {
    return config.hasPath(GEN_AI_CONFIG_PATH)
        ? config.getConfig(GEN_AI_CONFIG_PATH)
        : ConfigFactory.empty();
  }
}
