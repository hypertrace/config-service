package ai.traceable.risk.config.service.processor;

import ai.traceable.risk.config.service.v1.RiskScoreCategory;
import com.google.protobuf.Message;
import com.google.protobuf.util.JsonFormat;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigRenderOptions;
import io.grpc.Status;
import lombok.SneakyThrows;

public abstract class RiskConfigUtils<M extends Message> {

  private static final JsonFormat.Parser JSON_PARSER = JsonFormat.parser().ignoringUnknownFields();
  private static final ConfigRenderOptions CONFIG_RENDER_CONCISE = ConfigRenderOptions.concise();

  @SneakyThrows
  public M mergeConfigs(Config config, String filePath) {
    return mergeConfigs(
        (M) mergeFromConfig(config, getNewBuilder()).build(),
        (M) mergeFromConfigFile(filePath, getNewBuilder()).build());
  }

  public abstract M.Builder getNewBuilder();

  public abstract M mergeConfigs(M highPriorityConfig, M lowPriorityConfig);

  public abstract boolean isConfigDefault(M specificConfig, M defaultConfig);

  public abstract Status validateConfig(M config);

  protected boolean isValidScore(int score) {
    return score >= 0 && score <= 10;
  }

  protected boolean isValidScoreCategory(RiskScoreCategory scoreCategory) {
    return scoreCategory != RiskScoreCategory.RISK_SCORE_CATEGORY_UNSPECIFIED;
  }

  @SneakyThrows
  private M.Builder mergeFromConfig(Config config, M.Builder builder) {
    JSON_PARSER.merge(config.root().render(CONFIG_RENDER_CONCISE), builder);
    return builder;
  }

  @SneakyThrows
  private M.Builder mergeFromConfigFile(String filePath, M.Builder builder) {
    JSON_PARSER.merge(loadConfigFile(filePath).root().render(CONFIG_RENDER_CONCISE), builder);
    return builder;
  }

  private Config loadConfigFile(String filePath) {
    try {
      return ConfigFactory.parseResources(filePath);
    } catch (Exception e) {
      throw new RuntimeException(String.format("Unable to read config file: %s", filePath), e);
    }
  }
}
