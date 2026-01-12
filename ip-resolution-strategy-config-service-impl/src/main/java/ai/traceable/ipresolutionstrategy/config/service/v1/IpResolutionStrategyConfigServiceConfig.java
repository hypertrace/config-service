package ai.traceable.ipresolutionstrategy.config.service.v1;

import com.google.protobuf.Message;
import com.google.protobuf.util.JsonFormat;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigRenderOptions;
import java.util.List;
import java.util.stream.Collectors;
import lombok.Getter;
import lombok.SneakyThrows;

public class IpResolutionStrategyConfigServiceConfig {
  private static final String DEFAULT_IP_RESOLUTION_STRATEGY_CONFIGS_FILE_PATH =
      "default-ip-resolution-strategy-configs.conf";
  private static final JsonFormat.Parser JSON_PARSER = JsonFormat.parser().ignoringUnknownFields();
  private static final ConfigRenderOptions CONFIG_RENDER_CONCISE = ConfigRenderOptions.concise();
  private static final String DEFAULT_CONFIGS_PATH = "ipResolutionStrategyConfigs";

  private static final String CONFIG_KEY = "ip.resolution.strategy.config.service";
  private static final String DEFAULT_CONFIGS_OVERRIDE_PATH = "defaultIpResolutionStrategyConfigs";

  private final Config config;

  @Getter private final List<IpResolutionStrategyConfig> defaultIpResolutionStrategyConfigs;

  public IpResolutionStrategyConfigServiceConfig(Config config) {
    this.config = config.hasPath(CONFIG_KEY) ? config.getConfig(CONFIG_KEY) : ConfigFactory.empty();
    this.defaultIpResolutionStrategyConfigs = loadDefaultIpResolutionStrategyConfigs();
  }

  private List<IpResolutionStrategyConfig> loadDefaultIpResolutionStrategyConfigs() {
    List<IpResolutionStrategyConfig> defaults =
        convertToIpResolutionStrategyConfigs(
            ConfigFactory.parseResources(DEFAULT_IP_RESOLUTION_STRATEGY_CONFIGS_FILE_PATH)
                .getConfigList(DEFAULT_CONFIGS_PATH));

    if (config.hasPath(DEFAULT_CONFIGS_OVERRIDE_PATH)) {
      defaults =
          java.util.stream.Stream.concat(
                  defaults.stream(),
                  convertToIpResolutionStrategyConfigs(
                      config.getConfigList(DEFAULT_CONFIGS_OVERRIDE_PATH))
                      .stream())
              .collect(Collectors.toUnmodifiableList());
    }

    return defaults;
  }

  private List<IpResolutionStrategyConfig> convertToIpResolutionStrategyConfigs(
      List<? extends Config> configList) {
    return configList.stream()
        .map(
            config -> {
              IpResolutionStrategyConfig.Builder builder = IpResolutionStrategyConfig.newBuilder();
              mergeFromConfig(config, builder);
              return builder.build();
            })
        .collect(Collectors.toUnmodifiableList());
  }

  @SneakyThrows
  private void mergeFromConfig(Config config, Message.Builder builder) {
    JSON_PARSER.merge(config.root().render(CONFIG_RENDER_CONCISE), builder);
  }
}
