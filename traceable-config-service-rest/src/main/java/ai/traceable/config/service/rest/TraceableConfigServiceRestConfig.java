package ai.traceable.config.service.rest;

import com.typesafe.config.Config;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import lombok.Value;

@Value
public class TraceableConfigServiceRestConfig {
  private static final String GRPC_CODEGEN_JAXRS_RESOURCES_CONFIG =
      "grpc.codegen.jaxrs.resources.config";
  private static final String REST_TIMEOUT_CONFIG = "service.rest.timeout.seconds";
  private static final Duration DEFAULT_TIMEOUT = Duration.ofSeconds(5);

  Duration timeout;
  GrpcApiSpecsConfig grpcApiSpecsConfig;

  public static TraceableConfigServiceRestConfig from(Config serviceConfig) {
    GrpcApiSpecsConfig grpcApiSpecsConfig = null;
    if (serviceConfig.hasPath(GRPC_CODEGEN_JAXRS_RESOURCES_CONFIG)) {
      grpcApiSpecsConfig =
          new GrpcApiSpecsConfig(serviceConfig.getConfig(GRPC_CODEGEN_JAXRS_RESOURCES_CONFIG));
    }
    Duration timeout = DEFAULT_TIMEOUT;
    if (serviceConfig.hasPath(REST_TIMEOUT_CONFIG)) {
      timeout = Duration.ofSeconds(serviceConfig.getInt(REST_TIMEOUT_CONFIG));
    }
    return new TraceableConfigServiceRestConfig(timeout, grpcApiSpecsConfig);
  }

  @Value
  public static class GrpcApiSpecsConfig {
    private static final String CONFIG_PATH_GRPC_API_SPECS = "grpc.api.specs";

    List<ChannelAndApiSpecsConfig> channelAndApiSpecsConfigs;

    private GrpcApiSpecsConfig(Config config) {
      if (config.hasPath(CONFIG_PATH_GRPC_API_SPECS)) {
        this.channelAndApiSpecsConfigs =
            config.getConfigList(CONFIG_PATH_GRPC_API_SPECS).stream()
                .map(ChannelAndApiSpecsConfig::new)
                .collect(Collectors.toList());
      } else {
        this.channelAndApiSpecsConfigs = Collections.emptyList();
      }
    }
  }

  @Value
  public static class ChannelAndApiSpecsConfig {
    private static final String CONFIG_PATH_CHANNEL = "channel";
    private static final String CONFIG_PATH_GRPC_CLASSES = "grpcClassNames";
    ChannelConfig channelConfig;
    List<String> grpcClassNames;

    private ChannelAndApiSpecsConfig(Config config) {
      this.channelConfig = new ChannelConfig(config.getConfig(CONFIG_PATH_CHANNEL));
      this.grpcClassNames = config.getStringList(CONFIG_PATH_GRPC_CLASSES);
    }
  }

  @Value
  public static class ChannelConfig {
    private static final String CONFIG_PATH_HOST = "host";
    private static final String CONFIG_PATH_PORT = "port";
    String host;
    int port;

    private ChannelConfig(Config config) {
      this.host = config.getString(CONFIG_PATH_HOST);
      if (config.hasPath(CONFIG_PATH_PORT)) {
        this.port = config.getInt(CONFIG_PATH_PORT);
      } else {
        this.port = -1;
      }
    }
  }
}
