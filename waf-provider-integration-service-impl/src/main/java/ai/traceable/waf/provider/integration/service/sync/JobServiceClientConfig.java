package ai.traceable.waf.provider.integration.service.sync;

import com.typesafe.config.Config;
import java.time.Duration;
import lombok.Value;

@Value
public class JobServiceClientConfig {
  private static final String CONFIG_PATH = "job.service.config";
  private static final String HOST_KEY = "host";
  private static final String PORT_KEY = "port";
  private static final String TIMEOUT_KEY = "request.timeout.duration";

  String host;
  int port;
  Duration requestTimeout;

  public static JobServiceClientConfig from(final Config rootConfig) {
    final Config config = rootConfig.getConfig(CONFIG_PATH);
    return new JobServiceClientConfig(
        config.getString(HOST_KEY), config.getInt(PORT_KEY), config.getDuration(TIMEOUT_KEY));
  }
}
