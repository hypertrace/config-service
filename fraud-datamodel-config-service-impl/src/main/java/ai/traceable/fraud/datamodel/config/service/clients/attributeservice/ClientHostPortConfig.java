package ai.traceable.fraud.datamodel.config.service.clients.attributeservice;

import com.typesafe.config.Config;
import java.time.Duration;
import lombok.Value;

@Value
public class ClientHostPortConfig {
  private static final String CONFIG_PATH_ATTRIBUTE_SERVICE_CLIENT = "attribute.service.config";
  private static final String CONFIG_PATH_HOST = "host";
  private static final String CONFIG_PATH_PORT = "port";
  private static final String CONFIG_PATH_TIMEOUT = "request.timeout";
  String host;
  int port;
  Duration timeout;

  public ClientHostPortConfig(Config config) {
    Config attributeSvcConfig = config.getConfig(CONFIG_PATH_ATTRIBUTE_SERVICE_CLIENT);
    this.host = attributeSvcConfig.getString(CONFIG_PATH_HOST);
    this.port = attributeSvcConfig.getInt(CONFIG_PATH_PORT);
    this.timeout = attributeSvcConfig.getDuration(CONFIG_PATH_TIMEOUT);
  }
}
