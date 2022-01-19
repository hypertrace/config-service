package ai.traceable.localprocessing.config.service.config;

import com.google.inject.Inject;
import com.typesafe.config.Config;
import java.util.List;

public class ApiNamingConfig {
  private final Config config;

  @Inject
  public ApiNamingConfig(Config config) {
    this.config = config.getConfig("api.naming.config");
  }

  public List<String> getFallbackRegexes() {
    return this.config.getStringList("regex.fallbacks");
  }
}
