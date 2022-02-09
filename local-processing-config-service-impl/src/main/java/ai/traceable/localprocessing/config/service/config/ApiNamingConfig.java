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

  public int getDefaultEmbryonicThreshold() {
    return this.config.getInt("default.embryonic.threshold");
  }

  public long getDiffLogsRetentionPeriod() {
    return this.config.getDuration("trieDiffLog.diff.logs.retention.period").toMillis();
  }

  public String getBaseDirectory() {
    return this.config.getString("trieDiffLog.model.store.directory");
  }
}
