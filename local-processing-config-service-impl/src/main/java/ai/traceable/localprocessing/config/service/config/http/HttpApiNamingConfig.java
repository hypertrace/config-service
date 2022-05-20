package ai.traceable.localprocessing.config.service.config.http;

import com.google.inject.Inject;
import com.typesafe.config.Config;
import java.util.List;

public class HttpApiNamingConfig {
  private final Config config;
  private final Config localApiNamingConfig;

  @Inject
  public HttpApiNamingConfig(Config config) {
    this.config = config.getConfig("api.naming.config");
    this.localApiNamingConfig = config.getConfig("local.api.naming.config");
  }

  public List<String> getFallbackRegexes() {
    return this.config.getStringList("regex.fallbacks");
  }

  public int getDefaultEmbryonicThreshold() {
    return this.config.getInt("default.embryonic.threshold");
  }

  public int getDefaultMaxNumberOfTriePaths() {
    return this.config.getInt("default.max.number.of.trie.paths");
  }

  public long getDiffLogsRetentionPeriod() {
    return this.config.getDuration("trieDiffLog.diff.logs.retention.period").toMillis();
  }

  public String getBaseDirectory() {
    return this.config.getString("trieDiffLog.model.store.directory");
  }

  public Config getFullTrieReloadConfig() {
    return this.localApiNamingConfig.getConfig("full.trie.reload.config");
  }
}
