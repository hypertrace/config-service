package ai.traceable.localprocessing.config.service.config.http;

import com.google.inject.Inject;
import com.typesafe.config.Config;
import java.util.List;

public class HttpApiNamingConfig {
  private static final String API_NAMING_CONFIG = "api.naming.config";
  private static final String LOCAL_API_NAMING_CONFIG = "local.api.naming.config";
  private static final String FALLBACK_REGEX = "regex.fallbacks";
  private static final String DEFAULT_EMBRYONIC_THRESHOLD = "default.embryonic.threshold";
  private static final String DEFAULT_MAX_NUMBER_OF_TRIE_PATHS = "default.max.number.of.trie.paths";
  private static final String TRIE_DIFFLOG_BASEDIR = "trieDiffLog.model.store.directory";
  private static final String TRIE_DIFFLOG_RETENTION_PERIOD =
      "trieDiffLog.diff.logs.retention.period";
  private static final String FULL_TRIE_RELOAD_CONFIG = "full.trie.reload.config";
  private final Config config;
  private final Config localApiNamingConfig;

  @Inject
  public HttpApiNamingConfig(Config config) {
    this.config = config.getConfig(API_NAMING_CONFIG);
    this.localApiNamingConfig = config.getConfig(LOCAL_API_NAMING_CONFIG);
  }

  public List<String> getFallbackRegexes() {
    return this.config.getStringList(FALLBACK_REGEX);
  }

  public int getDefaultEmbryonicThreshold() {
    return this.config.getInt(DEFAULT_EMBRYONIC_THRESHOLD);
  }

  public int getDefaultMaxNumberOfTriePaths() {
    return this.config.getInt(DEFAULT_MAX_NUMBER_OF_TRIE_PATHS);
  }

  public long getDiffLogsRetentionPeriod() {
    return this.config.getDuration(TRIE_DIFFLOG_RETENTION_PERIOD).toMillis();
  }

  public String getBaseDirectory() {
    return this.config.getString(TRIE_DIFFLOG_BASEDIR);
  }

  public Config getFullTrieReloadConfig() {
    return this.localApiNamingConfig.getConfig(FULL_TRIE_RELOAD_CONFIG);
  }
}
