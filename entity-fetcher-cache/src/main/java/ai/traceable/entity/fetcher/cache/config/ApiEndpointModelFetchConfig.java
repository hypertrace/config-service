package ai.traceable.entity.fetcher.cache.config;

import com.typesafe.config.Config;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import lombok.Value;

@Value
public class ApiEndpointModelFetchConfig {

  private static final String REQUEST_KEY_MATCH_CLAUSES = "requestKeyMatchClauses";
  private static final String RESPONSE_KEY_MATCH_CLAUSES = "responseKeyMatchClauses";
  private static final String PREFIX = "prefix";
  private static final String SUFFIX = "suffix";
  List<KeyMatchClauseConfig> requestKeyMatchClauses;
  List<KeyMatchClauseConfig> responseKeyMatchClauses;

  public static ApiEndpointModelFetchConfig from(Config config) {
    return new ApiEndpointModelFetchConfig(
        parseKeyMatchClauses(config, REQUEST_KEY_MATCH_CLAUSES),
        parseKeyMatchClauses(config, RESPONSE_KEY_MATCH_CLAUSES));
  }

  private static List<KeyMatchClauseConfig> parseKeyMatchClauses(Config config, String path) {
    if (!config.hasPath(path)) {
      return Collections.emptyList();
    }
    return config.getConfigList(path).stream()
        .map(KeyMatchClauseConfig::from)
        .collect(Collectors.toUnmodifiableList());
  }

  @Value
  public static class KeyMatchClauseConfig {
    String prefix;
    String suffix;

    public static KeyMatchClauseConfig from(Config config) {
      String prefix = config.hasPath(PREFIX) ? config.getString(PREFIX) : null;
      String suffix = config.hasPath(SUFFIX) ? config.getString(SUFFIX) : null;
      return new KeyMatchClauseConfig(prefix, suffix);
    }
  }
}
