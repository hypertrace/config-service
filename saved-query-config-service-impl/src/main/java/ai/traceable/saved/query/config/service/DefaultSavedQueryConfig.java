package ai.traceable.saved.query.config.service;

import static java.util.function.Function.identity;

import ai.traceable.saved.query.config.service.v1.SavedQuery;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import com.google.protobuf.util.JsonFormat;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigObject;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import javax.inject.Inject;
import lombok.SneakyThrows;

public class DefaultSavedQueryConfig {

  private static final String SAVED_QUERY_CONFIG_SERVICE = "saved.query.config.service";
  private static final String DEFAULT_SAVED_QUERIES = "default.saved.queries";
  private final Map<String, SavedQuery> defaultSavedQueriesMap;

  @Inject
  public DefaultSavedQueryConfig(Config config) {
    Config savedQueryConfig = config.getConfig(SAVED_QUERY_CONFIG_SERVICE);
    List<? extends ConfigObject> defaultSavedQueriesObjs =
        savedQueryConfig.getObjectList(DEFAULT_SAVED_QUERIES);
    this.defaultSavedQueriesMap =
        defaultSavedQueriesObjs.stream()
            .map(DefaultSavedQueryConfig::buildSavedQueryFromConfig)
            .collect(ImmutableMap.toImmutableMap(SavedQuery::getId, identity()));
  }

  public Collection<SavedQuery> getQueriesForScope(String scope) {
    return defaultSavedQueriesMap.values().stream()
        .filter(query -> Strings.isNullOrEmpty(scope) || scope.equals(query.getScope()))
        .collect(Collectors.toUnmodifiableList());
  }

  public boolean isDefaultQuery(String id) {
    return defaultSavedQueriesMap.containsKey(id);
  }

  public Optional<SavedQuery> getDefaultQuery(String id) {
    return Optional.ofNullable(defaultSavedQueriesMap.get(id));
  }

  @SneakyThrows
  private static SavedQuery buildSavedQueryFromConfig(
      com.typesafe.config.ConfigObject configObject) {
    String jsonString = configObject.render();
    SavedQuery.Builder builder = SavedQuery.newBuilder();
    JsonFormat.parser().merge(jsonString, builder);
    return builder.build();
  }
}
