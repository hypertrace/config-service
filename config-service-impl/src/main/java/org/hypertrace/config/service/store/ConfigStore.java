package org.hypertrace.config.service.store;

import com.google.protobuf.Value;
import com.typesafe.config.Config;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.hypertrace.config.service.ConfigResource;
import org.hypertrace.config.service.ConfigResourceContext;
import org.hypertrace.config.service.v1.ContextSpecificConfig;
import org.hypertrace.config.service.v1.UpsertAllConfigsResponse.UpsertedConfig;

/**
 * Abstraction for the backend which stores and serves the configuration data for multiple
 * resources.
 */
public interface ConfigStore {

  /**
   * Initialize the config store
   *
   * @param config
   */
  void init(Config config);

  /**
   * Write the config value associated with the specified config resource to the store.
   *
   * @param configResourceContext
   * @param userId
   * @param config
   * @return the config written to the store
   */
  UpsertedConfig writeConfig(
      ConfigResourceContext configResourceContext, String userId, Value config) throws IOException;

  /**
   * Get the config with the latest version for the specified resource.
   *
   * @param configResourceContext
   * @return
   */
  ContextSpecificConfig getConfig(ConfigResourceContext configResourceContext) throws IOException;

  /**
   * Get all the configs with the latest version(along with the context to which it applies) for the
   * specified resource keys
   *
   * @param configResourceContexts
   * @return the configs
   * @throws IOException
   */
  Map<ConfigResourceContext, ContextSpecificConfig> getAllContextConfigs(
      Set<ConfigResourceContext> configResourceContexts) throws IOException;

  /**
   * Get all the configs with the latest version(along with the context to which it applies) for the
   * specified parameters, sorted in the descending order of their creation time.
   *
   * @param configResource
   * @return
   * @throws IOException
   */
  List<ContextSpecificConfig> getAllConfigs(ConfigResource configResource) throws IOException;

  /**
   * Write each of the provided config value associated with the specified config resource to the
   * store.
   *
   * @param resourceContextValueMap
   * @param userId
   * @return the upserted configs
   */
  List<UpsertedConfig> writeAllConfigs(
      Map<ConfigResourceContext, Value> resourceContextValueMap, String userId) throws IOException;

  /**
   * delete the config values associated with the specified config resources.
   *
   * @param configResourceContexts
   */
  void deleteConfigs(Set<ConfigResourceContext> configResourceContexts) throws IOException;

  /**
   * Health check for the backend store
   *
   * @return
   */
  boolean healthCheck();
}
