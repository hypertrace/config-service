package org.hypertrace.config.service.store;

import com.google.protobuf.Value;
import com.typesafe.config.Config;
import java.io.IOException;
import java.util.List;
import org.hypertrace.config.service.ConfigResource;
import org.hypertrace.config.service.v1.ContextSpecificConfig;

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
   * @param configResource
   * @param userId
   * @param config
   * @return the context specific config written to the store
   */
  ContextSpecificConfig writeConfig(ConfigResource configResource, String userId, Value config)
      throws IOException;

  /**
   * Get the config with the latest version for the specified resource.
   *
   * @param configResource
   * @return
   */
  ContextSpecificConfig getConfig(ConfigResource configResource) throws IOException;

  /**
   * Get all the configs with the latest version(along with the context to which it applies) for the
   * specified parameters, sorted in the descending order of their creation time.
   *
   * @param resourceName
   * @param resourceNamespace
   * @param tenantId
   * @return
   * @throws IOException
   */
  List<ContextSpecificConfig> getAllConfigs(
      String resourceName, String resourceNamespace, String tenantId) throws IOException;

  /**
   * Health check for the backend store
   *
   * @return
   */
  boolean healthCheck();
}
