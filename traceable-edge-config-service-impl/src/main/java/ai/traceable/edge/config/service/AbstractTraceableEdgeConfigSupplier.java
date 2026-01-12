package ai.traceable.edge.config.service;

import ai.traceable.config.utils.UuidGenerator;
import ai.traceable.edge.config.service.config.TraceableEdgeConfig;
import ai.traceable.edge.config.service.v1.AgentCapabilities;
import ai.traceable.edge.config.service.v1.ConfigPayloads;
import ai.traceable.edge.config.service.v1.ConfigResponseElement;

/**
 * Abstract base class for TraceableEdgeConfigSupplier implementations. Provides common
 * functionality for building ConfigResponseElement with unique hashes.
 */
public abstract class AbstractTraceableEdgeConfigSupplier implements TraceableEdgeConfigSupplier {

  protected final UuidGenerator uuidGenerator;
  protected final TraceableEdgeConfig config;

  protected AbstractTraceableEdgeConfigSupplier(
      UuidGenerator uuidGenerator, TraceableEdgeConfig config) {
    this.uuidGenerator = uuidGenerator;
    this.config = config;
  }

  /**
   * Builds a ConfigResponseElement with a unique hash that includes the configType. This prevents
   * hash collisions when different config types have identical payloads (e.g., empty configs).
   *
   * @param configPayloads the config payloads to include in the response
   * @param agentCapabilities the agent capabilities
   * @return a fully constructed ConfigResponseElement
   */
  protected ConfigResponseElement buildConfigResponseElement(
      ConfigPayloads configPayloads, AgentCapabilities agentCapabilities) {
    // Generate unique hash by combining payload hash with configType (using : as delimiter)
    String payloadHash = uuidGenerator.generateId(configPayloads);
    String uniqueHash = payloadHash + ":" + getConfigType();

    return ConfigResponseElement.newBuilder()
        .setConfigType(getConfigType())
        .setEnabled(true)
        .addSupportedAgentCapabilities(agentCapabilities)
        .setRefreshAfterDuration(config.getAgentPollingFrequency(getConfigType()))
        .setConfigPayloads(configPayloads)
        .setHash(uniqueHash)
        .build();
  }
}
