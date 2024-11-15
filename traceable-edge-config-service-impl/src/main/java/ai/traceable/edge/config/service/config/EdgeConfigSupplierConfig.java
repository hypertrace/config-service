package ai.traceable.edge.config.service.config;

import com.google.protobuf.Duration;

public class EdgeConfigSupplierConfig {
  String configType;
  String fullyQualifiedClassName;
  Duration agentPollingFrequency;

  public EdgeConfigSupplierConfig(
      String configType, String fullyQualifiedClassName, Duration agentPollingFrequency) {
    this.configType = configType;
    this.fullyQualifiedClassName = fullyQualifiedClassName;
    this.agentPollingFrequency = agentPollingFrequency;
  }

  public String getConfigType() {
    return configType;
  }

  public String getFullyQualifiedClassName() {
    return fullyQualifiedClassName;
  }

  public Duration getAgentPollingFrequency() {
    return agentPollingFrequency;
  }
}
