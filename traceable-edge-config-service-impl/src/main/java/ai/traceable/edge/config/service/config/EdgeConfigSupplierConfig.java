package ai.traceable.edge.config.service.config;

import com.google.protobuf.Duration;
import lombok.Getter;

@Getter
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
}
