package ai.traceable.edge.config.service;

import ai.traceable.edge.config.service.v1.AgentCapabilities;
import ai.traceable.edge.config.service.v1.ConfigRequestElement;
import ai.traceable.edge.config.service.v1.ConfigResponseElement;
import com.google.protobuf.Duration;
import com.typesafe.config.Config;
import org.hypertrace.core.grpcutils.context.RequestContext;

public interface TraceableEdgeConfigSupplier {
  String AGENT_POLLING_FREQUENCY_CONFIG_NAME = "agent.polling.frequency";
  String DEFAULT_AGENT_POLLING_FREQUENCY_CONFIG_NAME = "default.agent.polling.frequency";
  String CONFIG_TYPES_CONFIG_NAME = "config.types";

  String getConfigType();

  ConfigResponseElement getConfigs(
      RequestContext requestContext,
      ConfigRequestElement requestElement,
      AgentCapabilities agentCapabilities);

  default Duration getAgentPollingFrequency(Config config, String configType) {
    var configDuration = config.getDuration(DEFAULT_AGENT_POLLING_FREQUENCY_CONFIG_NAME);
    var configTypeSpecificDurationPath = configType + "." + AGENT_POLLING_FREQUENCY_CONFIG_NAME;
    if (config.hasPath(configTypeSpecificDurationPath)) {
      configDuration = config.getDuration(configTypeSpecificDurationPath);
    }
    return Duration.newBuilder()
        .setSeconds(configDuration.getSeconds())
        .setNanos(configDuration.getNano())
        .build();
  }
}
