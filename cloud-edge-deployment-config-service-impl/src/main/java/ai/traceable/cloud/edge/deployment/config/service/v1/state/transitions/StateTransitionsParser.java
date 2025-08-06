package ai.traceable.cloud.edge.deployment.config.service.v1.state.transitions;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class StateTransitionsParser {

  private static final ObjectMapper YAML_OBJECT_MAPPER =
      new ObjectMapper(new YAMLFactory())
          .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
  private static final String CLOUD_EDGE_DEPLOYMENT_STATE_TRANSITIONS_YAML =
      "cloud_edge_deployment_state_transitions.yaml";
  private static final StateTransitionsConfig STATE_TRANSITIONS_CONFIG =
      loadStateTransitionsConfig();

  private StateTransitionsParser() {
    throw new IllegalStateException("Cannot be instantiated");
  }

  static StateTransitionsConfig getStateTransitionsConfig() {
    return STATE_TRANSITIONS_CONFIG;
  }

  @SneakyThrows
  private static StateTransitionsConfig loadStateTransitionsConfig() {
    return YAML_OBJECT_MAPPER.readValue(
        StateTransitionsParser.class
            .getClassLoader()
            .getResourceAsStream(CLOUD_EDGE_DEPLOYMENT_STATE_TRANSITIONS_YAML),
        StateTransitionsConfig.class);
  }
}
