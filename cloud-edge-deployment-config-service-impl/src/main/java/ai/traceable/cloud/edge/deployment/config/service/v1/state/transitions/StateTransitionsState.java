package ai.traceable.cloud.edge.deployment.config.service.v1.state.transitions;

import java.util.List;
import java.util.Map;
import lombok.Getter;
import lombok.Setter;

public class StateTransitionsState {
  @Getter @Setter private String currentState;
  @Getter @Setter private List<Map<String, StateTransition>> transitions;
}
