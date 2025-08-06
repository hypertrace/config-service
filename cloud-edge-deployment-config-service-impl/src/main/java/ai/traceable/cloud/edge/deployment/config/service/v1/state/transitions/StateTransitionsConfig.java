package ai.traceable.cloud.edge.deployment.config.service.v1.state.transitions;

import java.util.List;
import lombok.Getter;
import lombok.Setter;

public class StateTransitionsConfig {
  @Getter @Setter private List<StateTransitionsState> states;
}
