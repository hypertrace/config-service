package ai.traceable.cloud.edge.deployment.config.service.v1.state.transitions;

import java.util.List;
import lombok.Getter;
import lombok.Setter;

public class StateTransition {
  @Getter @Setter private List<String> nextStates;
  @Getter @Setter private List<String> accessTypes;
}
