package ai.traceable.cloud.bot.deployment.config.service.v1.state.transitions;

import java.util.List;
import lombok.Getter;
import lombok.Setter;

final class StateTransition {
  @Getter @Setter private String nextState;
  @Getter @Setter private List<String> accessTypes;
}
