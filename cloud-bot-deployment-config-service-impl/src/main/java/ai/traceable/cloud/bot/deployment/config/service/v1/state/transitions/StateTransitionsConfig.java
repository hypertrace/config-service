package ai.traceable.cloud.bot.deployment.config.service.v1.state.transitions;

import java.util.List;
import lombok.Getter;
import lombok.Setter;

final class StateTransitionsConfig {
  @Getter @Setter private List<StateTransitionsState> states;
}
