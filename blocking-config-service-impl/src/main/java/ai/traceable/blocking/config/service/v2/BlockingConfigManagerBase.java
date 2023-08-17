package ai.traceable.blocking.config.service.v2;

import ai.traceable.blocking.config.service.common.rules.BlockingRulesSupplier;
import java.util.ArrayList;
import java.util.List;
import lombok.Value;

public interface BlockingConfigManagerBase {

  List<BlockingConfigResponseElement> generateBlockingElements(
      List<BlockingConfigRequestElement> requestElements,
      BlockingRulesSupplier blockingRulesSupplier);

  @Value
  class AgentRequestComponent {
    private final AgentCapabilities matchingRequestAgentCapabilities;
    private final String previousHash;
  }

  @Value
  class AgentResponseComponent<T> {
    private final T responseData;
    private final String responseHash;
    private final List<AgentRequestComponent> agentRequestComponents = new ArrayList<>();
  }
}
