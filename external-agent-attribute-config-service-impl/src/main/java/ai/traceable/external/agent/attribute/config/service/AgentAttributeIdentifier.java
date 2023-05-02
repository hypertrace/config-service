package ai.traceable.external.agent.attribute.config.service;

import java.util.Optional;
import lombok.Value;

@Value
public class AgentAttributeIdentifier {
  Optional<String> environmentName;
  boolean jwtExtractionSupported;
}
