package ai.traceable.external.agent.attribute.config.service.translator.jwtextraction.instruction;

import lombok.AllArgsConstructor;
import lombok.Getter;

@AllArgsConstructor
@Getter
enum PartOfJwt {
  HEADER("header"),
  PAYLOAD("payload");
  private final String value;
}
