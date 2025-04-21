package ai.traceable.genai.config.service.v1.store;

import ai.traceable.config.utils.UuidGenerator;
import ai.traceable.genai.config.service.v1.GenAiScope;
import jakarta.inject.Inject;
import lombok.AllArgsConstructor;

@AllArgsConstructor(onConstructor_ = {@Inject})
public class GenAiConfigIdGenerator {
  private final UuidGenerator uuidGenerator;

  public String generateId(GenAiScope genAiScope) {
    String idString = "";
    if (genAiScope.hasEnvironmentScope()) {
      idString = idString.concat(genAiScope.getEnvironmentScope().getEnvironmentId());
    } else {
      idString = idString.concat("GLOBAL");
    }
    return uuidGenerator.generateId(idString);
  }
}
