package ai.traceable.span.processing.config.service.utils;

import ai.traceable.config.utils.UuidGenerator;
import jakarta.inject.Inject;
import java.util.List;
import lombok.AllArgsConstructor;

@AllArgsConstructor(onConstructor_ = {@Inject})
public class ApiNamingRuleIdGenerator {

  private final UuidGenerator uuidGenerator;

  public String generateGenAiRuleId(List<String> regexes) {
    StringBuilder idBuilder = new StringBuilder();

    regexes.forEach(regex -> idBuilder.append(regex).append("_"));

    return uuidGenerator.generateId(idBuilder.toString());
  }
}
