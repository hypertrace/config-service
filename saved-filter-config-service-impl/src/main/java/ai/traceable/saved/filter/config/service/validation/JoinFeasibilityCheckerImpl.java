package ai.traceable.saved.filter.config.service.validation;

import static java.util.Collections.emptyMap;
import static java.util.Collections.emptySet;

import io.grpc.Status;
import java.util.Map;
import java.util.Set;

public class JoinFeasibilityCheckerImpl implements JoinFeasibilityChecker {
  private static final Map<String, Set<String>> JOIN_FEASIBILITY_MAP = emptyMap();

  @Override
  public void validate(String source, String target) {
    if (!JOIN_FEASIBILITY_MAP.getOrDefault(source, emptySet()).contains(target)) {
      throw Status.INVALID_ARGUMENT
          .withDescription(
              String.format(
                  "Join is not possible with source (%s) and target (%s)", source, target))
          .asRuntimeException();
    }
  }
}
