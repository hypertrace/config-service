package ai.traceable.anomaly.config.service.trainer.trainingconfig.filter;

import static java.util.function.Function.identity;
import static java.util.stream.Collectors.collectingAndThen;
import static java.util.stream.Collectors.toMap;

import ai.traceable.anomaly.config.service.v1.trainer.TrainingConfigTypeSpecificFilter.TypeCase;
import jakarta.inject.Inject;
import java.util.Collections;
import java.util.EnumMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

public class TrainingConfigSpecificFilterRegistryImpl
    implements TrainingConfigSpecificFilterRegistry {

  private final Map<TypeCase, TrainingConfigTypeFilterMatcher> configCaseToilterMatcherMap;

  @Inject
  public TrainingConfigSpecificFilterRegistryImpl(
      final Set<TrainingConfigTypeFilterMatcher> operations) {
    this.configCaseToilterMatcherMap =
        operations.stream()
            .collect(
                collectingAndThen(
                    toMap(
                        TrainingConfigTypeFilterMatcher::type,
                        identity(),
                        (oldVal, newVal) -> {
                          throw new IllegalArgumentException(
                              String.format("Duplicate keys found for type: %s", newVal.type()));
                        },
                        () -> new EnumMap<>(TypeCase.class)),
                    Collections::unmodifiableMap));
  }

  @Override
  public TrainingConfigTypeFilterMatcher type(TypeCase typeCase) {
    return Optional.ofNullable(configCaseToilterMatcherMap.get(typeCase))
        .orElseThrow(
            () ->
                new UnsupportedOperationException(
                    String.format("Unsupported training config type: %s", typeCase)));
  }
}
