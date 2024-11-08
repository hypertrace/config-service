package ai.traceable.saved.filter.config.service.validation;

import static lombok.AccessLevel.PRIVATE;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Supplier;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor(access = PRIVATE)
public class EnumSwitcher<E extends Enum<E>, I, C, R> {
  private final Map<E, BiFunction<I, C, R>> applierMap;
  private final ExceptionSupplier<E, I, C> exceptionSupplier;

  public static <E extends Enum<E>, I, C, R> EnumSwitcherBuilder<E, I, C, R> builder(
      final Class<E> enumClass) {
    return new EnumSwitcherBuilder<>(enumClass);
  }

  public R apply(final E caseEnum, final I input, final C context) {
    final BiFunction<I, C, R> applier = applierMap.get(caseEnum);
    if (applier == null) {
      throw exceptionSupplier.get(input, context, caseEnum);
    }

    return applier.apply(input, context);
  }

  public R apply(final E caseEnum) {
    return apply(caseEnum, null, null);
  }

  @RequiredArgsConstructor
  public static class EnumSwitcherBuilder<E extends Enum<E>, I, C, R> {
    private static final String INVALID_CASE_FORMAT = "Unhandled case: %s";

    private final Set<E> exclusions = new HashSet<>();
    private final Map<E, BiFunction<I, C, R>> applierMap = new HashMap<>();
    private final Class<E> enumClass;

    private ExceptionSupplier<E, I, C> exceptionSupplier =
        (input, context, caseEnum) ->
            new UnsupportedOperationException(String.format(INVALID_CASE_FORMAT, caseEnum));

    public EnumSwitcherBuilder<E, I, C, R> addCase(
        final E caseEnum, final BiFunction<I, C, R> caseApplier) {
      final BiFunction<I, C, R> oldMapping = applierMap.put(caseEnum, caseApplier);

      if (oldMapping != null) {
        throw new UnsupportedOperationException("Duplicate case: " + caseEnum);
      }

      return this;
    }

    public EnumSwitcherBuilder<E, I, C, R> addCase(
        final E caseEnum, final BiConsumer<I, C> caseApplier) {
      return addCase(
          caseEnum,
          (input, context) -> {
            caseApplier.accept(input, context);
            return null;
          });
    }

    public EnumSwitcherBuilder<E, I, C, R> addCase(
        final E caseEnum, final Supplier<R> caseApplier) {
      return addCase(
          caseEnum,
          (input, context) -> {
            return caseApplier.get();
          });
    }

    public EnumSwitcherBuilder<E, I, C, R> exceptionSupplier(
        final ExceptionSupplier<E, I, C> exceptionSupplier) {
      this.exceptionSupplier = exceptionSupplier;
      return this;
    }

    public EnumSwitcherBuilder<E, I, C, R> addExclusion(final E exclusion) {
      this.exclusions.add(exclusion);
      return this;
    }

    public EnumSwitcher<E, I, C, R> build() {
      final Set<E> allCases = Set.of(enumClass.getEnumConstants());
      final Set<E> handledCases = applierMap.keySet();
      final Set<E> missingCases =
          Sets.difference(Sets.difference(allCases, handledCases), exclusions);

      if (!missingCases.isEmpty()) {
        throw new IllegalArgumentException("Missing cases: " + missingCases);
      }

      return new EnumSwitcher<>(Maps.immutableEnumMap(applierMap), exceptionSupplier);
    }
  }

  @FunctionalInterface
  public interface ExceptionSupplier<E extends Enum<E>, I, C> {
    RuntimeException get(final I input, final C context, final E caseEnum);
  }
}
