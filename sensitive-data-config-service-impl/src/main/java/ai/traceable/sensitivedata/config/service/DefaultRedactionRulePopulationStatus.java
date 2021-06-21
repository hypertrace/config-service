package ai.traceable.sensitivedata.config.service;

import static java.util.Collections.emptySet;

import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableSortedSet;
import com.google.common.collect.Ordering;
import com.google.common.collect.Sets;
import com.google.protobuf.Value;
import com.google.protobuf.util.Values;
import java.util.Collection;
import java.util.Collections;
import java.util.Set;
import java.util.SortedSet;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import lombok.EqualsAndHashCode;
import lombok.ToString;

@EqualsAndHashCode
@ToString
class DefaultRedactionRulePopulationStatus {

  private static final String AUTO_REDACTION_RULE_PREFIX = "auto_redaction_rule";

  private final SortedSet<String> populatedRuleIds;
  // Skipped rule ids indicate that they haven't been populated, but should still be skipped
  private final Set<Predicate<String>> ruleExclusions;

  private DefaultRedactionRulePopulationStatus(Collection<String> populatedRuleIds) {
    this(populatedRuleIds, Collections.emptySet());
  }

  private DefaultRedactionRulePopulationStatus(
      Collection<String> populatedRuleIds, Set<Predicate<String>> ruleExclusions) {
    this.populatedRuleIds =
        populatedRuleIds.stream()
            .collect(ImmutableSortedSet.toImmutableSortedSet(Ordering.natural()));
    this.ruleExclusions = Set.copyOf(ruleExclusions);
  }

  /** Returns true if the rule should be prepopulated */
  boolean shouldPopulateRule(String ruleId) {
    return !this.hasRuleBeenPopulated(ruleId)
        && this.ruleExclusions.stream().noneMatch(predicate -> predicate.test(ruleId));
  }

  /** Returns true if the rule has already been prepopulated */
  boolean hasRuleBeenPopulated(String ruleId) {
    return this.populatedRuleIds.contains(ruleId);
  }

  DefaultRedactionRulePopulationStatus withAdditionalPopulatedRules(
      Collection<String> populatedIds) {
    return new DefaultRedactionRulePopulationStatus(
        Sets.union(this.populatedRuleIds, Set.copyOf(populatedIds)));
  }

  DefaultRedactionRulePopulationStatus forAutomaticRedactionState(
      BooleanSupplier autoRedactionStateSupplier) {
    // We briefly cache as a small optimization so that successive checks for this server value
    // across multiple rules don't re-execute
    Supplier<Boolean> memoizedRedactionStateSupplier =
        Suppliers.memoizeWithExpiration(
            autoRedactionStateSupplier::getAsBoolean, 10, TimeUnit.SECONDS);

    return withRuleExclusion(
        ruleId ->
            ruleId.startsWith(AUTO_REDACTION_RULE_PREFIX) && !memoizedRedactionStateSupplier.get());
  }

  private DefaultRedactionRulePopulationStatus withRuleExclusion(Predicate<String> ruleExclusion) {
    return new DefaultRedactionRulePopulationStatus(
        this.populatedRuleIds, Sets.union(this.ruleExclusions, Set.of(ruleExclusion)));
  }

  Value toValue() {
    return Values.of(this.populatedRuleIds.stream().map(Values::of).collect(Collectors.toList()));
  }

  static DefaultRedactionRulePopulationStatus empty() {
    return new DefaultRedactionRulePopulationStatus(emptySet());
  }

  static DefaultRedactionRulePopulationStatus of(Collection<String> populatedRuleIds) {
    return new DefaultRedactionRulePopulationStatus(populatedRuleIds);
  }

  static DefaultRedactionRulePopulationStatus fromValue(Value value) {
    Set<String> ruleIds =
        value.getListValue().getValuesList().stream()
            .map(Value::getStringValue)
            .collect(Collectors.toSet());

    return new DefaultRedactionRulePopulationStatus(ruleIds);
  }
}
