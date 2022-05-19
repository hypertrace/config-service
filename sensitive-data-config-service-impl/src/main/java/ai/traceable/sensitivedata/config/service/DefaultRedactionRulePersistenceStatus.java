package ai.traceable.sensitivedata.config.service;

import static java.util.Collections.emptySet;

import com.google.common.collect.ImmutableSortedSet;
import com.google.common.collect.Ordering;
import com.google.common.collect.Sets;
import com.google.protobuf.Value;
import com.google.protobuf.util.Values;
import java.util.Collection;
import java.util.Set;
import java.util.SortedSet;
import java.util.stream.Collectors;
import lombok.EqualsAndHashCode;
import lombok.ToString;

@EqualsAndHashCode
@ToString
class DefaultRedactionRulePersistenceStatus {
  private final SortedSet<String> persistedRuleIds;

  private DefaultRedactionRulePersistenceStatus(Collection<String> persistedRuleIds) {
    this.persistedRuleIds =
        persistedRuleIds.stream()
            .collect(ImmutableSortedSet.toImmutableSortedSet(Ordering.natural()));
  }

  /** Returns true if the rule has already been persisted (edited or deleted) */
  boolean hasRuleBeenPersisted(String ruleId) {
    return this.persistedRuleIds.contains(ruleId);
  }

  DefaultRedactionRulePersistenceStatus withAdditionalPersistedRuleId(String ruleId) {
    return new DefaultRedactionRulePersistenceStatus(
        Sets.union(this.persistedRuleIds, Set.of(ruleId)));
  }

  Value toValue() {
    return Values.of(this.persistedRuleIds.stream().map(Values::of).collect(Collectors.toList()));
  }

  static DefaultRedactionRulePersistenceStatus empty() {
    return new DefaultRedactionRulePersistenceStatus(emptySet());
  }

  static DefaultRedactionRulePersistenceStatus of(Collection<String> persistedRuleIds) {
    return new DefaultRedactionRulePersistenceStatus(persistedRuleIds);
  }

  static DefaultRedactionRulePersistenceStatus fromValue(Value value) {
    Set<String> ruleIds =
        value.getListValue().getValuesList().stream()
            .map(Value::getStringValue)
            .collect(Collectors.toSet());

    return new DefaultRedactionRulePersistenceStatus(ruleIds);
  }
}
