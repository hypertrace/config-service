package ai.traceable.sensitivedata.config.service;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.google.protobuf.util.Values;
import java.util.List;
import org.junit.jupiter.api.Test;

class DefaultRedactionRulePersistenceStatusTest {

  @Test
  void testSerDe() {
    assertEquals(
        DefaultRedactionRulePersistenceStatus.of(List.of("first-id", "second-id")),
        DefaultRedactionRulePersistenceStatus.fromValue(
            Values.of(List.of(Values.of("first-id"), Values.of("second-id")))));

    assertEquals(
        DefaultRedactionRulePersistenceStatus.of(List.of("first-id", "second-id")).toValue(),
        Values.of(List.of(Values.of("first-id"), Values.of("second-id"))));
  }

  @Test
  void testAddingIdInOrder() {
    assertEquals(
        DefaultRedactionRulePersistenceStatus.of(List.of("first-id", "second-id")),
        DefaultRedactionRulePersistenceStatus.of(List.of("first-id"))
            .withAdditionalPersistedRuleId("second-id"));
  }

  @Test
  void testAddingIdOutOfOrder() {
    assertEquals(
        DefaultRedactionRulePersistenceStatus.of(List.of("first-id", "second-id")),
        DefaultRedactionRulePersistenceStatus.of(List.of("second-id"))
            .withAdditionalPersistedRuleId("first-id"));
  }
}
