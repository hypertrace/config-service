package ai.traceable.fraud.datamodel.event.kind.eventkind;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import ai.traceable.fraud.datamodel.event.kind.v1.DataModelEventKind;
import ai.traceable.fraud.datamodel.event.kind.v1.EventKindFilter;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class DefaultEventKindProviderTest {

  private DefaultEventKindProvider provider;

  @BeforeEach
  void setUp() {
    provider = new DefaultEventKindProvider();
  }

  @Test
  void yamlFileIsValidAndParseable() {
    // Test that YAML file loads without throwing exceptions
    assertDoesNotThrow(() -> new DefaultEventKindProvider());
  }

  @Test
  void getEventKinds_returnsNonEmptyList() {
    List<DataModelEventKind> kinds = provider.getEventKinds(EventKindFilter.getDefaultInstance());

    assertNotNull(kinds);
    assertFalse(kinds.isEmpty(), "Event kinds list should not be empty");
  }

  @Test
  void allEventKindsHaveRequiredFields() {
    List<DataModelEventKind> kinds = provider.getEventKinds(EventKindFilter.getDefaultInstance());

    for (DataModelEventKind kind : kinds) {
      assertFalse(kind.getId().isEmpty(), "Event kind ID should not be empty: " + kind);
      assertFalse(
          kind.getDisplayName().isEmpty(), "Event kind display_name should not be empty: " + kind);
      assertFalse(
          kind.getDescription().isEmpty(), "Event kind description should not be empty: " + kind);
    }
  }

  @Test
  void containsExpectedSystemKinds() {
    List<DataModelEventKind> kinds = provider.getEventKinds(EventKindFilter.getDefaultInstance());

    assertTrue(
        kinds.stream().anyMatch(k -> k.getId().equals("system_event_kind_value")),
        "Should contain root value kind");
    assertTrue(
        kinds.stream().anyMatch(k -> k.getId().equals("system_event_kind_string")),
        "Should contain string kind");
    assertTrue(
        kinds.stream().anyMatch(k -> k.getId().equals("system_event_kind_boolean")),
        "Should contain boolean kind");
    assertTrue(
        kinds.stream().anyMatch(k -> k.getId().equals("system_event_kind_integer")),
        "Should contain integer kind");
    assertTrue(
        kinds.stream().anyMatch(k -> k.getId().equals("system_event_kind_ip_address")),
        "Should contain IP address kind");
  }

  @Test
  void systemOnlyFilterReturnsAllKinds() {
    // Currently all kinds are system-defined, so filter should return same result
    EventKindFilter filter = EventKindFilter.newBuilder().setSystemOnly(true).build();

    List<DataModelEventKind> allKinds =
        provider.getEventKinds(EventKindFilter.getDefaultInstance());
    List<DataModelEventKind> systemKinds = provider.getEventKinds(filter);

    assertTrue(allKinds.size() == systemKinds.size(), "All kinds should be system-defined");
  }
}
