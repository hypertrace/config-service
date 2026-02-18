package ai.traceable.fraud.datamodel.event.kind.eventkind;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import ai.traceable.fraud.datamodel.event.kind.v1.ComplexDataModelEventKind;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class EventKindHierarchyResolverTest {

  private EventKindHierarchyResolver resolver;

  @BeforeEach
  void setUp() {
    resolver = new EventKindHierarchyResolver(new DefaultEventKindProvider());
  }

  @Test
  void isCompatible_exactMatch_returnsTrue() {
    ComplexDataModelEventKind functionKind =
        ComplexDataModelEventKind.newBuilder().setKindId("system_event_kind_string").build();
    ComplexDataModelEventKind requestedKind =
        ComplexDataModelEventKind.newBuilder().setKindId("system_event_kind_string").build();

    assertTrue(resolver.isCompatible(functionKind, requestedKind));
  }

  @Test
  void isCompatible_parentKind_returnsTrue() {
    // Function accepts "value", caller requests "numeric" (numeric inherits from value)
    ComplexDataModelEventKind functionKind =
        ComplexDataModelEventKind.newBuilder().setKindId("system_event_kind_value").build();
    ComplexDataModelEventKind requestedKind =
        ComplexDataModelEventKind.newBuilder().setKindId("system_event_kind_numeric").build();

    assertTrue(
        resolver.isCompatible(functionKind, requestedKind),
        "Function for parent kind should be compatible with child kind");
  }

  @Test
  void isCompatible_childKind_returnsFalse() {
    // Function accepts "numeric", caller requests "value" (value is parent, not child)
    ComplexDataModelEventKind functionKind =
        ComplexDataModelEventKind.newBuilder().setKindId("system_event_kind_numeric").build();
    ComplexDataModelEventKind requestedKind =
        ComplexDataModelEventKind.newBuilder().setKindId("system_event_kind_value").build();

    assertFalse(
        resolver.isCompatible(functionKind, requestedKind),
        "Function for child kind should not be compatible with parent kind");
  }

  @Test
  void isCompatible_grandparentKind_returnsTrue() {
    // Function accepts "value", caller requests "email" (email -> string -> value)
    ComplexDataModelEventKind functionKind =
        ComplexDataModelEventKind.newBuilder().setKindId("system_event_kind_value").build();
    ComplexDataModelEventKind requestedKind =
        ComplexDataModelEventKind.newBuilder().setKindId("system_event_kind_email").build();

    assertTrue(
        resolver.isCompatible(functionKind, requestedKind),
        "Function for grandparent kind should be compatible with grandchild kind");
  }

  @Test
  void isCompatible_unrelatedKinds_returnsFalse() {
    // Function accepts "numeric", caller requests "string" (unrelated siblings)
    ComplexDataModelEventKind functionKind =
        ComplexDataModelEventKind.newBuilder().setKindId("system_event_kind_numeric").build();
    ComplexDataModelEventKind requestedKind =
        ComplexDataModelEventKind.newBuilder().setKindId("system_event_kind_string").build();

    assertFalse(
        resolver.isCompatible(functionKind, requestedKind),
        "Unrelated kinds should not be compatible");
  }

  @Test
  void isCompatible_typeParameterRef_matchesAny() {
    ComplexDataModelEventKind functionKind =
        ComplexDataModelEventKind.newBuilder().setTypeParameterRef("T").build();
    ComplexDataModelEventKind requestedKind =
        ComplexDataModelEventKind.newBuilder().setKindId("system_event_kind_string").build();

    assertTrue(
        resolver.isCompatible(functionKind, requestedKind),
        "type_parameter_ref should match any kind");
  }

  @Test
  void isCompatible_arrayTypes_checksElementCompatibility() {
    // Function accepts array of value, caller requests array of string
    ComplexDataModelEventKind functionKind =
        ComplexDataModelEventKind.newBuilder()
            .setArrayOf(ComplexDataModelEventKind.newBuilder().setKindId("system_event_kind_value"))
            .build();
    ComplexDataModelEventKind requestedKind =
        ComplexDataModelEventKind.newBuilder()
            .setArrayOf(
                ComplexDataModelEventKind.newBuilder().setKindId("system_event_kind_string"))
            .build();

    assertTrue(
        resolver.isCompatible(functionKind, requestedKind),
        "Array of parent should be compatible with array of child");
  }

  @Test
  void isCompatible_arrayWithTypeParameterRef_matchesAnyArray() {
    ComplexDataModelEventKind functionKind =
        ComplexDataModelEventKind.newBuilder()
            .setArrayOf(ComplexDataModelEventKind.newBuilder().setTypeParameterRef("T"))
            .build();
    ComplexDataModelEventKind requestedKind =
        ComplexDataModelEventKind.newBuilder()
            .setArrayOf(
                ComplexDataModelEventKind.newBuilder().setKindId("system_event_kind_string"))
            .build();

    assertTrue(
        resolver.isCompatible(functionKind, requestedKind),
        "Array with type_parameter_ref should match any array");
  }
}
