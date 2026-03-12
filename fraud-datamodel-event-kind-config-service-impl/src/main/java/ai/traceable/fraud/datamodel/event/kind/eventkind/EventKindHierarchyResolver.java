package ai.traceable.fraud.datamodel.event.kind.eventkind;

import ai.traceable.fraud.datamodel.event.kind.v1.ComplexDataModelEventKind;
import ai.traceable.fraud.datamodel.event.kind.v1.DataModelEventKind;
import ai.traceable.fraud.datamodel.event.kind.v1.DataType;
import ai.traceable.fraud.datamodel.event.kind.v1.EventKindFilter;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * Resolves event kind hierarchy for type compatibility checks. This allows functions defined for
 * parent types (e.g., "value") to be available for child types (e.g., "numeric", "string").
 *
 * <p>The hierarchy is an internal implementation detail - callers receive a flat list of compatible
 * functions without needing to know about the inheritance structure.
 */
@Singleton
public class EventKindHierarchyResolver {

  private final Map<String, Set<String>> kindToAncestors;
  private final Map<String, DataType> kindToDataType;

  @Inject
  public EventKindHierarchyResolver(EventKindProvider eventKindProvider) {
    Map<String, String> kindToParent = new HashMap<>();
    Map<String, DataType> dataTypes = new HashMap<>();

    for (DataModelEventKind kind :
        eventKindProvider.getEventKinds(EventKindFilter.getDefaultInstance())) {
      if (!kind.getParentKindId().isEmpty()) {
        kindToParent.put(kind.getId(), kind.getParentKindId());
      }
      if (kind.getDataType() != DataType.DATA_TYPE_UNSPECIFIED) {
        dataTypes.put(kind.getId(), kind.getDataType());
      }
    }

    this.kindToAncestors = buildAncestors(kindToParent);
    this.kindToDataType = dataTypes;
  }

  private Map<String, Set<String>> buildAncestors(Map<String, String> kindToParent) {
    Map<String, Set<String>> result = new HashMap<>();
    for (String kindId : kindToParent.keySet()) {
      Set<String> ancestors = new HashSet<>();
      String current = kindToParent.get(kindId);
      while (current != null) {
        ancestors.add(current);
        current = kindToParent.get(current);
      }
      result.put(kindId, ancestors);
    }
    return result;
  }

  public Optional<DataType> getDataType(String kindId) {
    return Optional.ofNullable(kindToDataType.get(kindId));
  }

  /**
   * Checks if a function's input kind is compatible with the requested kind. A function is
   * compatible if either the kinds match exactly or the function's input kind is an ancestor of the
   * requested kind.
   */
  public boolean isCompatible(
      ComplexDataModelEventKind functionInputKind, ComplexDataModelEventKind requestedKind) {
    // Handle type_parameter_ref (generic type parameter) - matches anything
    if (functionInputKind.hasTypeParameterRef()) {
      return true;
    }

    // Handle simple kind_id matching with hierarchy
    if (functionInputKind.hasKindId() && requestedKind.hasKindId()) {
      return isKindCompatible(functionInputKind.getKindId(), requestedKind.getKindId());
    }

    // Handle array types - check element type compatibility
    if (functionInputKind.hasArrayOf() && requestedKind.hasArrayOf()) {
      return isCompatible(functionInputKind.getArrayOf(), requestedKind.getArrayOf());
    }

    return false;
  }

  /**
   * Checks if the function's kind is compatible with the requested kind. Compatible means either
   * exact match or function kind is an ancestor of requested kind
   */
  private boolean isKindCompatible(String functionKindId, String requestedKindId) {
    // Exact match
    if (functionKindId.equals(requestedKindId)) {
      return true;
    }

    // Check if function's kind is an ancestor of requested kind
    Set<String> ancestors = kindToAncestors.get(requestedKindId);
    return ancestors != null && ancestors.contains(functionKindId);
  }
}
