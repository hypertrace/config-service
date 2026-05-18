package ai.traceable.fraud.datamodel.event.kind.eventkind;

import ai.traceable.fraud.datamodel.event.kind.v1.ComplexDataModelEventKind;
import ai.traceable.fraud.datamodel.event.kind.v1.DataModelEventKind;
import ai.traceable.fraud.datamodel.event.kind.v1.DataType;
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

    for (DataModelEventKind kind : eventKindProvider.getAllEventKinds()) {
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
   * Checks if a candidate kind is assignable to a target kind. A candidate is assignable if either
   * the kinds match exactly or the target kind is an ancestor of the candidate kind.
   */
  public boolean isCompatible(
      ComplexDataModelEventKind targetKind, ComplexDataModelEventKind candidateKind) {
    // Handle type_parameter_ref (generic type parameter) - matches anything
    if (targetKind.hasTypeParameterRef()) {
      return true;
    }

    // Handle simple kind_id matching with hierarchy
    if (targetKind.hasKindId() && candidateKind.hasKindId()) {
      return isKindCompatible(targetKind.getKindId(), candidateKind.getKindId());
    }

    // Handle array types - check element type compatibility
    if (targetKind.hasArrayOf() && candidateKind.hasArrayOf()) {
      return isCompatible(targetKind.getArrayOf(), candidateKind.getArrayOf());
    }

    if (targetKind.hasStringMapOf() && candidateKind.hasStringMapOf()) {
      return isCompatible(targetKind.getStringMapOf(), candidateKind.getStringMapOf());
    }

    return false;
  }

  /**
   * Checks if the candidate kind is assignable to the target kind. Assignable means either exact
   * match or target kind is an ancestor of candidate kind.
   */
  private boolean isKindCompatible(String targetKindId, String candidateKindId) {
    // Exact match
    if (targetKindId.equals(candidateKindId)) {
      return true;
    }

    // Check if target kind is an ancestor of candidate kind
    Set<String> ancestors = kindToAncestors.get(candidateKindId);
    return ancestors != null && ancestors.contains(targetKindId);
  }
}
