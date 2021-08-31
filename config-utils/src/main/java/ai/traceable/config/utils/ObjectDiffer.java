package ai.traceable.config.utils;

import static java.util.function.Predicate.not;

import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public class ObjectDiffer {

  /**
   * Returns list of objects from the updated object collection that don't exist in the existing
   * object collection. Result list is ordered based on iteration order of updated object
   * collection.
   *
   * @param existingObjects
   * @param updatedObjects
   * @return
   */
  public <T> List<T> getNewOrUpdatedObjects(
      Collection<T> existingObjects, Collection<T> updatedObjects) {
    Set<T> existingObjectSet = Set.copyOf(existingObjects);

    return updatedObjects.stream()
        .filter(not(existingObjectSet::contains))
        .collect(Collectors.toUnmodifiableList());
  }
}
