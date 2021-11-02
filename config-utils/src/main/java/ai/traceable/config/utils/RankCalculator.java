package ai.traceable.config.utils;

import java.util.Collection;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;
import lombok.Value;

public class RankCalculator<T, I> {
  private static final int HIGHEST_RANK = 1;
  private final RankConfig<T, I> config;

  public RankCalculator(RankConfig<T, I> config) {
    this.config = config;
  }

  /**
   * Updates object ranks such that the target object immediately follows the preceding object.
   *
   * @param targetObjectId
   * @param precedingObjectId
   * @param currentObjects
   * @return list of objects with new ranks assigned
   */
  public List<T> rerankAfterOtherObject(
      I targetObjectId, I precedingObjectId, List<T> currentObjects) {
    T targetObject = this.findObjectById(currentObjects, targetObjectId);
    T precedingObject = this.findObjectById(currentObjects, precedingObjectId);

    int oldTargetRank = this.config.rankGetter.apply(targetObject);
    int oldPrecedingRank = this.config.rankGetter.apply(precedingObject);
    // If the object is being ranked lower (that is, higher rank value) then its rank will be the
    // preceding object's old rank because the preceding object will increase in rank after
    // the target is moved below it. If it is being ranked higher (lower rank value), the preceding
    // object won't move and we calculate the new target rank by incrementing instead.
    int newRank = oldPrecedingRank > oldTargetRank ? oldPrecedingRank : oldPrecedingRank + 1;
    return this.rerank(targetObject, newRank, currentObjects);
  }

  /**
   * Updates object ranks such that the target object is assigned the highest rank.
   *
   * @param targetObjectId
   * @param currentObjects
   * @return list of objects with new ranks assigned
   */
  public List<T> rerankAsHighestRank(I targetObjectId, List<T> currentObjects) {
    return this.rerank(
        this.findObjectById(currentObjects, targetObjectId), HIGHEST_RANK, currentObjects);
  }

  /**
   * Takes a newly created object, assigns it an initial rank (lowest) and merges it into the
   * existing list returning the complete sorted list.
   */
  public List<T> rankAndMergeNewObject(T newObject, List<T> currentObjects) {
    List<T> objects = new LinkedList<>(currentObjects);
    objects.add(newObject);
    return this.rankFromOrder(objects);
  }

  /** Updates the provided objects to remove any gaps in rankings */
  public List<T> rankFromOrder(List<T> objects) {
    AtomicInteger nextRank = new AtomicInteger(HIGHEST_RANK);
    return objects.stream()
        .map(object -> this.config.rankApplicator.apply(object, nextRank.getAndIncrement()))
        .collect(Collectors.toUnmodifiableList());
  }

  public List<T> orderFromRanks(Collection<T> objects) {
    return this.orderFromRanks(objects, Function.identity());
  }

  public <V> List<V> orderFromRanks(Collection<V> objects, Function<V, T> mapper) {
    return objects.stream()
        .sorted(
            Comparator.comparing(object -> this.config.getRankGetter().apply(mapper.apply(object))))
        .collect(Collectors.toUnmodifiableList());
  }

  private List<T> rerank(T targetObject, int newRank, List<T> currentObjects) {
    List<T> objects = new LinkedList<>(currentObjects);
    objects.remove(targetObject);
    objects.add(
        newRank - HIGHEST_RANK,
        targetObject); // Convert to index by subtracting the highest rank which is index 0
    return this.rankFromOrder(objects);
  }

  private T findObjectById(List<T> objects, I id) {
    return objects.stream()
        .filter(object -> Objects.equals(this.config.idGetter.apply(object), id))
        .findFirst()
        .orElseThrow(() -> new IllegalArgumentException("Could not find object with ID: " + id));
  }

  @Value
  public static class RankConfig<T, I> {
    Function<T, Integer> rankGetter;
    Function<T, I> idGetter;
    BiFunction<T, Integer, T> rankApplicator;
  }
}
