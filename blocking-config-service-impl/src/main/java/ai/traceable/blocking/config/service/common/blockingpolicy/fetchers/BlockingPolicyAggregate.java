package ai.traceable.blocking.config.service.common.blockingpolicy.fetchers;

import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.Value;

@Value
public class BlockingPolicyAggregate<T> {
  List<T> blockingPolicyList;
  LinkedHashMap<String, List<T>> serviceScopedBlockingPolicyMap;

  public BlockingPolicyAggregate(List<T> blockingPolicyList) {
    this.blockingPolicyList = blockingPolicyList;
    this.serviceScopedBlockingPolicyMap = null;
  }

  public BlockingPolicyAggregate(LinkedHashMap<String, List<T>> serviceScopedBlockingPolicyMap) {
    this.blockingPolicyList = null;
    this.serviceScopedBlockingPolicyMap = serviceScopedBlockingPolicyMap;
  }

  public static <T> BlockingPolicyAggregate<T> merge(
      BlockingPolicyAggregate<T> a, BlockingPolicyAggregate<T> b) {
    if (a.getBlockingPolicyList() != null) {
      if (b.getBlockingPolicyList() != null) {
        return new BlockingPolicyAggregate<>(
            mergeList(a.getBlockingPolicyList(), b.getBlockingPolicyList()));
      } else {
        return mergeListsWithMap(a.getBlockingPolicyList(), b.getServiceScopedBlockingPolicyMap());
      }
    } else if (b.getBlockingPolicyList() != null) {
      return mergeListsWithMap(b.getBlockingPolicyList(), a.getServiceScopedBlockingPolicyMap());
    }
    return mergeMaps(a.getServiceScopedBlockingPolicyMap(), b.getServiceScopedBlockingPolicyMap());
  }

  public static <T> BlockingPolicyAggregate<T> sort(
      BlockingPolicyAggregate<T> a, Comparator<T> comparator) {
    if (a.getBlockingPolicyList() != null) {
      return new BlockingPolicyAggregate<>(sortList(a.getBlockingPolicyList(), comparator));
    }
    LinkedHashMap<String, List<T>> updatedMap = new LinkedHashMap<>();
    a.getServiceScopedBlockingPolicyMap()
        .forEach((key, values) -> updatedMap.put(key, sortList(values, comparator)));
    return new BlockingPolicyAggregate<>(updatedMap);
  }

  public static <R, T> BlockingPolicyAggregate<R> convert(
      BlockingPolicyAggregate<T> a, Function<T, R> converter) {
    if (a.getBlockingPolicyList() != null) {
      return new BlockingPolicyAggregate<>(convertList(a.getBlockingPolicyList(), converter));
    }
    LinkedHashMap<String, List<R>> updatedMap = new LinkedHashMap<>();
    a.getServiceScopedBlockingPolicyMap()
        .forEach((key, values) -> updatedMap.put(key, convertList(values, converter)));
    return new BlockingPolicyAggregate<>(updatedMap);
  }

  private static <T> BlockingPolicyAggregate<T> mergeListsWithMap(
      List<T> list, LinkedHashMap<String, List<T>> map) {
    if (map.isEmpty()) {
      return new BlockingPolicyAggregate<>(list);
    }
    LinkedHashMap<String, List<T>> updatedMap = new LinkedHashMap<>(map);
    map.forEach((key, values) -> updatedMap.merge(key, list, BlockingPolicyAggregate::mergeList));
    return new BlockingPolicyAggregate<>(updatedMap);
  }

  private static <T> List<T> mergeList(List<T> a, List<T> b) {
    return Stream.concat(a.stream(), b.stream()).collect(Collectors.toList());
  }

  private static <T> List<T> sortList(List<T> a, Comparator<T> comparator) {
    return a.stream().sorted(comparator).collect(Collectors.toUnmodifiableList());
  }

  private static <T, R> List<R> convertList(List<T> a, Function<T, R> converter) {
    return a.stream()
        .map(converter)
        .filter(Objects::nonNull)
        .collect(Collectors.toUnmodifiableList());
  }

  private static <T> BlockingPolicyAggregate<T> mergeMaps(
      LinkedHashMap<String, List<T>> mapA, LinkedHashMap<String, List<T>> mapB) {
    LinkedHashMap<String, List<T>> updatedMap = new LinkedHashMap<>(mapA);
    mapB.forEach(
        (key, values) -> updatedMap.merge(key, values, BlockingPolicyAggregate::mergeList));
    return new BlockingPolicyAggregate<>(updatedMap);
  }
}
