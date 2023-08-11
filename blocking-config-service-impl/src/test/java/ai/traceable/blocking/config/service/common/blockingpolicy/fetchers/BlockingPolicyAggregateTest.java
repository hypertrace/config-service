package ai.traceable.blocking.config.service.common.blockingpolicy.fetchers;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.LinkedHashMap;
import java.util.List;
import org.junit.jupiter.api.Test;

class BlockingPolicyAggregateTest {

  @Test
  void mergeTest() {
    BlockingPolicyAggregate<String> a = new BlockingPolicyAggregate<>(List.of("a", "b"));
    BlockingPolicyAggregate<String> b = new BlockingPolicyAggregate<>(List.of("c", "d"));
    assertEquals(
        new BlockingPolicyAggregate<>(List.of("a", "b", "c", "d")),
        BlockingPolicyAggregate.merge(a, b));

    LinkedHashMap<String, List<String>> map1 = new LinkedHashMap<>();
    map1.put("s1", List.of("a1", "b1"));
    map1.put("s2", List.of("a2", "b2"));

    LinkedHashMap<String, List<String>> map2 = new LinkedHashMap<>();
    map2.put("s2", List.of("c1", "d1"));
    map2.put("s3", List.of("c2", "d2"));

    BlockingPolicyAggregate<String> c = new BlockingPolicyAggregate<>(map1);
    BlockingPolicyAggregate<String> d = new BlockingPolicyAggregate<>(map2);

    LinkedHashMap<String, List<String>> expectedMap1 = new LinkedHashMap<>();
    expectedMap1.put("s2", List.of("c1", "d1", "a", "b"));
    expectedMap1.put("s3", List.of("c2", "d2", "a", "b"));

    assertEquals(
        expectedMap1, BlockingPolicyAggregate.merge(a, d).getServiceScopedBlockingPolicyMap());
    assertEquals(
        expectedMap1, BlockingPolicyAggregate.merge(d, a).getServiceScopedBlockingPolicyMap());
    assertEquals(
        a, BlockingPolicyAggregate.merge(a, new BlockingPolicyAggregate<>(new LinkedHashMap<>())));

    LinkedHashMap<String, List<String>> expectedMap2 = new LinkedHashMap<>();
    expectedMap2.put("s1", List.of("a1", "b1"));
    expectedMap2.put("s2", List.of("a2", "b2", "c1", "d1"));
    expectedMap2.put("s3", List.of("c2", "d2"));
    assertEquals(new BlockingPolicyAggregate<>(expectedMap2), BlockingPolicyAggregate.merge(c, d));
  }

  @Test
  void sortTest() {
    BlockingPolicyAggregate<String> a = new BlockingPolicyAggregate<>(List.of("b", "a"));
    assertEquals(
        new BlockingPolicyAggregate<>(List.of("a", "b")),
        BlockingPolicyAggregate.sort(a, String::compareTo));

    LinkedHashMap<String, List<String>> map = new LinkedHashMap<>();
    map.put("s2", List.of("c1", "b1"));
    map.put("s3", List.of("c2", "d2"));
    LinkedHashMap<String, List<String>> expectedMap = new LinkedHashMap<>();
    expectedMap.put("s2", List.of("b1", "c1"));
    expectedMap.put("s3", List.of("c2", "d2"));
    assertEquals(
        new BlockingPolicyAggregate<>(expectedMap),
        BlockingPolicyAggregate.sort(new BlockingPolicyAggregate<>(map), String::compareTo));
  }
}
