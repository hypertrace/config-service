package ai.traceable.config.utils;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

class ObjectDifferTest {
  private static final AtomicInteger OBJ_1 = new AtomicInteger(1);
  private static final AtomicInteger OBJ_2 = new AtomicInteger(2);
  private static final AtomicInteger OBJ_3 = new AtomicInteger(3);
  private static final List<AtomicInteger> OBJ_1_AND_2 = List.of(OBJ_1, OBJ_2);

  private final ObjectDiffer differ = new ObjectDiffer();

  @Test
  void detectsUpdatedObjects() {
    assertEquals(List.of(OBJ_3), differ.getNewOrUpdatedObjects(OBJ_1_AND_2, List.of(OBJ_1, OBJ_3)));
  }
}
